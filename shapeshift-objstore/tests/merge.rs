//! Merge through the object-store sink (a `file://` URL, which runs the same code an
//! `s3://` table does): the same deltas merged into a local table and into an
//! object-store table must leave the same live rows and report the same merge.
//! The S3 protocol itself — ranged GETs, multipart uploads — is exercised by
//! `examples/merge/verify_duckdb.py --tables s3://…` against an S3-compatible server.

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, MergeSummary, Shaper, Sink};
use shapeshift_iceberg::{
    current_scan_files, inspect, read_manifest_entries, read_manifest_list, CompactOptions,
    ExpireOptions, IcebergSink, Identity, RelocateOptions,
};
use shapeshift_objstore::{
    compact_iceberg, expire_iceberg, inspect_iceberg, relocate_iceberg, ObjectStoreIcebergSink,
};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ss-obj-merge-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn batch(rows: &[Value]) -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let col = |name: &str, ty| ColumnSpec {
        name: name.into(),
        from: None,
        ty,
        transform: None,
        required: false,
    };
    let mut sh = Shaper::new(
        vec![
            col("id", ColumnType::Int64),
            col("region", ColumnType::String),
            col("v", ColumnType::Int64),
        ],
        false,
    )
    .unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

fn row(id: i64, region: &str, v: i64) -> Value {
    json!({ "id": id, "region": region, "v": v })
}

/// The live rows of a table on local disk, as `(id, region, v)`, sorted.
fn live(dir: &std::path::Path) -> Vec<(Option<i64>, String, i64)> {
    let mut out = Vec::new();
    for f in current_scan_files(dir).unwrap() {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&f.path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let mut pos = 0u64;
        for b in reader {
            let b = b.unwrap();
            let id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let region = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            let v = b
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                if f.deleted_positions.binary_search(&pos).is_err() {
                    out.push((
                        (!id.is_null(i)).then(|| id.value(i)),
                        region.value(i).to_string(),
                        v.value(i),
                    ));
                }
                pos += 1;
            }
        }
    }
    out.sort();
    out
}

const PARTS: &[&str] = &["region"];

fn deltas() -> Vec<Vec<Value>> {
    vec![
        vec![row(1, "eu", 1), row(2, "us", 2), row(3, "eu", 3)],
        // id 1 moves eu → us, id 2 is updated twice in one write, one row has no key.
        vec![
            row(1, "us", 10),
            row(2, "us", 20),
            row(2, "us", 21),
            json!({ "region": "ap", "v": 99 }),
        ],
        // id 2 again (its previous version already superseded a deleted row), and a new id.
        vec![row(2, "eu", 22), row(4, "ap", 40)],
    ]
}

fn merge_local(dir: &std::path::Path, rows: &[Value]) -> MergeSummary {
    let (schema, b) = batch(rows);
    let parts: Vec<String> = PARTS.iter().map(|s| s.to_string()).collect();
    let key = ["id".to_string()];
    let mut s = IcebergSink::create_merge(dir, schema, Compression::Snappy, &parts, &key).unwrap();
    s.write_batch(&b).unwrap();
    s.finish().unwrap().merge.unwrap()
}

fn merge_url(url: &str, rows: &[Value]) -> MergeSummary {
    let (schema, b) = batch(rows);
    let parts: Vec<String> = PARTS.iter().map(|s| s.to_string()).collect();
    let key = ["id".to_string()];
    let mut s =
        ObjectStoreIcebergSink::create_merge(url, schema, Compression::Snappy, &parts, &key)
            .unwrap();
    s.write_batch(&b).unwrap();
    s.finish().unwrap().merge.unwrap()
}

#[test]
fn an_object_store_merge_leaves_what_a_local_merge_does() {
    let local = tmp_dir("local");
    let remote = tmp_dir("remote");
    let url = format!("file://{}", remote.display());

    for (round, rows) in deltas().iter().enumerate() {
        let a = merge_local(&local, rows);
        let b = merge_url(&url, rows);
        assert_eq!(
            a, b,
            "round {round}: the two sinks reported different merges"
        );
        assert_eq!(
            live(&local),
            live(&remote),
            "round {round}: different live rows"
        );
    }
    assert_eq!(
        live(&remote),
        vec![
            (None, "ap".into(), 99),
            (Some(1), "us".into(), 10),
            (Some(2), "eu".into(), 22),
            (Some(3), "eu".into(), 3),
            (Some(4), "ap".into(), 40),
        ]
    );
    let info = shapeshift_objstore::inspect_iceberg(&url).unwrap();
    assert_eq!(info.live_records(), 5);
    assert_eq!(
        info.total_position_deletes,
        inspect(&local).unwrap().total_position_deletes
    );

    // The delete files landed in the store under their partition, beside the data.
    let mut deletes = Vec::new();
    for e in walk(&remote) {
        if e.contains("/delete-") {
            deletes.push(e);
        }
    }
    assert!(!deletes.is_empty());
    assert!(
        deletes.iter().all(|p| p.contains("/data/region=")),
        "{deletes:?}"
    );

    std::fs::remove_dir_all(&local).ok();
    std::fs::remove_dir_all(&remote).ok();
}

#[test]
fn an_object_store_compaction_leaves_what_a_local_compaction_does() {
    let local = tmp_dir("compact-local");
    let remote = tmp_dir("compact-remote");
    let url = format!("file://{}", remote.display());
    for rows in deltas() {
        merge_local(&local, &rows);
        merge_url(&url, &rows);
    }
    let before = live(&remote);
    let options = shapeshift_iceberg::CompactOptions::default();
    let a = shapeshift_iceberg::compact_table(&local, &options).unwrap();
    let b = shapeshift_objstore::compact_iceberg(&url, &options).unwrap();
    let counts = |s: &shapeshift_iceberg::CompactSummary| {
        (
            s.data_files_rewritten,
            s.data_files_written,
            s.data_files_kept,
            s.delete_files_removed,
            s.position_deletes_applied,
            s.rows_rewritten,
        )
    };
    assert_eq!(counts(&a), counts(&b));
    assert!(b.delete_files_removed > 0);
    assert_eq!(live(&remote), before);
    assert_eq!(live(&local), before);
    let info = shapeshift_objstore::inspect_iceberg(&url).unwrap();
    assert_eq!(info.total_position_deletes, 0);
    assert_eq!(info.live_records(), 5);

    // Nothing left to do: no new snapshot.
    let again = shapeshift_objstore::compact_iceberg(&url, &options).unwrap();
    assert_eq!(again.snapshot_id, None);

    // Expiring everything but the compaction deletes the same files from both tables.
    let expire = shapeshift_iceberg::ExpireOptions {
        older_than_ms: i64::MAX,
        retain_last: 1,
        dry_run: false,
    };
    let files_before = walk(&remote).len();
    let a = shapeshift_iceberg::expire_table(&local, &expire).unwrap();
    let b = shapeshift_objstore::expire_iceberg(&url, &expire).unwrap();
    let counts = |s: &shapeshift_iceberg::ExpireSummary| {
        (
            s.expired_snapshots.len(),
            s.data_files_deleted,
            s.delete_files_deleted,
            s.manifests_deleted,
            s.manifest_lists_deleted,
            s.files_not_deleted,
        )
    };
    assert_eq!(counts(&a), counts(&b));
    assert!(b.committed && b.data_files_deleted > 0, "{b:?}");
    let deleted = b.data_files_deleted
        + b.delete_files_deleted
        + b.manifests_deleted
        + b.manifest_lists_deleted;
    assert_eq!(
        walk(&remote).len(),
        files_before - deleted as usize + 1, // one new metadata file
    );
    assert_eq!(live(&remote), before);
    assert_eq!(live(&local), before);

    let missing = shapeshift_objstore::compact_iceberg(&format!("{url}-nope"), &options)
        .unwrap_err()
        .to_string();
    assert!(missing.contains("no Iceberg table"), "{missing}");
    std::fs::remove_dir_all(&local).ok();
    std::fs::remove_dir_all(&remote).ok();
}

#[test]
fn a_bad_merge_key_is_refused_before_anything_is_staged_or_uploaded() {
    let remote = tmp_dir("badkey");
    let url = format!("file://{}", remote.display());
    let (schema, _) = batch(&[row(1, "eu", 1)]);
    let err = ObjectStoreIcebergSink::create_merge(
        &url,
        schema,
        Compression::Snappy,
        &[],
        &["nope".to_string()],
    )
    .err()
    .expect("an unknown key column must be refused")
    .to_string();
    assert!(err.contains("not in the schema"), "{err}");
    assert!(!remote.exists(), "nothing was uploaded");
}

fn walk(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p.to_string_lossy().into_owned());
            }
        }
    }
    // Directory order is not guaranteed: compare file sets, not listings.
    out.sort();
    out
}

#[test]
fn of_two_object_store_writers_on_one_version_the_second_is_refused_and_cleaned_up() {
    let remote = tmp_dir("race");
    let url = format!("file://{}", remote.display());
    merge_url(&url, &[row(1, "eu", 1)]);
    let open = |rows: &[Value]| {
        let (schema, b) = batch(rows);
        let parts: Vec<String> = PARTS.iter().map(|s| s.to_string()).collect();
        let key = ["id".to_string()];
        let mut s =
            ObjectStoreIcebergSink::create_merge(&url, schema, Compression::Snappy, &parts, &key)
                .unwrap();
        s.write_batch(&b).unwrap();
        s
    };
    let mut a = open(&[row(2, "us", 2)]);
    let mut b = open(&[row(3, "ap", 3)]);
    a.finish().unwrap();
    let after_a = walk(&remote);
    let err = b.finish().unwrap_err();
    assert!(
        matches!(err, shapeshift_core::ShapeError::Conflict(_)),
        "expected a conflict, got {err}"
    );
    assert_eq!(
        walk(&remote),
        after_a,
        "nothing of the refused write was left"
    );
    assert_eq!(live(&remote).len(), 2);

    // A stale hint is followed; the re-run builds on the winner.
    std::fs::write(remote.join("metadata/version-hint.text"), "1").unwrap();
    assert_eq!(
        shapeshift_objstore::inspect_iceberg(&url)
            .unwrap()
            .total_records,
        2
    );
    merge_url(&url, &[row(3, "ap", 3)]);
    assert_eq!(live(&remote).len(), 3);
    std::fs::remove_dir_all(&remote).ok();
}

#[test]
fn an_object_store_attempt_that_loses_to_an_earlier_attempt_of_its_run_reports_that_snapshot() {
    let remote = tmp_dir("run-race");
    let url = format!("file://{}", remote.display());
    merge_url(&url, &[row(1, "eu", 1)]);
    let open = |rows: &[Value]| {
        let (schema, b) = batch(rows);
        let parts: Vec<String> = PARTS.iter().map(|s| s.to_string()).collect();
        let mut s = ObjectStoreIcebergSink::create(&url, schema, Compression::Snappy, true, &parts)
            .unwrap();
        s.set_run_id("r-9").unwrap();
        s.write_batch(&b).unwrap();
        s
    };
    let mut first = open(&[row(2, "us", 2)]);
    let mut second = open(&[row(2, "us", 2)]);
    let won = first.finish().unwrap();
    let after_first = walk(&remote);
    let lost = second.finish().unwrap();
    assert!(lost.earlier_attempt);
    assert_eq!(lost.snapshot_id, won.snapshot_id);
    assert_eq!(
        walk(&remote),
        after_first,
        "nothing of the second attempt was left"
    );
    assert_eq!(live(&remote).len(), 2, "the run's row is in the table once");
    let found = shapeshift_objstore::find_run(&url, "r-9").unwrap().unwrap();
    assert_eq!(Some(found.snapshot_id), won.snapshot_id);
    assert_eq!(shapeshift_objstore::find_run(&url, "r-10").unwrap(), None);
    // An attempt that begins after the run committed finds it in the table it loaded.
    let late = open(&[row(2, "us", 2)]).finish().unwrap();
    assert!(late.earlier_attempt);
    assert_eq!(walk(&remote), after_first);
    std::fs::remove_dir_all(&remote).ok();
}

/// The object-store twin of the local test of the same name: a table moved, merged into and
/// moved again is read at its own keys wherever its paths were written, by a merge, a
/// compaction and an expiry, and leaves what the same merges leave in a table that never
/// moved.
#[test]
fn a_table_moved_merged_into_and_moved_again_reads_its_own_files() {
    let root = tmp_dir("moved");
    std::fs::create_dir_all(&root).unwrap();
    let still = root.join("still");
    let url = |name: &str| format!("file://{}", root.join(name).display());
    for (round, (rows, at)) in deltas().iter().zip(["a", "b", "c"]).enumerate() {
        if round > 0 {
            let from = ["a", "b"][round - 1];
            std::fs::rename(root.join(from), root.join(at)).unwrap();
        }
        assert_eq!(
            merge_url(&url(at), rows),
            merge_local(&still, rows),
            "round {round}"
        );
    }
    let c = root.join("c");
    assert_eq!(live(&c), live(&still));
    let info = inspect_iceberg(&url("c")).unwrap();
    assert!(info.moved && info.location.ends_with("/a"), "{info:?}");

    compact_iceberg(&url("c"), &CompactOptions::default()).unwrap();
    assert_eq!(live(&c), live(&still));
    let options = ExpireOptions {
        older_than_ms: i64::MAX,
        retain_last: 1,
        dry_run: false,
    };
    let s = expire_iceberg(&url("c"), &options).unwrap();
    assert_eq!(s.expired_snapshots.len(), 3, "{s:?}");
    assert_eq!(s.files_outside_table, 0, "{s:?}");
    assert_eq!(s.files_not_deleted, 0, "{s:?}");
    assert_eq!(live(&c), live(&still));
    std::fs::remove_dir_all(&root).ok();
}

/// Relocation through the object-store path: a table moved, merged into and moved again is
/// relocated in place; its metadata, manifest lists, manifests and files all name where it
/// is, it reads what the same merges leave in a table that never moved, a second relocation
/// does nothing, and it takes more merges. A copy whose original still answers is refused
/// until named one.
#[test]
fn a_moved_table_relocates_in_a_store() {
    let root = tmp_dir("relocate");
    std::fs::create_dir_all(&root).unwrap();
    let still = root.join("still");
    let url = |name: &str| format!("file://{}", root.join(name).display());
    for (round, (rows, at)) in deltas().iter().zip(["a", "b", "c"]).enumerate() {
        if round > 0 {
            std::fs::rename(root.join(["a", "b"][round - 1]), root.join(at)).unwrap();
        }
        merge_url(&url(at), rows);
        merge_local(&still, rows);
    }
    let options = |identity, dry_run| RelocateOptions { identity, dry_run };
    let s = relocate_iceberg(&url("c"), &options(None, false)).unwrap();
    assert!(s.committed && !s.new_uuid, "{s:?}");
    assert!(s.delete_files_rewritten > 0, "{s:?}");
    assert_eq!(s.superseded_deleted, s.superseded);

    let c = root.join("c");
    let hint = std::fs::read_to_string(c.join("metadata/version-hint.text")).unwrap();
    let meta: Value = serde_json::from_slice(
        &std::fs::read(c.join(format!("metadata/v{}.metadata.json", hint.trim()))).unwrap(),
    )
    .unwrap();
    let here = url("c");
    assert_eq!(meta["location"], here.as_str());
    let local = |uri: &str| std::fs::read(uri.strip_prefix("file://").unwrap()).unwrap();
    for snap in meta["snapshots"].as_array().unwrap() {
        let list = snap["manifest-list"].as_str().unwrap();
        assert!(list.starts_with(&here), "{list}");
        for m in read_manifest_list(&local(list)).unwrap() {
            assert!(m.path.starts_with(&here), "{}", m.path);
            for e in read_manifest_entries(&local(&m.path)).unwrap() {
                assert!(e.file_path.starts_with(&here), "{}", e.file_path);
            }
        }
    }
    assert_eq!(live(&c), live(&still));
    assert!(!inspect_iceberg(&here).unwrap().moved);

    let again = relocate_iceberg(&here, &options(None, false)).unwrap();
    assert!(again.already_anchored && !again.committed, "{again:?}");
    let more = vec![row(1, "eu", 100), row(9, "ap", 9)];
    merge_url(&here, &more);
    merge_local(&still, &more);
    assert_eq!(live(&c), live(&still));

    // A copy of it, the original alive: refused until named one.
    let copy = root.join("copy");
    std::process::Command::new("cp")
        .args(["-r", c.to_str().unwrap(), copy.to_str().unwrap()])
        .status()
        .unwrap();
    let e = relocate_iceberg(&url("copy"), &options(None, false))
        .unwrap_err()
        .to_string();
    assert!(e.contains("still answers at"), "{e}");
    let s = relocate_iceberg(&url("copy"), &options(Some(Identity::Copy), false)).unwrap();
    assert!(s.committed && s.new_uuid, "{s:?}");
    assert_eq!(live(&copy), live(&still));
    assert_eq!(live(&c), live(&still), "the original reads as before");
    assert_eq!(
        inspect(&c).unwrap().table_uuid,
        meta["table-uuid"].as_str().unwrap()
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A place the table's metadata names in another kind of store than the table's (here `s3://`
/// for a `file://` table) is not probed: whether a table answers there is unknown, so the
/// relocation asks rather than guess, and goes ahead once told.
#[test]
fn a_place_in_another_kind_of_store_is_not_probed() {
    let root = tmp_dir("other-store");
    std::fs::create_dir_all(&root).unwrap();
    let url = |name: &str| format!("file://{}", root.join(name).display());
    merge_url(&url("a"), &deltas()[0]);
    std::fs::rename(root.join("a"), root.join("b")).unwrap();
    let meta_path = root.join("b/metadata/v1.metadata.json");
    let mut meta: Value = serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    meta["location"] = json!("s3://someone-elses-bucket/t");
    std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    let options = |identity| RelocateOptions {
        identity,
        dry_run: false,
    };
    let e = relocate_iceberg(&url("b"), &options(None))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("s3://someone-elses-bucket/t") && e.contains("another kind of store"),
        "{e}"
    );
    let s = relocate_iceberg(&url("b"), &options(Some(Identity::Moved))).unwrap();
    assert!(s.committed, "{s:?}");
    std::fs::remove_dir_all(&root).ok();
}
