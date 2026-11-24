//! Merge through the object-store sink (a `file://` URL, which runs the same code an
//! `s3://` table does): the same deltas merged into a local table and into an
//! object-store table must leave the same live rows and report the same merge.
//! The S3 protocol itself — ranged GETs, multipart uploads — is exercised by
//! `examples/merge/verify_duckdb.py --tables s3://…` against an S3-compatible server.

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, MergeSummary, Shaper, Sink};
use shapeshift_iceberg::{current_scan_files, inspect, IcebergSink};
use shapeshift_objstore::ObjectStoreIcebergSink;

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
