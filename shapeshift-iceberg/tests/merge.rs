//! Merge (`IcebergSink::create_merge`): the rows a reader sees after a merge, the delete
//! files and manifests that produce them, and the report. Live rows are read back through
//! `current_scan_files` — every data file minus the positions its delete files remove —
//! which is the same rule an Iceberg reader applies (DuckDB's `iceberg_scan` is checked
//! against a model of the same semantics in `examples/merge/verify_duckdb.py`).

use std::collections::BTreeMap;

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, MergeSummary, Shaper, Sink};
use shapeshift_iceberg::{
    current_data_files, current_scan_files, inspect, read_manifest_entries, read_manifest_list,
    IcebergSink, PartVal,
};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ss-merge-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn col(name: &str, ty: ColumnType) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        from: None,
        ty,
        transform: None,
        required: false,
    }
}

/// Shape `rows` (JSON objects over `id`, `region`, `v`, plus any `extra` columns) into one
/// batch.
fn shape(
    rows: &[Value],
    extra: &[&str],
) -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let mut cols = vec![
        col("id", ColumnType::Int64),
        col("region", ColumnType::String),
        col("v", ColumnType::Int64),
    ];
    cols.extend(extra.iter().map(|n| col(n, ColumnType::String)));
    let mut sh = Shaper::new(cols, false).unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

/// Merge `rows` into the table at `dir` on `key`, returning the merge report.
fn merge(
    dir: &std::path::Path,
    rows: &[Value],
    key: &[&str],
    partition_by: &[&str],
) -> MergeSummary {
    let (schema, batch) = shape(rows, &[]);
    let key: Vec<String> = key.iter().map(|s| s.to_string()).collect();
    let parts: Vec<String> = partition_by.iter().map(|s| s.to_string()).collect();
    let mut sink =
        IcebergSink::create_merge(dir, schema, Compression::Snappy, &parts, &key).unwrap();
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap().merge.expect("a merge reports")
}

/// The table's live rows as `(id, region, v)`, sorted — data files minus their deletes.
fn live(dir: &std::path::Path) -> Vec<(Option<i64>, String, i64)> {
    let mut out = Vec::new();
    for f in current_scan_files(dir).unwrap() {
        let file = std::fs::File::open(&f.path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut pos = 0u64;
        for batch in reader {
            let batch = batch.unwrap();
            let get = |name: &str| batch.column(batch.schema().index_of(name).unwrap()).clone();
            let id = get("id");
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            let region = get("region");
            let region = region.as_any().downcast_ref::<StringArray>().unwrap();
            let v = get("v");
            let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..batch.num_rows() {
                if f.deleted_positions.binary_search(&pos).is_err() {
                    let id = (!id.is_null(i)).then(|| id.value(i));
                    out.push((id, region.value(i).to_string(), v.value(i)));
                }
                pos += 1;
            }
        }
    }
    out.sort();
    out
}

fn row(id: i64, region: &str, v: i64) -> Value {
    json!({ "id": id, "region": region, "v": v })
}

#[test]
fn a_merge_replaces_matching_keys_and_keeps_the_last_occurrence() {
    let dir = tmp_dir("upsert");
    let first = merge(
        &dir,
        &[row(1, "eu", 10), row(2, "eu", 20), row(3, "us", 30)],
        &["id"],
        &[],
    );
    assert_eq!((first.keys_inserted, first.keys_updated), (3, 0));
    assert_eq!(first.position_deletes, 0);
    assert_eq!(
        first.delete_files, 0,
        "nothing to delete writes no delete file"
    );

    let second = merge(
        &dir,
        &[
            row(2, "eu", 21),
            row(4, "us", 40),
            row(4, "us", 41),                   // the same key again: this one wins
            json!({ "region": "ap", "v": 99 }), // no key
        ],
        &["id"],
        &[],
    );
    assert_eq!(second.distinct_keys, 2);
    assert_eq!(second.keys_updated, 1, "id 2 was in the table");
    assert_eq!(second.keys_inserted, 1, "id 4 was not");
    assert_eq!(second.prior_rows_replaced, 1);
    assert_eq!(second.duplicates_in_input, 1);
    assert_eq!(second.null_key_rows, 1);
    assert_eq!(second.position_deletes, 2);
    assert_eq!(second.delete_files, 1);
    assert_eq!(second.data_files_scanned, 1);

    assert_eq!(
        live(&dir),
        vec![
            (None, "ap".into(), 99),
            (Some(1), "eu".into(), 10),
            (Some(2), "eu".into(), 21),
            (Some(3), "us".into(), 30),
            (Some(4), "us".into(), 41),
        ]
    );
    let info = inspect(&dir).unwrap();
    assert_eq!(
        info.total_records, 7,
        "every written row is still in a data file"
    );
    assert_eq!(info.total_position_deletes, 2);
    assert_eq!(info.live_records(), 5);

    // The snapshot is an overwrite with the delete counts in its summary, and its
    // manifest list carries a delete manifest (content 1) beside the data manifests.
    let meta: Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v2.metadata.json")).unwrap(),
    )
    .unwrap();
    let summary = &meta["snapshots"][1]["summary"];
    assert_eq!(summary["operation"], "overwrite");
    assert_eq!(summary["added-position-deletes"], "2");
    assert_eq!(summary["added-delete-files"], "1");
    assert_eq!(summary["total-records"], "7");
    let ml = meta["snapshots"][1]["manifest-list"].as_str().unwrap();
    let manifests = read_manifest_list(&std::fs::read(ml).unwrap()).unwrap();
    assert_eq!(manifests.iter().filter(|m| m.content == 1).count(), 1);
    assert_eq!(
        manifests.iter().filter(|m| m.content == 0).count(),
        2,
        "both data manifests stay live — no data file is rewritten"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_key_merged_again_is_deleted_once_and_counts_stay_exact() {
    let dir = tmp_dir("again");
    merge(&dir, &[row(1, "eu", 1), row(2, "eu", 2)], &["id"], &[]);
    merge(&dir, &[row(1, "eu", 10)], &["id"], &[]);
    // Id 1 now has a deleted row in file A and a live one in file B. Merging it again
    // must delete only the live one — re-deleting the dead row would make
    // `total-position-deletes` overcount and the live count wrong.
    let third = merge(&dir, &[row(1, "eu", 100)], &["id"], &[]);
    assert_eq!(third.prior_rows_replaced, 1);
    assert_eq!(third.keys_updated, 1);
    let info = inspect(&dir).unwrap();
    assert_eq!(info.total_records, 4);
    assert_eq!(info.total_position_deletes, 2);
    assert_eq!(info.live_records(), 2);
    assert_eq!(
        live(&dir),
        vec![(Some(1), "eu".into(), 100), (Some(2), "eu".into(), 2)]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_key_that_moves_partition_is_deleted_from_its_old_one() {
    let dir = tmp_dir("move");
    merge(
        &dir,
        &[row(1, "eu", 1), row(2, "us", 2)],
        &["id"],
        &["region"],
    );
    // Id 1 moves eu → us. Its old row lives in the eu partition, so the delete for it
    // must carry the eu partition tuple: a reader applies a position delete only to
    // data files in the delete file's own partition.
    let m = merge(&dir, &[row(1, "us", 10)], &["id"], &["region"]);
    assert_eq!(m.prior_rows_replaced, 1);
    assert_eq!(
        live(&dir),
        vec![(Some(1), "us".into(), 10), (Some(2), "us".into(), 2)]
    );

    let meta: Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v2.metadata.json")).unwrap(),
    )
    .unwrap();
    let ml = meta["snapshots"][1]["manifest-list"].as_str().unwrap();
    let manifests = read_manifest_list(&std::fs::read(ml).unwrap()).unwrap();
    let delete_manifest = manifests.iter().find(|m| m.content == 1).unwrap();
    let deletes = read_manifest_entries(&std::fs::read(&delete_manifest.path).unwrap()).unwrap();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].content, 1);
    assert_eq!(deletes[0].partition, vec![PartVal::Str("eu".into())]);
    assert!(
        deletes[0].file_path.contains("/data/region=eu/delete-"),
        "{}",
        deletes[0].file_path
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn merges_read_large_delete_files_and_adjacent_key_chunks() {
    // Big enough that the files outgrow the 64 KiB footer prefetch, so reads come from
    // per-chunk segments: the composite key's two columns are adjacent chunks, and the
    // second merge's 20,000 deletes make a delete file whose two columns are too.
    let dir = tmp_dir("large");
    let n = 20_000i64;
    let rows = |v: i64| -> Vec<Value> { (0..n).map(|i| row(i, "eu", v)).collect() };
    merge(&dir, &rows(1), &["id", "region"], &[]);
    let second = merge(&dir, &rows(2), &["id", "region"], &[]);
    assert_eq!(second.prior_rows_replaced, n as u64);
    // The third merge reads the 20,000-row delete file to know which rows are already
    // gone, then replaces a handful of keys.
    let third = merge(
        &dir,
        &[row(7, "eu", 3), row(19_999, "eu", 3)],
        &["id", "region"],
        &[],
    );
    assert_eq!(
        third.prior_rows_replaced, 2,
        "only the live copies are replaced"
    );
    let live = live(&dir);
    assert_eq!(live.len(), n as usize);
    assert!(live.iter().all(|(id, _, v)| *v
        == if matches!(id, Some(7) | Some(19_999)) {
            3
        } else {
            2
        }));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_write_with_only_null_keys_scans_nothing() {
    let dir = tmp_dir("nullonly");
    merge(&dir, &[row(1, "eu", 1)], &["id"], &[]);
    let m = merge(&dir, &[json!({ "region": "eu", "v": 2 })], &["id"], &[]);
    assert_eq!(m.null_key_rows, 1);
    assert_eq!((m.data_files_scanned, m.data_files_pruned), (0, 0));
    assert_eq!(m.position_deletes, 0);
    assert_eq!(live(&dir).len(), 2);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_composite_key_matches_on_every_column() {
    let dir = tmp_dir("composite");
    merge(
        &dir,
        &[row(1, "eu", 1), row(1, "us", 2)],
        &["region", "id"],
        &[],
    );
    let m = merge(&dir, &[row(1, "us", 20)], &["region", "id"], &[]);
    assert_eq!(
        m.prior_rows_replaced, 1,
        "only (us, 1) matches — (eu, 1) is another key"
    );
    assert_eq!(
        live(&dir),
        vec![(Some(1), "eu".into(), 1), (Some(1), "us".into(), 20)]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_merge_replaces_every_duplicate_an_append_left_behind() {
    let dir = tmp_dir("dupes");
    let (schema, batch) = shape(&[row(7, "eu", 1), row(7, "eu", 2), row(8, "eu", 3)], &[]);
    let mut s = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    s.write_batch(&batch).unwrap();
    assert!(
        s.finish().unwrap().merge.is_none(),
        "a plain write reports no merge"
    );

    let m = merge(&dir, &[row(7, "eu", 70)], &["id"], &[]);
    assert_eq!(m.keys_updated, 1);
    assert_eq!(m.prior_rows_replaced, 2, "both appended copies of id 7 go");
    assert_eq!(
        live(&dir),
        vec![(Some(7), "eu".into(), 70), (Some(8), "eu".into(), 3)]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn files_whose_key_bounds_cannot_match_are_not_read() {
    let dir = tmp_dir("prune");
    merge(&dir, &[row(1, "eu", 1), row(2, "eu", 2)], &["id"], &[]);
    merge(&dir, &[row(100, "eu", 1), row(101, "eu", 2)], &["id"], &[]);
    let m = merge(&dir, &[row(101, "eu", 9)], &["id"], &[]);
    assert_eq!(m.data_files_pruned, 1, "the [1, 2] file cannot hold id 101");
    assert_eq!(m.data_files_scanned, 1);
    assert_eq!(m.prior_rows_replaced, 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_merge_can_add_a_column() {
    let dir = tmp_dir("evolve");
    merge(&dir, &[row(1, "eu", 1), row(2, "eu", 2)], &["id"], &[]);
    let (schema, batch) = shape(
        &[json!({ "id": 2, "region": "eu", "v": 20, "note": "late" })],
        &["note"],
    );
    let key = ["id".to_string()];
    let mut s = IcebergSink::create_merge(&dir, schema, Compression::Snappy, &[], &key).unwrap();
    s.write_batch(&batch).unwrap();
    let m = s.finish().unwrap().merge.unwrap();
    assert_eq!(m.prior_rows_replaced, 1);
    assert_eq!(
        live(&dir),
        vec![(Some(1), "eu".into(), 1), (Some(2), "eu".into(), 20)]
    );
    assert_eq!(inspect(&dir).unwrap().fields.len(), 4);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_bad_key_is_refused_before_anything_is_written() {
    let dir = tmp_dir("badkey");
    let (schema, _) = shape(&[row(1, "eu", 1)], &[]);
    for (key, why) in [
        (vec!["nope"], "not in the schema"),
        (vec!["id", "id"], "twice"),
        (Vec::new(), "no key column"),
    ] {
        let key: Vec<String> = key.iter().map(|s| s.to_string()).collect();
        let err = IcebergSink::create_merge(&dir, schema.clone(), Compression::Snappy, &[], &key)
            .err()
            .unwrap_or_else(|| panic!("{key:?} must be refused"))
            .to_string();
        assert!(err.contains(why), "{key:?}: {err}");
    }
    let parquet = walk(&dir)
        .iter()
        .filter(|p| p.ends_with(".parquet"))
        .count();
    assert_eq!(parquet, 0, "a refused merge leaves no data file behind");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn listing_a_merged_tables_data_files_alone_is_refused() {
    let dir = tmp_dir("list");
    merge(&dir, &[row(1, "eu", 1)], &["id"], &[]);
    assert_eq!(current_data_files(&dir).unwrap().len(), 1, "no deletes yet");
    merge(&dir, &[row(1, "eu", 2)], &["id"], &[]);
    let err = current_data_files(&dir).unwrap_err().to_string();
    assert!(err.contains("resurrect deleted rows"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn partitioned_tables_list_their_data_files() {
    // The manifest reader decodes the partition record from the file's own schema, so
    // a partitioned table's committed files list exactly (they used to fall back to a
    // directory scan).
    let dir = tmp_dir("list-part");
    let (schema, batch) = shape(&[row(1, "eu", 1), row(2, "us", 2), row(3, "us", 3)], &[]);
    let parts = ["region".to_string()];
    let mut s = IcebergSink::create(&dir, schema, Compression::Snappy, false, &parts).unwrap();
    s.write_batch(&batch).unwrap();
    s.finish().unwrap();
    let files = current_data_files(&dir).unwrap();
    assert_eq!(files.len(), 2);
    let mut by_region: BTreeMap<bool, usize> = BTreeMap::new();
    for f in &files {
        *by_region.entry(f.contains("region=eu")).or_default() += 1;
    }
    assert_eq!(by_region.len(), 2);
    std::fs::remove_dir_all(&dir).ok();
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
