//! Compaction (`compact_table`): the rows a reader sees never change, the deletes are gone
//! afterwards, small files are packed per partition, the snapshot is a `replace` over one
//! data manifest, and every earlier snapshot still reads its own files. Live rows are
//! read back through `current_scan_files`, as in the merge tests; DuckDB's `iceberg_scan`
//! is checked against a model in `examples/merge/verify_duckdb.py`.

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_iceberg::{
    compact_table, current_data_files, current_scan_files, inspect, read_manifest_entries,
    read_manifest_list, CompactOptions, CompactSummary, IcebergSink,
};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ss-compact-{}-{}", std::process::id(), name));
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

fn merge(dir: &std::path::Path, rows: &[Value], partition_by: &[&str]) {
    let (schema, batch) = shape(rows, &[]);
    let parts: Vec<String> = partition_by.iter().map(|s| s.to_string()).collect();
    let key = ["id".to_string()];
    let mut sink =
        IcebergSink::create_merge(dir, schema, Compression::Snappy, &parts, &key).unwrap();
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap();
}

fn append(dir: &std::path::Path, rows: &[Value], extra: &[&str], partition_by: &[&str]) {
    let (schema, batch) = shape(rows, extra);
    let parts: Vec<String> = partition_by.iter().map(|s| s.to_string()).collect();
    let mut sink = IcebergSink::create(dir, schema, Compression::Snappy, true, &parts).unwrap();
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap();
}

fn compact(dir: &std::path::Path, target_file_size: u64) -> CompactSummary {
    compact_table(
        dir,
        &CompactOptions {
            target_file_size,
            ..CompactOptions::default()
        },
    )
    .unwrap()
}

/// The table's live rows as `(id, region, v, note)`, sorted. `note` is `None` where the
/// column is null or the file predates it.
type Row = (Option<i64>, String, i64, Option<String>);

fn live(dir: &std::path::Path) -> Vec<Row> {
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
            let get = |name: &str| {
                batch
                    .schema()
                    .index_of(name)
                    .ok()
                    .map(|i| batch.column(i).clone())
            };
            let id = get("id").unwrap();
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            let region = get("region").unwrap();
            let region = region.as_any().downcast_ref::<StringArray>().unwrap();
            let v = get("v").unwrap();
            let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
            let note = get("note");
            let note = note
                .as_ref()
                .map(|n| n.as_any().downcast_ref::<StringArray>().unwrap());
            for i in 0..batch.num_rows() {
                if f.deleted_positions.binary_search(&pos).is_err() {
                    out.push((
                        (!id.is_null(i)).then(|| id.value(i)),
                        region.value(i).to_string(),
                        v.value(i),
                        note.filter(|n| !n.is_null(i))
                            .map(|n| n.value(i).to_string()),
                    ));
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

fn metadata(dir: &std::path::Path) -> Value {
    let hint = std::fs::read_to_string(dir.join("metadata/version-hint.text")).unwrap();
    let path = dir.join(format!("metadata/v{}.metadata.json", hint.trim()));
    serde_json::from_reader(std::fs::File::open(path).unwrap()).unwrap()
}

fn current_snapshot(meta: &Value) -> &Value {
    let id = &meta["current-snapshot-id"];
    meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| &s["snapshot-id"] == id)
        .unwrap()
}

/// A history of merges into an unpartitioned table: three data files, two delete files.
fn merged_table(name: &str) -> std::path::PathBuf {
    let dir = tmp_dir(name);
    merge(
        &dir,
        &[row(1, "eu", 1), row(2, "eu", 2), row(3, "us", 3)],
        &[],
    );
    merge(&dir, &[row(2, "eu", 20), row(4, "us", 4)], &[]);
    merge(&dir, &[row(3, "us", 30), row(2, "eu", 200)], &[]);
    dir
}

#[test]
fn compaction_applies_every_delete_and_keeps_the_rows() {
    let dir = merged_table("apply");
    let before = live(&dir);
    let info = inspect(&dir).unwrap();
    assert_eq!(info.total_position_deletes, 3);
    assert!(
        current_data_files(&dir).is_err(),
        "a merged table has deletes"
    );

    let s = compact(&dir, CompactOptions::default().target_file_size);
    assert!(s.snapshot_id.is_some());
    assert_eq!(
        s.data_files_rewritten, 3,
        "every file carried a delete or was small"
    );
    assert_eq!(s.data_files_written, 1, "packed into one");
    assert_eq!(s.data_files_kept, 0);
    assert_eq!(s.delete_files_removed, 2);
    assert_eq!(s.position_deletes_applied, 3);
    assert_eq!(s.rows_rewritten, 4);
    assert_eq!(s.partitions_compacted, 1);

    assert_eq!(live(&dir), before, "a compaction changes no row");
    let info = inspect(&dir).unwrap();
    assert_eq!(info.total_records, 4);
    assert_eq!(info.total_position_deletes, 0);
    assert_eq!(info.live_records(), 4);
    assert_eq!(
        current_data_files(&dir).unwrap().len(),
        1,
        "with no deletes left, the data files read alone again"
    );

    let meta = metadata(&dir);
    let snap = current_snapshot(&meta);
    assert_eq!(snap["summary"]["operation"], "replace");
    assert_eq!(snap["summary"]["deleted-data-files"], "3");
    assert_eq!(snap["summary"]["removed-delete-files"], "2");
    assert_eq!(snap["summary"]["removed-position-deletes"], "3");
    assert_eq!(snap["summary"]["total-delete-files"], "0");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn one_data_manifest_records_what_was_added_kept_and_removed() {
    let dir = tmp_dir("manifest");
    // A big file that a merge does not touch is kept; the small ones the merges wrote
    // are packed.
    let big: Vec<Value> = (100..20_100).map(|i| row(i, "eu", i)).collect();
    merge(&dir, &big, &[]);
    merge(&dir, &[row(1, "eu", 1), row(2, "eu", 2)], &[]);
    merge(&dir, &[row(2, "eu", 20)], &[]);
    let big_file = {
        let files = current_scan_files(&dir).unwrap();
        files
            .iter()
            .max_by_key(|f| f.record_count)
            .unwrap()
            .path
            .clone()
    };
    let size = std::fs::metadata(&big_file).unwrap().len();

    let s = compact(&dir, size); // the big file is not smaller than the target
    assert_eq!(
        (
            s.data_files_rewritten,
            s.data_files_written,
            s.data_files_kept
        ),
        (2, 1, 1)
    );

    let meta = metadata(&dir);
    let seq = current_snapshot(&meta)["sequence-number"].as_i64().unwrap();
    let ml = current_snapshot(&meta)["manifest-list"].as_str().unwrap();
    let manifests = read_manifest_list(&std::fs::read(ml).unwrap()).unwrap();
    assert_eq!(
        manifests.len(),
        2,
        "one data manifest, one delete manifest: {manifests:?}"
    );
    let data = manifests.iter().find(|m| m.content == 0).unwrap();
    assert_eq!(
        (data.added_files, data.existing_files, data.deleted_files),
        (1, 1, 2)
    );
    assert_eq!(
        data.min_sequence_number, 1,
        "the kept file is from the first snapshot"
    );
    let entries = read_manifest_entries(&std::fs::read(&data.path).unwrap()).unwrap();
    let mut seen: Vec<(i32, Option<i64>)> = entries
        .iter()
        .map(|e| (e.status, e.sequence_number))
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![(0, Some(1)), (1, None), (2, Some(2)), (2, Some(3))],
        "EXISTING and DELETED entries keep the sequence number they were written with; \
         the ADDED one inherits {seq}"
    );
    let deletes = manifests.iter().find(|m| m.content == 1).unwrap();
    assert_eq!((deletes.live_files(), deletes.deleted_files), (0, 1));

    // The next write leaves the emptied delete manifest behind.
    append(&dir, &[row(9, "eu", 9)], &[], &[]);
    let meta = metadata(&dir);
    let ml = current_snapshot(&meta)["manifest-list"].as_str().unwrap();
    let manifests = read_manifest_list(&std::fs::read(ml).unwrap()).unwrap();
    assert_eq!(manifests.iter().filter(|m| m.content == 1).count(), 0);
    assert_eq!(live(&dir).len(), 20_000 + 2 + 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn every_earlier_snapshot_still_reads_its_own_files() {
    let dir = merged_table("travel");
    compact(&dir, CompactOptions::default().target_file_size);
    let meta = metadata(&dir);
    let snaps = meta["snapshots"].as_array().unwrap();
    assert_eq!(snaps.len(), 4);
    for snap in &snaps[..3] {
        let ml = snap["manifest-list"].as_str().unwrap();
        for m in read_manifest_list(&std::fs::read(ml).unwrap()).unwrap() {
            for e in read_manifest_entries(&std::fs::read(&m.path).unwrap()).unwrap() {
                assert!(
                    std::path::Path::new(&e.file_path).exists(),
                    "snapshot {} lost {}",
                    snap["snapshot-id"],
                    e.file_path
                );
            }
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn merges_and_compactions_after_a_compaction_stay_exact() {
    let dir = merged_table("after");
    compact(&dir, CompactOptions::default().target_file_size);
    // The merge reads the compacted file (an ADDED entry of the replace snapshot) and
    // deletes from it.
    merge(&dir, &[row(1, "eu", 10), row(5, "ap", 5)], &[]);
    let info = inspect(&dir).unwrap();
    assert_eq!((info.total_records, info.total_position_deletes), (6, 1));
    let expected = vec![
        (Some(1), "eu".to_string(), 10, None),
        (Some(2), "eu".to_string(), 200, None),
        (Some(3), "us".to_string(), 30, None),
        (Some(4), "us".to_string(), 4, None),
        (Some(5), "ap".to_string(), 5, None),
    ];
    assert_eq!(live(&dir), expected);

    let s = compact(&dir, CompactOptions::default().target_file_size);
    assert_eq!(s.delete_files_removed, 1);
    assert_eq!(live(&dir), expected);
    assert_eq!(inspect(&dir).unwrap().total_position_deletes, 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn nothing_to_compact_commits_nothing() {
    let dir = merged_table("noop");
    compact(&dir, CompactOptions::default().target_file_size);
    let version = metadata(&dir)["last-sequence-number"].clone();
    let again = compact(&dir, CompactOptions::default().target_file_size);
    assert_eq!(again.snapshot_id, None);
    assert_eq!(again.data_files_kept, 1);
    assert_eq!(metadata(&dir)["last-sequence-number"], version);

    // One clean file per partition, however small, has nothing to be packed with.
    let one = tmp_dir("noop-one");
    append(&one, &[row(1, "eu", 1), row(2, "us", 2)], &[], &["region"]);
    assert_eq!(
        compact(&one, CompactOptions::default().target_file_size).snapshot_id,
        None
    );
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&one).ok();
}

#[test]
fn small_files_are_packed_within_their_partition() {
    let dir = tmp_dir("pack");
    for i in 0..4 {
        append(
            &dir,
            &[row(i, "eu", i), row(100 + i, "us", i)],
            &[],
            &["region"],
        );
    }
    let before = live(&dir);
    assert_eq!(current_data_files(&dir).unwrap().len(), 8);

    // With a zero target nothing is small, and with no deletes there is nothing to do.
    assert_eq!(compact(&dir, 0).snapshot_id, None);

    let s = compact(&dir, CompactOptions::default().target_file_size);
    assert_eq!((s.data_files_rewritten, s.data_files_written), (8, 2));
    assert_eq!(s.partitions_compacted, 2);
    assert_eq!(s.delete_files_removed, 0);
    let files = current_data_files(&dir).unwrap();
    assert_eq!(files.len(), 2);
    assert!(files.iter().any(|f| f.contains("/data/region=eu/")));
    assert!(files.iter().any(|f| f.contains("/data/region=us/")));
    assert_eq!(live(&dir), before);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_zero_target_only_applies_deletes() {
    let dir = merged_table("zero");
    let before = live(&dir);
    let s = compact(&dir, 0);
    // Each file that carries a delete is rewritten on its own; the third file (written
    // by the last merge) carries none and is kept.
    assert_eq!(
        (
            s.data_files_rewritten,
            s.data_files_written,
            s.data_files_kept
        ),
        (2, 2, 1)
    );
    assert_eq!(live(&dir), before);
    assert_eq!(inspect(&dir).unwrap().total_position_deletes, 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_file_whose_rows_were_all_replaced_is_dropped() {
    let dir = tmp_dir("emptied");
    merge(&dir, &[row(1, "eu", 1), row(2, "eu", 2)], &[]);
    merge(&dir, &[row(1, "eu", 10), row(2, "eu", 20)], &[]);
    let s = compact(&dir, 0);
    assert_eq!(
        (
            s.data_files_rewritten,
            s.data_files_written,
            s.data_files_kept
        ),
        (1, 0, 1)
    );
    assert_eq!(s.rows_rewritten, 0);
    assert_eq!(
        live(&dir),
        vec![
            (Some(1), "eu".to_string(), 10, None),
            (Some(2), "eu".to_string(), 20, None)
        ]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_column_a_file_predates_reads_as_null_after_packing() {
    let dir = tmp_dir("evolved");
    append(&dir, &[row(1, "eu", 1)], &[], &[]);
    append(
        &dir,
        &[json!({ "id": 2, "region": "eu", "v": 2, "note": "late" })],
        &["note"],
        &[],
    );
    let s = compact(&dir, CompactOptions::default().target_file_size);
    assert_eq!((s.data_files_rewritten, s.data_files_written), (2, 1));
    assert_eq!(
        live(&dir),
        vec![
            (Some(1), "eu".to_string(), 1, None),
            (Some(2), "eu".to_string(), 2, Some("late".to_string()))
        ]
    );
    assert_eq!(inspect(&dir).unwrap().fields.len(), 4);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_partitioned_merge_history_compacts_per_partition() {
    let dir = tmp_dir("partitioned");
    merge(
        &dir,
        &[row(1, "eu", 1), row(2, "us", 2), row(3, "us", 3)],
        &["region"],
    );
    // Id 1 moves eu → us; id 3 is updated in place.
    merge(&dir, &[row(1, "us", 10), row(3, "us", 30)], &["region"]);
    let before = live(&dir);
    let s = compact(&dir, CompactOptions::default().target_file_size);
    assert_eq!(s.delete_files_removed, 2, "one delete file per partition");
    assert_eq!(live(&dir), before);
    let files = current_data_files(&dir).unwrap();
    assert_eq!(files.len(), 1, "eu has no live row left: {files:?}");
    assert!(files[0].contains("/data/region=us/"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_compaction_that_fails_leaves_no_file_and_no_snapshot_behind() {
    let dir = merged_table("fails");
    let files = |dir: &std::path::Path| -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir.join("data"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        out.sort();
        out
    };
    let before = files(&dir);
    let version = metadata(&dir)["last-sequence-number"].clone();
    // The last merge's file now holds the first merge's three rows where its manifest
    // records two. By the time the compaction reaches it, it has already started writing
    // the output (one row per row group), so the failure has a file to clean up.
    let mut data: Vec<_> = current_scan_files(&dir).unwrap();
    data.sort_by_key(|f| std::fs::metadata(&f.path).unwrap().modified().unwrap());
    std::fs::copy(&data[0].path, &data[2].path).unwrap();

    let err = compact_table(
        &dir,
        &CompactOptions {
            row_group_rows: 1,
            ..CompactOptions::default()
        },
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("refusing to rewrite"), "{err}");
    assert_eq!(files(&dir), before, "the partial output was removed");
    assert_eq!(
        metadata(&dir)["last-sequence-number"],
        version,
        "nothing committed"
    );
    std::fs::remove_dir_all(&dir).ok();
}
