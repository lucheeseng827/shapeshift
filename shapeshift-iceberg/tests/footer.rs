//! Footer key-value pairs (`IcebergSink::set_footer`): every data file a write produces
//! carries them, made from that file's own row count; a write without them leaves the
//! footer as before; a merge's delete files and a compaction's new files carry none.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_iceberg::{
    compact_table, current_scan_files, CompactOptions, FileFooter, IcebergSink,
};

const KEY: &str = "example.written_by";

fn tmp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-footer-{}-{}", std::process::id(), name));
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

fn shape(rows: &[Value]) -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let cols = vec![
        col("id", ColumnType::Int64),
        col("region", ColumnType::String),
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

fn rows(ids: std::ops::Range<i64>, region: &str) -> Vec<Value> {
    ids.map(|id| json!({"id": id, "region": region})).collect()
}

/// A footer that names the write and the file's row count.
fn footer(write: &str) -> FileFooter {
    let write = write.to_string();
    Arc::new(move |rows| vec![(KEY.to_string(), format!("{write} rows={rows}"))])
}

fn write(dir: &Path, rows: &[Value], append: bool, partition_by: &[&str], f: Option<FileFooter>) {
    let (schema, batch) = shape(rows);
    let parts: Vec<String> = partition_by.iter().map(|s| s.to_string()).collect();
    let mut sink = IcebergSink::create(dir, schema, Compression::Snappy, append, &parts).unwrap();
    if let Some(f) = f {
        sink.set_footer(f);
    }
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap();
}

/// The footer value under [`KEY`] of the Parquet file at `path`, if any.
fn footer_of(path: &str) -> Option<String> {
    let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
    let kv = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    kv.into_iter()
        .flatten()
        .find(|kv| kv.key == KEY)
        .and_then(|kv| kv.value)
}

/// Each live data file's footer value, by path.
fn live_footers(dir: &Path) -> BTreeMap<String, Option<String>> {
    current_scan_files(dir)
        .unwrap()
        .into_iter()
        .map(|f| (f.path.clone(), footer_of(&f.path)))
        .collect()
}

fn parquet_files(dir: &Path, out: &mut Vec<String>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            parquet_files(&p, out);
        } else if p.extension().and_then(|x| x.to_str()) == Some("parquet") {
            out.push(p.to_string_lossy().into_owned());
        }
    }
}

#[test]
fn every_data_file_carries_the_footer_made_from_its_own_rows() {
    let dir = tmp_dir("unpartitioned");
    write(&dir, &rows(0..5, "eu"), false, &[], Some(footer("first")));
    let files = live_footers(&dir);
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.values().next().unwrap().as_deref(),
        Some("first rows=5")
    );
}

#[test]
fn a_partitioned_write_stamps_each_file_with_its_own_count() {
    let dir = tmp_dir("partitioned");
    let mut input = rows(0..3, "eu");
    input.extend(rows(3..10, "us"));
    write(&dir, &input, false, &["region"], Some(footer("p")));
    let mut got: Vec<String> = live_footers(&dir)
        .into_values()
        .map(Option::unwrap)
        .collect();
    got.sort();
    assert_eq!(got, vec!["p rows=3", "p rows=7"]);
}

#[test]
fn a_write_without_a_footer_adds_nothing() {
    let dir = tmp_dir("none");
    write(&dir, &rows(0..4, "eu"), false, &[], None);
    assert!(live_footers(&dir).values().all(Option::is_none));
}

#[test]
fn an_append_stamps_its_own_files_and_leaves_earlier_ones_alone() {
    let dir = tmp_dir("append");
    write(&dir, &rows(0..2, "eu"), false, &[], Some(footer("first")));
    write(&dir, &rows(2..5, "eu"), true, &[], Some(footer("second")));
    write(&dir, &rows(5..6, "eu"), true, &[], None);
    let mut got: Vec<Option<String>> = live_footers(&dir).into_values().collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            None,
            Some("first rows=2".to_string()),
            Some("second rows=3".to_string())
        ]
    );
}

#[test]
fn a_merge_stamps_its_data_files_but_not_its_deletes() {
    let dir = tmp_dir("merge");
    write(&dir, &rows(0..4, "eu"), false, &[], None);
    let (schema, batch) = shape(&rows(2..6, "eu"));
    let mut sink =
        IcebergSink::create_merge(&dir, schema, Compression::Snappy, &[], &["id".to_string()])
            .unwrap();
    sink.set_footer(footer("merge"));
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap();

    let live = live_footers(&dir);
    let mut stamped: Vec<&str> = live.values().flatten().map(String::as_str).collect();
    stamped.sort();
    assert_eq!(stamped, vec!["merge rows=4"]);

    let mut all = Vec::new();
    parquet_files(&dir, &mut all);
    let deletes: Vec<&String> = all.iter().filter(|p| !live.contains_key(*p)).collect();
    assert!(!deletes.is_empty(), "the merge wrote a delete file");
    assert!(deletes.iter().all(|p| footer_of(p).is_none()));
}

#[test]
fn compaction_writes_its_files_without_a_footer() {
    let dir = tmp_dir("compact");
    write(&dir, &rows(0..2, "eu"), false, &[], Some(footer("a")));
    write(&dir, &rows(2..4, "eu"), true, &[], Some(footer("b")));
    let summary = compact_table(&dir, &CompactOptions::default()).unwrap();
    assert!(
        summary.snapshot_id.is_some(),
        "two small files are packed into one"
    );
    let live = live_footers(&dir);
    assert_eq!(live.len(), 1);
    assert!(live.values().all(Option::is_none));
}
