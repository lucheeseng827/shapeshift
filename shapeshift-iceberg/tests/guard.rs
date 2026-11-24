//! The commit guard: a commit creates `v{N+1}.metadata.json` only if no other writer did,
//! so two writers that built on the same version cannot both win. The loser gets
//! `ShapeError::Conflict`, leaves nothing behind, and succeeds when run again. Writers and
//! readers follow committed versions past a stale `version-hint.text`, but never past a
//! file that does not continue the table's history.
//!
//! Each race here is made deterministic by opening a sink (which loads the table's
//! current version) before another writer commits, then finishing it.

use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, ShapeError, Shaper, Sink};
use shapeshift_iceberg::{
    compact_table, current_scan_files, expire_table, inspect, local_version, CompactOptions,
    ExpireOptions, IcebergSink,
};

fn tmp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-guard-{}-{}", std::process::id(), name));
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
        vec![col("id", ColumnType::Int64), col("v", ColumnType::Int64)],
        false,
    )
    .unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

fn row(id: i64, v: i64) -> Value {
    json!({ "id": id, "v": v })
}

/// A sink opened now (it loads the table's current version) and written, not finished.
fn open(dir: &Path, rows: &[Value], how: &str) -> IcebergSink {
    let (schema, b) = batch(rows);
    let key = ["id".to_string()];
    let mut s = match how {
        "merge" => IcebergSink::create_merge(dir, schema, Compression::Snappy, &[], &key),
        "append" => IcebergSink::create(dir, schema, Compression::Snappy, true, &[]),
        _ => IcebergSink::create(dir, schema, Compression::Snappy, false, &[]),
    }
    .unwrap();
    s.write_batch(&b).unwrap();
    s
}

fn commit(dir: &Path, rows: &[Value], how: &str) {
    open(dir, rows, how).finish().unwrap();
}

fn live(dir: &Path) -> usize {
    current_scan_files(dir)
        .unwrap()
        .iter()
        .map(|f| f.record_count as usize - f.deleted_positions.len())
        .sum()
}

fn data_files(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir.join("data"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// The data files the current snapshot reads, by file name.
fn referenced(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = current_scan_files(dir)
        .unwrap()
        .iter()
        .map(|f| {
            Path::new(&f.path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    out.sort();
    out
}

fn assert_conflict(err: ShapeError) {
    assert!(
        matches!(err, ShapeError::Conflict(_)),
        "expected a commit conflict, got {err}"
    );
}

#[test]
fn of_two_writers_on_one_version_the_second_is_refused_and_leaves_nothing() {
    let dir = tmp_dir("two");
    commit(&dir, &[row(1, 1)], "append");
    let mut a = open(&dir, &[row(2, 2)], "append");
    let mut b = open(&dir, &[row(3, 3)], "append");
    a.finish().unwrap();

    let err = b.finish().unwrap_err();
    assert!(err.to_string().contains("Run it again"), "{err}");
    assert_conflict(err);
    assert_eq!(
        data_files(&dir),
        referenced(&dir),
        "the loser's data file was removed: every data file left is the table's"
    );
    assert_eq!(
        live(&dir),
        2,
        "the winner's row is there, the loser's is not"
    );
    assert_eq!(local_version(&dir).unwrap(), Some(2));

    // Run again, it builds on the winner.
    commit(&dir, &[row(3, 3)], "append");
    assert_eq!(live(&dir), 3);
    assert_eq!(local_version(&dir).unwrap(), Some(3));
    let leftovers: Vec<_> = std::fs::read_dir(dir.join("metadata"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "no temporary file is left: {leftovers:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_merge_that_started_before_a_compaction_loses_loudly() {
    let dir = tmp_dir("compact");
    commit(&dir, &[row(1, 1), row(2, 2)], "merge");
    commit(&dir, &[row(2, 20)], "merge");
    let mut late = open(&dir, &[row(1, 100)], "merge");
    let s = compact_table(&dir, &CompactOptions::default()).unwrap();
    assert!(s.snapshot_id.is_some());

    // The merge built on the version before the compaction: committing it would drop the
    // compaction (or, before the guard, silently overwrite it).
    assert_conflict(late.finish().unwrap_err());
    assert_eq!(live(&dir), 2);
    assert_eq!(inspect(&dir).unwrap().total_position_deletes, 0);

    commit(&dir, &[row(1, 100)], "merge");
    assert_eq!(live(&dir), 2);
    assert_eq!(inspect(&dir).unwrap().total_position_deletes, 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_merge_that_started_before_an_expiry_loses_loudly() {
    let dir = tmp_dir("expire");
    commit(&dir, &[row(1, 1)], "merge");
    commit(&dir, &[row(1, 10)], "merge");
    compact_table(&dir, &CompactOptions::default()).unwrap();
    let mut late = open(&dir, &[row(2, 2)], "merge");
    let s = expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert!(s.committed);
    assert_conflict(late.finish().unwrap_err());
    commit(&dir, &[row(2, 2)], "merge");
    assert_eq!(live(&dir), 2);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_stale_hint_is_followed_to_the_latest_commit() {
    // A writer that committed but never updated the hint (it crashed, or a slower writer
    // wrote an older number over it) still committed; the next one builds on it.
    let dir = tmp_dir("stale-hint");
    commit(&dir, &[row(1, 1)], "append");
    commit(&dir, &[row(2, 2)], "append");
    std::fs::write(dir.join("metadata/version-hint.text"), "1").unwrap();
    assert_eq!(local_version(&dir).unwrap(), Some(2));
    assert_eq!(inspect(&dir).unwrap().total_records, 2);

    commit(&dir, &[row(3, 3)], "append");
    assert_eq!(live(&dir), 3);
    assert_eq!(
        std::fs::read_to_string(dir.join("metadata/version-hint.text")).unwrap(),
        "3"
    );
    // No hint at all: the table's first metadata file is enough.
    std::fs::remove_file(dir.join("metadata/version-hint.text")).unwrap();
    assert_eq!(local_version(&dir).unwrap(), Some(3));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_write_without_append_replaces_the_table_as_its_next_version() {
    let dir = tmp_dir("replace");
    commit(&dir, &[row(1, 1)], "append");
    commit(&dir, &[row(2, 2)], "append");
    commit(&dir, &[row(9, 9)], "fresh");
    assert_eq!(
        local_version(&dir).unwrap(),
        Some(3),
        "never written over v1"
    );
    assert_eq!(live(&dir), 1, "a fresh table: only the new row");
    let meta: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("metadata/v3.metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["snapshots"].as_array().unwrap().len(), 1);
    assert!(meta["metadata-log"][0]["metadata-file"]
        .as_str()
        .unwrap()
        .ends_with("/v2.metadata.json"));
    commit(&dir, &[row(10, 10)], "append");
    assert_eq!(live(&dir), 2);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_leftover_from_an_earlier_table_is_reported_not_retried() {
    // What shapeshift 0.3 and earlier left after a rewrite without --append: v1 rewritten
    // in place, an older v2 still on disk that does not continue from it.
    let dir = tmp_dir("leftover");
    commit(&dir, &[row(1, 1)], "append");
    std::fs::copy(
        dir.join("metadata/v1.metadata.json"),
        dir.join("metadata/v2.metadata.json"),
    )
    .unwrap();
    assert_eq!(
        local_version(&dir).unwrap(),
        Some(1),
        "the leftover is not a newer commit"
    );
    let before = data_files(&dir);
    let err = open(&dir, &[row(2, 2)], "append").finish().unwrap_err();
    assert!(!matches!(err, ShapeError::Conflict(_)), "{err}");
    let msg = err.to_string();
    assert!(msg.contains("left over from an earlier table"), "{msg}");
    assert_eq!(data_files(&dir), before);
    std::fs::remove_dir_all(&dir).ok();
}
