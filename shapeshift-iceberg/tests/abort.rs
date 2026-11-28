//! A run that fails before its commit (a strict coercion, `drift.policy: error`, a source error)
//! drops its sink unfinished. Nothing of it may be left where a table is: the data files it
//! flushed are removed, and so is the table directory when the run made it. A table that was
//! already there keeps every file it had, and a directory the run found stays.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_iceberg::{inspect, IcebergSink};

fn tmp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-abort-{}-{}", std::process::id(), name));
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
        ],
        false,
    )
    .unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

fn rows() -> Vec<Value> {
    vec![
        json!({ "id": 1, "region": "eu" }),
        json!({ "id": 2, "region": "us" }),
        json!({ "id": 3, "region": "eu" }),
    ]
}

/// Every file under `dir`, relative to it.
fn files(dir: &Path) -> BTreeSet<String> {
    fn walk(base: &Path, d: &Path, out: &mut BTreeSet<String>) {
        for e in std::fs::read_dir(d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.insert(p.strip_prefix(base).unwrap().to_string_lossy().into_owned());
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(dir, dir, &mut out);
    out
}

/// Open a sink on `dir`, write the rows, and drop it unfinished: a failed run.
fn fail_a_run(dir: &Path, append: bool, partition_by: &[String]) {
    let (schema, b) = batch(&rows());
    let mut s =
        IcebergSink::create(dir, schema, Compression::Snappy, append, partition_by).unwrap();
    s.write_batch(&b).unwrap();
    // The batch is on disk now: the run fails here, and its sink goes out of scope.
    assert!(
        !files(dir).iter().all(|f| f.starts_with("metadata")),
        "a data file should have been written before the failure: {:?}",
        files(dir)
    );
    drop(s);
}

#[test]
fn a_failed_first_run_leaves_no_table_directory() {
    for (name, partition_by) in [("flat", vec![]), ("parts", vec!["region".to_string()])] {
        let dir = tmp_dir(name);
        fail_a_run(&dir, false, &partition_by);
        assert!(!dir.exists(), "{name}: {:?} is left", files(&dir));
    }
}

#[test]
fn a_failed_run_on_an_existing_table_leaves_it_as_it_was() {
    let dir = tmp_dir("existing");
    let (schema, b) = batch(&rows());
    let mut s = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    s.write_batch(&b).unwrap();
    s.finish().unwrap();
    let before = files(&dir);

    fail_a_run(&dir, true, &[]);
    assert_eq!(files(&dir), before, "the failed append left or took files");
    assert_eq!(
        inspect(&dir).unwrap().total_records,
        3,
        "the table still reads its 3 rows"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_directory_the_run_found_stays_and_only_its_files_go() {
    let dir = tmp_dir("found");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("README"), "kept").unwrap();
    fail_a_run(&dir, false, &[]);
    assert!(dir.exists());
    assert_eq!(files(&dir), BTreeSet::from(["README".to_string()]));
    // Nor the empty `data/` and `metadata/` the sink made in it.
    assert!(!dir.join("data").exists() && !dir.join("metadata").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_sink_refused_at_creation_leaves_no_table_directory() {
    let dir = tmp_dir("refused");
    let (schema, _) = batch(&rows());
    let refused = IcebergSink::create(
        &dir,
        schema,
        Compression::Snappy,
        false,
        &["no_such_column".to_string()],
    );
    assert!(
        refused.is_err(),
        "a partition column that does not exist is refused"
    );
    assert!(!dir.exists(), "{:?} is left", files(&dir));
}

/// A partition directory that is a symlink by the time the run fails is not followed: the
/// cleanup removes nothing through it.
#[cfg(unix)]
#[test]
fn a_cleanup_never_follows_a_symlinked_directory_out() {
    let dir = tmp_dir("symlink");
    let outside = tmp_dir("symlink-outside");
    let (schema, b) = batch(&rows());
    let mut s = IcebergSink::create(
        &dir,
        schema,
        Compression::Snappy,
        false,
        &["region".to_string()],
    )
    .unwrap();
    s.write_batch(&b).unwrap();
    // Swap the `region=eu` partition directory for a link to a directory outside the table
    // that holds a file of the same name.
    let part = dir.join("data").join("region=eu");
    let name = std::fs::read_dir(&part)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::rename(&part, outside.join("moved")).unwrap();
    std::fs::write(outside.join(&name), "not the table's").unwrap();
    std::os::unix::fs::symlink(&outside, &part).unwrap();

    drop(s);
    assert!(
        outside.join(&name).exists(),
        "the cleanup followed the link out"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&outside);
}

/// Two writers open one new table; the first is dropped before it writes (a run refused
/// later). Its cleanup removes the empty `metadata/` both made, and the second still commits.
#[test]
fn a_dropped_sink_does_not_break_another_on_the_same_new_table() {
    let dir = tmp_dir("two-writers");
    let (schema, b) = batch(&rows());
    let first = IcebergSink::create(
        &dir,
        schema.clone(),
        Compression::Snappy,
        false,
        &["region".to_string()],
    )
    .unwrap();
    let mut second = IcebergSink::create(
        &dir,
        schema,
        Compression::Snappy,
        false,
        &["region".to_string()],
    )
    .unwrap();
    second.write_batch(&b).unwrap();
    drop(first);
    assert!(
        !dir.join("metadata").exists(),
        "the first sink's cleanup ran"
    );
    second.finish().unwrap();
    assert_eq!(inspect(&dir).unwrap().total_records, 3);
    std::fs::remove_dir_all(&dir).unwrap();
}
