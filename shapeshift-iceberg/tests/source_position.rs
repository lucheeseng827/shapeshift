//! Seam C: a pulled source's position (`shapeshift.source.<kind>`) is committed with the
//! rows, carried forward by every later snapshot, and read back.

use serde_json::json;
use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_iceberg::{expire_table, source_position_local, ExpireOptions, IcebergSink};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ss-srcpos-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Append one row, recording `position` for an `http` source when given.
fn write(dir: &std::path::Path, id: i64, position: Option<serde_json::Value>, append: bool) {
    write_kind(dir, id, "http", position, append)
}

fn write_kind(
    dir: &std::path::Path,
    id: i64,
    kind: &str,
    position: Option<serde_json::Value>,
    append: bool,
) {
    let cols = vec![ColumnSpec {
        name: "id".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: false,
    }];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({ "id": id })).unwrap();
    let mut sink = IcebergSink::create(dir, sh.schema(), Compression::Snappy, append, &[]).unwrap();
    if let Some(p) = position {
        sink.set_source_position(kind, &p).unwrap();
    }
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    sink.finish().unwrap();
}

#[test]
fn a_position_is_committed_carried_forward_and_survives_expiry() {
    let dir = tmp_dir("carry");
    assert_eq!(
        source_position_local(&dir, "http").unwrap(),
        None,
        "no table"
    );
    write(&dir, 1, Some(json!({"at": 5})), false);
    assert_eq!(
        source_position_local(&dir, "http").unwrap(),
        Some(json!({"at": 5}))
    );
    // Another write that pulls nothing keeps saying where the source is.
    write(&dir, 2, None, true);
    assert_eq!(
        source_position_local(&dir, "http").unwrap(),
        Some(json!({"at": 5}))
    );
    // A table holds one source: another kind is refused, not started from no position.
    let e = source_position_local(&dir, "mysql")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("`http` source") && e.contains("`mysql` source"),
        "{e}"
    );
    write(&dir, 3, Some(json!({"inside": 9, "after": "k"})), true);
    write(&dir, 4, None, true);
    expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert_eq!(
        source_position_local(&dir, "http").unwrap(),
        Some(json!({"inside": 9, "after": "k"}))
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_table_written_from_mysql_refuses_a_postgres_run() {
    let dir = tmp_dir("switch");
    write_kind(
        &dir,
        1,
        "mysql",
        Some(json!({"at": 5, "of": "mysql://h/db"})),
        false,
    );
    assert!(source_position_local(&dir, "mysql").unwrap().is_some());
    let e = source_position_local(&dir, "postgres")
        .unwrap_err()
        .to_string();
    assert!(e.contains("table of its own"), "{e}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_bad_kind_is_refused_and_a_file_sink_cannot_record_one() {
    let dir = tmp_dir("kind");
    let cols = vec![ColumnSpec {
        name: "id".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: false,
    }];
    let sh = Shaper::new(cols, false).unwrap();
    let mut sink = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, false, &[]).unwrap();
    for bad in ["", "HTTP", "a.b", "x y"] {
        assert!(
            sink.set_source_position(bad, &json!({"at": 1})).is_err(),
            "{bad:?}"
        );
    }
    // A sink that is not a table (a Parquet file) refuses, by the trait's default.
    struct File;
    impl Sink for File {
        fn write_batch(&mut self, _: &shapeshift_core::RecordBatch) -> shapeshift_core::Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> shapeshift_core::Result<shapeshift_core::SinkSummary> {
            unreachable!()
        }
    }
    let e = File
        .set_source_position("http", &json!({"at": 1}))
        .unwrap_err()
        .to_string();
    assert!(e.contains("iceberg"), "{e}");
    std::fs::remove_dir_all(&dir).ok();
}
