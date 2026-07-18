//! Shape JSON → Parquet → read it back and assert the values survived.

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_parquet::{inspect, ParquetSink};

fn tmp(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ss-pq-{}-{}", std::process::id(), name))
}

fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            from: None,
            ty: ColumnType::Int64,
            transform: None,
            required: true,
        },
        ColumnSpec {
            name: "amt".into(),
            from: None,
            ty: ColumnType::Float64,
            transform: None,
            required: false,
        },
        ColumnSpec {
            name: "name".into(),
            from: Some("user.name".into()),
            ty: ColumnType::String,
            transform: None,
            required: false,
        },
    ]
}

#[test]
fn roundtrip_values_and_schema() {
    let path = tmp("roundtrip.parquet");
    let recs = vec![
        json!({"id": 1, "amt": 2.5, "user": {"name": "Ada"}}),
        json!({"id": 2, "amt": 4, "user": {}}), // missing name → null; int amt → float
    ];

    let mut sh = Shaper::new(cols(), false).unwrap();
    for r in &recs {
        sh.push(r).unwrap();
    }
    let batch = sh.flush().unwrap().unwrap();

    let mut sink = ParquetSink::create(&path, sh.schema(), Compression::Snappy).unwrap();
    sink.write_batch(&batch).unwrap();
    let summary = sink.finish().unwrap();
    assert_eq!(summary.rows, 2);
    assert!(summary.bytes > 0);

    // Metadata read-back.
    let info = inspect(&path).unwrap();
    assert_eq!(info.rows, 2);
    assert_eq!(info.columns.len(), 3);

    // Value read-back via the parquet reader.
    let file = std::fs::File::open(&path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let got = reader.next().unwrap().unwrap();
    assert_eq!(got.num_rows(), 2);
    let id = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let amt = got
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amt.value(0), 2.5);
    assert_eq!(amt.value(1), 4.0);
    let name = got
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "Ada");
    assert!(name.is_null(1));

    let _ = Arc::new(0); // keep the Arc import used across arrow versions
    std::fs::remove_file(&path).ok();
}

/// Without the `zstd` feature (the lean, musl-static default), selecting zstd must be a
/// clear error at sink creation — never a silent codec substitution, and never a
/// truncated target: the codec is validated before `File::create` touches the path.
#[cfg(not(feature = "zstd"))]
#[test]
fn zstd_without_the_feature_is_a_clear_error() {
    let path = tmp("zstd-lean.parquet");
    // Pre-populate the target: the rejected create must leave it byte-identical.
    std::fs::write(&path, b"precious bytes").unwrap();

    let sh = Shaper::new(cols(), false).unwrap();
    let err = ParquetSink::create(&path, sh.schema(), Compression::Zstd);
    let msg = format!(
        "{}",
        err.err().expect("zstd must be refused in the lean build")
    );
    assert!(msg.contains("zstd"), "unexpected error: {msg}");
    assert!(
        msg.contains("--features zstd"),
        "should point at the fat build: {msg}"
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"precious bytes",
        "a rejected create must not truncate an existing file"
    );
    std::fs::remove_file(&path).ok();
}

/// With the `zstd` feature (the opt-in fat build), a zstd file writes and reads back —
/// and its footer really says ZSTD, not a silently substituted codec.
#[cfg(feature = "zstd")]
#[test]
fn zstd_with_the_feature_roundtrips() {
    let path = tmp("zstd-fat.parquet");
    let mut sh = Shaper::new(cols(), false).unwrap();
    sh.push(&json!({"id": 1, "amt": 2.5, "user": {"name": "Ada"}}))
        .unwrap();
    let batch = sh.flush().unwrap().unwrap();

    let mut sink = ParquetSink::create(&path, sh.schema(), Compression::Zstd).unwrap();
    sink.write_batch(&batch).unwrap();
    assert_eq!(sink.finish().unwrap().rows, 1);

    // The footer must report ZSTD for the data columns.
    let file = std::fs::File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let rg = builder.metadata().row_group(0);
    assert!(
        (0..rg.num_columns())
            .all(|i| { format!("{:?}", rg.column(i).compression()).starts_with("ZSTD") }),
        "expected every column chunk to be ZSTD-compressed"
    );
    // ...and the values decode back intact, not just the row count.
    let file = std::fs::File::open(&path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let got = reader.next().unwrap().unwrap();
    assert_eq!(got.num_rows(), 1);
    let id = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.value(0), 1);
    let amt = got
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amt.value(0), 2.5);
    let name = got
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "Ada");
    std::fs::remove_file(&path).ok();
}

#[test]
fn multiple_row_groups_stream() {
    let path = tmp("rowgroups.parquet");
    let mut sh = Shaper::new(
        vec![ColumnSpec {
            name: "id".into(),
            from: None,
            ty: ColumnType::Int64,
            transform: None,
            required: false,
        }],
        false,
    )
    .unwrap();
    let mut sink = ParquetSink::create(&path, sh.schema(), Compression::Uncompressed).unwrap();
    // Three row groups of 4 rows each.
    for g in 0..3 {
        for i in 0..4 {
            sh.push(&json!({"id": g * 4 + i})).unwrap();
        }
        let b = sh.flush().unwrap().unwrap();
        sink.write_batch(&b).unwrap();
    }
    sink.finish().unwrap();
    let info = inspect(&path).unwrap();
    assert_eq!(info.rows, 12);
    assert_eq!(info.row_groups, 3);
    std::fs::remove_file(&path).ok();
}
