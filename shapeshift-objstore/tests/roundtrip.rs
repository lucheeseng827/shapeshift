//! Shape JSON → object-store Parquet (via a `file://` URL, which persists to real
//! disk) → read it back and assert the values survived the multipart upload.

use arrow_array::{Array, Float64Array, Int64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_objstore::{is_object_url, ObjectStoreParquetSink};

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
    ]
}

#[test]
fn url_detection() {
    assert!(is_object_url("s3://bucket/k.parquet"));
    assert!(is_object_url("gs://bucket/k.parquet"));
    assert!(is_object_url("file:///tmp/x.parquet"));
    assert!(!is_object_url("/tmp/local.parquet"));
    assert!(!is_object_url("out.parquet"));
    assert!(!is_object_url("relative/dir/"));
}

#[test]
fn unsupported_scheme_is_a_clear_error() {
    let err = ObjectStoreParquetSink::create(
        "ftp://host/x.parquet",
        {
            let sh = Shaper::new(cols(), false).unwrap();
            sh.schema()
        },
        Compression::Snappy,
    );
    assert!(err.is_err());
    assert!(format!("{}", err.err().unwrap()).contains("unsupported object-store scheme"));
}

#[test]
fn writes_parquet_to_file_url_and_reads_back() {
    let dir = std::env::temp_dir().join(format!("ss-obj-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let out = dir.join("events.parquet");
    let url = format!("file://{}", out.display());

    let mut sh = Shaper::new(cols(), false).unwrap();
    sh.push(&json!({"id": 1, "amt": 2.5})).unwrap();
    sh.push(&json!({"id": 2, "amt": 4})).unwrap(); // int → float
    let batch = sh.flush().unwrap().unwrap();

    let mut sink = ObjectStoreParquetSink::create(&url, sh.schema(), Compression::Snappy).unwrap();
    sink.write_batch(&batch).unwrap();
    let summary = sink.finish().unwrap();
    assert_eq!(summary.rows, 2);
    assert!(summary.bytes > 0);

    // LocalFileSystem wrote the real file; read it back and check the values.
    assert!(out.exists(), "expected {} to exist", out.display());
    let file = std::fs::File::open(&out).unwrap();
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

    std::fs::remove_dir_all(&dir).ok();
}
