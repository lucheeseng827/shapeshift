//! Shape JSON → object-store Iceberg table (via a `file://` URL, which persists to
//! real disk) → assert all five files land, the metadata is anchored at the location,
//! and `inspect_iceberg` reads the table back.

use serde_json::json;

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_objstore::{inspect_iceberg, ObjectStoreIcebergSink};

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
fn unsupported_scheme_is_a_clear_error() {
    let sh = Shaper::new(cols(), false).unwrap();
    let err = ObjectStoreIcebergSink::create(
        "ftp://host/tbl",
        sh.schema(),
        Compression::Snappy,
        false,
        &[],
    );
    assert!(err.is_err());
    assert!(format!("{}", err.err().unwrap()).contains("unsupported object-store scheme"));
}

#[test]
fn writes_iceberg_table_to_file_url_and_inspects_back() {
    let dir = std::env::temp_dir().join(format!("ss-ice-obj-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let url = format!("file://{}", dir.display());

    let mut sh = Shaper::new(cols(), false).unwrap();
    sh.push(&json!({"id": 1, "amt": 2.5})).unwrap();
    sh.push(&json!({"id": 2, "amt": 4})).unwrap(); // int → float
    let batch = sh.flush().unwrap().unwrap();

    let mut sink =
        ObjectStoreIcebergSink::create(&url, sh.schema(), Compression::Snappy, false, &[]).unwrap();
    sink.write_batch(&batch).unwrap();
    let summary = sink.finish().unwrap();
    assert_eq!(summary.rows, 2);
    assert!(summary.bytes > 0);

    // All five files landed on disk (LocalFileSystem, via the file:// URL): the data
    // Parquet plus the four metadata files.
    assert!(dir.join("metadata/version-hint.text").exists());
    assert!(dir.join("metadata/v1.metadata.json").exists());
    let data_files: Vec<_> = std::fs::read_dir(dir.join("data")).unwrap().collect();
    assert_eq!(data_files.len(), 1, "expected exactly one data Parquet");

    // The metadata `location` is anchored at the object-store URL, not a local path.
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v1.metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["location"], url);
    // ...and the data file path in the snapshot's manifest resolves under that URL.
    assert!(meta["snapshots"][0]["manifest-list"]
        .as_str()
        .unwrap()
        .starts_with(&url));

    // Reading the table back through the object store returns the same shape.
    let info = inspect_iceberg(&url).unwrap();
    assert_eq!(info.format_version, 2);
    assert_eq!(info.total_records, 2);
    assert_eq!(info.fields.len(), 2);

    std::fs::remove_dir_all(&dir).ok();
}

fn batch(ids: &[i64]) -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let mut sh = Shaper::new(cols(), false).unwrap();
    for &id in ids {
        sh.push(&json!({ "id": id, "amt": id as f64 })).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

#[test]
fn appends_a_snapshot_to_an_object_store_table() {
    let dir = std::env::temp_dir().join(format!("ss-ice-obj-app-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let url = format!("file://{}", dir.display());

    // Fresh table: two rows.
    let (schema, b1) = batch(&[1, 2]);
    let mut s1 =
        ObjectStoreIcebergSink::create(&url, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&b1).unwrap();
    s1.finish().unwrap();
    assert_eq!(inspect_iceberg(&url).unwrap().total_records, 2);

    // Append a snapshot: one more row. The prior manifest list is read back from the
    // store and carried forward.
    let (schema, b2) = batch(&[3]);
    let mut s2 =
        ObjectStoreIcebergSink::create(&url, schema, Compression::Snappy, true, &[]).unwrap();
    s2.write_batch(&b2).unwrap();
    s2.finish().unwrap();

    // Version bumped, all three rows live across two snapshots.
    assert_eq!(
        std::fs::read_to_string(dir.join("metadata/version-hint.text"))
            .unwrap()
            .trim(),
        "2"
    );
    assert_eq!(inspect_iceberg(&url).unwrap().total_records, 3);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn append_evolves_schema_object_store() {
    let dir = std::env::temp_dir().join(format!("ss-ice-obj-ev-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let url = format!("file://{}", dir.display());

    // Fresh two-column table.
    let (schema, b1) = batch(&[1, 2]);
    let mut s1 =
        ObjectStoreIcebergSink::create(&url, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&b1).unwrap();
    s1.finish().unwrap();

    // Append with one new OPTIONAL column — additive evolution through the object
    // store: existing columns keep their field-ids, `note` is assigned the next one.
    let mut wide = cols();
    wide.push(ColumnSpec {
        name: "note".into(),
        from: None,
        ty: ColumnType::String,
        transform: None,
        required: false,
    });
    let mut sh = Shaper::new(wide, false).unwrap();
    sh.push(&json!({"id": 3, "amt": 3.0, "note": "evolved"}))
        .unwrap();
    let b2 = sh.flush().unwrap().unwrap();
    let mut s2 =
        ObjectStoreIcebergSink::create(&url, sh.schema(), Compression::Snappy, true, &[]).unwrap();
    s2.write_batch(&b2).unwrap();
    s2.finish().unwrap();

    // Version bumped; the read-back current schema is the evolved three-column one.
    let info = inspect_iceberg(&url).unwrap();
    assert_eq!(info.total_records, 3);
    assert_eq!(info.fields.len(), 3);

    // The v2 metadata keeps BOTH schemas and stable ids: id/amt keep 1/2, note gets 3.
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v2.metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["schemas"].as_array().unwrap().len(), 2);
    assert_eq!(meta["current-schema-id"], 1);
    assert_eq!(meta["last-column-id"], 3);
    let current = &meta["schemas"][1]["fields"];
    assert_eq!(current[0]["name"], "id");
    assert_eq!(current[0]["id"], 1);
    assert_eq!(current[2]["name"], "note");
    assert_eq!(current[2]["id"], 3);
    assert_eq!(current[2]["required"], false);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn append_with_mismatched_schema_is_rejected_object_store() {
    let dir = std::env::temp_dir().join(format!("ss-ice-obj-mm-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let url = format!("file://{}", dir.display());

    // Fresh two-column table.
    let (schema, b1) = batch(&[1]);
    let mut s1 =
        ObjectStoreIcebergSink::create(&url, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&b1).unwrap();
    s1.finish().unwrap();

    // A one-column append is refused at create() (fail-fast), before any data is staged or
    // uploaded — the object-store table keeps only its original data file.
    let one_col = vec![ColumnSpec {
        name: "id".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: true,
    }];
    let sh = Shaper::new(one_col, false).unwrap();
    let err = ObjectStoreIcebergSink::create(&url, sh.schema(), Compression::Snappy, true, &[]);
    let msg = format!("{}", err.err().expect("mismatched append must be rejected"));
    assert!(
        msg.contains("dropping or renaming"),
        "unexpected error: {msg}"
    );

    // Untouched: still version 1, still exactly one data file (no orphan uploaded).
    assert_eq!(
        std::fs::read_to_string(dir.join("metadata/version-hint.text"))
            .unwrap()
            .trim(),
        "1"
    );
    assert_eq!(
        std::fs::read_dir(dir.join("data")).unwrap().count(),
        1,
        "the rejected append uploaded no orphaned data file"
    );

    std::fs::remove_dir_all(&dir).ok();
}
