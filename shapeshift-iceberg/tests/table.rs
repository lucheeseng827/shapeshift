//! Write an Iceberg v2 table and assert its structure: the layout on disk, the
//! metadata read-back, the field-id-carrying data Parquet, and (structurally) the
//! manifest chain.

use serde_json::json;

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_iceberg::{inspect, IcebergSink};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ss-ice-{}-{}", std::process::id(), name))
}

fn build_batch() -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let cols = vec![
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
            name: "day".into(),
            from: None,
            ty: ColumnType::Date,
            transform: None,
            required: false,
        },
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 1, "amt": 1.5, "day": "2026-01-01"}))
        .unwrap();
    sh.push(&json!({"id": 2, "amt": 2.5, "day": "2026-01-02"}))
        .unwrap();
    sh.push(&json!({"id": 3, "amt": 3.5, "day": "2026-01-03"}))
        .unwrap();
    let schema = sh.schema();
    let batch = sh.flush().unwrap().unwrap();
    (schema, batch)
}

/// A batch with the columns `id` (required) + `amt`, one row per id.
fn batch_ids(ids: &[i64]) -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let cols = vec![
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
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    for &id in ids {
        sh.push(&json!({ "id": id, "amt": id as f64 })).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

fn version_hint(dir: &std::path::Path) -> String {
    std::fs::read_to_string(dir.join("metadata/version-hint.text"))
        .unwrap()
        .trim()
        .to_string()
}

#[test]
fn append_adds_a_snapshot_and_keeps_all_rows() {
    let dir = tmp_dir("append");
    let _ = std::fs::remove_dir_all(&dir);

    // Snapshot 1: two rows, fresh table.
    let (schema, batch) = batch_ids(&[1, 2]);
    let mut s1 = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&batch).unwrap();
    assert_eq!(s1.finish().unwrap().rows, 2);
    assert_eq!(version_hint(&dir), "1");

    // Snapshot 2: three more rows, appended onto the existing table.
    let (schema, batch) = batch_ids(&[3, 4, 5]);
    let mut s2 = IcebergSink::create(&dir, schema, Compression::Snappy, true, &[]).unwrap();
    s2.write_batch(&batch).unwrap();
    assert_eq!(s2.finish().unwrap().rows, 3);

    // Version bumped to 2; both data files kept.
    assert_eq!(version_hint(&dir), "2");
    assert!(dir.join("metadata/v2.metadata.json").exists());
    let data = std::fs::read_dir(dir.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "parquet")
                .unwrap_or(false)
        })
        .count();
    assert_eq!(data, 2, "both snapshots' data files remain");

    // The table now reports all five rows across two chained snapshots.
    assert_eq!(inspect(&dir).unwrap().total_records, 5);
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v2.metadata.json")).unwrap(),
    )
    .unwrap();
    let snaps = meta["snapshots"].as_array().unwrap();
    assert_eq!(snaps.len(), 2);
    assert_eq!(
        snaps[1]["parent-snapshot-id"], snaps[0]["snapshot-id"],
        "the new snapshot chains onto the first"
    );
    assert_eq!(meta["last-sequence-number"], 2);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn append_with_mismatched_schema_is_rejected() {
    let dir = tmp_dir("append-mismatch");
    let _ = std::fs::remove_dir_all(&dir);

    // A two-column table.
    let (schema, batch) = batch_ids(&[1]);
    let mut s1 = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&batch).unwrap();
    s1.finish().unwrap();

    // Appending a one-column batch DROPS the table's `amt` column — evolution is
    // additive only, so this must be refused up front — at create(), before any data
    // file is written — so it neither corrupts the table nor leaves an orphaned
    // Parquet behind.
    let cols = vec![ColumnSpec {
        name: "id".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: true,
    }];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({ "id": 2 })).unwrap();
    let err = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]);
    let msg = format!("{}", err.err().expect("mismatched append must be rejected"));
    assert!(
        msg.contains("dropping or renaming"),
        "unexpected error: {msg}"
    );

    // The table is untouched: still version 1, and no orphaned data file was written.
    assert_eq!(version_hint(&dir), "1");
    assert_eq!(
        walk_parquet(&dir.join("data")).len(),
        1,
        "the rejected append left no orphaned data file"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn append_evolves_schema_by_adding_optional_columns() {
    let dir = tmp_dir("append-evolve");
    let _ = std::fs::remove_dir_all(&dir);

    // Snapshot 1: a two-column table {id, amt} (field-ids 1, 2).
    let (schema, batch) = batch_ids(&[1, 2]);
    let mut s1 = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&batch).unwrap();
    s1.finish().unwrap();

    // Snapshot 2 adds an optional `region` column — additive evolution.
    let cols = vec![
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
            name: "region".into(),
            from: None,
            ty: ColumnType::String,
            transform: None,
            required: false,
        },
    ];
    let mut sh = Shaper::new(cols.clone(), false).unwrap();
    sh.push(&json!({ "id": 3, "amt": 3.0, "region": "us" }))
        .unwrap();
    let batch = sh.flush().unwrap().unwrap();
    let mut s2 = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]).unwrap();
    s2.write_batch(&batch).unwrap();
    s2.finish().unwrap();

    // The metadata gained a NEW schema: existing columns keep ids 1 and 2, `region`
    // gets id 3, current-schema-id and last-column-id advance, and the old schema
    // stays in `schemas` so prior snapshots remain readable.
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v2.metadata.json")).unwrap(),
    )
    .unwrap();
    let schemas = meta["schemas"].as_array().unwrap();
    assert_eq!(schemas.len(), 2, "old schema kept + new schema added");
    let current_id = meta["current-schema-id"].as_i64().unwrap();
    let current = schemas
        .iter()
        .find(|s| s["schema-id"].as_i64() == Some(current_id))
        .unwrap();
    let fields = current["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 3);
    let by_name = |n: &str| {
        fields
            .iter()
            .find(|f| f["name"] == n)
            .unwrap_or_else(|| panic!("missing field {n}"))
    };
    assert_eq!(by_name("id")["id"], 1, "existing column keeps its field-id");
    assert_eq!(
        by_name("amt")["id"],
        2,
        "existing column keeps its field-id"
    );
    assert_eq!(by_name("region")["id"], 3, "new column gets the next id");
    assert_eq!(meta["last-column-id"], 3);
    assert_eq!(inspect(&dir).unwrap().total_records, 3);

    // Snapshot 3 with the SAME evolved schema does not add a redundant schema object.
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({ "id": 4, "amt": 4.0, "region": "eu" }))
        .unwrap();
    let batch = sh.flush().unwrap().unwrap();
    let mut s3 = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]).unwrap();
    s3.write_batch(&batch).unwrap();
    s3.finish().unwrap();
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v3.metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        meta["schemas"].as_array().unwrap().len(),
        2,
        "an unchanged schema is carried forward, not re-added"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn append_refuses_type_changes_and_new_required_columns() {
    let dir = tmp_dir("append-evolve-bad");
    let _ = std::fs::remove_dir_all(&dir);

    let (schema, batch) = batch_ids(&[1]);
    let mut s1 = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    s1.write_batch(&batch).unwrap();
    s1.finish().unwrap();

    // Type change: `amt` as string instead of float64.
    let cols = vec![
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
            ty: ColumnType::String,
            transform: None,
            required: false,
        },
    ];
    let sh = Shaper::new(cols, false).unwrap();
    let err = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]);
    let msg = format!("{}", err.err().expect("type change must be refused"));
    assert!(msg.contains("changed type"), "unexpected error: {msg}");

    // New REQUIRED column: refused (old rows would have no value for it).
    let cols = vec![
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
            name: "must_have".into(),
            from: None,
            ty: ColumnType::String,
            transform: None,
            required: true,
        },
    ];
    let sh = Shaper::new(cols, false).unwrap();
    let err = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]);
    let msg = format!(
        "{}",
        err.err().expect("new required column must be refused")
    );
    assert!(msg.contains("must be optional"), "unexpected error: {msg}");

    // The table is untouched in both cases.
    assert_eq!(version_hint(&dir), "1");
    std::fs::remove_dir_all(&dir).ok();
}

/// Collect every `.parquet` under `dir`, recursing into partition subdirectories.
fn walk_parquet(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().map(|x| x == "parquet").unwrap_or(false) {
                out.push(p);
            }
        }
    }
    out
}

#[test]
fn partitioned_table_writes_a_spec_and_a_file_per_partition() {
    let dir = tmp_dir("part-sink");
    let _ = std::fs::remove_dir_all(&dir);

    let cols = vec![
        ColumnSpec {
            name: "id".into(),
            from: None,
            ty: ColumnType::Int64,
            transform: None,
            required: true,
        },
        ColumnSpec {
            name: "region".into(),
            from: None,
            ty: ColumnType::String,
            transform: None,
            required: false,
        },
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({ "id": 1, "region": "us" })).unwrap();
    sh.push(&json!({ "id": 2, "region": "eu" })).unwrap();
    sh.push(&json!({ "id": 3, "region": "us" })).unwrap();
    let schema = sh.schema();
    let batch = sh.flush().unwrap().unwrap();

    let mut sink = IcebergSink::create(
        &dir,
        schema,
        Compression::Snappy,
        false,
        &["region".to_string()],
    )
    .unwrap();
    sink.write_batch(&batch).unwrap();
    assert_eq!(sink.finish().unwrap().rows, 3);

    // One data file per partition, under Hive-style `region=<value>` directories.
    let files = walk_parquet(&dir.join("data"));
    assert_eq!(files.len(), 2, "one file for `us`, one for `eu`");
    assert!(files
        .iter()
        .any(|p| p.to_string_lossy().contains("region=us")));
    assert!(files
        .iter()
        .any(|p| p.to_string_lossy().contains("region=eu")));

    // The metadata records a non-empty identity partition spec.
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v1.metadata.json")).unwrap(),
    )
    .unwrap();
    let spec = &meta["partition-specs"][0]["fields"];
    assert_eq!(spec[0]["name"], "region");
    assert_eq!(spec[0]["transform"], "identity");
    assert_eq!(spec[0]["field-id"], 1000);
    assert_eq!(inspect(&dir).unwrap().total_records, 3);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn hidden_partitioning_day_transform_end_to_end() {
    let dir = tmp_dir("part-day");
    let _ = std::fs::remove_dir_all(&dir);

    let cols = vec![
        ColumnSpec {
            name: "id".into(),
            from: None,
            ty: ColumnType::Int64,
            transform: None,
            required: true,
        },
        ColumnSpec {
            name: "event_at".into(),
            from: None,
            ty: ColumnType::Timestamp,
            transform: None,
            required: false,
        },
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    // Two rows on Jan 1 (different hours), one on Jan 2 — day() must group by day.
    sh.push(&json!({ "id": 1, "event_at": "2026-01-01T10:00:00Z" }))
        .unwrap();
    sh.push(&json!({ "id": 2, "event_at": "2026-01-01T23:59:59Z" }))
        .unwrap();
    sh.push(&json!({ "id": 3, "event_at": "2026-01-02T00:00:01Z" }))
        .unwrap();
    let schema = sh.schema();
    let batch = sh.flush().unwrap().unwrap();

    let mut sink = IcebergSink::create(
        &dir,
        schema,
        Compression::Snappy,
        false,
        &["day(event_at)".to_string()],
    )
    .unwrap();
    sink.write_batch(&batch).unwrap();
    assert_eq!(sink.finish().unwrap().rows, 3);

    // One file per DAY (not per timestamp), under human-readable directories.
    let files = walk_parquet(&dir.join("data"));
    assert_eq!(files.len(), 2, "one file for Jan 1, one for Jan 2");
    assert!(files
        .iter()
        .any(|p| p.to_string_lossy().contains("event_at_day=2026-01-01")));
    assert!(files
        .iter()
        .any(|p| p.to_string_lossy().contains("event_at_day=2026-01-02")));

    // The spec records the hidden transform, not identity.
    let meta: serde_json::Value = serde_json::from_reader(
        std::fs::File::open(dir.join("metadata/v1.metadata.json")).unwrap(),
    )
    .unwrap();
    let spec = &meta["partition-specs"][0]["fields"];
    assert_eq!(spec[0]["name"], "event_at_day");
    assert_eq!(spec[0]["transform"], "day");
    assert_eq!(inspect(&dir).unwrap().total_records, 3);

    std::fs::remove_dir_all(&dir).ok();
}

/// Build a batch of `{id, region}` rows.
fn batch_id_region(
    rows: &[(i64, &str)],
) -> (shapeshift_core::SchemaRef, shapeshift_core::RecordBatch) {
    let cols = vec![
        ColumnSpec {
            name: "id".into(),
            from: None,
            ty: ColumnType::Int64,
            transform: None,
            required: true,
        },
        ColumnSpec {
            name: "region".into(),
            from: None,
            ty: ColumnType::String,
            transform: None,
            required: false,
        },
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    for &(id, region) in rows {
        sh.push(&json!({ "id": id, "region": region })).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

#[test]
fn append_onto_a_partitioned_table_keeps_all_rows_and_partitions() {
    let dir = tmp_dir("append-part");
    let _ = std::fs::remove_dir_all(&dir);
    let part = ["region".to_string()];

    // Snapshot 1: a partitioned table over regions us + eu.
    let (schema, batch) = batch_id_region(&[(1, "us"), (2, "eu")]);
    let mut s1 = IcebergSink::create(&dir, schema, Compression::Snappy, false, &part).unwrap();
    s1.write_batch(&batch).unwrap();
    assert_eq!(s1.finish().unwrap().rows, 2);
    assert_eq!(version_hint(&dir), "1");

    // Snapshot 2: append rows for an existing partition (us) and a brand-new one (ap).
    let (schema, batch) = batch_id_region(&[(3, "us"), (4, "ap")]);
    let mut s2 = IcebergSink::create(&dir, schema, Compression::Snappy, true, &part).unwrap();
    s2.write_batch(&batch).unwrap();
    assert_eq!(s2.finish().unwrap().rows, 2);
    assert_eq!(version_hint(&dir), "2");

    // All four rows are visible across the two chained snapshots.
    assert_eq!(inspect(&dir).unwrap().total_records, 4);

    // Every partition value has at least one file, including the one only the append added.
    let files = walk_parquet(&dir.join("data"));
    for region in ["region=us", "region=eu", "region=ap"] {
        assert!(
            files.iter().any(|p| p.to_string_lossy().contains(region)),
            "missing partition dir for {region}"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn writes_v2_table_layout_and_metadata() {
    let dir = tmp_dir("table");
    let _ = std::fs::remove_dir_all(&dir);
    let (schema, batch) = build_batch();

    let mut sink = IcebergSink::create(&dir, schema, Compression::Snappy, false, &[]).unwrap();
    sink.write_batch(&batch).unwrap();
    let summary = sink.finish().unwrap();
    assert_eq!(summary.rows, 3);

    // Layout: one data parquet, one manifest, one manifest list, metadata + hint.
    assert!(dir.join("metadata/v1.metadata.json").exists());
    assert!(dir.join("metadata/version-hint.text").exists());
    let data_files: Vec<_> = std::fs::read_dir(dir.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "parquet")
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(data_files.len(), 1);
    let avro_files: Vec<_> = std::fs::read_dir(dir.join("metadata"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "avro").unwrap_or(false))
        .collect();
    assert_eq!(avro_files.len(), 2, "one manifest + one manifest list");

    // Metadata read-back.
    let info = inspect(&dir).unwrap();
    assert_eq!(info.format_version, 2);
    assert_eq!(info.total_records, 3);
    assert!(info.current_snapshot_id > 0);
    let names: Vec<_> = info.fields.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["id", "amt", "day"]);
    assert_eq!(info.fields[0].1, "long");
    assert_eq!(info.fields[2].1, "date");

    // The manifest's embedded Avro schema must carry field-ids (the detail that
    // makes the table readable by an Iceberg engine).
    let manifest = avro_files
        .iter()
        .find(|e| e.path().to_string_lossy().contains("-m0.avro"))
        .unwrap();
    let bytes = std::fs::read(manifest.path()).unwrap();
    assert_eq!(&bytes[..4], b"Obj\x01");
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(1000)]);
    assert!(head.contains("field-id"));

    // The data Parquet carries PARQUET:field_id 1..=N.
    let data = &data_files[0].path();
    let file = std::fs::File::open(data).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let descr = builder.metadata().file_metadata().schema_descr();
    let ids: Vec<i32> = descr
        .columns()
        .iter()
        .map(|c| c.self_type().get_basic_info().id())
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);

    std::fs::remove_dir_all(&dir).ok();
}
