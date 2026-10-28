//! Merge through the object-store sink (a `file://` URL, which runs the same code an
//! `s3://` table does): the same deltas merged into a local table and into an
//! object-store table must leave the same live rows and report the same merge.
//! The S3 protocol itself — ranged GETs, multipart uploads — is exercised by
//! `examples/merge/verify_duckdb.py --tables s3://…` against an S3-compatible server.

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, MergeSummary, Shaper, Sink};
use shapeshift_iceberg::{current_scan_files, inspect, IcebergSink};
use shapeshift_objstore::ObjectStoreIcebergSink;

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ss-obj-merge-{}-{}", std::process::id(), name));
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
            col("v", ColumnType::Int64),
        ],
        false,
    )
    .unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    (sh.schema(), sh.flush().unwrap().unwrap())
}

fn row(id: i64, region: &str, v: i64) -> Value {
    json!({ "id": id, "region": region, "v": v })
}

/// The live rows of a table on local disk, as `(id, region, v)`, sorted.
fn live(dir: &std::path::Path) -> Vec<(Option<i64>, String, i64)> {
    let mut out = Vec::new();
    for f in current_scan_files(dir).unwrap() {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&f.path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let mut pos = 0u64;
        for b in reader {
            let b = b.unwrap();
            let id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let region = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            let v = b
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                if f.deleted_positions.binary_search(&pos).is_err() {
                    out.push((
                        (!id.is_null(i)).then(|| id.value(i)),
                        region.value(i).to_string(),
                        v.value(i),
                    ));
                }
                pos += 1;
            }
        }
    }
    out.sort();
    out
}

const PARTS: &[&str] = &["region"];

fn deltas() -> Vec<Vec<Value>> {
    vec![
        vec![row(1, "eu", 1), row(2, "us", 2), row(3, "eu", 3)],
        // id 1 moves eu → us, id 2 is updated twice in one write, one row has no key.
        vec![
            row(1, "us", 10),
            row(2, "us", 20),
            row(2, "us", 21),
            json!({ "region": "ap", "v": 99 }),
        ],
        // id 2 again (its previous version already superseded a deleted row), and a new id.
        vec![row(2, "eu", 22), row(4, "ap", 40)],
    ]
}

fn merge_local(dir: &std::path::Path, rows: &[Value]) -> MergeSummary {
    let (schema, b) = batch(rows);
    let parts: Vec<String> = PARTS.iter().map(|s| s.to_string()).collect();
    let key = ["id".to_string()];
    let mut s = IcebergSink::create_merge(dir, schema, Compression::Snappy, &parts, &key).unwrap();
    s.write_batch(&b).unwrap();
    s.finish().unwrap().merge.unwrap()
}

fn merge_url(url: &str, rows: &[Value]) -> MergeSummary {
    let (schema, b) = batch(rows);
    let parts: Vec<String> = PARTS.iter().map(|s| s.to_string()).collect();
    let key = ["id".to_string()];
    let mut s =
        ObjectStoreIcebergSink::create_merge(url, schema, Compression::Snappy, &parts, &key)
            .unwrap();
    s.write_batch(&b).unwrap();
    s.finish().unwrap().merge.unwrap()
}

#[test]
fn an_object_store_merge_leaves_what_a_local_merge_does() {
    let local = tmp_dir("local");
    let remote = tmp_dir("remote");
    let url = format!("file://{}", remote.display());

    for (round, rows) in deltas().iter().enumerate() {
        let a = merge_local(&local, rows);
        let b = merge_url(&url, rows);
        assert_eq!(
            a, b,
            "round {round}: the two sinks reported different merges"
        );
        assert_eq!(
            live(&local),
            live(&remote),
            "round {round}: different live rows"
        );
    }
    assert_eq!(
        live(&remote),
        vec![
            (None, "ap".into(), 99),
            (Some(1), "us".into(), 10),
            (Some(2), "eu".into(), 22),
            (Some(3), "eu".into(), 3),
            (Some(4), "ap".into(), 40),
        ]
    );
    let info = shapeshift_objstore::inspect_iceberg(&url).unwrap();
    assert_eq!(info.live_records(), 5);
    assert_eq!(
        info.total_position_deletes,
        inspect(&local).unwrap().total_position_deletes
    );

    // The delete files landed in the store under their partition, beside the data.
    let mut deletes = Vec::new();
    for e in walk(&remote) {
        if e.contains("/delete-") {
            deletes.push(e);
        }
    }
    assert!(!deletes.is_empty());
    assert!(
        deletes.iter().all(|p| p.contains("/data/region=")),
        "{deletes:?}"
    );

    std::fs::remove_dir_all(&local).ok();
    std::fs::remove_dir_all(&remote).ok();
}

#[test]
fn a_bad_merge_key_is_refused_before_anything_is_staged_or_uploaded() {
    let remote = tmp_dir("badkey");
    let url = format!("file://{}", remote.display());
    let (schema, _) = batch(&[row(1, "eu", 1)]);
    let err = ObjectStoreIcebergSink::create_merge(
        &url,
        schema,
        Compression::Snappy,
        &[],
        &["nope".to_string()],
    )
    .err()
    .expect("an unknown key column must be refused")
    .to_string();
    assert!(err.contains("not in the schema"), "{err}");
    assert!(!remote.exists(), "nothing was uploaded");
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
