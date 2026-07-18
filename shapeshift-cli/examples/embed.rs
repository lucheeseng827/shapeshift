use serde_json::json;
use shapeshift_core::{infer_columns, run_pipeline, Compression, Shaper};
use shapeshift_parquet::ParquetSink;

fn main() -> shapeshift_core::Result<()> {
    let records = vec![
        json!({"id": 1, "user": {"name": "Ada"},   "amount": 12.5}),
        json!({"id": 2, "user": {"name": "Grace"}, "amount": 3.0}),
    ];
    let cols = infer_columns(records.iter(), true);
    let shaper = Shaper::new(cols, /* strict = */ false)?;
    let mut sink = ParquetSink::create("out.parquet", shaper.schema(), Compression::Snappy)?;
    let (report, summary) = run_pipeline(shaper, records, &mut sink, 50_000)?;
    println!("rows_out={} bytes={}", report.rows_out, summary.bytes);
    Ok(())
}
