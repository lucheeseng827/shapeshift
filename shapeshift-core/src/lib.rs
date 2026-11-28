//! # shapeshift-core — the JSON→columnar shaping engine
//!
//! shapeshift turns a stream of JSON/JSONL records into a columnar table
//! (Parquet, or an Apache Iceberg v2 table) under a small declarative **dataset
//! transform spec**. This crate is the engine: the spec model, schema inference,
//! the named transform library, and the [`Shaper`] hot loop that emits Arrow
//! RecordBatches. It carries **no source or sink I/O dependency** — the JSON reader
//! and the Parquet/Iceberg writers live in sibling crates and are driven through
//! the [`Sink`] trait and the plain `serde_json::Value` record model.
//!
//! The product boundary is the spec: you write one per source *shape*, then shape
//! unbounded rows on your own hardware — the answer to per-row (MAR) billing. See
//! [`mar`] for the cost arithmetic that makes that concrete.
//!
//! ## Shape a stream in a few lines
//!
//! ```
//! use serde_json::json;
//! use shapeshift_core::{infer_columns, PushOutcome, Shaper};
//!
//! let records = vec![
//!     json!({"id": 1, "user": {"name": "Ada"}, "amount": 12.5}),
//!     json!({"id": 2, "user": {"name": "Grace"}, "amount": 3.0}),
//! ];
//! // Infer a column list (flatten nested objects into dotted columns).
//! let cols = infer_columns(records.iter(), true);
//! let mut shaper = Shaper::new(cols, false).unwrap();
//! for r in &records {
//!     assert_eq!(shaper.push(r).unwrap(), PushOutcome::Appended);
//! }
//! let batch = shaper.flush().unwrap().unwrap();
//! assert_eq!(batch.num_rows(), 2);
//! assert!(batch.schema().column_with_name("user.name").is_some());
//! ```

mod changes;
mod drift;
mod error;
mod infer;
mod mar;
pub mod metrics;
mod position;
mod predicate;
mod pull;
mod record;
mod shaper;
mod sink;
mod spec;
mod transform;
mod types;
mod value;

#[cfg(test)]
mod tests;

pub use changes::{
    parse_duration_ms, ApplyMode, ChangeKind, ChangesPreset, ChangesSpec, SeedVersion,
    CHANGE_COLUMN, CHANGE_DELETE, CHANGE_UPSERT, OP_COLUMN,
};
pub use drift::{DriftPolicy, DriftReport, DriftSpec, NewField, TypeMismatch, MAX_TRACKED_PATHS};
pub use error::{Result, ShapeError};
pub use infer::{infer_columns, looks_like_date, looks_like_timestamp};
pub use mar::{estimate as estimate_mar, MarInputs, MarReport};
pub use position::{Bounds, Cursor, Position, Progress, Rules, Tracker, Verdict};
pub use predicate::{Predicate, Tested, MAX_WHERE_BYTES};
pub use pull::{
    check_source_host, cursor_marks, digest, duration_ms, mysql_identifier_ok, recorded_position,
    resume_from, DbKind, DbSourceSpec, HeaderEnv, HttpAuth, HttpFormat, HttpMethod, HttpPaging,
    HttpSourceSpec, Incremental, MysqlSourceSpec, PostgresSourceSpec, SoapSpec, SoapVersion,
    SortOrder, Source, DEFAULT_MAX_TIE,
};
pub use shaper::{
    effective_columns, run_pipeline, ChangeReport, FilterReport, PushOutcome, RunReport, Shaper,
};
pub use sink::{MergeSummary, Sink, SinkSummary};
pub use spec::{
    partition_source_column, CatalogSpec, ColumnSpec, Compression, CsvOptions, CsvTypes,
    DatasetSpec, Options, OutputFormat, OutputSpec, SchemaMode, SourceFormat, SourceSpec,
    XmlOptions,
};
pub use transform::Transform;
pub use types::ColumnType;
pub use value::{flatten_leaves, segments, select, MAX_PATH_SEGMENTS, MAX_RECORD_DEPTH};

// Re-export the Arrow types embedders need to work with emitted batches without
// pinning arrow themselves.
pub use arrow_array::RecordBatch;
pub use arrow_schema::{Schema, SchemaRef};
pub use record::{Kind, Record};
