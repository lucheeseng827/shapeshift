//! The declarative **dataset transform spec** — shapeshift's answer to
//! per-row (MAR) billing. One small YAML/JSON document describes how a stream of
//! JSON records becomes one columnar table: which fields to select, what logical
//! type each becomes, which named transform to apply, and where/how to write it.
//!
//! The spec is intentionally *dataset-scoped*, not row-scoped: you pay the cost of
//! writing it once per source shape, then shape unbounded rows for free on your own
//! hardware. It is the whole product boundary — everything else is an engine that
//! executes this document.

use serde::{Deserialize, Serialize};

use crate::transform::Transform;
use crate::types::ColumnType;

/// A complete dataset shaping spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatasetSpec {
    /// Logical table name (used for the Iceberg table dir / Parquet file stem when
    /// the output path is a directory).
    pub dataset: String,

    /// How to read the input.
    #[serde(default)]
    pub source: SourceSpec,

    /// How/where to write the output.
    pub output: OutputSpec,

    /// Schema handling. `infer` (default) lets undeclared columns ride along;
    /// `strict` writes *only* declared columns and errors on a required miss.
    #[serde(default)]
    pub schema: SchemaMode,

    /// Explicit column declarations. In `infer` mode these override/augment the
    /// inferred schema; in `strict` mode they *are* the schema.
    #[serde(default)]
    pub columns: Vec<ColumnSpec>,

    /// Dataset-wide shaping options.
    #[serde(default)]
    pub options: Options,
}

/// Input description.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceSpec {
    /// `jsonl` (one JSON object per line, the default) or `json-array` (a single
    /// top-level array).
    #[serde(default)]
    pub format: SourceFormat,

    /// Optional input path. The CLI's `--input` overrides this; leaving it unset
    /// means "read from the path passed on the command line / stdin".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl Default for SourceSpec {
    fn default() -> Self {
        SourceSpec {
            format: SourceFormat::Jsonl,
            path: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SourceFormat {
    #[default]
    Jsonl,
    JsonArray,
}

/// Output description.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputSpec {
    /// `parquet` (a single file, or `<path>/<dataset>.parquet` when `path` is a
    /// directory) or `iceberg` (an Iceberg v2 table rooted at `path`).
    pub format: OutputFormat,

    /// Destination path. For `parquet`, a file or directory; for `iceberg`, the
    /// table root directory.
    pub path: String,

    /// Parquet page compression. Snappy by default (musl-static, no C codecs).
    #[serde(default)]
    pub compression: Compression,

    /// Iceberg partition fields: identity columns (`region`) and/or hidden-transform
    /// expressions — `bucket(N, col)`, `truncate(W, col)`, `year|month|day|hour(col)`
    /// (see [`partition_source_column`] for how validation reads the source column).
    /// For Parquet output this is recorded in the spec but a single file is still
    /// written — Hive-style directory partitioning for Parquet is on the roadmap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OutputFormat {
    Parquet,
    Iceberg,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Compression {
    #[default]
    Snappy,
    Zstd,
    Uncompressed,
}

/// Schema strictness.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SchemaMode {
    /// Infer the schema from a sample and let undeclared columns through
    /// (declarations refine it). The forgiving default for messy sources.
    #[default]
    Infer,
    /// Only declared columns are written; a required column that is missing/null
    /// aborts the run. The contract mode for downstream stability.
    Strict,
}

/// One output column: where it comes from, what it becomes, and how it is
/// transformed on the way.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnSpec {
    /// Output column name.
    pub name: String,

    /// Source path into each record. Dotted (`user.name`), optionally `$`-prefixed
    /// (`$.user.name`); numeric segments index into arrays (`tags.0`). Defaults to
    /// `name` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,

    /// Target logical type.
    #[serde(rename = "type")]
    pub ty: ColumnType,

    /// Optional named transform applied to the selected value before coercion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<Transform>,

    /// When true, a missing/null value rejects the row (lenient) or aborts
    /// (strict). When false (default), it is written as null.
    #[serde(default)]
    pub required: bool,
}

impl ColumnSpec {
    /// The effective source path (`from` or, failing that, `name`).
    pub fn source_path(&self) -> &str {
        self.from.as_deref().unwrap_or(&self.name)
    }
}

/// Dataset-wide shaping options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Options {
    /// Flatten nested objects into dotted column names during inference
    /// (`{"user":{"name":..}}` → `user.name`). Arrays become JSON-encoded string
    /// leaves. Applies to inference only; explicit columns always win.
    #[serde(default = "default_true")]
    pub flatten: bool,

    /// Rows to buffer before flushing a RecordBatch to the sink. Bounds RAM — a
    /// 100M-row shape never holds more than this many rows at once.
    #[serde(default = "default_row_group")]
    pub row_group_rows: usize,

    /// Records to sample when inferring the schema (0 = scan the whole input).
    #[serde(default = "default_sample")]
    pub infer_sample: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            flatten: true,
            row_group_rows: default_row_group(),
            infer_sample: default_sample(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_row_group() -> usize {
    50_000
}
fn default_sample() -> usize {
    1_000
}

impl DatasetSpec {
    /// Parse a spec from YAML or JSON (auto-detected: a leading `{` is JSON).
    pub fn parse(text: &str) -> crate::error::Result<Self> {
        let trimmed = text.trim_start();
        let spec: DatasetSpec = if trimmed.starts_with('{') {
            serde_json::from_str(text).map_err(|e| crate::error::ShapeError::Spec(e.to_string()))?
        } else {
            serde_yaml::from_str(text).map_err(|e| crate::error::ShapeError::Spec(e.to_string()))?
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Serialize to YAML (the canonical on-disk form).
    pub fn to_yaml(&self) -> crate::error::Result<String> {
        serde_yaml::to_string(self).map_err(|e| crate::error::ShapeError::Spec(e.to_string()))
    }

    /// Structural checks beyond serde's shape validation.
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::ShapeError;
        if self.dataset.trim().is_empty() {
            return Err(ShapeError::Spec("`dataset` must not be empty".into()));
        }
        if self.schema == SchemaMode::Strict && self.columns.is_empty() {
            return Err(ShapeError::Spec(
                "strict schema mode requires at least one declared column".into(),
            ));
        }
        // Duplicate output column names are a hard error — they would collide in the
        // Arrow schema.
        let mut seen = std::collections::HashSet::new();
        for c in &self.columns {
            if !seen.insert(&c.name) {
                return Err(ShapeError::Spec(format!(
                    "duplicate column name `{}`",
                    c.name
                )));
            }
        }
        // In strict schema mode, partition columns must be among the declared columns.
        // In infer mode `columns` may be only a partial pin-list (the rest ride along from
        // the inferred schema), so partition columns are resolved against the full inferred
        // schema by the sink instead — don't reject them here. A `partition_by` entry may
        // be a transform expression (`day(event_at)`, `bucket(16, id)`); the declared-column
        // check applies to its SOURCE column — the Iceberg sink validates the transform
        // itself (name, arity, type applicability).
        if self.schema == SchemaMode::Strict {
            for p in &self.output.partition_by {
                let col = partition_source_column(p);
                if !self.columns.iter().any(|c| c.name == col) {
                    return Err(ShapeError::Spec(format!(
                        "partition column `{col}` is not a declared column (declare it to partition by it)"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The source column named by a `partition_by` entry — the entry itself for identity
/// (`region`), or the last parenthesized argument for a transform expression
/// (`day(event_at)` → `event_at`, `bucket(16, id)` → `id`). Full transform parsing
/// (name, arity, type applicability) is the Iceberg sink's job; this is just enough
/// for spec validation to know which column is being partitioned on.
pub fn partition_source_column(expr: &str) -> &str {
    match (expr.find('('), expr.rfind(')')) {
        (Some(o), Some(c)) if c > o => expr[o + 1..c].rsplit(',').next().unwrap_or("").trim(),
        _ => expr.trim(),
    }
}
