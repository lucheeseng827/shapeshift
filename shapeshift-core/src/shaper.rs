//! The shaper: the hot loop that turns JSON records into Arrow RecordBatches.
//!
//! It holds one typed Arrow builder per output column and appends **transactionally
//! per row** — every cell for a record is computed and validated first, so a
//! rejected row never leaves the column builders misaligned. Rows accumulate until
//! `row_group_rows`, then `flush` emits a RecordBatch and the builders reset,
//! bounding RAM regardless of input size.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use serde_json::Value;

use crate::error::{Result, ShapeError};
use crate::sink::Sink;
use crate::spec::{ColumnSpec, DatasetSpec, SchemaMode};
use crate::transform::Transform;
use crate::types::ColumnType;
use crate::value::select;

/// A column compiled from a [`ColumnSpec`] into the exact work the hot loop does.
#[derive(Debug, Clone)]
struct Compiled {
    name: String,
    path: String,
    ty: ColumnType,
    transform: Option<Transform>,
    required: bool,
}

/// One typed cell, produced by coercion, ready to append to the matching builder.
enum Cell {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Date(i32),
    Ts(i64),
    Str(String),
}

/// The result of coercing one selected value.
enum Coerced {
    Cell(Cell),
    /// The value was absent or JSON null.
    Null,
    /// The value was present but not coercible to the target type.
    Fail(String),
}

/// A per-column Arrow builder.
enum ColBuilder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Str(StringBuilder),
    Date(Date32Builder),
    Ts(TimestampMicrosecondBuilder),
}

impl ColBuilder {
    fn for_type(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Bool => ColBuilder::Bool(BooleanBuilder::new()),
            ColumnType::Int64 => ColBuilder::Int(Int64Builder::new()),
            ColumnType::Float64 => ColBuilder::Float(Float64Builder::new()),
            ColumnType::String | ColumnType::Json => ColBuilder::Str(StringBuilder::new()),
            ColumnType::Date => ColBuilder::Date(Date32Builder::new()),
            ColumnType::Timestamp => ColBuilder::Ts(TimestampMicrosecondBuilder::new()),
        }
    }

    fn append(&mut self, cell: Cell) {
        match (self, cell) {
            (ColBuilder::Bool(b), Cell::Bool(v)) => b.append_value(v),
            (ColBuilder::Bool(b), _) => b.append_null(),
            (ColBuilder::Int(b), Cell::I64(v)) => b.append_value(v),
            (ColBuilder::Int(b), _) => b.append_null(),
            (ColBuilder::Float(b), Cell::F64(v)) => b.append_value(v),
            (ColBuilder::Float(b), _) => b.append_null(),
            (ColBuilder::Str(b), Cell::Str(v)) => b.append_value(v),
            (ColBuilder::Str(b), _) => b.append_null(),
            (ColBuilder::Date(b), Cell::Date(v)) => b.append_value(v),
            (ColBuilder::Date(b), _) => b.append_null(),
            (ColBuilder::Ts(b), Cell::Ts(v)) => b.append_value(v),
            (ColBuilder::Ts(b), _) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Bool(b) => Arc::new(b.finish()),
            ColBuilder::Int(b) => Arc::new(b.finish()),
            ColBuilder::Float(b) => Arc::new(b.finish()),
            ColBuilder::Str(b) => Arc::new(b.finish()),
            ColBuilder::Date(b) => Arc::new(b.finish()),
            ColBuilder::Ts(b) => Arc::new(b.finish()),
        }
    }
}

/// What happened to one pushed record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    /// The row was appended to the current batch.
    Appended,
    /// The row was dropped (lenient policy). Carries the reason.
    Rejected(String),
}

/// The shaping engine over a fixed output schema.
pub struct Shaper {
    columns: Vec<Compiled>,
    schema: SchemaRef,
    builders: Vec<ColBuilder>,
    pending: usize,
    strict: bool,
}

impl Shaper {
    /// Build a shaper for the given effective columns and schema mode.
    pub fn new(columns: Vec<ColumnSpec>, strict: bool) -> Result<Self> {
        if columns.is_empty() {
            return Err(ShapeError::Schema("no output columns".into()));
        }
        let compiled: Vec<Compiled> = columns
            .iter()
            .map(|c| Compiled {
                name: c.name.clone(),
                path: c.source_path().to_string(),
                ty: c.ty,
                transform: c.transform,
                required: c.required,
            })
            .collect();
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.ty.arrow_type(), !c.required))
            .collect();
        let builders = columns.iter().map(|c| ColBuilder::for_type(c.ty)).collect();
        Ok(Shaper {
            columns: compiled,
            schema: Arc::new(Schema::new(fields)),
            builders,
            pending: 0,
            strict,
        })
    }

    /// Build a shaper directly from a spec plus an inferred column list (empty in
    /// strict mode). Declared columns win over inferred ones of the same name.
    pub fn from_spec(spec: &DatasetSpec, inferred: &[ColumnSpec]) -> Result<Self> {
        let cols = effective_columns(spec, inferred);
        Shaper::new(cols, spec.schema == SchemaMode::Strict)
    }

    /// The Arrow schema every emitted batch carries.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Rows buffered but not yet flushed.
    pub fn pending(&self) -> usize {
        self.pending
    }

    /// Shape one record into the current batch.
    pub fn push(&mut self, record: &Value) -> Result<PushOutcome> {
        // Phase 1: compute every cell, bailing before any append if a required cell
        // is absent / a strict coercion fails. This keeps the builders aligned.
        let mut cells: Vec<Cell> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let selected = select(record, &col.path).cloned();
            let coerced = match selected {
                None | Some(Value::Null) => Coerced::Null,
                Some(v) => {
                    let v = match col.transform {
                        Some(t) => t.apply(v),
                        None => v,
                    };
                    if matches!(v, Value::Null) {
                        Coerced::Null
                    } else {
                        coerce(v, col.ty)
                    }
                }
            };
            match coerced {
                Coerced::Cell(c) => cells.push(c),
                Coerced::Null => {
                    if col.required {
                        let reason = format!("required column `{}` is null/missing", col.name);
                        if self.strict {
                            return Err(ShapeError::Record(reason));
                        }
                        return Ok(PushOutcome::Rejected(reason));
                    }
                    cells.push(Cell::Null);
                }
                Coerced::Fail(why) => {
                    let reason = format!("column `{}`: {why}", col.name);
                    if self.strict {
                        return Err(ShapeError::Record(reason));
                    }
                    if col.required {
                        return Ok(PushOutcome::Rejected(reason));
                    }
                    // Lenient, optional column: a bad value is written as null.
                    cells.push(Cell::Null);
                }
            }
        }
        // Phase 2: commit.
        for (builder, cell) in self.builders.iter_mut().zip(cells) {
            builder.append(cell);
        }
        self.pending += 1;
        Ok(PushOutcome::Appended)
    }

    /// Emit the buffered rows as a RecordBatch and reset the builders. Returns
    /// `None` when nothing is pending.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending == 0 {
            return Ok(None);
        }
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        self.pending = 0;
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        Ok(Some(batch))
    }
}

/// Merge declared and inferred columns into the effective output column list.
/// Strict mode uses declared columns only; infer mode appends inferred columns not
/// already declared (declared columns keep their position and win on name).
pub fn effective_columns(spec: &DatasetSpec, inferred: &[ColumnSpec]) -> Vec<ColumnSpec> {
    match spec.schema {
        SchemaMode::Strict => spec.columns.clone(),
        SchemaMode::Infer => {
            let mut cols = spec.columns.clone();
            let declared: std::collections::HashSet<&str> =
                spec.columns.iter().map(|c| c.name.as_str()).collect();
            for c in inferred {
                if !declared.contains(c.name.as_str()) {
                    cols.push(c.clone());
                }
            }
            cols
        }
    }
}

/// Aggregate result of a full pipeline run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunReport {
    /// Records read from the source (excluding source parse errors handled upstream).
    pub rows_in: u64,
    /// Rows written to the sink.
    pub rows_out: u64,
    /// Rows dropped by a lenient reject.
    pub rows_rejected: u64,
    /// RecordBatches (row groups) flushed.
    pub batches: u64,
}

/// Drive a whole shape end to end: pull records, shape them, flush at row-group
/// boundaries, and finalize the sink. The convenience entry point for embedders;
/// the CLI uses the lower-level [`Shaper`] API so it can sidecar source parse
/// errors.
pub fn run_pipeline<I, S>(
    mut shaper: Shaper,
    source: I,
    sink: &mut S,
    row_group_rows: usize,
) -> Result<(RunReport, crate::sink::SinkSummary)>
where
    I: IntoIterator<Item = Value>,
    S: Sink,
{
    let rg = row_group_rows.max(1);
    let mut report = RunReport::default();
    for record in source {
        report.rows_in += 1;
        match shaper.push(&record)? {
            PushOutcome::Appended => report.rows_out += 1,
            PushOutcome::Rejected(_) => report.rows_rejected += 1,
        }
        if shaper.pending() >= rg {
            if let Some(batch) = shaper.flush()? {
                sink.write_batch(&batch)?;
                report.batches += 1;
            }
        }
    }
    if let Some(batch) = shaper.flush()? {
        sink.write_batch(&batch)?;
        report.batches += 1;
    }
    let summary = sink.finish()?;
    Ok((report, summary))
}

// ---------------------------------------------------------------------------
// Coercion
// ---------------------------------------------------------------------------

fn coerce(v: Value, ty: ColumnType) -> Coerced {
    match ty {
        ColumnType::Bool => match v {
            Value::Bool(b) => Coerced::Cell(Cell::Bool(b)),
            other => Coerced::Fail(format!("expected bool, got {}", kind(&other))),
        },
        ColumnType::Int64 => match &v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Coerced::Cell(Cell::I64(i))
                } else if let Some(f) = n.as_f64() {
                    // `f as i64` saturates silently on overflow, so bound-check first.
                    // `i64::MIN as f64` is exactly -2^63; its negation is exactly 2^63
                    // (= i64::MAX + 1), the first value that would saturate.
                    const MIN: f64 = i64::MIN as f64;
                    const MAX: f64 = -(i64::MIN as f64);
                    if f.fract() != 0.0 || !f.is_finite() {
                        Coerced::Fail(format!("non-integer number {f} for int64"))
                    } else if (MIN..MAX).contains(&f) {
                        Coerced::Cell(Cell::I64(f as i64))
                    } else {
                        Coerced::Fail(format!("number {f} out of i64 range"))
                    }
                } else {
                    Coerced::Fail("number out of i64 range".into())
                }
            }
            Value::String(s) => match s.parse::<i64>() {
                Ok(i) => Coerced::Cell(Cell::I64(i)),
                Err(_) => Coerced::Fail(format!("string {s:?} is not an int64")),
            },
            other => Coerced::Fail(format!("expected int64, got {}", kind(other))),
        },
        ColumnType::Float64 => match &v {
            Value::Number(n) => match n.as_f64() {
                Some(f) => Coerced::Cell(Cell::F64(f)),
                None => Coerced::Fail("number not representable as f64".into()),
            },
            Value::String(s) => match s.parse::<f64>() {
                Ok(f) => Coerced::Cell(Cell::F64(f)),
                Err(_) => Coerced::Fail(format!("string {s:?} is not a float64")),
            },
            other => Coerced::Fail(format!("expected float64, got {}", kind(other))),
        },
        ColumnType::String => match v {
            Value::String(s) => Coerced::Cell(Cell::Str(s)),
            Value::Bool(b) => Coerced::Cell(Cell::Str(b.to_string())),
            Value::Number(n) => Coerced::Cell(Cell::Str(n.to_string())),
            other => Coerced::Fail(format!(
                "expected string scalar, got {} (use type `json` to keep it)",
                kind(&other)
            )),
        },
        ColumnType::Json => match v {
            Value::String(s) => Coerced::Cell(Cell::Str(s)),
            other => Coerced::Cell(Cell::Str(other.to_string())),
        },
        ColumnType::Date => match &v {
            Value::String(s) => match parse_date_days(s) {
                Some(d) => Coerced::Cell(Cell::Date(d)),
                None => Coerced::Fail(format!("string {s:?} is not a YYYY-MM-DD / RFC3339 date")),
            },
            // A bare integer is read as epoch-days (Arrow Date32 semantics).
            Value::Number(n) => match n.as_i64() {
                Some(i) if i >= i32::MIN as i64 && i <= i32::MAX as i64 => {
                    Coerced::Cell(Cell::Date(i as i32))
                }
                _ => Coerced::Fail("integer out of Date32 (epoch-day) range".into()),
            },
            other => Coerced::Fail(format!("expected date, got {}", kind(other))),
        },
        ColumnType::Timestamp => match &v {
            Value::String(s) => match parse_ts_micros(s) {
                Some(t) => Coerced::Cell(Cell::Ts(t)),
                None => Coerced::Fail(format!("string {s:?} is not an RFC3339 timestamp")),
            },
            // A bare integer is read as epoch-milliseconds → microseconds.
            Value::Number(n) => match n.as_i64() {
                Some(ms) => match ms.checked_mul(1000) {
                    Some(us) => Coerced::Cell(Cell::Ts(us)),
                    None => Coerced::Fail("epoch-ms overflows i64 microseconds".into()),
                },
                None => Coerced::Fail("timestamp number not an integer epoch-ms".into()),
            },
            other => Coerced::Fail(format!("expected timestamp, got {}", kind(other))),
        },
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Parse a `YYYY-MM-DD` (or the date part of an RFC3339 string) into days since the
/// Unix epoch.
fn parse_date_days(s: &str) -> Option<i32> {
    let date = if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        d
    } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        dt.date_naive()
    } else {
        return None;
    };
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((date - epoch).num_days() as i32)
}

/// Parse an RFC3339 timestamp (offset required), or a zone-less
/// `YYYY-MM-DDThh:mm:ss[.fff]` read as UTC, into microseconds since the epoch.
fn parse_ts_micros(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ndt.and_utc().timestamp_micros());
        }
    }
    None
}
