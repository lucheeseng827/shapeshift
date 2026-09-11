//! The shaper: the hot loop that turns JSON records into Arrow RecordBatches.
//!
//! It holds one typed Arrow builder per output column and appends **transactionally
//! per row** — every cell for a record is computed and validated first, so a
//! rejected row never leaves the column builders misaligned. Rows accumulate until
//! `row_group_rows`, then `flush` emits a RecordBatch and the builders reset,
//! bounding RAM regardless of input size.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use serde_json::{Map, Value};

use crate::drift::{DriftPolicy, DriftReport, DriftSpec, DriftTracker};
use crate::error::{Result, ShapeError};
use crate::record::{Kind, Record};
use crate::sink::Sink;
use crate::spec::{ColumnSpec, DatasetSpec, SchemaMode};
use crate::transform::Transform;
use crate::types::ColumnType;
use crate::value::{compile_path, select_compiled, Segment};

/// A column compiled from a [`ColumnSpec`] into the exact work the hot loop does.
#[derive(Debug, Clone)]
struct Compiled {
    name: String,
    path: String,
    /// `path`, split once at build time. The hot loop selects through this, so a run
    /// never re-splits (or re-allocates) the path per row.
    segs: Vec<Segment>,
    ty: ColumnType,
    transform: Option<Transform>,
    required: bool,
}

/// One typed cell, produced by coercion, ready to append to the matching builder.
///
/// `Str` holds a [`Cow`] so the common case — a string/date/timestamp/json column read
/// straight out of the record — **borrows** the record's own bytes instead of cloning
/// them. Only a value the engine had to synthesize (a transform's output, a number
/// stringified, a nested value serialized) owns its storage.
enum Cell<'a> {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Date(i32),
    Ts(i64),
    Str(Cow<'a, str>),
}

/// The result of coercing one selected value.
enum Coerced<'a> {
    Cell(Cell<'a>),
    /// The value was absent or JSON null.
    Null,
    /// The value was present but not coercible to the target type. Carries the reason
    /// *and* the value back, so a lenient run can report (and rescue) what it dropped
    /// without the hot loop cloning every value on the off-chance it fails. The value is
    /// only materialized where it is actually reported.
    Fail(String, Value),
}

impl Coerced<'_> {
    /// Detach from the borrowed source. Used only on the transform path, where the
    /// coerced value points into a temporary the transform produced.
    fn into_owned(self) -> Coerced<'static> {
        match self {
            Coerced::Cell(c) => Coerced::Cell(match c {
                Cell::Str(s) => Cell::Str(Cow::Owned(s.into_owned())),
                Cell::Null => Cell::Null,
                Cell::Bool(v) => Cell::Bool(v),
                Cell::I64(v) => Cell::I64(v),
                Cell::F64(v) => Cell::F64(v),
                Cell::Date(v) => Cell::Date(v),
                Cell::Ts(v) => Cell::Ts(v),
            }),
            Coerced::Null => Coerced::Null,
            Coerced::Fail(why, raw) => Coerced::Fail(why, raw),
        }
    }
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

    fn append(&mut self, cell: Cell<'_>) {
        match (self, cell) {
            (ColBuilder::Bool(b), Cell::Bool(v)) => b.append_value(v),
            (ColBuilder::Bool(b), _) => b.append_null(),
            (ColBuilder::Int(b), Cell::I64(v)) => b.append_value(v),
            (ColBuilder::Int(b), _) => b.append_null(),
            (ColBuilder::Float(b), Cell::F64(v)) => b.append_value(v),
            (ColBuilder::Float(b), _) => b.append_null(),
            (ColBuilder::Str(b), Cell::Str(v)) => b.append_value(&*v),
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
    /// Drift detection, when the policy is anything but `ignore`. `None` is the
    /// zero-cost path: the hot loop does no drift work at all.
    drift: Option<DriftTracker>,
    /// Index of the rescue builder (always last in `builders`) under `policy: rescue`.
    rescue: Option<usize>,
}

impl Shaper {
    /// Build a shaper for the given effective columns and schema mode, with **no drift
    /// detection** — the low-level constructor. Use [`Shaper::with_drift`] (or
    /// [`Shaper::from_spec`], which reads the spec's `drift` block) to detect what the
    /// schema drops.
    pub fn new(columns: Vec<ColumnSpec>, strict: bool) -> Result<Self> {
        let off = DriftSpec {
            policy: DriftPolicy::Ignore,
            ..Default::default()
        };
        Shaper::build(columns, strict, &off, true)
    }

    /// Build a shaper that also watches for schema drift. `flatten` should match the
    /// spec's `options.flatten`, so an uncovered path is named the way inference would
    /// have named the column.
    pub fn with_drift(
        columns: Vec<ColumnSpec>,
        strict: bool,
        drift: &DriftSpec,
        flatten: bool,
    ) -> Result<Self> {
        Shaper::build(columns, strict, drift, flatten)
    }

    fn build(
        columns: Vec<ColumnSpec>,
        strict: bool,
        drift: &DriftSpec,
        flatten: bool,
    ) -> Result<Self> {
        if columns.is_empty() {
            return Err(ShapeError::Schema("no output columns".into()));
        }
        let compiled: Vec<Compiled> = columns
            .iter()
            .map(|c| Compiled {
                name: c.name.clone(),
                path: c.source_path().to_string(),
                segs: compile_path(c.source_path()),
                ty: c.ty,
                transform: c.transform,
                required: c.required,
            })
            .collect();
        let mut fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.ty.arrow_type(), !c.required))
            .collect();
        let mut builders: Vec<ColBuilder> =
            columns.iter().map(|c| ColBuilder::for_type(c.ty)).collect();

        // The rescue column is appended *after* every real column, so turning rescue on
        // is an additive schema change (an Iceberg `--append` accepts it as a new
        // optional column) and column order is otherwise untouched.
        let rescue = if drift.policy == DriftPolicy::Rescue {
            let name = drift.rescue_column.trim();
            if name.is_empty() {
                return Err(ShapeError::Schema(
                    "`drift.rescue_column` must not be empty under `policy: rescue`".into(),
                ));
            }
            if columns.iter().any(|c| c.name == name) {
                return Err(ShapeError::Schema(format!(
                    "`drift.rescue_column` `{name}` collides with an output column \
                     (pick another name)"
                )));
            }
            fields.push(Field::new(name, ColumnType::Json.arrow_type(), true));
            builders.push(ColBuilder::for_type(ColumnType::Json));
            Some(builders.len() - 1)
        } else {
            None
        };

        let tracker = drift
            .policy
            .detects()
            .then(|| DriftTracker::new(drift, flatten, compiled.iter().map(|c| c.path.as_str())));

        Ok(Shaper {
            columns: compiled,
            schema: Arc::new(Schema::new(fields)),
            builders,
            pending: 0,
            strict,
            drift: tracker,
            rescue,
        })
    }

    /// Build a shaper directly from a spec plus an inferred column list (empty in
    /// strict mode). Declared columns win over inferred ones of the same name, and the
    /// spec's `drift` block decides what happens to whatever the schema misses.
    pub fn from_spec(spec: &DatasetSpec, inferred: &[ColumnSpec]) -> Result<Self> {
        let cols = effective_columns(spec, inferred);
        Shaper::with_drift(
            cols,
            spec.schema == SchemaMode::Strict,
            &spec.drift,
            spec.options.flatten,
        )
    }

    /// The Arrow schema every emitted batch carries.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Rows buffered but not yet flushed.
    pub fn pending(&self) -> usize {
        self.pending
    }

    /// What the run has seen drift so far — `None` under `policy: ignore`.
    pub fn drift_report(&self) -> Option<&DriftReport> {
        self.drift.as_ref().map(|d| d.report())
    }

    /// Take the drift report at the end of a run.
    pub fn into_drift_report(self) -> Option<DriftReport> {
        self.drift.map(|d| d.into_report())
    }

    /// Shape one record into the current batch.
    pub fn push<'a, R: Record<'a>>(&mut self, record: R) -> Result<PushOutcome> {
        // Phase 0: find source paths no column reaches. Done before anything is
        // appended, so an `error` policy fails with the builders still aligned.
        let mut rescued: Option<Map<String, Value>> = self.rescue.map(|_| Map::new());
        let mut drift_reason: Option<String> = None;
        if let Some(d) = self.drift.as_mut() {
            drift_reason = d.scan_record(record, rescued.as_mut());
        }

        // Phase 1: compute every cell, bailing before any append if a required cell
        // is absent / a strict coercion fails. This keeps the builders aligned.
        let mut cells: Vec<Cell> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let selected = select_compiled(record, &col.segs);
            let coerced = match selected {
                None => Coerced::Null,
                Some(v) if v.kind() == Kind::Null => Coerced::Null,
                // No transform: coerce straight off the record, so a string / date /
                // timestamp / json column never copies the source bytes.
                Some(v) if col.transform.is_none() => coerce(v, col.ty),
                // A transform synthesizes a new value, so its result owns its storage.
                Some(v) => {
                    let t = col.transform.expect("matched Some above");
                    let produced = t.apply(v.to_owned_value());
                    if matches!(produced, Value::Null) {
                        Coerced::Null
                    } else {
                        coerce(&produced, col.ty).into_owned()
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
                Coerced::Fail(why, raw) => {
                    let reason = format!("column `{}`: {why}", col.name);
                    if self.strict {
                        return Err(ShapeError::Record(reason));
                    }
                    if col.required {
                        return Ok(PushOutcome::Rejected(reason));
                    }
                    // Lenient, optional column: the value is dropped and written as
                    // null. Nothing downstream would ever show it — so it is drift.
                    if let Some(d) = self.drift.as_mut() {
                        d.note_type_mismatch(&col.name, col.ty, &why, &raw);
                        if drift_reason.is_none() {
                            drift_reason = Some(reason);
                        }
                        if let Some(m) = rescued.as_mut() {
                            m.insert(col.name.clone(), raw);
                        }
                    }
                    cells.push(Cell::Null);
                }
            }
        }

        // Phase 1b: every event for this row is known — apply the policy.
        if let (Some(reason), Some(d)) = (drift_reason, self.drift.as_mut()) {
            d.note_row_drifted();
            match d.policy {
                DriftPolicy::Error => {
                    return Err(ShapeError::Drift(format!(
                        "record {}: {reason} (drift.policy = error)",
                        d.rows_scanned()
                    )));
                }
                DriftPolicy::Quarantine => {
                    d.note_row_quarantined();
                    return Ok(PushOutcome::Rejected(format!("schema drift: {reason}")));
                }
                _ => {}
            }
        }

        // Phase 2: commit.
        for (builder, cell) in self.builders.iter_mut().zip(cells) {
            builder.append(cell);
        }
        if let Some(i) = self.rescue {
            // `rescued` is Some whenever `self.rescue` is.
            let cell = match rescued {
                Some(m) if !m.is_empty() => {
                    if let Some(d) = self.drift.as_mut() {
                        d.note_row_rescued();
                    }
                    Cell::Str(Cow::Owned(Value::Object(m).to_string()))
                }
                _ => Cell::Null,
            };
            self.builders[i].append(cell);
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
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunReport {
    /// Records read from the source (excluding source parse errors handled upstream).
    pub rows_in: u64,
    /// Rows written to the sink.
    pub rows_out: u64,
    /// Rows dropped by a lenient reject — including rows quarantined for drift.
    pub rows_rejected: u64,
    /// RecordBatches (row groups) flushed.
    pub batches: u64,
    /// What the source did that the schema does not cover — `None` under
    /// `drift.policy: ignore`.
    pub drift: Option<DriftReport>,
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
    report.drift = shaper.into_drift_report();
    Ok((report, summary))
}

// ---------------------------------------------------------------------------
// Coercion
// ---------------------------------------------------------------------------

/// Coerce one value to a column's logical type, **reading it in place**. A string that
/// survives as a string (or is only parsed, as for date/timestamp) never leaves the
/// record: the cell borrows it, whether that is a `serde_json::String`'s buffer or a
/// slice of the reader's input. Only a synthesized value — a stringified number, a
/// serialized nested value — allocates.
///
/// On failure the value is materialized into [`Coerced::Fail`] for the report. That is
/// the one place a copy is made, and only on the path that is going to print it anyway.
fn coerce<'a, R: Record<'a>>(v: R, ty: ColumnType) -> Coerced<'a> {
    let k = v.kind();
    let fail = |why: String| Coerced::Fail(why, v.to_owned_value());
    match ty {
        ColumnType::Bool => match v.as_bool() {
            Some(b) => Coerced::Cell(Cell::Bool(b)),
            None => fail(format!("expected bool, got {}", k.name())),
        },
        ColumnType::Int64 => match k {
            Kind::Int => match v.as_i64() {
                Some(i) => Coerced::Cell(Cell::I64(i)),
                None => fail("number out of i64 range".into()),
            },
            Kind::Float => match v.as_f64() {
                // `f as i64` saturates silently on overflow, so bound-check first.
                // `i64::MIN as f64` is exactly -2^63; its negation is exactly 2^63
                // (= i64::MAX + 1), the first value that would saturate.
                Some(f) => {
                    const MIN: f64 = i64::MIN as f64;
                    const MAX: f64 = -(i64::MIN as f64);
                    if f.fract() != 0.0 || !f.is_finite() {
                        fail(format!("non-integer number {f} for int64"))
                    } else if (MIN..MAX).contains(&f) {
                        Coerced::Cell(Cell::I64(f as i64))
                    } else {
                        fail(format!("number {f} out of i64 range"))
                    }
                }
                None => fail("number out of i64 range".into()),
            },
            Kind::Str => {
                let s = v.as_str().expect("Kind::Str has a string body");
                match s.parse::<i64>() {
                    Ok(i) => Coerced::Cell(Cell::I64(i)),
                    Err(_) => fail(format!("string {s:?} is not an int64")),
                }
            }
            _ => fail(format!("expected int64, got {}", k.name())),
        },
        ColumnType::Float64 => match k {
            Kind::Int | Kind::Float => match v.as_f64() {
                Some(f) => Coerced::Cell(Cell::F64(f)),
                None => fail("number not representable as f64".into()),
            },
            Kind::Str => {
                let s = v.as_str().expect("Kind::Str has a string body");
                match s.parse::<f64>() {
                    Ok(f) => Coerced::Cell(Cell::F64(f)),
                    Err(_) => fail(format!("string {s:?} is not a float64")),
                }
            }
            _ => fail(format!("expected float64, got {}", k.name())),
        },
        ColumnType::String => match k {
            Kind::Str => Coerced::Cell(Cell::Str(Cow::Borrowed(
                v.as_str().expect("Kind::Str has a string body"),
            ))),
            Kind::Bool | Kind::Int | Kind::Float => {
                Coerced::Cell(Cell::Str(Cow::Owned(v.to_json_text())))
            }
            _ => fail(format!(
                "expected string scalar, got {} (use type `json` to keep it)",
                k.name()
            )),
        },
        ColumnType::Json => match k {
            Kind::Str => Coerced::Cell(Cell::Str(Cow::Borrowed(
                v.as_str().expect("Kind::Str has a string body"),
            ))),
            _ => Coerced::Cell(Cell::Str(Cow::Owned(v.to_json_text()))),
        },
        ColumnType::Date => match k {
            Kind::Str => {
                let s = v.as_str().expect("Kind::Str has a string body");
                match parse_date_days(s) {
                    Some(d) => Coerced::Cell(Cell::Date(d)),
                    None => fail(format!("string {s:?} is not a YYYY-MM-DD / RFC3339 date")),
                }
            }
            // A bare integer is read as epoch-days (Arrow Date32 semantics).
            Kind::Int => match v.as_i64() {
                Some(i) if i >= i32::MIN as i64 && i <= i32::MAX as i64 => {
                    Coerced::Cell(Cell::Date(i as i32))
                }
                _ => fail("integer out of Date32 (epoch-day) range".into()),
            },
            _ => fail(format!("expected date, got {}", k.name())),
        },
        ColumnType::Timestamp => match k {
            Kind::Str => {
                let s = v.as_str().expect("Kind::Str has a string body");
                match parse_ts_micros(s) {
                    Some(t) => Coerced::Cell(Cell::Ts(t)),
                    None => fail(format!("string {s:?} is not an RFC3339 timestamp")),
                }
            }
            // A bare integer is read as epoch-milliseconds → microseconds.
            Kind::Int => match v.as_i64().and_then(|ms| ms.checked_mul(1000)) {
                Some(us) => Coerced::Cell(Cell::Ts(us)),
                None => fail("epoch-ms overflows i64 microseconds".into()),
            },
            Kind::Float => fail("timestamp number not an integer epoch-ms".into()),
            _ => fail(format!("expected timestamp, got {}", k.name())),
        },
    }
}

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
