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
    TimestampMicrosecondBuilder, UInt8Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value};

use crate::changes::{
    ApplyMode, ChangeKind, ChangesSpec, SeedVersion, CHANGE_COLUMN, CHANGE_DELETE, CHANGE_UPSERT,
    OP_COLUMN,
};
use crate::drift::{DriftPolicy, DriftReport, DriftSpec, DriftTracker};
use crate::error::{Result, ShapeError};
use crate::predicate::Predicate;
use crate::record::{Kind, Record};
use crate::sink::Sink;
use crate::spec::{ColumnSpec, DatasetSpec, SchemaMode};
use crate::transform::Transform;
use crate::types::ColumnType;
use crate::value::{compile_path, segments, select_compiled, Segment, MAX_PATH_SEGMENTS};

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
    /// Under `changes:`, a path that starts with `$` reads the whole record (the
    /// envelope); any other reads the change's row.
    root: bool,
}

/// One typed cell, produced by coercion, ready to append to the matching builder.
///
/// `Str` holds a [`Cow`] so the common case — a string/date/timestamp/json column read
/// straight out of the record — **borrows** the record's own bytes instead of cloning
/// them. Only a value the engine had to synthesize (a transform's output, a number
/// stringified, a nested value serialized) owns its storage.
#[derive(Debug, Clone)]
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
    /// The spec's `where` did not keep the record. Not an error: the run is about other
    /// rows. Counted (see [`Shaper::filter_report`]) and otherwise left alone.
    Filtered,
    /// A change feed's record that carries no change of its own (a `changes.skip` op,
    /// such as Flink's `-U`). Counted (see [`Shaper::change_report`]).
    Skipped,
}

/// What a run's `changes:` saw: how many changes of each kind it shaped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeReport {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    /// Records a `changes.skip` op set aside.
    pub skipped: u64,
}

/// `changes:`, compiled for the hot loop.
#[derive(Debug, Clone)]
struct Changes {
    spec: ChangesSpec,
    mode: ApplyMode,
    op: Vec<Segment>,
    row: Option<Vec<Segment>>,
    key_from: Option<Vec<Segment>>,
    /// Indices into the columns: the merge key, and the version.
    key_cols: Vec<usize>,
    order_cols: Vec<usize>,
    /// A seed row's version, column by column (see [`ChangesPreset::seed_ops`]): empty when
    /// the feed has no seed rows, or versions nothing.
    seed: Vec<(usize, Cell<'static>)>,
    report: ChangeReport,
}

/// The column a change feed adds to every batch: [`CHANGE_COLUMN`] (a flag the merge
/// sink reads and drops) or, for a changelog, [`OP_COLUMN`] (written to the table).
enum OpBuilder {
    Flag(UInt8Builder),
    Word(StringBuilder),
}

/// What a run's `where` did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterReport {
    /// The expression, as written.
    pub expr: String,
    /// Records it did not keep.
    pub filtered: u64,
    /// Records in which some comparison met values of different kinds (a number against
    /// a string, say), and so could not be true.
    pub type_mismatches: u64,
    /// The first such comparison, described.
    pub first_mismatch: Option<String>,
}

/// A shaper's filter and what it has done so far.
#[derive(Debug, Clone)]
struct Filter {
    predicate: Predicate,
    report: FilterReport,
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
    /// The spec's `where`, when it has one. `None` costs the hot loop nothing.
    filter: Option<Filter>,
    /// How drift detection was set up, so a change feed with `row` can set it up again in
    /// the row's coordinates.
    drift_conf: (DriftSpec, bool),
    /// The spec's `changes:`, when it has one, and the column it adds to each batch.
    changes: Option<Changes>,
    op_builder: Option<OpBuilder>,
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
                root: c.source_path().starts_with('$'),
            })
            .collect();
        // Every column enters here (declared, inferred, or a library caller's), and the
        // drift tracker builds its trie from these paths recursively: bound them all,
        // not only the ones a spec's validation sees.
        if let Some(c) = compiled.iter().find(|c| c.segs.len() > MAX_PATH_SEGMENTS) {
            // An inferred column is named after its path, so neither is printed whole.
            let short = |s: &str| match s.char_indices().nth(40) {
                Some((i, _)) => format!("{}…", &s[..i]),
                None => s.to_string(),
            };
            return Err(ShapeError::Schema(format!(
                "column `{}`: its path `{}` has {} segments, more than {MAX_PATH_SEGMENTS}",
                short(&c.name),
                short(&c.path),
                c.segs.len()
            )));
        }
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
            filter: None,
            drift_conf: (drift.clone(), flatten),
            changes: None,
            op_builder: None,
        })
    }

    /// Read the records as a feed of changes (see [`ChangesSpec`]). `merge_on` names the key a
    /// change applies to (empty for `apply: changelog`). Every batch then carries one more
    /// column: for `upsert`, [`CHANGE_COLUMN`], which says which rows are deletes and which
    /// the merge sink takes off before writing (see [`Shaper::table_schema`]); for
    /// `changelog`, [`OP_COLUMN`], which is written.
    pub fn with_changes(
        mut self,
        spec: &ChangesSpec,
        mode: ApplyMode,
        merge_on: &[String],
    ) -> Result<Self> {
        if self.changes.is_some() {
            return Err(ShapeError::Spec(
                "this shaper already reads a change feed".into(),
            ));
        }
        if self.pending != 0 {
            return Err(ShapeError::Spec(
                "read a change feed from the first record: call `with_changes` before `push`"
                    .into(),
            ));
        }
        // A spec built in code, not read: expand its preset here (its version columns are
        // added by `DatasetSpec::apply_preset`, which reading a spec runs).
        let mut expanded = spec.clone();
        expanded.apply_preset();
        let spec = &expanded;
        spec.validate()?;
        if mode == ApplyMode::Upsert && merge_on.is_empty() {
            return Err(ShapeError::Spec(
                "`apply: upsert` applies each change to a key: name the merge keys".into(),
            ));
        }
        let find = |name: &str, what: &str| -> Result<usize> {
            self.columns
                .iter()
                .position(|c| c.name == name)
                .ok_or_else(|| {
                    ShapeError::Schema(format!("{what} `{name}` is not an output column"))
                })
        };
        let key_cols = merge_on
            .iter()
            .map(|k| find(k, "merge key"))
            .collect::<Result<Vec<_>>>()?;
        let mut order_cols = Vec::new();
        for c in &spec.order_by {
            let i = find(c, "`changes.order_by` column")?;
            let ty = self.columns[i].ty;
            if !matches!(
                ty,
                ColumnType::Int64 | ColumnType::Date | ColumnType::Timestamp | ColumnType::String
            ) {
                return Err(ShapeError::Schema(format!(
                    "`changes.order_by` column `{c}` is {}: a version is an int64, a date, a \
                     timestamp or a string",
                    ty.as_str()
                )));
            }
            // A delete reads its columns from `key_from`, the row as it was: a version read
            // from there is the live row's own, so every delete would be stale.
            if spec.key_from.is_some() && mode == ApplyMode::Upsert && !self.columns[i].root {
                return Err(ShapeError::Schema(key_from_version(c)));
            }
            order_cols.push(i);
        }
        let (name, field, builder) = match mode {
            ApplyMode::Upsert => (
                CHANGE_COLUMN,
                Field::new(CHANGE_COLUMN, DataType::UInt8, false),
                OpBuilder::Flag(UInt8Builder::new()),
            ),
            ApplyMode::Changelog => (
                OP_COLUMN,
                Field::new(OP_COLUMN, DataType::Utf8, false),
                OpBuilder::Word(StringBuilder::new()),
            ),
        };
        if self.schema.fields().iter().any(|f| f.name() == name) {
            return Err(ShapeError::Schema(format!(
                "the change feed adds the column `{name}`, and the schema already has one: rename it"
            )));
        }
        let mut fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(field);
        self.schema = Arc::new(Schema::new(fields));
        // With `row`, drift is looked for in the row: a column covers a row field by its
        // path in the row, and one that reads only the envelope (`$` outside the row)
        // covers nothing there.
        if let (Some(row), true) = (&spec.row, self.drift.is_some()) {
            let row = segments(row);
            let paths: Vec<String> = self
                .columns
                .iter()
                .filter_map(|c| {
                    if !c.root {
                        return Some(c.path.clone());
                    }
                    let segs = segments(&c.path);
                    segs.starts_with(&row).then(|| segs[row.len()..].join("."))
                })
                .collect();
            let (conf, flatten) = &self.drift_conf;
            let mut tracker = DriftTracker::new(conf, *flatten, paths.iter().map(String::as_str));
            // A field the `where` reads is one the spec knows about, here as in `with_where`:
            // those that lie in the row stay covered, in the row's coordinates.
            if let Some(f) = &self.filter {
                for segs in f.predicate.paths() {
                    let keys: Vec<&str> = segs.iter().map(|s| s.key()).collect();
                    if keys.len() > row.len() && keys.starts_with(&row) {
                        tracker.cover(keys[row.len()..].iter().copied());
                    }
                }
            }
            self.drift = Some(tracker);
        }
        // When the record is the row, the fields that describe the change are part of it,
        // and not drift.
        if spec.row.is_none() {
            if let Some(d) = self.drift.as_mut() {
                d.cover(segments(&spec.op));
                if let Some(k) = &spec.key_from {
                    d.cover(segments(k));
                }
            }
        }
        // A seed row is older than every change: in `upsert`, where the version decides, it
        // takes the preset's lowest version in each of the preset's columns, and the lowest a
        // column of its type holds in any other `order_by` column the spec names.
        let seed: Vec<(usize, Cell<'static>)> = match spec.preset {
            Some(p) if mode == ApplyMode::Upsert && !p.seed_ops().is_empty() => order_cols
                .iter()
                .map(|&i| {
                    let c = &self.columns[i];
                    // A preset column the spec re-declared with another type takes that
                    // type's lowest instead.
                    let cell = match (p.seed_version(&c.name), c.ty) {
                        (Some(SeedVersion::Int(v)), ColumnType::Int64) => Cell::I64(v),
                        (Some(SeedVersion::Str(s)), ColumnType::String) => {
                            Cell::Str(Cow::Borrowed(s))
                        }
                        _ => lowest(c.ty),
                    };
                    (i, cell)
                })
                .collect(),
            _ => Vec::new(),
        };
        self.changes = Some(Changes {
            seed,
            op: compile_path(&spec.op),
            row: spec.row.as_deref().map(compile_path),
            key_from: spec.key_from.as_deref().map(compile_path),
            spec: spec.clone(),
            mode,
            key_cols,
            order_cols,
            report: ChangeReport::default(),
        });
        self.op_builder = Some(builder);
        Ok(self)
    }

    /// What the change feed has seen so far: `None` without `changes:`.
    pub fn change_report(&self) -> Option<&ChangeReport> {
        self.changes.as_ref().map(|c| &c.report)
    }

    /// The schema of the table a run writes: [`Shaper::schema`] without the column that
    /// marks deletes for the merge sink. Create the sink with this one.
    pub fn table_schema(&self) -> SchemaRef {
        match self.schema.index_of(CHANGE_COLUMN) {
            Ok(i) => {
                let fields: Vec<Field> = self
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, f)| f.as_ref().clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            Err(_) => self.schema.clone(),
        }
    }

    /// Take only the records `expr` keeps (see [`Predicate`]); the others come back from
    /// [`Shaper::push`] as [`PushOutcome::Filtered`], before drift detection or shaping
    /// sees them. A shaper has one filter: a second call is refused, since the first
    /// one's paths are already counted as read.
    pub fn with_where(mut self, expr: &str) -> Result<Self> {
        if let Some(f) = &self.filter {
            return Err(ShapeError::Spec(format!(
                "this shaper already filters with `where {}`; combine the two with `and`",
                f.report.expr
            )));
        }
        let predicate = Predicate::parse(expr)?;
        // A field the filter reads is one the spec knows about: not drift, even when no
        // column keeps it (`record_type` in a table of one record type).
        if let Some(d) = self.drift.as_mut() {
            for segs in predicate.paths() {
                d.cover(segs.iter().map(|s| s.key()));
            }
        }
        self.filter = Some(Filter {
            report: FilterReport {
                expr: predicate.text().to_string(),
                ..FilterReport::default()
            },
            predicate,
        });
        Ok(self)
    }

    /// What the `where` has done so far — `None` when the shaper has none.
    pub fn filter_report(&self) -> Option<&FilterReport> {
        self.filter.as_ref().map(|f| &f.report)
    }

    /// Build a shaper directly from a spec plus an inferred column list (empty in
    /// strict mode). Declared columns win over inferred ones of the same name, and the
    /// spec's `drift` block decides what happens to whatever the schema misses.
    pub fn from_spec(spec: &DatasetSpec, inferred: &[ColumnSpec]) -> Result<Self> {
        let cols = effective_columns(spec, inferred);
        let shaper = Shaper::with_drift(
            cols,
            spec.schema == SchemaMode::Strict,
            &spec.drift,
            spec.options.flatten,
        )?;
        let shaper = match &spec.filter {
            Some(expr) => shaper.with_where(expr)?,
            None => shaper,
        };
        match &spec.changes {
            Some(c) => {
                let mode = spec.output.apply.unwrap_or_default();
                let keys: &[String] = match mode {
                    ApplyMode::Upsert => &spec.output.merge_on,
                    ApplyMode::Changelog => &[],
                };
                shaper.with_changes(c, mode, keys)
            }
            None => Ok(shaper),
        }
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
        // The filter first: a record the run is not about must not reach drift
        // detection, a required column or a strict coercion.
        if let Some(f) = self.filter.as_mut() {
            let tested = f.predicate.test(record);
            if tested.mismatch {
                f.report.type_mismatches += 1;
                if f.report.first_mismatch.is_none() {
                    f.report.first_mismatch = f.predicate.explain_mismatch(record);
                }
            }
            if !tested.keep {
                f.report.filtered += 1;
                if let Some(d) = self.drift.as_mut() {
                    d.skip();
                }
                return Ok(PushOutcome::Filtered);
            }
        }

        // A change feed: what the record says happened, and where its row is. Columns read
        // the row (`src`), or the whole record when their path starts with `$`.
        let mut kind: Option<ChangeKind> = None;
        // A seed row (a read of the table as it stood) takes the lowest version, below.
        let mut seed = false;
        let mut src = record;
        if let Some(ch) = self.changes.as_mut() {
            let reject = |why: String, strict: bool| -> Result<PushOutcome> {
                if strict {
                    Err(ShapeError::Record(why))
                } else {
                    Ok(PushOutcome::Rejected(why))
                }
            };
            let op = match select_compiled(record, &ch.op) {
                Some(v) if v.kind() != Kind::Null => v.to_owned_value(),
                _ => {
                    if let Some(d) = self.drift.as_mut() {
                        d.skip();
                    }
                    return reject(
                        format!(
                            "the change has no `{}`, which says what happened",
                            ch.spec.op
                        ),
                        self.strict,
                    );
                }
            };
            let Some(k) = ch.spec.kind_of(&op) else {
                if let Some(d) = self.drift.as_mut() {
                    d.skip();
                }
                return reject(
                    format!(
                        "`{}` is {op}, which is none of the ops the spec names ({})",
                        ch.spec.op,
                        ch.spec.known_ops()
                    ),
                    self.strict,
                );
            };
            if k == ChangeKind::Skip {
                ch.report.skipped += 1;
                if let Some(d) = self.drift.as_mut() {
                    d.skip();
                }
                return Ok(PushOutcome::Skipped);
            }
            let present = |r: Option<R>| r.filter(|v| v.kind() != Kind::Null);
            let row = match &ch.row {
                Some(segs) => present(select_compiled(record, segs)),
                None => Some(record),
            };
            let from = match (k, &ch.key_from) {
                (ChangeKind::Delete, Some(segs)) => present(select_compiled(record, segs)).or(row),
                _ => row,
            };
            match from {
                Some(r) => src = r,
                None => {
                    if let Some(d) = self.drift.as_mut() {
                        d.skip();
                    }
                    let at = ch.spec.row.as_deref().unwrap_or("$");
                    return reject(
                        format!("the {} has no row at `{at}`", k.word()),
                        self.strict,
                    );
                }
            }
            kind = Some(k);
            seed = !ch.seed.is_empty() && ch.spec.is_seed(&op);
        }
        // In `apply: upsert` a delete carries only its key and version: nothing else of it
        // is written, so nothing else is read, checked or reported as drift.
        let key_only = kind == Some(ChangeKind::Delete)
            && self
                .changes
                .as_ref()
                .is_some_and(|c| c.mode == ApplyMode::Upsert);

        // Phase 0: find source paths no column reaches. Done before anything is
        // appended, so an `error` policy fails with the builders still aligned.
        let mut rescued: Option<Map<String, Value>> = self.rescue.map(|_| Map::new());
        let mut drift_reason: Option<String> = None;
        if let Some(d) = self.drift.as_mut() {
            if key_only {
                d.skip();
            } else {
                drift_reason = d.scan_record(src, rescued.as_mut());
            }
        }

        // Phase 1: compute every cell, bailing before any append if a required cell
        // is absent / a strict coercion fails. This keeps the builders aligned.
        let mut cells: Vec<Cell> = Vec::with_capacity(self.columns.len());
        for (i, col) in self.columns.iter().enumerate() {
            if key_only
                && !self
                    .changes
                    .as_ref()
                    .is_some_and(|c| c.key_cols.contains(&i) || c.order_cols.contains(&i))
            {
                // Never written (the merge sink drops the row), but the batch still needs a
                // value its field accepts.
                cells.push(if col.required {
                    placeholder(col.ty)
                } else {
                    Cell::Null
                });
                continue;
            }
            let base = if col.root { record } else { src };
            let selected = select_compiled(base, &col.segs);
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
                        d.position()
                    )));
                }
                DriftPolicy::Quarantine => {
                    d.note_row_quarantined();
                    return Ok(PushOutcome::Rejected(format!("schema drift: {reason}")));
                }
                _ => {}
            }
        }

        // A seed row is older than every change: whatever version it
        // was read with, it takes the lowest, so a change to its key wins in either order of
        // arrival, and it only fills a key no change has touched. Debezium reads a table on
        // Postgres at an LSN above that of a transaction still open across the read, so the
        // version it carries would make that transaction's changes stale.
        if seed {
            if let Some(ch) = self.changes.as_ref() {
                for (i, c) in &ch.seed {
                    cells[*i] = c.clone();
                }
            }
        } else if let Some(ch) = self.changes.as_ref().filter(|c| !c.seed.is_empty()) {
            // A change versioned at exactly a seed row's version would tie with it, and a tie
            // is stale: it could lose to a snapshot row with nothing said. No preset's change
            // carries it (an LSN is never 0, a binlog file always has a name), but a spec's own
            // `order_by` column may: such a change is refused, so it is counted, not lost.
            if ch.seed.iter().all(|(i, c)| same_version(&cells[*i], c)) {
                let why = format!(
                    "the {} is versioned at the lowest value of its `changes.order_by` \
                     columns, which snapshot rows take: it cannot be told from one",
                    kind.map_or("change", ChangeKind::word)
                );
                if self.strict {
                    return Err(ShapeError::Record(why));
                }
                return Ok(PushOutcome::Rejected(why));
            }
        }

        // A change needs its version, and a delete its key: one without cannot be placed.
        if let (Some(k), Some(ch)) = (kind, self.changes.as_ref()) {
            let missing = |idx: &[usize]| {
                idx.iter()
                    .find(|&&i| matches!(cells[i], Cell::Null))
                    .copied()
            };
            let why = match (k, missing(&ch.order_cols), missing(&ch.key_cols)) {
                (_, Some(i), _) => Some(format!(
                    "the {} has no version: `changes.order_by` column `{}` is null",
                    k.word(),
                    self.columns[i].name
                )),
                (ChangeKind::Delete, None, Some(i)) if ch.mode == ApplyMode::Upsert => Some(format!(
                    "the delete has no key: `{}` is null (a delete that matches nothing by accident is refused)",
                    self.columns[i].name
                )),
                _ => None,
            };
            if let Some(why) = why {
                if self.strict {
                    return Err(ShapeError::Record(why));
                }
                return Ok(PushOutcome::Rejected(why));
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
        if let (Some(k), Some(ch), Some(b)) =
            (kind, self.changes.as_mut(), self.op_builder.as_mut())
        {
            match k {
                ChangeKind::Insert => ch.report.inserts += 1,
                ChangeKind::Update => ch.report.updates += 1,
                ChangeKind::Delete => ch.report.deletes += 1,
                ChangeKind::Skip => {}
            }
            match b {
                OpBuilder::Flag(b) => b.append_value(if k == ChangeKind::Delete {
                    CHANGE_DELETE
                } else {
                    CHANGE_UPSERT
                }),
                OpBuilder::Word(b) => b.append_value(k.word()),
            }
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
        let mut arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        match self.op_builder.as_mut() {
            Some(OpBuilder::Flag(b)) => arrays.push(Arc::new(b.finish())),
            Some(OpBuilder::Word(b)) => arrays.push(Arc::new(b.finish())),
            None => {}
        }
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
    /// Records the spec's `where` did not keep. `rows_in` is the sum of `rows_out`,
    /// `rows_rejected` and this.
    pub rows_filtered: u64,
    /// Records a change feed's `skip` op set aside (see [`ChangeReport`]).
    pub rows_skipped: u64,
    /// What the `where` did — `None` when the spec has none.
    pub filter: Option<FilterReport>,
    /// What the change feed shaped — `None` without `changes:`.
    pub changes: Option<ChangeReport>,
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
            PushOutcome::Filtered => report.rows_filtered += 1,
            PushOutcome::Skipped => report.rows_skipped += 1,
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
    report.filter = shaper.filter_report().cloned();
    report.changes = shaper.change_report().cloned();
    report.drift = shaper.into_drift_report();
    Ok((report, summary))
}

// ---------------------------------------------------------------------------
// Coercion
// ---------------------------------------------------------------------------

/// Why a version read from the row cannot order a feed whose deletes read `key_from`.
pub(crate) fn key_from_version(column: &str) -> String {
    format!(
        "`changes.order_by` column `{column}` is read from the row, but a delete reads `key_from` \
         (the row before it changed), whose version is the live row's own, so every delete \
         would be stale: read the version from the envelope, with a `from` that starts with \
         `$` (such as `$.source.ts_ms`)"
    )
}

/// A value a required column accepts, for a row that is never written (a delete's other
/// columns in `apply: upsert`).
/// The lowest value a version column of `ty` holds: what a seed row takes in an `order_by`
/// column its preset has no value for. (`order_by` takes only these four types.) A date or
/// a timestamp takes 0001-01-01, the first a date library can print, not the type's least.
fn lowest(ty: ColumnType) -> Cell<'static> {
    const YEAR_ONE_DAYS: i32 = -719_162;
    match ty {
        ColumnType::Int64 => Cell::I64(i64::MIN),
        ColumnType::Date => Cell::Date(YEAR_ONE_DAYS),
        ColumnType::Timestamp => Cell::Ts(YEAR_ONE_DAYS as i64 * 86_400_000_000),
        _ => Cell::Str(Cow::Borrowed("")),
    }
}

/// Whether two version cells hold the same value (the four `order_by` types).
fn same_version(a: &Cell<'_>, b: &Cell<'_>) -> bool {
    match (a, b) {
        (Cell::I64(x), Cell::I64(y)) | (Cell::Ts(x), Cell::Ts(y)) => x == y,
        (Cell::Date(x), Cell::Date(y)) => x == y,
        (Cell::Str(x), Cell::Str(y)) => x == y,
        _ => false,
    }
}

fn placeholder(ty: ColumnType) -> Cell<'static> {
    match ty {
        ColumnType::Bool => Cell::Bool(false),
        ColumnType::Int64 => Cell::I64(0),
        ColumnType::Float64 => Cell::F64(0.0),
        ColumnType::String | ColumnType::Json => Cell::Str(Cow::Borrowed("")),
        ColumnType::Date => Cell::Date(0),
        ColumnType::Timestamp => Cell::Ts(0),
    }
}

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
