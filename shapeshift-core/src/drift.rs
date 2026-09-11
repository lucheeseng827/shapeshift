//! Schema drift: what the source grew (or changed) that the spec's schema does not
//! cover, and what the run does about it.
//!
//! A spec is written once per source *shape*. Sources move anyway — a producer adds
//! a field, or starts sending `"12.50"` where it used to send `12.50`. Before this
//! module both were **invisible**: an undeclared field simply had no column and its
//! values were dropped, and an uncoercible value in an *optional* column under the
//! default lenient policy was written as null with nothing counted. Neither showed up
//! in `rows_in` / `rows_out` / `rejected`, so a drifted run looked like a clean one.
//!
//! Two kinds are tracked, and only these two, because they are exactly the losses that
//! leave no trace in the output:
//!
//! - **new field** — a path present in the record that no declared/inferred column
//!   reaches. Its data does not reach the table at all.
//! - **type mismatch** — a value that is present but not coercible to its column's
//!   declared type, in an *optional* column under the lenient policy, so it silently
//!   becomes null. (In a *required* column the row is already rejected and counted; in
//!   `strict` policy the run already aborts. Those are visible, so they are not drift.)
//!
//! A column that is simply *absent* from a record is deliberately **not** tracked: it
//! is already visible as a null in the output, and counting it on every row would cost
//! throughput to report something the data already says.
//!
//! [`DriftPolicy`] then decides what a detected drift *does* — from `ignore` (the v0.1
//! behaviour, and a hot loop with no drift work at all) through `warn` (count and
//! report) to `rescue` (keep the dropped values in a JSON catch-all column),
//! `quarantine` (keep the output exactly on-schema and reject the drifted rows), and
//! `error` (fail the run).
//!
//! This is detection and mitigation *inside one run* — it is not a schema registry, a
//! drift history, or an alerting plane. Those are orchestration, and orchestration
//! lives outside the engine on purpose (ARCHITECTURE §14).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::infer::Acc;
use crate::record::{Kind, Record};
use crate::spec::ColumnSpec;
use crate::types::ColumnType;
use crate::value::segments;

/// Distinct drifted paths a single run will track. Past this the counts stop growing
/// and the report is flagged `truncated` — a pathological source (UUIDs as keys) must
/// not turn a bounded-RAM run into an unbounded one.
pub const MAX_TRACKED_PATHS: usize = 1_000;

/// What a run does when the source's shape no longer matches the spec's schema.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DriftPolicy {
    /// Detect nothing, report nothing — the v0.1 behaviour. The only policy that adds
    /// no per-record work at all; choose it when you have measured that you need the
    /// last few percent of throughput more than you need to know.
    Ignore,
    /// Detect, count, and report at the end of the run. Writes exactly the rows
    /// `ignore` writes — the output is unchanged, it is no longer *silent*.
    #[default]
    Warn,
    /// `warn`, plus a JSON catch-all column (`drift.rescue_column`) holding whatever
    /// the schema would have dropped, per row. Nothing is lost, and the recovered
    /// values stay queryable until the spec catches up.
    Rescue,
    /// `warn`, plus: a row that drifted is not written. It is rejected like any other
    /// bad row (counted, and preserved in the reject sidecar), so the table holds only
    /// rows that matched the schema exactly.
    Quarantine,
    /// Fail the run on the first drift event. The contract mode: a downstream consumer
    /// that cannot tolerate a shape change would rather have no new data than
    /// quietly-different data.
    Error,
}

impl DriftPolicy {
    /// Whether this policy needs per-record drift detection at all.
    pub fn detects(self) -> bool {
        self != DriftPolicy::Ignore
    }

    /// The name this policy is written as in a spec / on the command line.
    pub fn as_str(self) -> &'static str {
        match self {
            DriftPolicy::Ignore => "ignore",
            DriftPolicy::Warn => "warn",
            DriftPolicy::Rescue => "rescue",
            DriftPolicy::Quarantine => "quarantine",
            DriftPolicy::Error => "error",
        }
    }
}

impl std::fmt::Display for DriftPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Drift handling, as declared in the spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DriftSpec {
    /// What a detected drift does. `warn` by default.
    #[serde(default)]
    pub policy: DriftPolicy,

    /// Output column that holds rescued values under `policy: rescue`. Typed `json`,
    /// always optional, appended after every other column; null on rows that did not
    /// drift.
    #[serde(default = "default_rescue_column")]
    pub rescue_column: String,

    /// Example values kept per drifted path, for the report. Counts are exact
    /// regardless; this only bounds what the report carries.
    #[serde(default = "default_max_examples")]
    pub max_examples: usize,
}

fn default_rescue_column() -> String {
    "_rescued".to_string()
}

fn default_max_examples() -> usize {
    3
}

impl Default for DriftSpec {
    fn default() -> Self {
        DriftSpec {
            policy: DriftPolicy::default(),
            rescue_column: default_rescue_column(),
            max_examples: default_max_examples(),
        }
    }
}

impl DriftSpec {
    /// True when this is exactly the default — lets `shapeshift infer` keep emitting a
    /// terse spec while an edited one round-trips whatever the user wrote.
    pub fn is_default(&self) -> bool {
        self == &DriftSpec::default()
    }
}

/// A source path with no column: its values never reach the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewField {
    /// Records in which this path appeared with a non-null value.
    pub count: u64,
    /// 1-based index of the record it first appeared in.
    pub first_record: u64,
    /// The column type inference would give it — i.e. what to declare to keep it.
    pub suggested_type: ColumnType,
    /// Up to `max_examples` observed values.
    pub examples: Vec<Value>,
    /// The live inference accumulator behind `suggested_type` (not part of the report).
    #[serde(skip)]
    acc: Acc,
}

impl NewField {
    fn new(first_record: u64) -> Self {
        NewField {
            count: 0,
            first_record,
            suggested_type: ColumnType::String,
            examples: Vec::new(),
            acc: Acc::new(),
        }
    }

    fn observe(&mut self, v: &Value, max_examples: usize) {
        self.count += 1;
        self.acc.observe(v);
        self.suggested_type = self.acc.decide();
        if self.examples.len() < max_examples {
            self.examples.push(v.clone());
        }
    }
}

/// A declared column whose values stopped coercing — silently nulled before this
/// existed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypeMismatch {
    /// Values that failed to coerce and were written as null.
    pub count: u64,
    /// 1-based index of the record it first happened in.
    pub first_record: u64,
    /// The type the column is declared as.
    pub declared_type: ColumnType,
    /// The type inference would give the values actually seen — i.e. what to widen to.
    pub suggested_type: ColumnType,
    /// The coercion failure, as reported for the first occurrence.
    pub reason: String,
    /// Up to `max_examples` of the values that were dropped.
    pub examples: Vec<Value>,
    #[serde(skip)]
    acc: Acc,
}

impl TypeMismatch {
    fn new(first_record: u64, declared_type: ColumnType, reason: String) -> Self {
        TypeMismatch {
            count: 0,
            first_record,
            declared_type,
            suggested_type: ColumnType::String,
            reason,
            examples: Vec::new(),
            acc: Acc::new(),
        }
    }

    fn observe(&mut self, v: &Value, max_examples: usize) {
        self.count += 1;
        self.acc.observe(v);
        self.suggested_type = self.acc.decide();
        if self.examples.len() < max_examples {
            self.examples.push(v.clone());
        }
    }
}

/// Everything one run learned about how its source drifted away from its schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct DriftReport {
    /// The policy the run applied.
    pub policy: DriftPolicy,
    /// Records the detector examined.
    pub rows_scanned: u64,
    /// Records that carried at least one drift event.
    pub rows_with_drift: u64,
    /// Records not written because they drifted (`policy: quarantine`).
    pub rows_quarantined: u64,
    /// Records that wrote a non-null rescue column (`policy: rescue`).
    pub rows_rescued: u64,
    /// Undeclared source paths, keyed by dotted path.
    pub new_fields: BTreeMap<String, NewField>,
    /// Columns whose values stopped coercing, keyed by output column name.
    pub type_mismatches: BTreeMap<String, TypeMismatch>,
    /// [`MAX_TRACKED_PATHS`] was hit: counts below are for the tracked paths only.
    pub truncated: bool,
}

impl DriftReport {
    /// No drift was seen (the run matched its schema).
    pub fn is_clean(&self) -> bool {
        self.new_fields.is_empty() && self.type_mismatches.is_empty()
    }

    /// Total drift events across every path (not rows — a row can drift several ways).
    pub fn events(&self) -> u64 {
        self.new_fields.values().map(|f| f.count).sum::<u64>()
            + self.type_mismatches.values().map(|t| t.count).sum::<u64>()
    }

    /// The columns to add to the spec to stop losing the new fields — inference's own
    /// verdict on the values actually seen, ready to paste under `columns:`.
    pub fn suggested_columns(&self) -> Vec<ColumnSpec> {
        self.new_fields
            .iter()
            .map(|(path, f)| ColumnSpec {
                name: path.clone(),
                from: None,
                ty: f.suggested_type,
                transform: None,
                required: false,
            })
            .collect()
    }

    fn note_new_field(&mut self, path: &str, v: &Value, record: u64, max_examples: usize) {
        if let Some(e) = self.new_fields.get_mut(path) {
            e.observe(v, max_examples);
        } else if self.new_fields.len() >= MAX_TRACKED_PATHS {
            self.truncated = true;
        } else {
            let mut e = NewField::new(record);
            e.observe(v, max_examples);
            self.new_fields.insert(path.to_string(), e);
        }
    }

    fn note_type_mismatch(
        &mut self,
        column: &str,
        declared: ColumnType,
        reason: &str,
        v: &Value,
        record: u64,
        max_examples: usize,
    ) {
        if let Some(e) = self.type_mismatches.get_mut(column) {
            e.observe(v, max_examples);
        } else if self.type_mismatches.len() >= MAX_TRACKED_PATHS {
            self.truncated = true;
        } else {
            let mut e = TypeMismatch::new(record, declared, reason.to_string());
            e.observe(v, max_examples);
            self.type_mismatches.insert(column.to_string(), e);
        }
    }
}

// ---------------------------------------------------------------------------
// The consumed-path trie
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Node {
    /// Sorted by key, looked up by binary search. A schema has a handful of paths per
    /// level, and this scan runs on **every record**: comparing a few short strings
    /// beats hashing one, and it keeps the crate free of a faster-hasher dependency.
    children: Vec<(String, Node)>,
    /// A column selects exactly here, so this whole subtree is consumed.
    terminal: bool,
}

impl Node {
    fn child(&self, key: &str) -> Option<&Node> {
        self.children
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| &self.children[i].1)
    }

    /// Insert-or-get during construction (unsorted; [`Node::sort`] finishes the job).
    fn child_mut(&mut self, key: &str) -> &mut Node {
        match self.children.iter().position(|(k, _)| k == key) {
            Some(i) => &mut self.children[i].1,
            None => {
                self.children.push((key.to_string(), Node::default()));
                &mut self.children.last_mut().unwrap().1
            }
        }
    }

    fn sort(&mut self) {
        self.children.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (_, c) in &mut self.children {
            c.sort();
        }
    }
}

/// The set of source paths the output schema reads, as a trie, so deciding whether a
/// record's key is covered is one small lookup per level instead of a scan over every
/// column.
///
/// A path is **covered** when a column selects it, selects an ancestor of it (a `json`
/// column over a subtree), or selects *into* it (`tags.0` reaches into the `tags`
/// array). The last case is why a miss is only reported where the trie has no child at
/// all: anything else means some column is still reading part of that value.
#[derive(Debug)]
pub(crate) struct ConsumedPaths {
    root: Node,
    /// Some column selects the whole record (`$`), so nothing can be uncovered.
    consumes_all: bool,
}

impl ConsumedPaths {
    pub(crate) fn build<'a, I: IntoIterator<Item = &'a str>>(paths: I) -> Self {
        let mut root = Node::default();
        let mut consumes_all = false;
        for path in paths {
            let segs = segments(path);
            if segs.is_empty() {
                // `$` — the whole record is one column's value.
                consumes_all = true;
                continue;
            }
            let mut node = &mut root;
            for seg in segs {
                node = node.child_mut(seg);
            }
            node.terminal = true;
        }
        root.sort();
        ConsumedPaths { root, consumes_all }
    }

    /// Call `out` once per uncovered leaf in `record`. `buf` is a scratch path buffer
    /// owned by the caller so a steady-state scan allocates nothing.
    fn scan<'a, R: Record<'a>, F: FnMut(&str, R)>(
        &self,
        record: R,
        flatten: bool,
        buf: &mut String,
        out: &mut F,
    ) {
        if self.consumes_all {
            return;
        }
        if record.kind() != Kind::Object {
            // A bare scalar/array record has no key to hang a column on — inference
            // skips it too, so there is nothing to call new.
            return;
        }
        buf.clear();
        walk(record, &self.root, flatten, buf, out);
    }
}

fn walk<'a, R: Record<'a>, F: FnMut(&str, R)>(
    record: R,
    node: &Node,
    flatten: bool,
    buf: &mut String,
    out: &mut F,
) {
    for (k, v) in record.entries() {
        // The trie lookup comes first and the path buffer is only touched afterwards:
        // on a run whose source has *not* drifted — the overwhelmingly common case —
        // every key hits a terminal node and the scan is one binary search per key,
        // with no string building at all.
        match node.child(k) {
            // A column selects exactly here (possibly as `json`) — the subtree is read.
            Some(child) if child.terminal => {}
            // A column reaches deeper. Descend if we can; if the value is not an object
            // the column is indexing into it (or its shape changed under the column),
            // which the coercion path reports — not a new field.
            Some(child) => {
                if v.kind() == Kind::Object {
                    let mark = push_segment(buf, k);
                    walk(v, child, flatten, buf, out);
                    buf.truncate(mark);
                }
            }
            // Nothing in the schema reaches this key: everything under it is lost.
            None => {
                let mark = push_segment(buf, k);
                emit_uncovered(v, flatten, buf, out);
                buf.truncate(mark);
            }
        }
    }
}

/// Append one dotted segment, returning the length to truncate back to.
fn push_segment(buf: &mut String, seg: &str) -> usize {
    let mark = buf.len();
    if mark > 0 {
        buf.push('.');
    }
    buf.push_str(seg);
    mark
}

/// Report an uncovered value the way inference would have named it: with `flatten` on,
/// one event per dotted leaf (exactly the columns you would declare to keep it); with
/// it off, one event for the whole value.
fn emit_uncovered<'a, R: Record<'a>, F: FnMut(&str, R)>(
    v: R,
    flatten: bool,
    buf: &mut String,
    out: &mut F,
) {
    match v.kind() {
        Kind::Object if flatten => {
            let mut any = false;
            for (k, vv) in v.entries() {
                any = true;
                let mark = push_segment(buf, k);
                emit_uncovered(vv, flatten, buf, out);
                buf.truncate(mark);
            }
            // An empty object is a leaf, exactly as inference treats it.
            if !any {
                out(buf, v);
            }
        }
        // A JSON null carries no data to lose — a column here would be all-null anyway.
        Kind::Null => {}
        _ => out(buf, v),
    }
}

// ---------------------------------------------------------------------------
// The per-run tracker
// ---------------------------------------------------------------------------

/// The detector the shaper drives: owns the trie, the scratch buffer, and the report.
#[derive(Debug)]
pub(crate) struct DriftTracker {
    pub(crate) policy: DriftPolicy,
    flatten: bool,
    max_examples: usize,
    consumed: ConsumedPaths,
    buf: String,
    report: DriftReport,
}

impl DriftTracker {
    /// `consumed_paths` are the effective columns' source paths.
    pub(crate) fn new<'a, I: IntoIterator<Item = &'a str>>(
        spec: &DriftSpec,
        flatten: bool,
        consumed_paths: I,
    ) -> Self {
        DriftTracker {
            policy: spec.policy,
            flatten,
            max_examples: spec.max_examples,
            consumed: ConsumedPaths::build(consumed_paths),
            buf: String::new(),
            report: DriftReport {
                policy: spec.policy,
                ..Default::default()
            },
        }
    }

    pub(crate) fn report(&self) -> &DriftReport {
        &self.report
    }

    pub(crate) fn into_report(self) -> DriftReport {
        self.report
    }

    pub(crate) fn rows_scanned(&self) -> u64 {
        self.report.rows_scanned
    }

    /// Scan one record for uncovered paths. `rescue` collects the dropped values when
    /// the policy is `rescue`. Returns the first event's description, if any.
    pub(crate) fn scan_record<'a, R: Record<'a>>(
        &mut self,
        record: R,
        rescue: Option<&mut Map<String, Value>>,
    ) -> Option<String> {
        self.report.rows_scanned += 1;
        let record_idx = self.report.rows_scanned;
        let DriftTracker {
            consumed,
            buf,
            report,
            max_examples,
            flatten,
            ..
        } = self;
        let max_examples = *max_examples;
        let flatten = *flatten;
        let mut first: Option<String> = None;
        let mut rescue = rescue;
        consumed.scan(record, flatten, buf, &mut |path, v| {
            // Only reached for a path the schema does not cover, so a clean run never
            // materializes anything here; a drifted one pays a copy of what it reports.
            let owned = v.to_owned_value();
            report.note_new_field(path, &owned, record_idx, max_examples);
            if first.is_none() {
                first = Some(format!("new field `{path}`"));
            }
            if let Some(m) = rescue.as_deref_mut() {
                m.insert(path.to_string(), owned);
            }
        });
        first
    }

    /// Record a value that was present but did not coerce, and was written as null.
    pub(crate) fn note_type_mismatch(
        &mut self,
        column: &str,
        declared: ColumnType,
        reason: &str,
        value: &Value,
    ) {
        let record_idx = self.report.rows_scanned.max(1);
        self.report.note_type_mismatch(
            column,
            declared,
            reason,
            value,
            record_idx,
            self.max_examples,
        );
    }

    pub(crate) fn note_row_drifted(&mut self) {
        self.report.rows_with_drift += 1;
    }

    pub(crate) fn note_row_quarantined(&mut self) {
        self.report.rows_quarantined += 1;
    }

    pub(crate) fn note_row_rescued(&mut self) {
        self.report.rows_rescued += 1;
    }
}
