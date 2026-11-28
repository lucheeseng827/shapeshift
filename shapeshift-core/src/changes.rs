//! Change patterns (`changes:`): a feed of inserts, updates and deletes, applied to a table.
//!
//! Each record says what happened (`op`), where the new row is (`row`), and, for a delete,
//! where its key is (`key_from`). With `output.apply: upsert` (the default) the table keeps
//! the current state: an insert or update upserts on `merge_on`, a delete removes the key's
//! row. With `apply: changelog` every change is appended with an `_op` column, and nothing
//! is removed. `order_by` names output columns that version a change: the newer change to a
//! key wins, whatever order the feed delivers them in. See `docs/SPEC.md` §5b.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, ShapeError};

/// The column a batch carries, in `apply: upsert`, to say which rows are deletes. It is
/// never written to a table: the merge sink takes it off. The name cannot be a column's,
/// since a spec's column names are checked against it.
pub const CHANGE_COLUMN: &str = "__shapeshift_change";

/// [`CHANGE_COLUMN`]'s values.
pub const CHANGE_UPSERT: u8 = 0;
pub const CHANGE_DELETE: u8 = 1;

/// The column `apply: changelog` adds: `insert`, `update` or `delete`.
pub const OP_COLUMN: &str = "_op";

/// What a change feed's records say, and how to read them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChangesSpec {
    /// A known feed's settings, filled in for whatever the spec leaves out (see
    /// [`ChangesPreset`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset: Option<ChangesPreset>,
    /// The field that says what happened, as a path into the record.
    #[serde(default)]
    pub op: String,
    /// The values of `op` that mean each kind of change. A value is a string, a number or
    /// a boolean (`delete: [true]` on an `is_deleted` field).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub insert: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub update: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delete: Vec<Value>,
    /// Values a feed sends that carry no change of their own (Flink's `-U`, the image of a
    /// row before its update): counted as skipped, and otherwise left alone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip: Vec<Value>,
    /// Where the new row is (`after`). Absent: the record itself is the row. Columns read
    /// their `from` paths against it, except a path that starts with `$`, which reads the
    /// whole record (the envelope).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    /// Where a delete finds its key (`before`). When that is absent or null, the key is read
    /// from `row`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_from: Option<String>,
    /// Output columns that version a change, compared left to right: a change applies only
    /// when it is newer than the key's live row. Empty: changes apply in arrival order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,
    /// With `order_by`: how long a delete is remembered (a duration: `7d`, `36h`), so that a
    /// change older than it, arriving later, does not bring the row back. Default 7 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_retention: Option<String>,
}

/// Feeds whose shape is known. A preset fills in `op`, the op values, `row`, `key_from` and
/// `order_by`, and adds the columns that version a change; anything the spec sets itself
/// wins, and a column the spec declares under the same name is left as declared.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ChangesPreset {
    /// Debezium's envelope from Postgres (`{before, after, op, source}`, the JSON converter
    /// with schemas off), ordered by the log sequence number (`cdc_lsn`).
    DebeziumPostgres,
    /// Debezium's envelope from MySQL, ordered by the binlog position (`cdc_file`,
    /// `cdc_pos`, `cdc_row`).
    DebeziumMysql,
    /// Flink's changelog JSON (`{data, op}`): `+I`, `+U`, `-D`, and `-U` skipped. It carries
    /// no version of its own, and a `-D` carries the row as it was, so a version read from
    /// the row would make every delete stale: changes apply in arrival order, and `order_by`
    /// is refused with this preset.
    FlinkCdc,
}

/// What a preset fills in.
pub struct PresetDefaults {
    pub op: &'static str,
    pub insert: &'static [&'static str],
    pub update: &'static [&'static str],
    pub delete: &'static [&'static str],
    pub skip: &'static [&'static str],
    pub row: &'static str,
    pub key_from: Option<&'static str>,
    pub order_by: &'static [&'static str],
}

impl ChangesPreset {
    /// The settings the preset fills in.
    pub fn defaults(self) -> PresetDefaults {
        match self {
            ChangesPreset::DebeziumPostgres => PresetDefaults {
                op: "op",
                insert: &["c", "r"],
                update: &["u"],
                delete: &["d"],
                skip: &[],
                row: "after",
                key_from: Some("before"),
                order_by: &["cdc_lsn"],
            },
            ChangesPreset::DebeziumMysql => PresetDefaults {
                op: "op",
                insert: &["c", "r"],
                update: &["u"],
                delete: &["d"],
                skip: &[],
                row: "after",
                key_from: Some("before"),
                order_by: &["cdc_file", "cdc_pos", "cdc_row"],
            },
            ChangesPreset::FlinkCdc => PresetDefaults {
                op: "op",
                insert: &["+I"],
                update: &["+U"],
                delete: &["-D"],
                skip: &["-U"],
                row: "data",
                key_from: None,
                order_by: &[],
            },
        }
    }

    /// The columns the preset adds: `(name, type, from)`.
    pub fn columns(self) -> &'static [(&'static str, crate::types::ColumnType, &'static str)] {
        use crate::types::ColumnType::{Int64, String as Str};
        match self {
            ChangesPreset::DebeziumPostgres => &[
                ("cdc_lsn", Int64, "$.source.lsn"),
                ("cdc_ts_ms", Int64, "$.source.ts_ms"),
            ],
            ChangesPreset::DebeziumMysql => &[
                ("cdc_file", Str, "$.source.file"),
                ("cdc_pos", Int64, "$.source.pos"),
                ("cdc_row", Int64, "$.source.row"),
                ("cdc_ts_ms", Int64, "$.source.ts_ms"),
            ],
            ChangesPreset::FlinkCdc => &[],
        }
    }

    /// The op values that are **seed rows**: a read of the table as it stood (Debezium's
    /// initial snapshot and its incremental snapshots, `r`), not a change. A seed row is
    /// older than every change: it takes [`ChangesPreset::seed_version`]
    /// in each `order_by` column, so a change to its key wins whichever arrives first, and a
    /// seed row only fills a key no change has touched.
    ///
    /// Why: a transaction open across Debezium's snapshot on Postgres commits after it, with
    /// LSNs lower than the snapshot's. Ordered by `source.lsn` as recorded, its updates and
    /// deletes lose to the snapshot rows and are dropped as stale (a real feed of it is kept in
    /// `examples/changes/debezium/handoff/`).
    pub fn seed_ops(self) -> &'static [&'static str] {
        match self {
            ChangesPreset::DebeziumPostgres | ChangesPreset::DebeziumMysql => &["r"],
            ChangesPreset::FlinkCdc => &[],
        }
    }

    /// The version a seed row takes in one of the preset's version columns: the lowest the
    /// column holds (an LSN, a binlog position and a row number are never negative, and an
    /// empty file name sorts first). `None` for a column the preset does not add.
    pub fn seed_version(self, column: &str) -> Option<SeedVersion> {
        match (self, column) {
            (ChangesPreset::DebeziumPostgres, "cdc_lsn")
            | (ChangesPreset::DebeziumMysql, "cdc_pos" | "cdc_row") => Some(SeedVersion::Int(0)),
            (ChangesPreset::DebeziumMysql, "cdc_file") => Some(SeedVersion::Str("")),
            _ => None,
        }
    }
}

/// The version a seed row takes in one `order_by` column (see [`ChangesPreset::seed_ops`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedVersion {
    Int(i64),
    Str(&'static str),
}
/// The shortest `changes.tombstone_retention` a spec may set (one hour).
pub const MIN_TOMBSTONE_RETENTION_MS: i64 = 3_600_000;

/// What `apply` does with a change feed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ApplyMode {
    /// The table holds the current state (the default).
    #[default]
    Upsert,
    /// The table holds every change, with an `_op` column.
    Changelog,
}

/// What one change is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
    Skip,
}

impl ChangeKind {
    /// The word `apply: changelog` writes in `_op`.
    pub fn word(self) -> &'static str {
        match self {
            ChangeKind::Insert => "insert",
            ChangeKind::Update => "update",
            ChangeKind::Delete => "delete",
            ChangeKind::Skip => "skip",
        }
    }
}

impl ChangesSpec {
    /// Fill in what the preset knows and the spec left out.
    pub fn apply_preset(&mut self) {
        let Some(p) = self.preset else { return };
        let d = p.defaults();
        let values =
            |l: &[&str]| -> Vec<Value> { l.iter().map(|v| Value::String(v.to_string())).collect() };
        if self.op.trim().is_empty() {
            self.op = d.op.to_string();
        }
        // Each list the spec leaves empty takes the preset's.
        for (mine, theirs) in [
            (&mut self.insert, d.insert),
            (&mut self.update, d.update),
            (&mut self.delete, d.delete),
            (&mut self.skip, d.skip),
        ] {
            if mine.is_empty() {
                *mine = values(theirs);
            }
        }
        if self.row.is_none() {
            self.row = Some(d.row.to_string());
        }
        if self.key_from.is_none() {
            self.key_from = d.key_from.map(str::to_string);
        }
        if self.order_by.is_empty() {
            self.order_by = d.order_by.iter().map(|c| c.to_string()).collect();
        }
    }

    /// Whether an `op` value is a seed row of the spec's preset (see
    /// [`ChangesPreset::seed_ops`]).
    pub fn is_seed(&self, op: &Value) -> bool {
        self.preset.is_some_and(|p| {
            p.seed_ops()
                .iter()
                .any(|s| same_scalar(&Value::String((*s).to_string()), op))
        })
    }

    /// The kind of change an `op` value names, if any.
    pub fn kind_of(&self, op: &Value) -> Option<ChangeKind> {
        let has = |list: &[Value]| list.iter().any(|v| same_scalar(v, op));
        if has(&self.insert) {
            Some(ChangeKind::Insert)
        } else if has(&self.update) {
            Some(ChangeKind::Update)
        } else if has(&self.delete) {
            Some(ChangeKind::Delete)
        } else if has(&self.skip) {
            Some(ChangeKind::Skip)
        } else {
            None
        }
    }

    /// How long a delete is remembered, in milliseconds (`tombstone_retention`, default 7
    /// days).
    pub fn retention_ms(&self) -> Result<i64> {
        match &self.tombstone_retention {
            None => Ok(7 * 86_400_000),
            Some(s) => {
                let ms = parse_duration_ms(s)
                    .map_err(|e| ShapeError::Spec(format!("`changes.tombstone_retention`: {e}")))?;
                // The window counts from the merge that applied the delete, a few seconds
                // before its commit: at an hour or more, that is a rounding error.
                if ms < MIN_TOMBSTONE_RETENTION_MS {
                    return Err(ShapeError::Spec(format!(
                        "`changes.tombstone_retention` is {s}: the least is 1h (a delete is \n                         remembered from the merge that applied it, and a shorter window would \n                         mostly be that merge's own commit)"
                    )));
                }
                Ok(ms)
            }
        }
    }

    /// Every op value the spec names, for a message about one it does not.
    pub fn known_ops(&self) -> String {
        let all: Vec<String> = [&self.insert, &self.update, &self.delete, &self.skip]
            .into_iter()
            .flatten()
            .map(|v| v.to_string())
            .collect();
        all.join(", ")
    }

    /// The spec's own checks; those that need the output (`merge_on`, `apply`) are in
    /// `DatasetSpec::validate`.
    pub fn validate(&self) -> Result<()> {
        let path_ok = |p: &str| !p.trim().is_empty() && !p.split('.').any(|s| s.is_empty());
        if !path_ok(self.op.trim_start_matches('$').trim_start_matches('.')) {
            return Err(ShapeError::Spec(
                "`changes.op` must name the field that says what happened".into(),
            ));
        }
        for (what, p) in [("row", &self.row), ("key_from", &self.key_from)] {
            if let Some(p) = p {
                if !path_ok(p) {
                    return Err(ShapeError::Spec(format!(
                        "`changes.{what}` {p:?} is not a path"
                    )));
                }
            }
        }
        if self.insert.is_empty() && self.update.is_empty() && self.delete.is_empty() {
            return Err(ShapeError::Spec(
                "`changes` names no op value: list the values of `op` under `insert`, \
                 `update` or `delete`"
                    .into(),
            ));
        }
        let lists = [
            ("insert", &self.insert),
            ("update", &self.update),
            ("delete", &self.delete),
            ("skip", &self.skip),
        ];
        let mut seen: Vec<(&str, &Value)> = Vec::new();
        for (name, list) in lists {
            for v in list {
                if !matches!(v, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
                    return Err(ShapeError::Spec(format!(
                        "`changes.{name}` has {v}: an op value is a string, a number or a boolean"
                    )));
                }
                if let Some((other, _)) = seen.iter().find(|(_, w)| same_scalar(w, v)) {
                    return Err(ShapeError::Spec(format!(
                        "op value {v} is in both `changes.{other}` and `changes.{name}`"
                    )));
                }
                seen.push((name, v));
            }
        }
        self.retention_ms()?;
        if self.preset == Some(ChangesPreset::FlinkCdc) && !self.order_by.is_empty() {
            return Err(ShapeError::Spec(
                "`changes.order_by` with `preset: flink-cdc`: Flink's changelog carries no \
                 version of its own, and a delete (`-D`) carries the row as it was, so a version \
                 read from the row would make every delete stale. Drop `order_by` (changes apply \
                 in arrival order)"
                    .into(),
            ));
        }
        if self.tombstone_retention.is_some() && self.order_by.is_empty() {
            return Err(ShapeError::Spec(
                "`changes.tombstone_retention` says how long a delete is remembered against \
                 `order_by`, and the spec has none: set `order_by`, or drop the retention"
                    .into(),
            ));
        }
        for (i, c) in self.order_by.iter().enumerate() {
            if c.trim().is_empty() {
                return Err(ShapeError::Spec(
                    "`changes.order_by` has an empty name".into(),
                ));
            }
            if self.order_by[..i].contains(c) {
                return Err(ShapeError::Spec(format!(
                    "`changes.order_by` names `{c}` twice"
                )));
            }
        }
        Ok(())
    }
}

/// A duration in milliseconds: a number and a unit (`s`, `m`, `h`, `d`, `w`).
pub fn parse_duration_ms(s: &str) -> std::result::Result<i64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: i64 = num
        .parse()
        .map_err(|_| format!("`{s}` is not a duration (try 5d, 12h, 30m or 0s)"))?;
    let unit_ms: i64 = match unit.trim() {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 7 * 86_400_000,
        _ => return Err(format!("`{s}`: give a unit (s, m, h, d or w)")),
    };
    n.checked_mul(unit_ms)
        .ok_or_else(|| format!("`{s}` is too long"))
}

/// Two op values are the same if they are equal JSON scalars. A number compares by value,
/// exactly: `1` and `1.0` are one op, but two integers past 2^53 that round to the same
/// float are not.
fn same_scalar(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let int = |n: &serde_json::Number| {
                n.as_i64()
                    .map(i128::from)
                    .or_else(|| n.as_u64().map(i128::from))
            };
            // A float equals an integer only when it is exactly that integer.
            let float_is = |f: f64, i: i128| {
                f.is_finite() && f.fract() == 0.0 && f.abs() < 1e38 && f as i128 == i
            };
            match (int(x), int(y)) {
                (Some(i), Some(j)) => i == j,
                (Some(i), None) => y.as_f64().is_some_and(|f| float_is(f, i)),
                (None, Some(j)) => x.as_f64().is_some_and(|f| float_is(f, j)),
                (None, None) => x.as_f64() == y.as_f64(),
            }
        }
        _ => a == b,
    }
}
