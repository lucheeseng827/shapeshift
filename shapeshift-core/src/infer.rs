//! Schema inference: watch a sample of JSON records and propose a column list.
//!
//! The rules are deliberately conservative and *widening* — when a path shows more
//! than one scalar shape it falls back to `string`; when it shows a nested/array
//! shape it falls back to `json` (lossless). Inference never invents a type it
//! cannot losslessly hold, so a downstream strict spec derived from it is safe.

use std::collections::HashMap;

use serde_json::Value;

use crate::spec::ColumnSpec;
use crate::types::ColumnType;
use crate::value::flatten_leaves;

/// Per-path accumulator of observed JSON shapes.
#[derive(Default, Clone)]
struct Acc {
    has_bool: bool,
    has_int: bool,
    has_float: bool,
    has_string: bool,
    has_json: bool,
    any_string: bool,
    all_date: bool,
    all_ts: bool,
}

impl Acc {
    fn new() -> Self {
        Acc {
            all_date: true,
            all_ts: true,
            ..Default::default()
        }
    }

    fn observe(&mut self, v: &Value) {
        match v {
            Value::Null => {}
            Value::Bool(_) => self.has_bool = true,
            Value::Number(n) => {
                if n.is_f64() && n.as_i64().is_none() {
                    self.has_float = true;
                } else {
                    self.has_int = true;
                }
            }
            Value::String(s) => {
                self.has_string = true;
                self.any_string = true;
                self.all_date &= looks_like_date(s);
                self.all_ts &= looks_like_timestamp(s);
            }
            Value::Array(_) | Value::Object(_) => self.has_json = true,
        }
    }

    fn decide(&self) -> ColumnType {
        if self.has_json {
            return ColumnType::Json;
        }
        let scalar_categories =
            self.has_bool as u8 + (self.has_int || self.has_float) as u8 + self.has_string as u8;
        if scalar_categories >= 2 {
            // Mixed scalar shapes across the sample → widen to string.
            return ColumnType::String;
        }
        if self.has_string {
            if self.any_string && self.all_ts {
                ColumnType::Timestamp
            } else if self.any_string && self.all_date {
                ColumnType::Date
            } else {
                ColumnType::String
            }
        } else if self.has_float {
            ColumnType::Float64
        } else if self.has_int {
            ColumnType::Int64
        } else if self.has_bool {
            ColumnType::Bool
        } else {
            // Only nulls (or never a value) — default to the lossless string.
            ColumnType::String
        }
    }
}

/// True for a bare `YYYY-MM-DD` calendar date. Uses strict calendar parsing, so an
/// impossible date like `2026-02-30` is *not* inferred as `date` (it falls back to
/// `string`). `%F` == `%Y-%m-%d`.
pub fn looks_like_date(s: &str) -> bool {
    chrono::NaiveDate::parse_from_str(s, "%F").is_ok()
}

/// True for an RFC 3339 timestamp (`2026-07-13T12:00:00Z`, offsets allowed).
pub fn looks_like_timestamp(s: &str) -> bool {
    // Must carry a time component; a bare date is a `date`, not a `timestamp`.
    s.contains('T') && chrono::DateTime::parse_from_rfc3339(s).is_ok()
}

/// Infer a column list from a sample of records. Column order is first-seen. When
/// `flatten` is on, nested objects expand into dotted columns (`user.name`) and
/// arrays become `json` leaves; when off, a top-level nested object becomes a
/// single `json` column.
pub fn infer_columns<'a, I>(samples: I, flatten: bool) -> Vec<ColumnSpec>
where
    I: IntoIterator<Item = &'a Value>,
{
    let mut order: Vec<String> = Vec::new();
    let mut accs: HashMap<String, Acc> = HashMap::new();

    for record in samples {
        if flatten {
            let mut leaves = Vec::new();
            flatten_leaves(record, "", &mut leaves);
            for (path, v) in leaves {
                if path.is_empty() {
                    // A non-object top-level record (e.g. a bare scalar line). Skip —
                    // there is no column name to hang it on.
                    continue;
                }
                observe(&mut order, &mut accs, path, v);
            }
        } else if let Value::Object(map) = record {
            for (k, v) in map {
                observe(&mut order, &mut accs, k.clone(), v);
            }
        }
    }

    order
        .into_iter()
        .map(|name| {
            let ty = accs[&name].decide();
            ColumnSpec {
                name: name.clone(),
                from: None, // path == name for inferred columns
                ty,
                transform: None,
                required: false,
            }
        })
        .collect()
}

fn observe(order: &mut Vec<String>, accs: &mut HashMap<String, Acc>, path: String, v: &Value) {
    match accs.get_mut(&path) {
        Some(acc) => acc.observe(v),
        None => {
            let mut acc = Acc::new();
            acc.observe(v);
            order.push(path.clone());
            accs.insert(path, acc);
        }
    }
}
