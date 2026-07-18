//! Path selection into a `serde_json::Value`.
//!
//! Paths are dotted (`user.name`), tolerate a leading `$` / `$.` (so JSON-pointer
//! muscle memory works), and treat an all-digits segment as an array index
//! (`tags.0`). A path that does not resolve yields `None` — never an error — which
//! the shaper turns into a null (or a row rejection for a `required` column).
//!
//! **Known limitation (v0.1):** the separator is a literal `.` with no escaping, so a
//! *literal* JSON key that contains a dot (`{"a.b": 1}`) is indistinguishable from the
//! nested path `a → b` (`{"a": {"b": 1}}`) — during `flatten` both surface as the
//! column `a.b`, and inference would merge them. Sources with dotted top-level keys are
//! uncommon; an escaped/unambiguous path codec is on the roadmap. Until then, declare
//! such columns explicitly (with a `from` that avoids the collision) or pre-rename the
//! keys upstream.

use serde_json::Value;

/// Split a path spec into its segments, dropping a leading `$`/`$.`.
pub fn segments(path: &str) -> Vec<&str> {
    let p = path.strip_prefix('$').unwrap_or(path);
    let p = p.strip_prefix('.').unwrap_or(p);
    if p.is_empty() {
        return Vec::new();
    }
    p.split('.').collect()
}

/// Resolve a dotted path against a record, returning a borrow of the leaf value if
/// present. An empty path (`$`) selects the whole record.
pub fn select<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = record;
    for seg in segments(path) {
        cur = match cur {
            Value::Object(map) => map.get(seg)?,
            Value::Array(arr) => {
                let idx: usize = seg.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(cur)
}

/// Recursively expand an object into `(dotted_path, leaf_value)` pairs. Objects are
/// descended into; arrays and scalars are leaves. Used by schema inference when
/// `flatten` is on. `prefix` is the accumulated dotted path ("" at the root).
pub fn flatten_leaves<'a>(value: &'a Value, prefix: &str, out: &mut Vec<(String, &'a Value)>) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_leaves(v, &key, out);
            }
        }
        // Scalars, arrays, and empty objects are leaves at `prefix`.
        _ => out.push((prefix.to_string(), value)),
    }
}
