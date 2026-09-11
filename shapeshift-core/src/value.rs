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

use crate::record::{Kind, Record};

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
///
/// This re-splits `path` on every call. The shaper's hot loop uses [`compile_path`] +
/// [`select_compiled`] instead, which does the split once per column at build time.
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

/// One path segment, compiled once. Carries the literal object key *and* its reading as
/// an array index (`None` when the segment is not all-digits), because which one applies
/// depends on the shape of the value actually met at that depth — an object is keyed, an
/// array is indexed.
#[derive(Debug, Clone)]
pub struct Segment {
    key: String,
    index: Option<usize>,
}

/// Compile a dotted path into its segments once, so the hot loop never re-splits it.
/// Same grammar as [`select`]: leading `$`/`$.` dropped, `.` separates, an all-digits
/// segment doubles as an array index.
pub fn compile_path(path: &str) -> Vec<Segment> {
    segments(path)
        .into_iter()
        .map(|s| Segment {
            key: s.to_string(),
            index: s.parse().ok(),
        })
        .collect()
}

/// Resolve a pre-compiled path against a record. Identical semantics to [`select`],
/// with no allocation per call — the whole point of compiling the path.
///
/// Generic over [`Record`], so the same walk serves an owned `serde_json::Value` and a
/// borrowed tape cursor. Which one applies at a given depth still depends on the value
/// actually met there: an object is keyed, an array is indexed.
pub fn select_compiled<'a, R: Record<'a>>(record: R, segs: &[Segment]) -> Option<R> {
    let mut cur = record;
    for seg in segs {
        cur = match cur.kind() {
            Kind::Object => cur.get_key(seg.key.as_str())?,
            Kind::Array => cur.get_index(seg.index?)?,
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
