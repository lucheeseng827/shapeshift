//! What the shaper needs of an input record — and nothing more.
//!
//! The engine only ever *reads* a record: it walks a path to a leaf, asks what kind of
//! value is there, and takes the scalar out. It never needs to own one. [`Record`] is
//! exactly that read surface, so the hot loop can run over whatever the source produces.
//!
//! **The `serde_json::Value` contract is unchanged.** `&Value` implements this trait
//! right here, in the crate that already depends on serde_json, so `Shaper::push(&value)`
//! keeps compiling and behaving as before — the trait is what a *second* record model
//! becomes possible through, not a replacement for the first. A source that can offer
//! something cheaper (`shapeshift-json` parses to a reusable simd-json tape, whose
//! strings borrow the input buffer instead of being copied into a `String` each) plugs in
//! without `core` learning about it: this crate names no parser.
//!
//! Cursors, not references. `get_key` returns `Self`, not `&Self`, because a tape cursor
//! is a position rather than a place — an index into a flat buffer. `&Value` is `Copy`
//! and satisfies the same shape, so both models fit one signature.

use serde_json::Value;

/// The JSON shapes coercion distinguishes. Narrower than a full JSON type: an integer and
/// a float are told apart because they coerce differently, and that is the whole point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Null,
    Bool,
    /// A number that is exactly representable as `i64`.
    Int,
    /// A number that is not — coercion falls back to `f64`.
    Float,
    Str,
    Array,
    Object,
}

impl Kind {
    /// The word used when a coercion failure has to say what it found.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Bool => "bool",
            Kind::Int | Kind::Float => "number",
            Kind::Str => "string",
            Kind::Array => "array",
            Kind::Object => "object",
        }
    }
}

/// One record, read-only, borrowed for `'a`.
///
/// `Copy` because implementations are cursors — a `&Value` or a tape position — and the
/// hot loop passes them around per column.
pub trait Record<'a>: Copy {
    /// The value under `key`, or `None` if this is not an object or has no such key.
    fn get_key(self, key: &str) -> Option<Self>;
    /// The value at `idx`, or `None` if this is not an array or is too short.
    fn get_index(self, idx: usize) -> Option<Self>;
    /// Which shape this is.
    fn kind(self) -> Kind;
    fn as_bool(self) -> Option<bool>;
    /// The value as an `i64`, if it is a number that fits one exactly.
    fn as_i64(self) -> Option<i64>;
    fn as_f64(self) -> Option<f64>;
    /// The string body, **borrowed for `'a`** — the property that lets a string column
    /// reach Arrow without being copied.
    fn as_str(self) -> Option<&'a str>;
    /// Compact JSON text for this value. Used for `json` columns, so it is on the hot
    /// path for those and should not do more than serialize.
    fn to_json_text(self) -> String;
    /// This value as an owned `serde_json::Value`. Only the rescue and reject paths need
    /// it — both already pay to keep a copy of what they are reporting — so an
    /// implementation may convert rather than borrow.
    fn to_owned_value(self) -> Value;
    /// The object's `(key, value)` pairs; empty for anything that is not an object.
    /// Drives drift detection, which walks a record against the schema's paths.
    fn entries(self) -> impl Iterator<Item = (&'a str, Self)>;
}

impl<'a> Record<'a> for &'a Value {
    fn get_key(self, key: &str) -> Option<Self> {
        match self {
            Value::Object(map) => map.get(key),
            _ => None,
        }
    }

    fn get_index(self, idx: usize) -> Option<Self> {
        match self {
            Value::Array(arr) => arr.get(idx),
            _ => None,
        }
    }

    fn kind(self) -> Kind {
        match self {
            Value::Null => Kind::Null,
            Value::Bool(_) => Kind::Bool,
            Value::Number(n) => {
                if n.as_i64().is_some() {
                    Kind::Int
                } else {
                    Kind::Float
                }
            }
            Value::String(_) => Kind::Str,
            Value::Array(_) => Kind::Array,
            Value::Object(_) => Kind::Object,
        }
    }

    fn as_bool(self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn as_i64(self) -> Option<i64> {
        match self {
            Value::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    fn as_f64(self) -> Option<f64> {
        match self {
            Value::Number(n) => n.as_f64(),
            _ => None,
        }
    }

    fn as_str(self) -> Option<&'a str> {
        match self {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    fn to_json_text(self) -> String {
        self.to_string()
    }

    fn to_owned_value(self) -> Value {
        self.clone()
    }

    fn entries(self) -> impl Iterator<Item = (&'a str, Self)> {
        let map = match self {
            Value::Object(m) => Some(m),
            _ => None,
        };
        map.into_iter()
            .flat_map(|m| m.iter().map(|(k, v)| (k.as_str(), v)))
    }
}
