//! The named transform library. A transform is a pure `Value -> Value` rewrite
//! applied to a selected field *before* it is coerced to the column's logical
//! type. Temporal parsing (string/epoch → date/timestamp) is not here — it is
//! driven by the target [`ColumnType`](crate::types::ColumnType) in coercion, so
//! the transform set stays small and orthogonal.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The closed set of value transforms.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Transform {
    /// Lowercase a string (non-strings pass through unchanged).
    Lowercase,
    /// Uppercase a string.
    Uppercase,
    /// Trim ASCII/Unicode whitespace from a string.
    Trim,
    /// Serialize any value to a compact JSON string (the way to keep a nested
    /// object/array as a lossless `string`/`json` column).
    JsonEncode,
    /// Stringify a scalar (numbers/bools → their JSON text; strings unchanged;
    /// null stays null).
    ToString,
    /// Multiply a number by 100 and floor to an integer — the canonical
    /// dollars→cents money normalization.
    DollarsToCents,
    /// Absolute value of a number.
    Abs,
    /// Coerce an empty string to null (so a downstream `required` check fires, or
    /// the cell writes as null instead of `""`).
    EmptyToNull,
}

impl Transform {
    /// Apply the transform. Total: it never fails; a type it does not apply to
    /// passes through untouched (coercion is where a genuine mismatch surfaces).
    pub fn apply(self, v: Value) -> Value {
        match self {
            Transform::Lowercase => match v {
                Value::String(s) => Value::String(s.to_lowercase()),
                other => other,
            },
            Transform::Uppercase => match v {
                Value::String(s) => Value::String(s.to_uppercase()),
                other => other,
            },
            Transform::Trim => match v {
                Value::String(s) => Value::String(s.trim().to_string()),
                other => other,
            },
            Transform::JsonEncode => match v {
                // Already a scalar string: leave it (encoding "abc" → "\"abc\"" is
                // surprising). Only wrap composite / non-string values.
                Value::String(_) => v,
                other => Value::String(other.to_string()),
            },
            Transform::ToString => match v {
                Value::Null => Value::Null,
                Value::String(_) => v,
                Value::Bool(b) => Value::String(b.to_string()),
                Value::Number(n) => Value::String(n.to_string()),
                other => Value::String(other.to_string()),
            },
            Transform::DollarsToCents => match v.as_f64() {
                Some(f) => Value::from((f * 100.0).round() as i64),
                None => v,
            },
            Transform::Abs => match &v {
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        // `i64::MIN.abs()` overflows and panics — surface its magnitude
                        // as f64 so a downstream int64 coercion rejects it instead.
                        match i.checked_abs() {
                            Some(a) => Value::from(a),
                            None => Value::from(i.unsigned_abs() as f64),
                        }
                    } else if let Some(f) = n.as_f64() {
                        Value::from(f.abs())
                    } else {
                        v
                    }
                }
                _ => v,
            },
            Transform::EmptyToNull => match &v {
                Value::String(s) if s.is_empty() => Value::Null,
                _ => v,
            },
        }
    }
}
