//! Postgres values as JSON records, and positions back as statement parameters.

use std::error::Error;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use postgres::types::{FromSql, Kind, ToSql, Type};
use serde_json::{Number, Value};
use shapeshift_core::{Result, ShapeError};

type Boxed = Box<dyn Error + Sync + Send>;

/// A NUMERIC, decoded from its binary form to the exact decimal text Postgres prints.
struct Numeric(String);

impl<'a> FromSql<'a> for Numeric {
    fn from_sql(_: &Type, raw: &'a [u8]) -> std::result::Result<Self, Boxed> {
        Ok(Numeric(numeric_text(raw)?))
    }
    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

/// NUMERIC's binary form: ndigits, weight, sign, dscale (each 16 bits), then base-10000 digits.
fn numeric_text(raw: &[u8]) -> std::result::Result<String, Boxed> {
    let word = |i: usize| -> std::result::Result<u16, Boxed> {
        raw.get(i * 2..i * 2 + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(|| "a NUMERIC shorter than its header".into())
    };
    let (ndigits, weight, sign, dscale) = (
        word(0)? as usize,
        word(1)? as i16,
        word(2)?,
        word(3)? as usize,
    );
    match sign {
        0xC000 => return Ok("NaN".into()),
        0xD000 => return Ok("Infinity".into()),
        0xF000 => return Ok("-Infinity".into()),
        _ => {}
    }
    let digits: Vec<u16> = (0..ndigits)
        .map(|i| word(4 + i))
        .collect::<std::result::Result<_, _>>()?;
    // The integer part: groups 0..=weight; the fraction: the groups after, to dscale digits.
    let mut int = String::new();
    for g in 0..=weight.max(-1) as i32 {
        let d = digits.get(g as usize).copied().unwrap_or(0);
        if int.is_empty() {
            int = d.to_string();
        } else {
            int.push_str(&format!("{d:04}"));
        }
    }
    if int.is_empty() {
        int = "0".into();
    }
    let mut frac = String::new();
    let mut g = weight as i32 + 1;
    while frac.len() < dscale {
        let d = if g >= 0 {
            digits.get(g as usize).copied().unwrap_or(0)
        } else {
            0
        };
        frac.push_str(&format!("{d:04}"));
        g += 1;
    }
    frac.truncate(dscale);
    let mut out = if sign == 0x4000 {
        format!("-{int}")
    } else {
        int
    };
    if dscale > 0 {
        out.push('.');
        out.push_str(&frac);
    }
    Ok(out)
}

/// An enum's label (its binary form is the label's text).
struct Label(String);

impl<'a> FromSql<'a> for Label {
    fn from_sql(_: &Type, raw: &'a [u8]) -> std::result::Result<Self, Boxed> {
        Ok(Label(std::str::from_utf8(raw)?.to_string()))
    }
    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`: the instant as a timestamp cursor reads it.
fn instant(t: NaiveDateTime) -> String {
    if t.nanosecond() == 0 {
        t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        t.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    }
}

/// Whether a column's values carry a position: Postgres orders them as the position rules
/// compare them. Text sorts by its collation and NUMERIC arrives as text, so neither may; a
/// UUID's text sorts as its bytes do, which makes it a tiebreak; a float is no tiebreak.
pub(crate) fn orderable(ty: &Type, tiebreak: bool) -> bool {
    match *ty {
        Type::INT2 | Type::INT4 | Type::INT8 | Type::OID => true,
        Type::DATE | Type::TIMESTAMP | Type::TIMESTAMPTZ => true,
        Type::FLOAT4 | Type::FLOAT8 => !tiebreak,
        Type::UUID => tiebreak,
        _ => false,
    }
}

/// Refuse a column no record holds faithfully, naming the cast, before any row is read.
pub(crate) fn check(name: &str, ty: &Type) -> Result<()> {
    let readable = matches!(
        *ty,
        Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::OID
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
            | Type::CHAR
            | Type::DATE
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::TIME
            | Type::JSON
            | Type::JSONB
            | Type::UUID
    ) || matches!(ty.kind(), Kind::Enum(_));
    if readable {
        return Ok(());
    }
    let fix = match *ty {
        Type::BYTEA => "select encode(…, 'hex') or encode(…, 'base64')",
        _ if matches!(ty.kind(), Kind::Array(_)) => "select to_json(…)",
        _ => "cast it to text (…::text)",
    };
    Err(ShapeError::Source(format!(
        "column `{name}` is of type {}, which this source does not read: {fix} in the query",
        ty.name()
    )))
}

fn bad(name: &str, e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Source(format!("column `{name}`: {e}"))
}

/// Column `i` of `row` as JSON. A session in UTC reads a timestamptz as UTC; a timestamp has
/// no zone and is read as UTC too.
pub(crate) fn json_of(row: &postgres::Row, i: usize, name: &str, ty: &Type) -> Result<Value> {
    macro_rules! get {
        ($t:ty) => {
            row.try_get::<_, Option<$t>>(i).map_err(|e| bad(name, e))?
        };
    }
    Ok(match *ty {
        Type::BOOL => get!(bool).map(Value::Bool),
        Type::INT2 => get!(i16).map(Value::from),
        Type::INT4 => get!(i32).map(Value::from),
        Type::INT8 => get!(i64).map(Value::from),
        Type::OID => get!(u32).map(Value::from),
        // A non-finite float is written as Postgres prints it, as a NUMERIC's NaN is: a JSON
        // number cannot hold it, and null would lose it.
        Type::FLOAT4 => get!(f32).map(|f| float(f64::from(f), Some(f.to_string()))),
        Type::FLOAT8 => get!(f64).map(|f| float(f, None)),
        Type::NUMERIC => get!(Numeric).map(|n| Value::String(n.0)),
        Type::DATE => get!(NaiveDate).map(|d| Value::String(d.format("%Y-%m-%d").to_string())),
        Type::TIMESTAMP => get!(NaiveDateTime).map(|t| Value::String(instant(t))),
        Type::TIMESTAMPTZ => get!(DateTime<Utc>).map(|t| Value::String(instant(t.naive_utc()))),
        Type::TIME => get!(NaiveTime).map(|t| {
            Value::String(if t.nanosecond() == 0 {
                t.format("%H:%M:%S").to_string()
            } else {
                t.format("%H:%M:%S%.6f").to_string()
            })
        }),
        Type::JSON | Type::JSONB => get!(Value),
        Type::UUID => get!(uuid::Uuid).map(|u| Value::String(u.to_string())),
        // The internal one-byte `"char"`.
        Type::CHAR => get!(i8).map(|c| Value::String(char::from(c as u8).to_string())),
        _ if matches!(ty.kind(), Kind::Enum(_)) => get!(Label).map(|l| Value::String(l.0)),
        _ => get!(String).map(Value::String),
    }
    .unwrap_or(Value::Null))
}

/// A float as JSON: a number, or for NaN and the infinities the text Postgres prints. A
/// `real`'s `shortest` is its own shortest decimal, so `1.1` stays `1.1`.
fn float(f: f64, shortest: Option<String>) -> Value {
    if f.is_nan() {
        return Value::String("NaN".into());
    }
    if f.is_infinite() {
        return Value::String(if f > 0.0 { "Infinity" } else { "-Infinity" }.into());
    }
    shortest
        .and_then(|s| s.parse::<f64>().ok())
        .or(Some(f))
        .and_then(Number::from_f64)
        .map_or(Value::Null, Value::Number)
}

/// A position's value, or a `start`, as a parameter of the type the statement expects there.
pub(crate) fn param_of(ty: &Type, v: &Value) -> Result<Box<dyn ToSql + Sync + Send>> {
    let text = match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let nope = || {
        ShapeError::Source(format!(
            "the lower bound {v} does not read as the {} the query compares it with",
            ty.name()
        ))
    };
    let instant = || -> Option<NaiveDateTime> {
        chrono::DateTime::parse_from_rfc3339(&text)
            .map(|t| t.naive_utc())
            .ok()
            .or_else(|| NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f").ok())
            .or_else(|| {
                NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
            })
    };
    Ok(match *ty {
        Type::INT2 => Box::new(text.parse::<i16>().map_err(|_| nope())?),
        Type::INT4 => Box::new(text.parse::<i32>().map_err(|_| nope())?),
        Type::INT8 => Box::new(text.parse::<i64>().map_err(|_| nope())?),
        Type::OID => Box::new(text.parse::<u32>().map_err(|_| nope())?),
        Type::FLOAT4 => Box::new(text.parse::<f32>().map_err(|_| nope())?),
        Type::FLOAT8 => Box::new(text.parse::<f64>().map_err(|_| nope())?),
        Type::DATE => Box::new(instant().ok_or_else(nope)?.date()),
        Type::TIMESTAMP => Box::new(instant().ok_or_else(nope)?),
        Type::TIMESTAMPTZ => Box::new(instant().ok_or_else(nope)?.and_utc()),
        Type::UUID => Box::new(uuid::Uuid::parse_str(&text).map_err(|_| nope())?),
        _ => Box::new(text),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> String {
        let mut raw = Vec::new();
        for w in [ndigits, weight as u16, sign, dscale] {
            raw.extend(w.to_be_bytes());
        }
        for d in digits {
            raw.extend(d.to_be_bytes());
        }
        numeric_text(&raw).unwrap()
    }

    #[test]
    fn numerics_decode_to_their_exact_text() {
        assert_eq!(numeric(2, 0, 0, 2, &[12, 5000]), "12.50");
        // Digits past the scale are not printed.
        assert_eq!(numeric(3, 1, 0, 0, &[1, 2345, 6789]), "12345");
        assert_eq!(numeric(1, -1, 0, 4, &[5]), "0.0005");
        assert_eq!(numeric(1, 0, 0x4000, 0, &[42]), "-42");
        assert_eq!(numeric(0, 0, 0, 2, &[]), "0.00");
        assert_eq!(numeric(0, 0, 0xC000, 0, &[]), "NaN");
        assert_eq!(numeric(3, 1, 0, 3, &[1, 2, 3000]), "10002.300");
    }

    #[test]
    fn only_numbers_instants_and_uuids_carry_a_position() {
        assert!(orderable(&Type::TIMESTAMPTZ, true));
        assert!(orderable(&Type::INT8, true));
        assert!(orderable(&Type::UUID, true) && !orderable(&Type::UUID, false));
        assert!(orderable(&Type::FLOAT8, false) && !orderable(&Type::FLOAT8, true));
        assert!(!orderable(&Type::TEXT, true) && !orderable(&Type::NUMERIC, false));
        let e = check("b", &Type::BYTEA).unwrap_err().to_string();
        assert!(e.contains("encode"), "{e}");
        assert!(check("n", &Type::NUMERIC).is_ok());
    }

    #[test]
    fn non_finite_floats_are_kept_as_text() {
        assert_eq!(float(f64::NAN, None), serde_json::json!("NaN"));
        assert_eq!(
            float(f64::NEG_INFINITY, None),
            serde_json::json!("-Infinity")
        );
        assert_eq!(
            float(f64::from(1.1f32), Some(1.1f32.to_string())),
            serde_json::json!(1.1)
        );
    }

    #[test]
    fn bounds_bind_as_the_type_compared_with() {
        assert!(param_of(
            &Type::TIMESTAMPTZ,
            &serde_json::json!("2026-11-01T06:39:00Z")
        )
        .is_ok());
        assert!(param_of(&Type::DATE, &serde_json::json!("2026-11-01")).is_ok());
        assert!(param_of(&Type::INT8, &serde_json::json!(42)).is_ok());
        assert!(param_of(&Type::INT8, &serde_json::json!("x")).is_err());
    }
}
