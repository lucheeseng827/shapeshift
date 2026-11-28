//! MySQL values as JSON records, and positions back as statement parameters.

use mysql::consts::ColumnType;
use serde_json::{Number, Value};
use shapeshift_core::{Result, ShapeError};

/// What a column's values need to become JSON.
#[derive(Debug, Clone)]
pub(crate) struct Meta {
    pub name: String,
    pub ty: ColumnType,
    /// The display length: `TINYINT(1)` is a boolean, `BIT(1)` too.
    pub length: u32,
    /// The binary character set: a BLOB or BINARY column, not text.
    pub binary: bool,
}

/// The character set number MySQL gives binary strings.
const BINARY_CHARSET: u16 = 63;

impl Meta {
    pub fn of(c: &mysql::Column) -> Meta {
        Meta {
            name: c.name_str().into_owned(),
            ty: c.column_type(),
            length: c.column_length(),
            binary: c.character_set() == BINARY_CHARSET,
        }
    }

    fn texty(&self) -> bool {
        use ColumnType::*;
        matches!(
            self.ty,
            MYSQL_TYPE_VARCHAR
                | MYSQL_TYPE_VAR_STRING
                | MYSQL_TYPE_STRING
                | MYSQL_TYPE_TINY_BLOB
                | MYSQL_TYPE_MEDIUM_BLOB
                | MYSQL_TYPE_LONG_BLOB
                | MYSQL_TYPE_BLOB
                | MYSQL_TYPE_ENUM
                | MYSQL_TYPE_SET
        )
    }

    /// Whether MySQL orders this column's values as the position rules compare them: numbers
    /// by value, instants and dates by time. Text is ordered by its collation (`a` before `B`
    /// under a case-insensitive one), which byte order is not, and a DECIMAL arrives as text;
    /// neither may carry a position. A tiebreak must also be exact: no floats.
    pub fn orderable(&self, tiebreak: bool) -> bool {
        use ColumnType::*;
        match self.ty {
            MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_YEAR => true,
            MYSQL_TYPE_DATE
            | MYSQL_TYPE_NEWDATE
            | MYSQL_TYPE_DATETIME
            | MYSQL_TYPE_DATETIME2
            | MYSQL_TYPE_TIMESTAMP
            | MYSQL_TYPE_TIMESTAMP2 => true,
            MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => !tiebreak,
            _ => false,
        }
    }

    /// Refuse a column no record can hold faithfully, before any row is read.
    pub fn check(&self) -> Result<()> {
        use ColumnType::*;
        let refuse = |what: &str, fix: &str| {
            Err(ShapeError::Source(format!(
                "column `{}` is {what}, which a record cannot hold as text: {fix} in the query",
                self.name
            )))
        };
        match self.ty {
            MYSQL_TYPE_GEOMETRY => refuse("a geometry", "select ST_AsText(…) or ST_AsGeoJSON(…)"),
            MYSQL_TYPE_VECTOR => refuse("a vector", "select VECTOR_TO_STRING(…)"),
            _ if self.texty() && self.binary => refuse(
                "binary (BLOB, BINARY, VARBINARY)",
                "select HEX(…), TO_BASE64(…) or CAST(… AS CHAR)",
            ),
            _ => Ok(()),
        }
    }
}

fn bad(m: &Meta, what: &str) -> ShapeError {
    ShapeError::Source(format!("column `{}`: {what}", m.name))
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`: the instant as a timestamp cursor reads it.
fn instant(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> String {
    if us == 0 {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    } else {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}Z")
    }
}

/// One value as JSON. The session's time zone is UTC, so a TIMESTAMP reads as UTC; a DATETIME
/// has no zone and is read as UTC too.
pub(crate) fn json_of(m: &Meta, v: mysql::Value) -> Result<Value> {
    use mysql::Value as V;
    use ColumnType::*;
    Ok(match (m.ty, v) {
        (_, V::NULL) => Value::Null,
        (MYSQL_TYPE_TINY, V::Int(i)) if m.length == 1 => Value::Bool(i != 0),
        (MYSQL_TYPE_TINY, V::UInt(i)) if m.length == 1 => Value::Bool(i != 0),
        (_, V::Int(i)) => Value::from(i),
        (_, V::UInt(u)) => Value::from(u),
        // The shortest decimal that reads back as the same FLOAT: `1.1`, not `1.100000023…`.
        (_, V::Float(f)) => f
            .to_string()
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map_or(Value::Null, Value::Number),
        (_, V::Double(f)) => Number::from_f64(f).map_or(Value::Null, Value::Number),
        // A zero date (`0000-00-00`) is no instant: null.
        (_, V::Date(0, 0, 0, ..)) => Value::Null,
        (MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE, V::Date(y, mo, d, ..)) => {
            Value::String(format!("{y:04}-{mo:02}-{d:02}"))
        }
        (_, V::Date(y, mo, d, h, mi, s, us)) => Value::String(instant(y, mo, d, h, mi, s, us)),
        (_, V::Time(neg, days, h, mi, s, us)) => {
            let hours = u64::from(days) * 24 + u64::from(h);
            let sign = if neg { "-" } else { "" };
            Value::String(if us == 0 {
                format!("{sign}{hours:02}:{mi:02}:{s:02}")
            } else {
                format!("{sign}{hours:02}:{mi:02}:{s:02}.{us:06}")
            })
        }
        (MYSQL_TYPE_BIT, V::Bytes(b)) => {
            if b.len() > 8 {
                return Err(bad(m, "a BIT wider than 64"));
            }
            let n = b.iter().fold(0u64, |n, x| (n << 8) | u64::from(*x));
            if m.length == 1 {
                Value::Bool(n != 0)
            } else {
                Value::from(n)
            }
        }
        (MYSQL_TYPE_JSON, V::Bytes(b)) => serde_json::from_slice(&b)
            .map_err(|e| bad(m, &format!("its JSON does not parse ({e})")))?,
        // DECIMAL stays a string: a float would round it.
        (_, V::Bytes(b)) => Value::String(
            String::from_utf8(b)
                .map_err(|_| bad(m, "its text is not UTF-8 (cast it in the query)"))?,
        ),
    })
}

/// A position's value, or a `start`, as a statement parameter: an instant or a date as a
/// date, a number as a number, anything else as text.
pub(crate) fn param_of(v: &Value) -> mysql::Value {
    match v {
        Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => mysql::Value::Int(i),
            (_, Some(u), _) => mysql::Value::UInt(u),
            (_, _, Some(f)) => mysql::Value::Double(f),
            _ => mysql::Value::Bytes(n.to_string().into_bytes()),
        },
        Value::Bool(b) => mysql::Value::Int(i64::from(*b)),
        Value::String(s) => text_param(s),
        other => mysql::Value::Bytes(other.to_string().into_bytes()),
    }
}

/// A `start` as written in a spec or on a command line.
pub(crate) fn start_param(s: &str) -> mysql::Value {
    let t = s.trim();
    if let Ok(i) = t.parse::<i64>() {
        return mysql::Value::Int(i);
    }
    text_param(t)
}

fn text_param(s: &str) -> mysql::Value {
    use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
    let date = |d: NaiveDateTime| {
        mysql::Value::Date(
            d.year().clamp(0, 9999) as u16,
            d.month() as u8,
            d.day() as u8,
            d.hour() as u8,
            d.minute() as u8,
            d.second() as u8,
            d.nanosecond().min(999_999_999) / 1000,
        )
    };
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return date(t.naive_utc());
    }
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(t) = NaiveDateTime::parse_from_str(s, f) {
            return date(t);
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return date(d.and_hms_opt(0, 0, 0).expect("midnight"));
    }
    mysql::Value::Bytes(s.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql::Value as V;
    use serde_json::json;

    fn meta(ty: ColumnType, length: u32, binary: bool) -> Meta {
        Meta {
            name: "c".into(),
            ty,
            length,
            binary,
        }
    }

    #[test]
    fn values_map_as_the_spec_says() {
        use ColumnType::*;
        let j = |ty, len, v| json_of(&meta(ty, len, false), v).unwrap();
        assert_eq!(j(MYSQL_TYPE_TINY, 1, V::Int(1)), json!(true));
        assert_eq!(j(MYSQL_TYPE_TINY, 4, V::Int(7)), json!(7));
        assert_eq!(
            j(MYSQL_TYPE_LONGLONG, 20, V::UInt(u64::MAX)),
            json!(u64::MAX)
        );
        assert_eq!(
            j(MYSQL_TYPE_NEWDECIMAL, 12, V::Bytes(b"12.50".to_vec())),
            json!("12.50")
        );
        assert_eq!(
            j(MYSQL_TYPE_DATETIME, 19, V::Date(2026, 11, 1, 6, 39, 0, 0)),
            json!("2026-11-01T06:39:00Z")
        );
        assert_eq!(
            j(
                MYSQL_TYPE_TIMESTAMP,
                26,
                V::Date(2026, 11, 1, 6, 39, 0, 120)
            ),
            json!("2026-11-01T06:39:00.000120Z")
        );
        assert_eq!(
            j(MYSQL_TYPE_DATE, 10, V::Date(2026, 2, 3, 0, 0, 0, 0)),
            json!("2026-02-03")
        );
        assert_eq!(
            j(MYSQL_TYPE_DATETIME, 19, V::Date(0, 0, 0, 0, 0, 0, 0)),
            json!(null)
        );
        assert_eq!(
            j(MYSQL_TYPE_TIME, 10, V::Time(true, 1, 2, 3, 4, 0)),
            json!("-26:03:04")
        );
        assert_eq!(
            j(MYSQL_TYPE_JSON, 0, V::Bytes(br#"{"a":[1]}"#.to_vec())),
            json!({"a": [1]})
        );
        assert_eq!(j(MYSQL_TYPE_BIT, 1, V::Bytes(vec![1])), json!(true));
        assert_eq!(j(MYSQL_TYPE_BIT, 16, V::Bytes(vec![1, 2])), json!(258));
        assert_eq!(
            j(MYSQL_TYPE_VAR_STRING, 40, V::Bytes(b"Ada".to_vec())),
            json!("Ada")
        );
        assert_eq!(j(MYSQL_TYPE_VAR_STRING, 40, V::NULL), json!(null));
        assert_eq!(j(MYSQL_TYPE_FLOAT, 12, V::Float(1.1)), json!(1.1));
        assert!(json_of(&meta(MYSQL_TYPE_VAR_STRING, 4, false), V::Bytes(vec![0xff])).is_err());
    }

    #[test]
    fn only_numbers_and_instants_carry_a_position() {
        use ColumnType::*;
        assert!(meta(MYSQL_TYPE_DATETIME, 19, true).orderable(true));
        assert!(meta(MYSQL_TYPE_LONGLONG, 20, true).orderable(true));
        assert!(meta(MYSQL_TYPE_DOUBLE, 22, true).orderable(false));
        assert!(!meta(MYSQL_TYPE_DOUBLE, 22, true).orderable(true));
        assert!(!meta(MYSQL_TYPE_VAR_STRING, 36, false).orderable(true));
        assert!(!meta(MYSQL_TYPE_NEWDECIMAL, 12, true).orderable(false));
    }

    #[test]
    fn binary_and_spatial_columns_are_refused_naming_the_fix() {
        use ColumnType::*;
        let e = meta(MYSQL_TYPE_BLOB, 0, true)
            .check()
            .unwrap_err()
            .to_string();
        assert!(e.contains("HEX"), "{e}");
        assert!(meta(MYSQL_TYPE_GEOMETRY, 0, true).check().is_err());
        // Numbers and dates carry the binary character set too, and are fine.
        assert!(meta(MYSQL_TYPE_LONG, 11, true).check().is_ok());
        assert!(
            meta(MYSQL_TYPE_BLOB, 0, false).check().is_ok(),
            "TEXT is a blob type"
        );
    }

    #[test]
    fn positions_and_starts_bind_as_their_kind() {
        assert_eq!(
            param_of(&json!("2026-11-01T06:39:00.5Z")),
            V::Date(2026, 11, 1, 6, 39, 0, 500_000)
        );
        assert_eq!(
            param_of(&json!("2026-02-03")),
            V::Date(2026, 2, 3, 0, 0, 0, 0)
        );
        assert_eq!(param_of(&json!(42)), V::Int(42));
        assert_eq!(param_of(&json!(u64::MAX)), V::UInt(u64::MAX));
        assert_eq!(param_of(&json!("abc")), V::Bytes(b"abc".to_vec()));
        assert_eq!(start_param("0"), V::Int(0));
        assert_eq!(
            start_param("2020-01-01 10:00:00"),
            V::Date(2020, 1, 1, 10, 0, 0, 0)
        );
    }
}
