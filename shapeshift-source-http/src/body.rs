//! A page's body, as records.

use std::io::{BufReader, Cursor};

use serde_json::Value;
use shapeshift_core::{CsvOptions, HttpFormat, HttpSourceSpec, Result, ShapeError, XmlOptions};
use shapeshift_json::{CsvReader, JsonError, JsonlReader, XmlReader};

use crate::client::{display_url, Page};

/// The records on `page`, and (for a JSON body) the body itself, which paging reads its next
/// link or token from. A record that does not parse fails the run: its cursor value cannot
/// be judged, so the position could not be trusted past it.
pub(crate) fn records(
    spec: &HttpSourceSpec,
    csv: Option<&CsvOptions>,
    xml: Option<&XmlOptions>,
    page: &Page,
) -> Result<(Vec<Value>, Option<Value>)> {
    let at = display_url(&page.url);
    let parse = |e: JsonError| {
        ShapeError::Source(format!(
            "a record from {at} does not parse ({e}): the run stops here"
        ))
    };
    let collect = |it: Box<dyn Iterator<Item = std::result::Result<Value, JsonError>> + '_>| {
        it.map(|r| r.map_err(parse)).collect::<Result<Vec<_>>>()
    };
    if spec.soap.is_some() {
        if let Some(f) = crate::soap::fault(&page.body) {
            return Err(ShapeError::Source(format!(
                "{at} answered a SOAP fault: {f}"
            )));
        }
    }
    match spec.body_format() {
        HttpFormat::Json => {
            let body: Value = if page.body.iter().all(u8::is_ascii_whitespace) {
                Value::Null
            } else {
                serde_json::from_slice(&page.body).map_err(|e| {
                    ShapeError::Source(format!("the body from {at} is not JSON ({e})"))
                })?
            };
            let found = match &spec.records {
                Some(path) => shapeshift_core::select(&body, path)
                    .cloned()
                    .unwrap_or(Value::Null),
                None => body.clone(),
            };
            let records = match found {
                Value::Null => Vec::new(),
                Value::Array(a) => a,
                Value::Object(_) if spec.records.is_none() => vec![found],
                other => {
                    return Err(ShapeError::Source(format!(
                        "`records: {}` in the body from {at} is {}, not an array of records",
                        spec.records.as_deref().unwrap_or(""),
                        kind(&other)
                    )))
                }
            };
            Ok((records, Some(body)))
        }
        HttpFormat::Jsonl => {
            let r = JsonlReader::new(BufReader::new(Cursor::new(&page.body[..])));
            Ok((collect(Box::new(r))?, None))
        }
        HttpFormat::Csv | HttpFormat::Tsv => {
            let opts = csv.cloned().unwrap_or_default();
            let r = CsvReader::new(
                Cursor::new(&page.body[..]),
                spec.body_format() == HttpFormat::Tsv,
                &opts,
            )
            .map_err(parse)?;
            Ok((collect(Box::new(r))?, None))
        }
        HttpFormat::Xml => {
            let opts = xml.ok_or_else(|| {
                ShapeError::Spec(
                    "an xml body needs the element that is one record: `source.xml.record`".into(),
                )
            })?;
            let r = XmlReader::new(BufReader::new(Cursor::new(&page.body[..])), opts);
            Ok((collect(Box::new(r))?, None))
        }
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Object(_) => "an object",
        _ => "something else",
    }
}

/// A text format's cursor value: `"42"` orders as the number 42, not as text before `"5"`.
pub(crate) fn numeric(v: Value) -> Value {
    match &v {
        Value::String(s) => {
            if let Ok(i) = s.trim().parse::<i64>() {
                return Value::from(i);
            }
            if let Ok(f) = s.trim().parse::<f64>() {
                if f.is_finite()
                    && s.trim()
                        .bytes()
                        .all(|b| b.is_ascii_digit() || b"+-.eE".contains(&b))
                {
                    return serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(v);
                }
            }
            v
        }
        _ => v,
    }
}
