//! CSV and TSV sources: one record per row, an object keyed by the column names.
//!
//! RFC 4180 quoting (embedded delimiters, quotes and line breaks inside quoted fields), a
//! UTF-8 byte-order mark skipped, and a row with the wrong number of fields rejected with
//! its line, exactly as a JSON line that does not parse is. With `types: auto` (the default)
//! a field that is plainly an integer, a decimal, `true` or `false` arrives as a JSON number
//! or boolean, so inference sees types; with `types: strings` every field is a string, for
//! the spec's types to coerce. A field in `null_values` (by default the empty field) is null.
//!
//! The reader streams: one row is held at a time, and a row may be no larger than one JSON
//! record may ([`crate::source::DEFAULT_MAX_LINE`]): a quote that never closes fails the input there,
//! rather than buffering the rest of the file as one field.

use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::{Map, Number, Value};
use shapeshift_core::{CsvOptions, CsvTypes};

use crate::cap::Capped;
use crate::JsonError;

/// Rows of a CSV or TSV document, as records.
pub struct CsvReader<R: Read> {
    rdr: csv::Reader<Capped<R>>,
    /// Where the current row starts, for the byte ceiling.
    mark: Arc<AtomicU64>,
    /// The column names; empty until the first row when there is no header and no
    /// `columns` (the first row's width then fixes how many there are).
    names: Vec<String>,
    null_values: Vec<String>,
    types: CsvTypes,
    delimiter: char,
    quote: char,
    row: csv::ByteRecord,
}

/// A row as CSV text again, quoted where a field needs it, so a rejected row in the sidecar
/// reads back as the same fields.
fn raw_row(row: &csv::StringRecord, delimiter: char, quote: char) -> String {
    let mut w = csv::WriterBuilder::new()
        .delimiter(delimiter as u8)
        .quote(quote as u8)
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    if w.write_record(row).is_err() {
        return String::new();
    }
    let mut bytes = w.into_inner().unwrap_or_default();
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn csv_error(e: csv::Error) -> JsonError {
    let line = e.position().map_or(0, |p| p.line());
    if e.is_io_error() {
        match e.into_kind() {
            csv::ErrorKind::Io(io) => JsonError::Io(io),
            _ => unreachable!("is_io_error"),
        }
    } else {
        JsonError::Parse {
            line,
            message: e.to_string(),
            raw: String::new(),
        }
    }
}

impl<R: Read> CsvReader<R> {
    /// Read `inner` as CSV (`tsv: false`) or TSV. The header row, if there is one, is read
    /// now: a header with an empty or repeated name is an error here, naming it.
    pub fn new(inner: R, tsv: bool, opts: &CsvOptions) -> Result<Self, JsonError> {
        // The parser reads ahead in blocks, so the ceiling has a block of slack.
        Self::with_limit(
            inner,
            tsv,
            opts,
            crate::source::DEFAULT_MAX_LINE as u64 + 64 * 1024,
        )
    }

    /// [`CsvReader::new`] with a byte ceiling on one row other than the default.
    pub fn with_limit(
        inner: R,
        tsv: bool,
        opts: &CsvOptions,
        limit: u64,
    ) -> Result<Self, JsonError> {
        let delimiter = opts.delimiter.unwrap_or(if tsv { '\t' } else { ',' });
        let (inner, mark) = Capped::new(inner, limit, "a CSV row");
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter as u8)
            .quote(opts.quote as u8)
            .has_headers(false)
            .flexible(true)
            .from_reader(inner);
        let mut names = opts.columns.clone();
        if opts.header {
            let mut first = csv::StringRecord::new();
            let read = rdr.read_record(&mut first).map_err(csv_error)?;
            if read && names.is_empty() {
                names = first
                    .iter()
                    .enumerate()
                    .map(|(i, h)| {
                        let h = if i == 0 {
                            h.trim_start_matches('\u{feff}')
                        } else {
                            h
                        };
                        h.trim().to_string()
                    })
                    .collect();
                let mut seen = std::collections::HashSet::new();
                for (i, n) in names.iter().enumerate() {
                    if n.is_empty() {
                        return Err(JsonError::Parse {
                            line: 1,
                            message: format!(
                                "the header's column {} has no name (name the columns with `source.csv.columns`)",
                                i + 1
                            ),
                            raw: raw_row(&first, delimiter, opts.quote),
                        });
                    }
                    if !seen.insert(n.clone()) {
                        return Err(JsonError::Parse {
                            line: 1,
                            message: format!(
                                "the header names `{n}` twice (name the columns with `source.csv.columns`)"
                            ),
                            raw: raw_row(&first, delimiter, opts.quote),
                        });
                    }
                }
            }
        }
        mark.store(rdr.position().byte(), Ordering::Relaxed);
        Ok(CsvReader {
            rdr,
            mark,
            names,
            null_values: opts.null_values.clone(),
            types: opts.types,
            delimiter,
            quote: opts.quote,
            row: csv::ByteRecord::new(),
        })
    }

    fn value(&self, field: &str) -> Value {
        if self.null_values.iter().any(|n| n == field) {
            return Value::Null;
        }
        match self.types {
            CsvTypes::Strings => Value::String(field.to_string()),
            CsvTypes::Auto => scalar(field),
        }
    }
}

/// `true`, `false`, an integer that fits `i64` (no sign but `-`, no leading zero, no
/// spaces), or a decimal written the same way with a fractional part: anything else is a
/// string, so a ZIP code like `02139` or an id like `1e5` stays as written.
fn scalar(field: &str) -> Value {
    match field {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    let digits = field.strip_prefix('-').unwrap_or(field);
    let (int, frac) = match digits.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (digits, None),
    };
    let int_ok = !int.is_empty()
        && int.bytes().all(|b| b.is_ascii_digit())
        && (int == "0" || !int.starts_with('0'));
    let frac_ok = frac.map_or(true, |f| {
        !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit())
    });
    if !(int_ok && frac_ok) {
        return Value::String(field.to_string());
    }
    if frac.is_none() {
        if let Ok(n) = field.parse::<i64>() {
            return Value::Number(n.into());
        }
        return Value::String(field.to_string());
    }
    match field.parse::<f64>().ok().and_then(Number::from_f64) {
        Some(n) => Value::Number(n),
        None => Value::String(field.to_string()),
    }
}

impl<R: Read> Iterator for CsvReader<R> {
    type Item = Result<Value, JsonError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut bytes = std::mem::take(&mut self.row);
        let read = self.rdr.read_byte_record(&mut bytes);
        self.mark
            .store(self.rdr.position().byte(), Ordering::Relaxed);
        match read {
            Ok(false) => {
                self.row = bytes;
                return None;
            }
            Err(e) => {
                self.row = bytes;
                return Some(Err(csv_error(e)));
            }
            Ok(true) => {}
        }
        // Read as bytes, so a row that is not UTF-8 still reaches the sidecar as text.
        let row = match csv::StringRecord::from_byte_record(bytes) {
            Ok(row) => row,
            Err(e) => {
                let bytes = e.into_byte_record();
                let line = bytes.position().map_or(0, |p| p.line());
                let lossy = csv::StringRecord::from_byte_record_lossy(bytes.clone());
                self.row = bytes;
                return Some(Err(JsonError::Parse {
                    line,
                    message: "the row is not UTF-8 text".into(),
                    raw: raw_row(&lossy, self.delimiter, self.quote),
                }));
            }
        };
        let out = {
            {
                if self.names.is_empty() {
                    self.names = (1..=row.len()).map(|i| format!("column_{i}")).collect();
                }
                let line = row.position().map_or(0, |p| p.line());
                if row.len() != self.names.len() {
                    Some(Err(JsonError::Parse {
                        line,
                        message: format!(
                            "the row has {} field{}, the columns are {}",
                            row.len(),
                            if row.len() == 1 { "" } else { "s" },
                            self.names.len()
                        ),
                        raw: raw_row(&row, self.delimiter, self.quote),
                    }))
                } else {
                    let mut m = Map::with_capacity(self.names.len());
                    for (name, field) in self.names.iter().zip(row.iter()) {
                        m.insert(name.clone(), self.value(field));
                    }
                    Some(Ok(Value::Object(m)))
                }
            }
        };
        self.row = row.into_byte_record();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn read(text: &str, tsv: bool, opts: &CsvOptions) -> Vec<Result<Value, String>> {
        CsvReader::new(text.as_bytes(), tsv, opts)
            .unwrap()
            .map(|r| r.map_err(|e| e.to_string()))
            .collect()
    }

    #[test]
    fn rows_become_records_with_types_where_they_are_plain() {
        let got = read(
            "id,name,amount,active,zip,day\n1,Ada,12.50,true,02139,2026-07-01\n2,\"Lin, Jr\",-3,false,,x\n",
            false,
            &CsvOptions::default(),
        );
        assert_eq!(
            got,
            vec![
                Ok(
                    json!({"id":1,"name":"Ada","amount":12.5,"active":true,"zip":"02139","day":"2026-07-01"})
                ),
                Ok(
                    json!({"id":2,"name":"Lin, Jr","amount":-3,"active":false,"zip":null,"day":"x"})
                ),
            ]
        );
    }

    #[test]
    fn quoting_newlines_bom_and_tabs() {
        let got = read(
            "\u{feff}a,b\n\"x\"\"y\",\"two\nlines\"\n",
            false,
            &CsvOptions::default(),
        );
        assert_eq!(got, vec![Ok(json!({"a":"x\"y","b":"two\nlines"}))]);
        let got = read("a\tb\n1\t\"q\"\n", true, &CsvOptions::default());
        assert_eq!(got, vec![Ok(json!({"a":1,"b":"q"}))]);
    }

    #[test]
    fn a_ragged_row_is_rejected_with_its_line_and_the_rest_continue() {
        let got = read("a,b\n1,2\n3\n4,5\n", false, &CsvOptions::default());
        assert_eq!(got[0], Ok(json!({"a":1,"b":2})));
        let err = got[1].clone().unwrap_err();
        assert!(
            err.contains("line 3") && err.contains("1 field, the columns are 2"),
            "{err}"
        );
        assert_eq!(got[2], Ok(json!({"a":4,"b":5})));
    }

    #[test]
    fn strings_mode_null_values_and_named_columns() {
        let opts = CsvOptions {
            header: false,
            columns: vec!["x".into(), "y".into()],
            types: CsvTypes::Strings,
            null_values: vec!["NA".into()],
            ..CsvOptions::default()
        };
        let got = read("1,NA\n,2\n", false, &opts);
        assert_eq!(
            got,
            vec![Ok(json!({"x":"1","y":null})), Ok(json!({"x":"","y":"2"}))]
        );
        // No header and no names: column_1, column_2, …
        let opts = CsvOptions {
            header: false,
            ..CsvOptions::default()
        };
        assert_eq!(
            read("7,8\n", false, &opts),
            vec![Ok(json!({"column_1":7,"column_2":8}))]
        );
        // A header row plus named columns: the header is skipped, the names win.
        let opts = CsvOptions {
            columns: vec!["p".into(), "q".into()],
            ..CsvOptions::default()
        };
        assert_eq!(
            read("a,b\n1,2\n", false, &opts),
            vec![Ok(json!({"p":1,"q":2}))]
        );
    }

    #[test]
    fn a_rejected_row_keeps_its_quoting_in_the_sidecar() {
        let bad = CsvReader::new(
            "a,b\n\"x,y\",z,w\n".as_bytes(),
            false,
            &CsvOptions::default(),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
        match bad {
            JsonError::Parse { raw, .. } => assert_eq!(raw, "\"x,y\",z,w"),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn a_bom_is_skipped_without_a_header_too() {
        let opts = CsvOptions {
            header: false,
            ..CsvOptions::default()
        };
        assert_eq!(
            read("\u{feff}7,8\n", false, &opts),
            vec![Ok(json!({"column_1":7,"column_2":8}))]
        );
    }

    #[test]
    fn a_row_that_is_not_utf8_reaches_the_sidecar_as_text() {
        let mut input = b"a,b\n1,2\n".to_vec();
        input.extend_from_slice(b"x\xff,3\n4,5\n");
        let got: Vec<_> = CsvReader::new(&input[..], false, &CsvOptions::default())
            .unwrap()
            .collect();
        assert_eq!(got[0].as_ref().unwrap(), &json!({"a":1,"b":2}));
        match &got[1] {
            Err(JsonError::Parse { line, message, raw }) => {
                assert_eq!(*line, 3);
                assert!(message.contains("UTF-8"), "{message}");
                assert_eq!(raw, "x\u{fffd},3");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(got[2].as_ref().unwrap(), &json!({"a":4,"b":5}));
    }

    #[test]
    fn a_quote_that_never_closes_fails_at_the_ceiling() {
        let mut input = String::from("a,b\n1,\"open\n");
        input.push_str(&"x".repeat(100_000));
        let got: Vec<_> =
            CsvReader::with_limit(input.as_bytes(), false, &CsvOptions::default(), 16 * 1024)
                .unwrap()
                .collect();
        let last = got.last().unwrap();
        assert!(
            matches!(last, Err(JsonError::Io(e)) if e.to_string().contains("runs past")),
            "{last:?}"
        );
        // Rows of ordinary size pass the same ceiling, however many there are.
        let many = format!("a,b\n{}", "1,2\n".repeat(50_000));
        let n = CsvReader::with_limit(many.as_bytes(), false, &CsvOptions::default(), 16 * 1024)
            .unwrap()
            .filter(|r| r.is_ok())
            .count();
        assert_eq!(n, 50_000);
    }

    #[test]
    fn a_header_with_a_repeated_or_empty_name_is_refused() {
        for h in ["a,a\n1,2\n", "a,,c\n1,2,3\n"] {
            let e = CsvReader::new(h.as_bytes(), false, &CsvOptions::default())
                .err()
                .unwrap();
            assert!(e.to_string().contains("line 1"), "{e}");
        }
    }

    #[test]
    fn scalars_stay_strings_unless_plain() {
        for s in [
            "01",
            "1e5",
            "+1",
            " 1",
            "1.",
            ".5",
            "1.2.3",
            "9223372036854775808",
            "TRUE",
            "-",
        ] {
            assert_eq!(scalar(s), Value::String(s.into()), "{s}");
        }
        assert_eq!(scalar("0"), json!(0));
        assert_eq!(scalar("-0.25"), json!(-0.25));
        assert_eq!(scalar("9223372036854775807"), json!(9223372036854775807i64));
    }

    #[test]
    fn an_empty_file_has_no_records() {
        assert!(read("", false, &CsvOptions::default()).is_empty());
        assert!(read("a,b\n", false, &CsvOptions::default()).is_empty());
    }
}
