//! The zero-copy record model: parse each line onto a **reusable simd-json tape**.
//!
//! A `serde_json::Value` is a tree of small heap allocations — a map per object, a
//! `String` per key and per string value. Building one per record is the single largest
//! remaining cost in a shape run, and it is worst exactly where it hurts most: on the
//! shipped musl-static binary, whose allocator takes one global lock. A tape is the
//! opposite shape — a flat `Vec` of nodes, reused between records, whose strings point
//! back into the input buffer.
//!
//! Measured on one machine over 1M records of the benchmark shape, parse only:
//!
//! | record model | glibc | musl |
//! |---|---|---|
//! | owned `serde_json::Value` | 540k rec/s | 256k rec/s |
//! | borrowed value | 855k rec/s | 488k rec/s |
//! | **tape, reused buffers** | **1,849k rec/s** | **1,592k rec/s** |
//!
//! Note what the third row does to the *gap*: the owned model is 2.1× slower on musl
//! than glibc, the tape only 1.16×. Nearly nothing is allocated, so the allocator
//! nearly stops mattering.
//!
//! **This is a lending reader, not an `Iterator`.** Each record borrows the buffer the
//! next one will overwrite, which `Iterator` cannot express, so [`TapeJsonlReader`]
//! offers `next_tape` and the caller drives a `while let` loop. The same reader also
//! offers [`TapeJsonlReader::next_owned`], which parses one record into a
//! `serde_json::Value` — inference needs to *keep* its sample, so it takes that path for
//! the first few records and the hot path for all the rest, off one reader over one
//! stream.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::Value;
use shapeshift_core::{Kind, Record};
use simd_json::prelude::*;

use crate::error::JsonError;
use crate::source::{JsonArrayReader, JsonlReader, DEFAULT_MAX_LINE, SIMD_SLACK};

/// One record, read straight off the tape.
///
/// A cursor — a position in the flat node buffer — so it is `Copy` and costs nothing to
/// pass around per column. Its strings borrow `'i`, the input buffer, which is what lets
/// a string column reach Arrow without a copy.
#[derive(Clone, Copy)]
pub struct TapeRecord<'t, 'i>(simd_json::tape::Value<'t, 'i>);

impl<'t, 'i> TapeRecord<'t, 'i> {
    pub fn new(v: simd_json::tape::Value<'t, 'i>) -> Self {
        TapeRecord(v)
    }
}

impl<'t, 'i> Record<'i> for TapeRecord<'t, 'i>
where
    'i: 't,
{
    fn get_key(self, key: &str) -> Option<Self> {
        self.0.get(key).map(TapeRecord)
    }

    fn get_index(self, idx: usize) -> Option<Self> {
        self.0.get_idx(idx).map(TapeRecord)
    }

    fn kind(self) -> Kind {
        use simd_json::ValueType;
        match self.0.value_type() {
            ValueType::Null => Kind::Null,
            ValueType::Bool => Kind::Bool,
            // Mirror the `serde_json` rule exactly: an integer is one that fits `i64`.
            // A `u64` past `i64::MAX` is *not* — it falls to float, which is what makes
            // an int64 column reject it with the same "out of i64 range" message either
            // reader produced it.
            ValueType::I64 | ValueType::U64 => {
                if self.0.as_i64().is_some() {
                    Kind::Int
                } else {
                    Kind::Float
                }
            }
            ValueType::F64 => Kind::Float,
            ValueType::String => Kind::Str,
            ValueType::Array => Kind::Array,
            ValueType::Object => Kind::Object,
            // simd-json's extended/custom types cannot occur in a parsed JSON tape. Map
            // anything unforeseen to `Object` anyway, because that is the kind whose
            // accessors cannot lie: a scalar column fails it with a reason, a `json`
            // column serializes it as null. `Kind::Str` would promise a string body that
            // `as_str` cannot deliver, and the shaper takes that promise literally.
            _ => Kind::Object,
        }
    }

    fn as_bool(self) -> Option<bool> {
        self.0.as_bool()
    }

    fn as_i64(self) -> Option<i64> {
        self.0.as_i64()
    }

    fn as_f64(self) -> Option<f64> {
        // simd-json's `as_f64` is exact-type: it answers `None` for an integer node.
        // `serde_json` widens, and a `float64` column must accept `3` as well as `3.0`,
        // so widen here too — otherwise an integer in a float column silently becomes
        // null. (Caught by `tape_and_serde_agree_on_every_accessor`.)
        self.0
            .as_f64()
            .or_else(|| self.0.as_i64().map(|i| i as f64))
            .or_else(|| self.0.as_u64().map(|u| u as f64))
    }

    fn as_str(self) -> Option<&'i str> {
        // `into_string` hands back `&'input str` — a slice of the reader's buffer —
        // rather than something tied to this cursor. That is the whole point.
        self.0.into_string()
    }

    fn to_json_text(self) -> String {
        // Rendered by serde_json, not by simd-json's own encoder, so the text is
        // identical to what the owned path produces. The two encoders do differ — simd
        // -json writes `1.7976931348623157e308` where serde writes `…e+308` — and a
        // column's bytes must never depend on which reader produced them. Assuming that
        // float exponents are the *only* disagreement would be the same mistake twice,
        // so the whole rendering goes through one implementation.
        //
        // Reached only by a `json` column holding a non-string, or a mixed-type column
        // widened to `string` — never by a plain string, which borrows instead.
        tape_to_serde(self.0).to_string()
    }

    fn to_owned_value(self) -> Value {
        tape_to_serde(self.0)
    }

    fn entries(self) -> impl Iterator<Item = (&'i str, Self)> {
        self.0
            .as_object()
            .into_iter()
            .flat_map(|o| o.iter().map(|(k, v)| (k, TapeRecord(v))))
    }
}

/// Materialize a tape value as a `serde_json::Value`.
///
/// Only the paths that *report* a value need this — a coercion failure and an uncovered
/// path during drift detection — so it is off the hot loop by construction.
fn tape_to_serde(v: simd_json::tape::Value<'_, '_>) -> Value {
    use simd_json::ValueType;
    match v.value_type() {
        ValueType::Null => Value::Null,
        ValueType::Bool => v.as_bool().map(Value::Bool).unwrap_or(Value::Null),
        // A `u64` past `i64::MAX` still has to survive as its exact digits, so try both
        // widths before giving up.
        ValueType::I64 | ValueType::U64 => v
            .as_i64()
            .map(|i| Value::Number(i.into()))
            .or_else(|| v.as_u64().map(|u| Value::Number(u.into())))
            .unwrap_or(Value::Null),
        ValueType::F64 => v
            .as_f64()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueType::String => v
            .into_string()
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        ValueType::Array => Value::Array(
            v.as_array()
                .into_iter()
                .flat_map(|a| a.iter().map(tape_to_serde))
                .collect(),
        ),
        ValueType::Object => Value::Object(
            v.as_object()
                .into_iter()
                .flat_map(|o| o.iter().map(|(k, vv)| (k.to_string(), tape_to_serde(vv))))
                .collect(),
        ),
        _ => Value::Null,
    }
}

/// A JSONL reader that parses onto a reusable tape.
///
/// Same framing, limits, and error semantics as [`JsonlReader`] — it borrows that
/// reader's line splitting — but the parse step lands on a tape instead of building a
/// `Value`. Bounded RAM: one line, one tape, one set of parser buffers, all reused.
pub struct TapeJsonlReader<R> {
    lines: JsonlReader<R>,
    /// Destructive-parse copy. simd-json rewrites what it parses, so the pristine line
    /// stays in `lines` for error reporting.
    scratch: Vec<u8>,
    /// simd-json's own working buffers, reused across records — the reason a record
    /// costs no allocation at all in the steady state.
    buffers: simd_json::Buffers,
}

impl<R: BufRead> TapeJsonlReader<R> {
    pub fn new(inner: R) -> Self {
        TapeJsonlReader {
            lines: JsonlReader::new(inner).with_max_line(DEFAULT_MAX_LINE),
            scratch: Vec::with_capacity(4096),
            buffers: simd_json::Buffers::new(4096),
        }
    }

    /// The next record as an owned `Value`, for a caller that must *keep* it — schema
    /// inference over the sample. Same position, same stream as [`Self::next_tape`], so
    /// one reader serves both and nothing is read twice.
    pub fn next_owned(&mut self) -> Option<Result<Value, JsonError>> {
        self.lines.next()
    }

    /// The next record on the tape. The returned record borrows this reader until it is
    /// dropped, which is why this is not an `Iterator`.
    pub fn next_tape(&mut self) -> Option<Result<simd_json::Tape<'_>, JsonError>> {
        match self.lines.next_line_bytes() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(())) => {
                let line = self.lines.current_line();
                self.scratch.clear();
                self.scratch.extend_from_slice(line);
                // simd-json's stage-1 scan reads the slice it is *given* in SIMD-width
                // blocks, so the allocation must carry slack past the length or the scan
                // reads out of bounds. `Buffers` pads its own internal copy, not this
                // one. The `Value` reader has always reserved this; the tape reader must
                // too.
                self.scratch.reserve(SIMD_SLACK);
                match simd_json::to_tape_with_buffers(&mut self.scratch, &mut self.buffers) {
                    Ok(t) => Some(Ok(t)),
                    Err(e) => Some(Err(JsonError::Parse {
                        line: self.lines.line_number(),
                        message: e.to_string(),
                        raw: self.lines.raw_line_text(),
                    })),
                }
            }
        }
    }
}

impl TapeJsonlReader<BufReader<File>> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JsonError> {
        Ok(TapeJsonlReader::new(BufReader::new(File::open(path)?)))
    }
}

/// A source of records on a reusable tape, whichever framing produced them.
///
/// `next_tape` hands back a cursor borrowing the reader, so this is a *lending*
/// source and deliberately not an `Iterator` — the next call overwrites the buffer
/// the previous record points into. The trait exists so the shape loop has one tape
/// path rather than one per input format.
pub trait TapeSource {
    /// The next record on the tape, borrowed until dropped.
    fn next_tape(&mut self) -> Option<Result<simd_json::Tape<'_>, JsonError>>;
    /// The next record as an owned `Value`, for a caller that must keep it (schema
    /// inference over the sample). Same position and stream as `next_tape`.
    fn next_owned(&mut self) -> Option<Result<Value, JsonError>>;
}

impl<R: BufRead> TapeSource for TapeJsonlReader<R> {
    fn next_tape(&mut self) -> Option<Result<simd_json::Tape<'_>, JsonError>> {
        TapeJsonlReader::next_tape(self)
    }
    fn next_owned(&mut self) -> Option<Result<Value, JsonError>> {
        TapeJsonlReader::next_owned(self)
    }
}

impl<R: BufRead> TapeSource for TapeJsonArrayReader<R> {
    fn next_tape(&mut self) -> Option<Result<simd_json::Tape<'_>, JsonError>> {
        TapeJsonArrayReader::next_tape(self)
    }
    fn next_owned(&mut self) -> Option<Result<Value, JsonError>> {
        TapeJsonArrayReader::next_owned(self)
    }
}

/// The [`TapeJsonlReader`] treatment for a single top-level JSON array.
///
/// The array reader already framed elements into a reusable buffer — the same shape
/// `JsonlReader` gives lines — so the only thing standing between `json-array` and
/// the tape was that nothing had been written to take advantage of it. Elements
/// stream exactly as before; what changes is that an element becomes a cursor into a
/// reused node buffer instead of a freshly allocated `serde_json::Value` tree.
///
/// Error and structural semantics are the array reader's, untouched: a bad element is
/// recoverable and reports its 1-based element index, a malformed array is reported
/// once and ends iteration.
pub struct TapeJsonArrayReader<R> {
    elems: JsonArrayReader<R>,
    /// Destructive-parse copy. simd-json rewrites what it parses, so the pristine
    /// element stays in `elems` for error reporting.
    scratch: Vec<u8>,
    /// simd-json's own working buffers, reused across elements — the reason an
    /// element costs no allocation at all in the steady state.
    buffers: simd_json::Buffers,
}

impl<R: BufRead> TapeJsonArrayReader<R> {
    /// Wrap a reader positioned at the start of a JSON document. Fails here, not
    /// mid-run, if the document is not an array.
    pub fn new(inner: R) -> Result<Self, JsonError> {
        Ok(TapeJsonArrayReader {
            elems: JsonArrayReader::new(inner)?.with_max_record(DEFAULT_MAX_LINE),
            scratch: Vec::with_capacity(4096),
            buffers: simd_json::Buffers::new(4096),
        })
    }

    /// Override the per-element byte ceiling (default [`DEFAULT_MAX_LINE`]).
    pub fn with_max_record(mut self, max_record: usize) -> Self {
        self.elems = self.elems.with_max_record(max_record);
        self
    }

    /// The next element as an owned `Value`, for a caller that must *keep* it —
    /// schema inference over the sample. Same position, same stream as
    /// [`Self::next_tape`], so one reader serves both and nothing is read twice.
    pub fn next_owned(&mut self) -> Option<Result<Value, JsonError>> {
        self.elems.next()
    }

    /// The next element on the tape. The returned record borrows this reader until it
    /// is dropped, which is why this is not an `Iterator`.
    pub fn next_tape(&mut self) -> Option<Result<simd_json::Tape<'_>, JsonError>> {
        match self.elems.next_element_bytes() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(())) => {
                self.scratch.clear();
                self.scratch.extend_from_slice(self.elems.current_element());
                // Stage one scans the slice it is *given*, so the allocation must
                // carry slack past the length or the scan reads out of bounds.
                // `Buffers` pads its own internal copy, not this one.
                self.scratch.reserve(SIMD_SLACK);
                match simd_json::to_tape_with_buffers(&mut self.scratch, &mut self.buffers) {
                    Ok(t) => Some(Ok(t)),
                    Err(e) => Some(Err(JsonError::Parse {
                        line: self.elems.element_index(),
                        message: e.to_string(),
                        raw: self.elems.raw_element_text(),
                    })),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two record models must answer identically, or a run's output depends on which
    /// reader produced it. This walks every value of a sample through both and compares
    /// each accessor — the check that makes the tape path safe to default to.
    #[test]
    fn tape_and_serde_agree_on_every_accessor() {
        // Every construct here has either broken one of the two models or come close:
        // integers in a float column, floats whose exponent forces `e+` formatting,
        // i64/u64 bounds, escapes, non-ASCII, empty containers, nulls.
        let sample = r#"{"id":1,"user":{"name":"Ada","plan":"pro"},"amount":12.50,"n":3,"z":0,"neg":-7,"big":9007199254740993,"imin":-9223372036854775808,"umax":18446744073709551615,"fmax":1.7976931348623157e308,"fmin":5e-324,"tiny":1e-7,"day":"2026-07-13","tags":["a","b",1.7976931348623157e308],"empty":[],"obj":{},"active":true,"nil":null,"esc":"a\"b\\c\td\u0001é/","f":1e3}"#;
        let owned: serde_json::Value = serde_json::from_str(sample).unwrap();
        let mut bytes = sample.as_bytes().to_vec();
        let mut buffers = simd_json::Buffers::new(1024);
        let tape = simd_json::to_tape_with_buffers(&mut bytes, &mut buffers).unwrap();
        compare(&owned, TapeRecord::new(tape.as_value()), "$");
    }

    fn compare(a: &serde_json::Value, b: TapeRecord<'_, '_>, path: &str) {
        assert_eq!(a.kind(), b.kind(), "kind at {path}");
        assert_eq!(a.as_bool(), b.as_bool(), "as_bool at {path}");
        assert_eq!(a.as_i64(), b.as_i64(), "as_i64 at {path}");
        assert_eq!(a.as_f64(), b.as_f64(), "as_f64 at {path}");
        assert_eq!(a.as_str(), b.as_str(), "as_str at {path}");
        // Text is compared by re-parsing: both must describe the same JSON, but neither
        // promises a byte-for-byte rendering (key order, number formatting).
        let (ta, tb) = (a.to_json_text(), b.to_json_text());
        let (pa, pb): (serde_json::Value, serde_json::Value) = (
            serde_json::from_str(&ta).unwrap_or_else(|e| panic!("{path}: {ta}: {e}")),
            serde_json::from_str(&tb).unwrap_or_else(|e| panic!("{path}: {tb}: {e}")),
        );
        assert_eq!(pa, pb, "to_json_text at {path}: {ta} vs {tb}");
        assert_eq!(
            a.to_owned_value(),
            b.to_owned_value(),
            "to_owned_value at {path}"
        );

        let mut ea: Vec<_> = a.entries().collect();
        let mut eb: Vec<_> = b.entries().collect();
        assert_eq!(ea.len(), eb.len(), "entry count at {path}");
        ea.sort_by_key(|(k, _)| *k);
        eb.sort_by_key(|(k, _)| *k);
        for ((ka, va), (kb, vb)) in ea.into_iter().zip(eb) {
            assert_eq!(ka, kb, "key at {path}");
            compare(va, vb, &format!("{path}.{ka}"));
        }
    }

    /// The array's two record models must agree on everything, not just the happy
    /// path. The `Value` reader owns the error semantics people rely on — recoverable
    /// per-element errors carrying a 1-based index, structural errors reported once —
    /// and the tape reader now shares its framing, so this drives both over the same
    /// battery and compares outcomes record for record.
    ///
    /// Written because the JSONL equivalent caught two real bugs when the tape first
    /// landed. The same divergence risk exists wherever a second model is added.
    #[test]
    fn tape_array_and_value_array_agree_on_every_outcome() {
        let cases: &[(&str, &str)] = &[
            (r#"[{"a":1},{"a":2},{"a":3}]"#, "plain elements"),
            (r#"[ ]"#, "empty array"),
            (
                r#"[{"a":1}, {"a":}, {"a":3}]"#,
                "bad element, then recovery",
            ),
            (
                r#"[{"n":{"deep":[1,2,{"s":"a,b]c"}]}}]"#,
                "nesting; commas and brackets inside a string",
            ),
            (r#"[1, "two", true, null, 3.5]"#, "scalar elements"),
            (r#"[{"a":1},"#, "truncated array (structural)"),
            (r#"[1,]"#, "trailing comma (structural)"),
            (r#"[{"a":1}] trailing"#, "trailing content after the close"),
            (
                r#"[{"u":18446744073709551615},{"i":-9007199254740993}]"#,
                "integer bounds",
            ),
            (
                r#"[{"esc":"a\"b\\c"},{"uni":"é中"}]"#,
                "escapes and non-ASCII",
            ),
            (r#"[   {"a":1}   ,   {"a":2}   ]"#, "generous whitespace"),
        ];

        for (input, what) in cases {
            let mut values = JsonArrayReader::new(input.as_bytes()).unwrap();
            let mut tapes = TapeJsonArrayReader::new(input.as_bytes()).unwrap();
            let mut n = 0;
            loop {
                n += 1;
                assert!(n < 100, "{what}: runaway");
                match (values.next(), tapes.next_tape()) {
                    (None, None) => break,
                    (Some(Ok(want)), Some(Ok(tape))) => {
                        let got = TapeRecord::new(tape.as_value()).to_owned_value();
                        assert_eq!(got, want, "{what}: element {n} differs");
                    }
                    (
                        Some(Err(JsonError::Parse {
                            line: wl, raw: wr, ..
                        })),
                        Some(Err(JsonError::Parse {
                            line: tl, raw: tr, ..
                        })),
                    ) => {
                        assert_eq!(wl, tl, "{what}: element {n} error index differs");
                        assert_eq!(wr, tr, "{what}: element {n} raw text differs");
                    }
                    (v, t) => panic!(
                        "{what}: element {n} disagreed — value yielded {}, tape yielded {}",
                        describe(&v),
                        describe(&t)
                    ),
                }
            }
        }
    }

    /// Describe a yielded item without needing the two item types to match.
    fn describe<T>(item: &Option<Result<T, JsonError>>) -> &'static str {
        match item {
            None => "end",
            Some(Ok(_)) => "a record",
            Some(Err(_)) => "an error",
        }
    }

    /// An oversized element is a per-record reject on the tape too, and the reader
    /// recovers to the next element rather than ending the run.
    #[test]
    fn tape_array_rejects_oversized_elements_like_the_value_reader() {
        let big = format!(r#"{{"pad":"{}"}}"#, "x".repeat(100));
        let data = format!(r#"[{{"a":1}}, {big}, {{"a":2}}]"#);
        let mut tapes = TapeJsonArrayReader::new(data.as_bytes())
            .unwrap()
            .with_max_record(16);

        let first = tapes.next_tape().unwrap().unwrap();
        assert_eq!(
            TapeRecord::new(first.as_value()).to_owned_value()["a"],
            serde_json::json!(1)
        );
        match tapes.next_tape().unwrap() {
            Err(JsonError::Parse { line, message, .. }) => {
                assert_eq!(line, 2, "element index");
                assert!(message.contains("exceeds"), "got: {message}");
            }
            other => panic!("expected an oversize reject, got {other:?}"),
        }
        let third = tapes.next_tape().unwrap().unwrap();
        assert_eq!(
            TapeRecord::new(third.as_value()).to_owned_value()["a"],
            serde_json::json!(2)
        );
        assert!(tapes.next_tape().is_none());
    }

    /// A document that is not an array fails when the reader is opened, not part-way
    /// through a run — so the CLI can name the file in the error.
    #[test]
    fn tape_array_rejects_a_non_array_document_up_front() {
        assert!(TapeJsonArrayReader::new(&b"{\"a\":1}"[..]).is_err());
        assert!(TapeJsonArrayReader::new(&b""[..]).is_err());
        assert!(TapeJsonArrayReader::new(&b"42"[..]).is_err());
    }
}
