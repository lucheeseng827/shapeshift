//! Streaming JSON readers.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde_json::Value;

use crate::error::JsonError;

/// A JSON record plus its 1-based source line (the 1-based **element index** in
/// array mode). Kept small so the CLI can attribute a shaping reject back to its
/// input line.
///
/// The `Send` bound lets a driver move the whole source onto a reader thread and
/// overlap parsing with shaping (see the CLI's threaded shape loop). Every reader here
/// is `Send`; stdin is taken as [`std::io::Stdin`] rather than its lock for that reason
/// — `StdinLock` holds a `MutexGuard` and cannot cross threads.
pub type RecordSource = Box<dyn Iterator<Item = Result<Value, JsonError>> + Send>;

/// simd-json reads the input buffer in SIMD-width chunks; give the owned line
/// buffer this many bytes of slack past its length so those reads never touch
/// memory outside the allocation.
pub(crate) const SIMD_SLACK: usize = 64;

/// Default per-record ceiling: a single JSONL line larger than this is rejected
/// (not buffered whole), so a pathological huge/unterminated line can't OOM the
/// process. Generous — real records are far smaller — but bounds the worst case.
pub const DEFAULT_MAX_LINE: usize = 256 * 1024 * 1024;

/// Streaming JSONL reader: one JSON value per non-blank line. Blank / whitespace-
/// only lines are skipped (not errors). Bounded RAM — at most one line (capped at
/// `max_line`) is held at a time.
pub struct JsonlReader<R> {
    inner: R,
    buf: Vec<u8>,
    /// Reused destructive-parse buffer. simd-json rewrites the bytes it parses, so it
    /// works on a copy here and `buf` keeps the pristine line for error reporting —
    /// which is what lets a good row avoid materializing its raw text at all.
    scratch: Vec<u8>,
    line: u64,
    done: bool,
    max_line: usize,
}

/// One line's worth of read.
enum LineOutcome {
    /// End of input, nothing buffered.
    Eof,
    /// A line was read into `buf` (newline excluded). `truncated` = the line hit
    /// `max_line` and the remainder was drained.
    Line { truncated: bool },
}

impl<R: BufRead> JsonlReader<R> {
    pub fn new(inner: R) -> Self {
        JsonlReader {
            inner,
            buf: Vec::with_capacity(256),
            scratch: Vec::with_capacity(256),
            line: 0,
            done: false,
            max_line: DEFAULT_MAX_LINE,
        }
    }

    /// Override the per-record byte ceiling (default [`DEFAULT_MAX_LINE`]).
    pub fn with_max_line(mut self, max_line: usize) -> Self {
        self.max_line = max_line.max(1);
        self
    }

    /// Read one line into `self.buf` (newline excluded), buffering at most
    /// `max_line` bytes and draining any overflow through the next newline so the
    /// reader stays positioned at the following record.
    fn read_line_bounded(&mut self) -> std::io::Result<LineOutcome> {
        self.buf.clear();
        let mut truncated = false;
        let mut saw_any = false;
        loop {
            let chunk = self.inner.fill_buf()?;
            if chunk.is_empty() {
                return Ok(if saw_any {
                    LineOutcome::Line { truncated }
                } else {
                    LineOutcome::Eof
                });
            }
            saw_any = true;
            match chunk.iter().position(|&b| b == b'\n') {
                Some(i) => {
                    if !truncated {
                        let room = self.max_line.saturating_sub(self.buf.len());
                        let end = i.min(room);
                        self.buf.extend_from_slice(&chunk[..end]);
                        if i > room {
                            truncated = true;
                        }
                    }
                    self.inner.consume(i + 1); // drain through the newline
                    return Ok(LineOutcome::Line { truncated });
                }
                None => {
                    if !truncated {
                        let room = self.max_line.saturating_sub(self.buf.len());
                        if chunk.len() > room {
                            self.buf.extend_from_slice(&chunk[..room]);
                            truncated = true;
                        } else {
                            self.buf.extend_from_slice(chunk);
                        }
                    }
                    let n = chunk.len();
                    self.inner.consume(n);
                }
            }
        }
    }

    /// The current line as text, for an error record. Only called on a reject, so a
    /// clean run never pays for it.
    fn raw_line(&self) -> String {
        String::from_utf8_lossy(self.buf.trim_ascii_end()).into_owned()
    }

    // ---- framing, shared with the tape reader ----------------------------------
    //
    // `TapeJsonlReader` parses onto a reusable tape instead of building a `Value`, but
    // the framing above it — bounded lines, blank-line skipping, the oversize reject —
    // is identical and must stay identical. Rather than keep a second copy of it, the
    // tape reader drives these.

    /// Advance to the next non-blank line, leaving it in `buf`. `None` at EOF.
    pub(crate) fn next_line_bytes(&mut self) -> Option<Result<(), JsonError>> {
        if self.done {
            return None;
        }
        loop {
            match self.read_line_bounded() {
                Err(e) => {
                    self.done = true;
                    return Some(Err(JsonError::Io(e)));
                }
                Ok(LineOutcome::Eof) => {
                    self.done = true;
                    return None;
                }
                Ok(LineOutcome::Line { truncated }) => {
                    self.line += 1;
                    if truncated {
                        return Some(Err(JsonError::Parse {
                            line: self.line,
                            message: format!(
                                "record exceeds the {}-byte limit; skipped",
                                self.max_line
                            ),
                            raw: self.raw_line(),
                        }));
                    }
                    if self.buf.iter().all(|b| b.is_ascii_whitespace()) {
                        continue;
                    }
                    return Some(Ok(()));
                }
            }
        }
    }

    /// The bytes of the line [`Self::next_line_bytes`] just read.
    pub(crate) fn current_line(&self) -> &[u8] {
        &self.buf
    }

    /// The 1-based number of that line.
    pub(crate) fn line_number(&self) -> u64 {
        self.line
    }

    /// That line as text, for an error record.
    pub(crate) fn raw_line_text(&self) -> String {
        self.raw_line()
    }
}

impl JsonlReader<BufReader<File>> {
    /// Open a JSONL file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JsonError> {
        Ok(JsonlReader::new(BufReader::new(File::open(path)?)))
    }
}

impl<R: BufRead> Iterator for JsonlReader<R> {
    type Item = Result<Value, JsonError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Framing is `next_line_bytes`' job, here and in the tape reader, so there is
        // exactly one copy of it to keep correct. This adds only the parse.
        match self.next_line_bytes()? {
            Err(e) => Some(Err(e)),
            Ok(()) => {
                // simd-json parses **destructively**, so it gets a scratch copy and
                // `buf` keeps the original bytes — that way the raw line is only
                // materialized on the error path, not allocated for every good row.
                self.scratch.clear();
                self.scratch.extend_from_slice(&self.buf);
                self.scratch.reserve(SIMD_SLACK);
                match simd_json::serde::from_slice::<Value>(&mut self.scratch) {
                    Ok(v) => Some(Ok(v)),
                    Err(e) => Some(Err(JsonError::Parse {
                        line: self.line,
                        message: e.to_string(),
                        raw: self.raw_line(),
                    })),
                }
            }
        }
    }
}

/// **Streaming** reader for a single top-level JSON array: `[ elem, elem, … ]`.
///
/// Arrays are not line-delimited, so this reader finds element boundaries itself
/// with a small depth- and string-aware scanner (`{`/`[` nest, `"…"` may contain
/// `,`/`]`/escapes), buffering **one element at a time** — a multi-GB array never
/// materializes. Each element parses via simd-json exactly like a JSONL line.
///
/// Error semantics mirror JSONL: an element that isn't valid JSON (or exceeds the
/// per-record cap) is a **recoverable** per-record error — the scanner already
/// knows where the element ends, so iteration continues at the next one. Errors
/// report the 1-based **element index** in the `line` field. A *structural* error
/// (input that isn't an array, a truncated array, trailing garbage after `]`) is
/// yielded once, then iteration ends. That the input is an array at all is checked
/// eagerly at construction.
pub struct JsonArrayReader<R> {
    inner: R,
    /// The element's bytes, kept intact so the raw text stays available for an
    /// error without being materialized for every good element.
    buf: Vec<u8>,
    /// A throwaway copy of `buf` for simd-json, which parses destructively.
    scratch: Vec<u8>,
    /// 1-based index of the element currently being read (for error reporting).
    index: u64,
    /// Whether a `]` at depth 0 is legal at the next element position: true right
    /// after `[`, false right after a `,` (a trailing comma is an error).
    allow_close: bool,
    /// The array's `]` was consumed; the next call runs the trailing-content
    /// check (deferred so the final element is yielded first) and ends.
    closed: bool,
    done: bool,
    max_record: usize,
}

/// One element's worth of scan.
enum ElemOutcome {
    /// `buf` holds the element's bytes. `truncated` = it hit `max_record` (the
    /// remainder was drained to the boundary). `last` = its terminator was the
    /// array's closing `]` rather than a comma.
    Elem { truncated: bool, last: bool },
    /// The array closed with no further element (`[]`, or `]` after an element).
    Close,
}

impl<R: BufRead> JsonArrayReader<R> {
    /// Wrap a buffered reader. Consumes leading whitespace and the opening `[`
    /// eagerly, so a document that isn't a JSON array fails here, not mid-run.
    pub fn new(mut inner: R) -> Result<Self, JsonError> {
        match skip_ws_peek(&mut inner)? {
            Some(b'[') => {
                inner.consume(1);
                Ok(JsonArrayReader {
                    inner,
                    buf: Vec::with_capacity(256),
                    scratch: Vec::with_capacity(256),
                    index: 0,
                    allow_close: true,
                    closed: false,
                    done: false,
                    max_record: DEFAULT_MAX_LINE,
                })
            }
            Some(first) => Err(JsonError::Parse {
                line: 0,
                message: format!(
                    "expected a top-level JSON array, got {}",
                    if first == b'{' {
                        "an object (use --format jsonl?)"
                    } else {
                        "a scalar"
                    }
                ),
                raw: String::new(),
            }),
            None => Err(JsonError::Parse {
                line: 0,
                message: "empty input; expected a top-level JSON array".into(),
                raw: String::new(),
            }),
        }
    }

    /// Advance to the next element, leaving its bytes in `buf`.
    ///
    /// `None` ends iteration (the array closed, and the trailing-content check has
    /// run). `Some(Err(..))` is either a *structural* error — reported once, after
    /// which iteration ends — or a per-element error such as an oversized record,
    /// after which iteration continues at the next element. `Some(Ok(()))` means
    /// `current_element` now holds one element's bytes.
    pub(crate) fn next_element_bytes(&mut self) -> Option<Result<(), JsonError>> {
        if self.done {
            return None;
        }
        if self.closed {
            // The `]` was consumed with the previous element; finish up now.
            self.done = true;
            return match self.check_trailing() {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            };
        }
        self.index += 1;
        let outcome = match self.read_element() {
            Ok(o) => o,
            Err(e) => {
                // Structural: report once, then end.
                self.done = true;
                return Some(Err(e));
            }
        };
        match outcome {
            ElemOutcome::Close => {
                self.done = true;
                match self.check_trailing() {
                    Ok(()) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            ElemOutcome::Elem { truncated, last } => {
                if last {
                    // The element's `]` terminator also closed the array; the
                    // trailing-content check runs on the next call, after this
                    // element is yielded.
                    self.closed = true;
                }
                if truncated {
                    return Some(Err(JsonError::Parse {
                        line: self.index,
                        message: format!(
                            "record exceeds the {}-byte limit; skipped",
                            self.max_record
                        ),
                        raw: self.raw_element(),
                    }));
                }
                Some(Ok(()))
            }
        }
    }

    /// The bytes of the element `next_element_bytes` just framed.
    pub(crate) fn current_element(&self) -> &[u8] {
        &self.buf
    }

    /// The 1-based index of the current element — the array's analogue of a line
    /// number, and what error reports carry.
    pub(crate) fn element_index(&self) -> u64 {
        self.index
    }

    /// The current element's text, for an error report. Lossy, trimmed, and
    /// allocated only when something has already gone wrong.
    fn raw_element(&self) -> String {
        String::from_utf8_lossy(self.buf.trim_ascii()).into_owned()
    }

    /// Same as [`Self::raw_element`], for the tape reader in the sibling module.
    pub(crate) fn raw_element_text(&self) -> String {
        self.raw_element()
    }

    /// Override the per-element byte ceiling (default [`DEFAULT_MAX_LINE`]).
    pub fn with_max_record(mut self, max_record: usize) -> Self {
        self.max_record = max_record.max(1);
        self
    }

    /// Scan the next element into `self.buf` (terminator excluded, consumed).
    fn read_element(&mut self) -> Result<ElemOutcome, JsonError> {
        // Between elements: skip whitespace, then decide element vs close.
        match skip_ws_peek(&mut self.inner).map_err(JsonError::Io)? {
            None => {
                return Err(self.structural("unexpected end of input inside the array"));
            }
            Some(b']') => {
                self.inner.consume(1);
                if self.allow_close {
                    return Ok(ElemOutcome::Close);
                }
                return Err(self.structural("trailing comma before `]`"));
            }
            Some(_) => {}
        }

        // Scan one element: bytes up to a `,` or `]` at depth 0 outside a string.
        self.buf.clear();
        let (mut depth, mut in_str, mut esc) = (0u32, false, false);
        let mut truncated = false;
        loop {
            let chunk = self.inner.fill_buf().map_err(JsonError::Io)?;
            if chunk.is_empty() {
                return Err(self.structural("unexpected end of input inside the array"));
            }
            let mut end = None; // (bytes of chunk belonging to the element, last?)
            for (i, &b) in chunk.iter().enumerate() {
                if in_str {
                    if esc {
                        esc = false;
                    } else if b == b'\\' {
                        esc = true;
                    } else if b == b'"' {
                        in_str = false;
                    }
                    continue;
                }
                match b {
                    b'"' => in_str = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' if depth > 0 => depth -= 1,
                    b']' => {
                        end = Some((i, true));
                        break;
                    }
                    b'}' => return Err(self.structural("unbalanced `}` in the array")),
                    b',' if depth == 0 => {
                        end = Some((i, false));
                        break;
                    }
                    _ => {}
                }
            }
            let (take, terminator) = match end {
                Some((i, last)) => (i, Some(last)),
                None => (chunk.len(), None),
            };
            if !truncated {
                let room = self.max_record.saturating_sub(self.buf.len());
                if take > room {
                    self.buf.extend_from_slice(&chunk[..room]);
                    truncated = true;
                } else {
                    self.buf.extend_from_slice(&chunk[..take]);
                }
            }
            match terminator {
                Some(last) => {
                    self.inner.consume(take + 1); // drain through `,` / `]`
                    self.allow_close = false; // after a comma the next `]` is trailing
                    return Ok(ElemOutcome::Elem { truncated, last });
                }
                None => self.inner.consume(take),
            }
        }
    }

    /// After the closing `]`, only whitespace may remain.
    fn check_trailing(&mut self) -> Result<(), JsonError> {
        match skip_ws_peek(&mut self.inner).map_err(JsonError::Io)? {
            None => Ok(()),
            Some(_) => Err(self.structural("trailing content after the array's `]`")),
        }
    }

    fn structural(&self, message: &str) -> JsonError {
        JsonError::Parse {
            line: self.index,
            message: message.into(),
            raw: String::new(),
        }
    }
}

/// Skip ASCII whitespace and peek the next byte without consuming it.
fn skip_ws_peek<R: BufRead>(inner: &mut R) -> std::io::Result<Option<u8>> {
    loop {
        let chunk = inner.fill_buf()?;
        if chunk.is_empty() {
            return Ok(None);
        }
        match chunk.iter().position(|b| !b.is_ascii_whitespace()) {
            Some(i) => {
                let b = chunk[i];
                inner.consume(i);
                return Ok(Some(b));
            }
            None => {
                let n = chunk.len();
                inner.consume(n);
            }
        }
    }
}

impl JsonArrayReader<BufReader<File>> {
    /// Open a JSON-array file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JsonError> {
        JsonArrayReader::new(BufReader::new(File::open(path)?))
    }
}

impl<R: Read> JsonArrayReader<BufReader<R>> {
    /// Build from any reader (buffered internally). Streaming — the reader is
    /// *not* consumed up front; elements are scanned incrementally.
    pub fn from_reader(r: R) -> Result<Self, JsonError> {
        JsonArrayReader::new(BufReader::new(r))
    }
}

impl<R: BufRead> Iterator for JsonArrayReader<R> {
    type Item = Result<Value, JsonError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Framing is `next_element_bytes`' job, here and in the tape reader, so the
        // array's state machine has exactly one implementation. This adds only the
        // parse — the same split the JSONL reader uses.
        match self.next_element_bytes()? {
            Err(e) => Some(Err(e)),
            Ok(()) => {
                // simd-json parses **destructively**, so it gets a scratch copy and
                // `buf` keeps the original bytes — that way the raw element is only
                // materialized on the error path, not allocated for every good one.
                // (The JSONL reader has worked this way since the per-row allocation
                // pass; the array reader was simply never brought across.)
                self.scratch.clear();
                self.scratch.extend_from_slice(&self.buf);
                self.scratch.reserve(SIMD_SLACK);
                match simd_json::serde::from_slice::<Value>(&mut self.scratch) {
                    Ok(v) => Some(Ok(v)),
                    Err(e) => Some(Err(JsonError::Parse {
                        line: self.index,
                        message: e.to_string(),
                        raw: self.raw_element(),
                    })),
                }
            }
        }
    }
}

/// Open a boxed record source for a path in the requested format. Both stream:
/// `jsonl` a line at a time, `json-array` an element at a time.
pub fn open_reader(
    path: impl AsRef<Path>,
    format: shapeshift_core::SourceFormat,
) -> Result<RecordSource, JsonError> {
    let path = path.as_ref();
    match (format, is_stdin(path)) {
        (shapeshift_core::SourceFormat::Jsonl, true) => {
            Ok(Box::new(JsonlReader::new(BufReader::new(std::io::stdin()))) as RecordSource)
        }
        (shapeshift_core::SourceFormat::Jsonl, false) => {
            Ok(Box::new(JsonlReader::open(path)?) as RecordSource)
        }
        (shapeshift_core::SourceFormat::JsonArray, true) => {
            Ok(Box::new(JsonArrayReader::new(BufReader::new(std::io::stdin()))?) as RecordSource)
        }
        (shapeshift_core::SourceFormat::JsonArray, false) => {
            Ok(Box::new(JsonArrayReader::open(path)?) as RecordSource)
        }
    }
}

/// `-` means standard input, the usual shell convention — so shapeshift composes in a
/// pipeline (`producer | shapeshift shape -i - -o out.parquet`).
pub fn is_stdin(path: &Path) -> bool {
    path.as_os_str() == "-"
}

/// Whether an input can be opened and read a second time. Standard input, a pipe, and a
/// FIFO cannot: bytes consumed by one pass are gone for the next. A caller that would
/// otherwise buffer a whole source to replay it uses this to refuse instead.
pub fn is_rereadable(path: &Path) -> bool {
    if is_stdin(path) {
        return false;
    }
    match std::fs::metadata(path) {
        Ok(m) => m.is_file(),
        // Non-existent / unreadable: let the real open report it properly.
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn jsonl_skips_blanks_and_streams() {
        let data = "{\"a\":1}\n   \n{\"a\":2}\n\n{\"a\":3}";
        let rows: Vec<Value> = JsonlReader::new(Cursor::new(data))
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["a"], 3);
    }

    #[test]
    fn jsonl_bad_line_reports_line_and_raw() {
        let data = "{\"a\":1}\noops not json\n{\"a\":3}";
        let mut it = JsonlReader::new(Cursor::new(data));
        assert!(it.next().unwrap().is_ok());
        match it.next().unwrap() {
            Err(JsonError::Parse { line, raw, .. }) => {
                assert_eq!(line, 2);
                assert_eq!(raw, "oops not json");
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
        // The reader recovers and yields the next good record.
        assert_eq!(it.next().unwrap().unwrap()["a"], 3);
    }

    #[test]
    fn json_array_reads_elements() {
        let data = r#"[{"a":1},{"a":2}]"#;
        let rows: Vec<Value> = JsonArrayReader::from_reader(Cursor::new(data))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn json_array_rejects_non_array() {
        let data = r#"{"a":1}"#;
        assert!(JsonArrayReader::from_reader(Cursor::new(data)).is_err());
    }

    #[test]
    fn json_array_handles_nesting_strings_and_whitespace() {
        // Commas and brackets inside strings and nested values must not split
        // elements; scalars mix with objects; arbitrary whitespace throughout.
        let data =
            "\n [ {\"s\": \"a,b]{\\\"\", \"n\": {\"xs\": [1,2,3]}} ,\n 42 ,\n \"x,y\" , null ]\n";
        let rows: Vec<Value> = JsonArrayReader::from_reader(Cursor::new(data))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["s"], "a,b]{\"");
        assert_eq!(rows[0]["n"]["xs"][2], 3);
        assert_eq!(rows[1], 42);
        assert_eq!(rows[2], "x,y");
        assert!(rows[3].is_null());
    }

    #[test]
    fn json_array_empty_is_zero_records() {
        let rows: Vec<Value> = JsonArrayReader::from_reader(Cursor::new(" [ ] "))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn json_array_bad_element_is_recoverable_with_index() {
        // A balanced-but-invalid element rejects with its element index and raw
        // bytes; the reader recovers to the next element (JSONL parity).
        let data = r#"[{"a":1}, {"a":}, {"a":3}]"#;
        let mut it = JsonArrayReader::from_reader(Cursor::new(data)).unwrap();
        assert_eq!(it.next().unwrap().unwrap()["a"], 1);
        match it.next().unwrap() {
            Err(JsonError::Parse { line, raw, .. }) => {
                assert_eq!(line, 2, "element index reported in `line`");
                assert_eq!(raw, r#"{"a":}"#);
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
        assert_eq!(it.next().unwrap().unwrap()["a"], 3);
        assert!(it.next().is_none());
    }

    #[test]
    fn json_array_oversized_element_is_rejected_and_reader_recovers() {
        let big = format!("{{\"pad\":\"{}\"}}", "x".repeat(100));
        let data = format!("[{{\"a\":1}}, {big}, {{\"a\":2}}]");
        let mut it = JsonArrayReader::from_reader(Cursor::new(data))
            .unwrap()
            .with_max_record(16);
        assert_eq!(it.next().unwrap().unwrap()["a"], 1);
        match it.next().unwrap() {
            Err(JsonError::Parse { line, message, .. }) => {
                assert_eq!(line, 2);
                assert!(message.contains("exceeds"));
            }
            other => panic!("expected an oversize reject, got {other:?}"),
        }
        assert_eq!(it.next().unwrap().unwrap()["a"], 2);
        assert!(it.next().is_none());
    }

    #[test]
    fn json_array_structural_errors_are_reported_once() {
        // Truncated array: the good prefix is yielded, then one structural error.
        let mut it = JsonArrayReader::from_reader(Cursor::new(r#"[{"a":1},"#)).unwrap();
        assert_eq!(it.next().unwrap().unwrap()["a"], 1);
        assert!(matches!(it.next(), Some(Err(JsonError::Parse { .. }))));
        assert!(it.next().is_none());

        // Trailing comma.
        let mut it = JsonArrayReader::from_reader(Cursor::new("[1,]")).unwrap();
        assert_eq!(it.next().unwrap().unwrap(), 1);
        match it.next().unwrap() {
            Err(JsonError::Parse { message, .. }) => assert!(message.contains("trailing comma")),
            other => panic!("expected a trailing-comma error, got {other:?}"),
        }
        assert!(it.next().is_none());

        // Trailing garbage after `]`: the final element still comes through first.
        let mut it = JsonArrayReader::from_reader(Cursor::new("[1,2] junk")).unwrap();
        assert_eq!(it.next().unwrap().unwrap(), 1);
        assert_eq!(it.next().unwrap().unwrap(), 2);
        match it.next().unwrap() {
            Err(JsonError::Parse { message, .. }) => assert!(message.contains("trailing content")),
            other => panic!("expected a trailing-content error, got {other:?}"),
        }
        assert!(it.next().is_none());
    }

    #[test]
    fn json_array_streams_without_materializing_the_document() {
        // A reader that yields the array in tiny chunks and counts how much was
        // pulled: after the first element, only a sliver of the input has been
        // consumed — proof the document is not slurped up front.
        struct Dribble {
            data: Vec<u8>,
            pos: usize,
        }
        impl Read for Dribble {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = out.len().min(7).min(self.data.len() - self.pos);
                out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }
        let mut data = b"[".to_vec();
        for i in 0..10_000 {
            if i > 0 {
                data.push(b',');
            }
            data.extend_from_slice(format!("{{\"i\":{i}}}").as_bytes());
        }
        data.push(b']');
        let total = data.len();
        let src = std::rc::Rc::new(std::cell::Cell::new(0usize));

        struct Counting {
            inner: Dribble,
            seen: std::rc::Rc<std::cell::Cell<usize>>,
        }
        impl Read for Counting {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = self.inner.read(out)?;
                self.seen.set(self.seen.get() + n);
                Ok(n)
            }
        }
        let mut it = JsonArrayReader::from_reader(Counting {
            inner: Dribble { data, pos: 0 },
            seen: src.clone(),
        })
        .unwrap();
        assert_eq!(it.next().unwrap().unwrap()["i"], 0);
        assert!(
            src.get() < total / 100,
            "after one element only a sliver should be consumed (saw {} of {total})",
            src.get()
        );
        assert_eq!(it.count(), 9_999); // the rest still arrive
    }

    #[test]
    fn oversized_line_is_rejected_and_reader_recovers() {
        // A huge line (over the cap) between two good ones: it must reject (not OOM),
        // then the reader keeps going to the next record.
        let big = "x".repeat(100);
        let data = format!("{{\"a\":1}}\n{big}\n{{\"a\":2}}\n");
        let mut it = JsonlReader::new(Cursor::new(data)).with_max_line(16);
        assert_eq!(it.next().unwrap().unwrap()["a"], 1);
        match it.next().unwrap() {
            Err(JsonError::Parse { line, message, .. }) => {
                assert_eq!(line, 2);
                assert!(message.contains("exceeds"));
            }
            other => panic!("expected an oversize reject, got {other:?}"),
        }
        assert_eq!(it.next().unwrap().unwrap()["a"], 2);
        assert!(it.next().is_none());
    }
}
