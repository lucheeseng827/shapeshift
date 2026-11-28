//! XML sources: each element at the record path is one record.
//!
//! An element becomes JSON the usual way: its attributes as `@name`, its child elements by
//! name (a name that repeats becomes an array, in document order), and its text as the
//! value itself when it has nothing else, or as `#text` beside attributes and children. An
//! empty element is null, or an object of its attributes. Text is trimmed of the
//! whitespace around it (the indentation between elements is not text), and its line breaks
//! are normalised as XML 1.0 says. Namespace prefixes are dropped, in the document and in the
//! record path alike (`soap:Body` is `Body`), and `xmlns` declarations are not attributes.
//!
//! **Safe by construction.** A document type declaration (`<!DOCTYPE …>`) is refused, so no
//! external entity is fetched and no entity expands; only the five predefined entities and
//! character references are resolved. A record nested deeper than [`MAX_DEPTH`] is refused.
//!
//! The reader streams: one record is held at a time. A document that is not well formed, or
//! uses an entity that is refused, is an error for the whole input, naming the record and the
//! byte where it broke: there is no reliable place to resume, and the records after it would
//! otherwise be lost while the run looked as if it had only skipped one.

use std::io::BufRead;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use serde_json::{Map, Value};
use shapeshift_core::XmlOptions;

use crate::cap::Capped;
use crate::JsonError;

/// The deepest a record's elements may nest. A repeated child adds an array between an
/// element and its children, so the JSON a record becomes can be twice as deep as its
/// elements: half the JSON limit keeps every record within it.
pub const MAX_DEPTH: usize = shapeshift_core::MAX_RECORD_DEPTH / 2;

/// Records of an XML document: the elements at the record path.
pub struct XmlReader<R: BufRead> {
    reader: Reader<Capped<R>>,
    /// Where the current event or record starts, for the byte ceiling.
    mark: Arc<AtomicU64>,
    buf: Vec<u8>,
    /// The record path's element names, outermost first.
    path: Vec<String>,
    /// The path starts at the root (`/a/b`), rather than matching at any depth (`a/b`).
    anchored: bool,
    /// The open elements outside any record.
    stack: Vec<String>,
    /// Records read so far.
    index: u64,
    done: bool,
}

fn local(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}

/// Why reading a record stopped.
enum Stop {
    /// The document is malformed, or does something refused: report it and stop.
    Parse(String),
    /// A document type declaration: refused, and fatal to the run.
    Refused(String),
    Io(std::io::Error),
}

impl<R: BufRead> XmlReader<R> {
    pub fn new(inner: R, opts: &XmlOptions) -> Self {
        Self::with_limit(inner, opts, crate::source::DEFAULT_MAX_LINE as u64)
    }

    /// [`XmlReader::new`] with a byte ceiling on one record (or one event outside a
    /// record) other than the default: past it, the input fails rather than growing in
    /// memory, as an oversized JSON record is refused.
    pub fn with_limit(inner: R, opts: &XmlOptions, limit: u64) -> Self {
        let (inner, mark) = Capped::new(inner, limit, "an XML record");
        // Text is not trimmed per event: an entity splits a text node into pieces, and
        // trimming each would lose the spaces around it. An element's text is trimmed
        // once, whole, instead.
        let reader = Reader::from_reader(inner);
        let record = opts.record.trim();
        XmlReader {
            reader,
            mark,
            buf: Vec::new(),
            path: record
                .trim_start_matches('/')
                .split('/')
                // A step may carry a prefix (`soap:Body`); elements are matched by local
                // name, so the step is too.
                .map(|s| {
                    let s = s.trim();
                    s.rsplit_once(':').map_or(s, |(_, l)| l).to_string()
                })
                .collect(),
            anchored: record.starts_with('/'),
            stack: Vec::new(),
            index: 0,
            done: false,
        }
    }

    fn at_record(&self) -> bool {
        if self.anchored {
            self.stack == self.path
        } else {
            self.stack.ends_with(&self.path)
        }
    }

    fn position(&self) -> u64 {
        self.reader.buffer_position()
    }

    fn fail(&self, what: impl std::fmt::Display) -> Stop {
        Stop::Parse(format!("at byte {}: {what}", self.position()))
    }

    /// Read the attributes of `e` into `m` as `@name`, leaving out namespace declarations.
    fn attributes(&self, e: &BytesStart<'_>, m: &mut Map<String, Value>) -> Result<(), Stop> {
        for a in e.attributes() {
            let a = a.map_err(|err| self.fail(err))?;
            let key = a.key;
            if key.as_ref() == b"xmlns" || key.as_ref().starts_with(b"xmlns:") {
                continue;
            }
            let v = a
                .normalized_value(quick_xml::XmlVersion::default())
                .map_err(|err| self.fail(err))?;
            m.insert(
                format!("@{}", local(key.local_name().as_ref())),
                Value::String(v.into_owned()),
            );
        }
        Ok(())
    }

    /// The value of an element whose start tag `e` was just read, through its end tag.
    fn element(&mut self, e: BytesStart<'static>, depth: usize) -> Result<Value, Stop> {
        if depth > MAX_DEPTH {
            return Err(self.fail(format!("a record nests deeper than {MAX_DEPTH} elements")));
        }
        let mut attrs = Map::new();
        self.attributes(&e, &mut attrs)?;
        let mut children: Vec<(String, Value)> = Vec::new();
        let mut text = String::new();
        let mut buf = Vec::new();
        loop {
            buf.clear();
            let ev = self
                .reader
                .read_event_into(&mut buf)
                .map_err(|err| match err {
                    quick_xml::Error::Io(io) => {
                        Stop::Io(std::io::Error::new(io.kind(), io.to_string()))
                    }
                    other => self.fail(other),
                })?;
            match ev {
                Event::Start(s) => {
                    let name = local(s.local_name().as_ref());
                    let s = s.into_owned();
                    let v = self.element(s, depth + 1)?;
                    children.push((name, v));
                }
                Event::Empty(s) => {
                    let name = local(s.local_name().as_ref());
                    let mut m = Map::new();
                    self.attributes(&s, &mut m)?;
                    children.push((name, if m.is_empty() { Value::Null } else { Value::Object(m) }));
                }
                Event::Text(t) => {
                    text.push_str(&t.xml10_content().map_err(|err| self.fail(err))?);
                }
                Event::CData(c) => {
                    text.push_str(&c.xml10_content().map_err(|err| self.fail(err))?);
                }
                Event::GeneralRef(r) => {
                    if let Some(ch) = r.resolve_char_ref().map_err(|err| self.fail(err))? {
                        text.push(ch);
                    } else {
                        let name = r.decode().map_err(|err| self.fail(err))?;
                        text.push(match name.as_ref() {
                            "amp" => '&',
                            "lt" => '<',
                            "gt" => '>',
                            "quot" => '"',
                            "apos" => '\'',
                            other => {
                                return Err(self.fail(format!(
                                    "the entity `&{other};` is not one of XML's five: it is refused"
                                )))
                            }
                        });
                    }
                }
                Event::End(_) => break,
                Event::DocType(_) => {
                    return Err(Stop::Refused(
                        "the document declares a document type (<!DOCTYPE>): refused, so no external entity is read".into(),
                    ))
                }
                Event::Eof => return Err(self.fail("the document ends inside a record")),
                Event::Comment(_) | Event::PI(_) | Event::Decl(_) => {}
            }
        }
        let text = text.trim();
        if attrs.is_empty() && children.is_empty() {
            return Ok(if text.is_empty() {
                Value::Null
            } else {
                Value::String(text.to_string())
            });
        }
        let mut m = attrs;
        for (name, v) in children {
            match m.get_mut(&name) {
                // An element value is never an array, so an array here is a repeat.
                Some(Value::Array(a)) => a.push(v),
                Some(prev) => {
                    let first = prev.take();
                    *prev = Value::Array(vec![first, v]);
                }
                None => {
                    m.insert(name, v);
                }
            }
        }
        if !text.is_empty() {
            m.insert("#text".into(), Value::String(text.to_string()));
        }
        Ok(Value::Object(m))
    }

    fn stop(&mut self, s: Stop) -> Option<Result<Value, JsonError>> {
        self.done = true;
        Some(Err(match s {
            // Not a `Parse` error: those are one record left out, and the run goes on.
            // Here everything after this point is unreadable, so the input fails whole.
            Stop::Parse(message) => JsonError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "the XML is not readable past record {}: {message}",
                    self.index + 1
                ),
            )),
            Stop::Refused(m) => {
                JsonError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, m))
            }
            Stop::Io(e) => JsonError::Io(e),
        }))
    }
}

impl<R: BufRead> Iterator for XmlReader<R> {
    type Item = Result<Value, JsonError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            // Everything outside a record is read an event at a time, and a record whole:
            // each starts the ceiling's count afresh.
            self.mark
                .store(self.reader.buffer_position(), Ordering::Relaxed);
            self.buf.clear();
            let ev = match self.reader.read_event_into(&mut self.buf) {
                Ok(ev) => ev.into_owned(),
                Err(quick_xml::Error::Io(io)) => {
                    let e = std::io::Error::new(io.kind(), io.to_string());
                    return self.stop(Stop::Io(e));
                }
                Err(e) => {
                    let s = self.fail(e);
                    return self.stop(s);
                }
            };
            match ev {
                Event::Start(s) => {
                    self.stack.push(local(s.local_name().as_ref()));
                    if self.at_record() {
                        let r = self.element(s, 1);
                        self.stack.pop();
                        return match r {
                            Ok(v) => {
                                self.index += 1;
                                Some(Ok(v))
                            }
                            Err(stop) => self.stop(stop),
                        };
                    }
                }
                Event::Empty(s) => {
                    self.stack.push(local(s.local_name().as_ref()));
                    let hit = self.at_record();
                    self.stack.pop();
                    if hit {
                        let mut m = Map::new();
                        if let Err(stop) = self.attributes(&s, &mut m) {
                            return self.stop(stop);
                        }
                        self.index += 1;
                        return Some(Ok(if m.is_empty() { Value::Null } else { Value::Object(m) }));
                    }
                }
                Event::End(_) => {
                    self.stack.pop();
                }
                Event::DocType(_) => {
                    return self.stop(Stop::Refused(
                        "the document declares a document type (<!DOCTYPE>): refused, so no external entity is read".into(),
                    ))
                }
                Event::Eof => {
                    self.done = true;
                    if let Some(open) = self.stack.last().cloned() {
                        let s = self.fail(format!("the document ends inside <{open}>"));
                        return self.stop(s);
                    }
                    return None;
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn read(doc: &str, record: &str) -> Vec<Result<Value, String>> {
        XmlReader::new(
            doc.as_bytes(),
            &XmlOptions {
                record: record.into(),
            },
        )
        .map(|r| r.map_err(|e| e.to_string()))
        .collect()
    }

    #[test]
    fn elements_at_the_path_become_records() {
        let doc = r#"<?xml version="1.0"?>
<Orders xmlns:x="urn:x">
  <Order id="1" x:src="web"><Customer>Ada</Customer><Line sku="a">2</Line><Line sku="b">1</Line><Note/></Order>
  <Order id="2"><Customer>Lin &amp; Co &#x41;</Customer><Total currency="EUR">9.50</Total></Order>
</Orders>"#;
        let want = vec![
            Ok(json!({"@id":"1","@src":"web","Customer":"Ada",
                      "Line":[{"@sku":"a","#text":"2"},{"@sku":"b","#text":"1"}],"Note":null})),
            Ok(
                json!({"@id":"2","Customer":"Lin & Co A","Total":{"@currency":"EUR","#text":"9.50"}}),
            ),
        ];
        assert_eq!(read(doc, "/Orders/Order"), want);
        assert_eq!(read(doc, "Order"), want, "unanchored: at any depth");
        assert!(
            read(doc, "/Order").is_empty(),
            "anchored at the root: no match"
        );
    }

    #[test]
    fn a_broken_document_fails_the_input_rather_than_one_record() {
        let mut r = XmlReader::new(
            "<r><a>1</a><a>2</b><a>3</a></r>".as_bytes(),
            &XmlOptions { record: "a".into() },
        );
        assert_eq!(r.next().unwrap().unwrap(), json!("1"));
        let e = r.next().unwrap().unwrap_err();
        assert!(matches!(e, JsonError::Io(_)), "{e}");
        assert!(e.to_string().contains("past record 2"), "{e}");
        assert!(r.next().is_none());
    }

    #[test]
    fn prefixes_in_the_record_path_are_dropped_too() {
        let doc = r#"<s:E xmlns:s="urn:s"><s:B><m:I xmlns:m="urn:m">1</m:I></s:B></s:E>"#;
        assert_eq!(read(doc, "/s:E/s:B/m:I"), vec![Ok(json!("1"))]);
        assert_eq!(read(doc, "m:I"), vec![Ok(json!("1"))]);
    }

    #[test]
    fn repeated_children_cannot_take_a_record_past_the_json_depth_limit() {
        // Each level repeats its child, so each element level adds an array level too.
        let open = "<b><b/>".repeat(MAX_DEPTH - 1);
        let close = "</b>".repeat(MAX_DEPTH - 1);
        let got = read(&format!("<r><a>{open}{close}</a></r>"), "a");
        let v = got[0].clone().unwrap();
        fn depth(v: &Value) -> usize {
            match v {
                Value::Array(a) => 1 + a.iter().map(depth).max().unwrap_or(0),
                Value::Object(m) => 1 + m.values().map(depth).max().unwrap_or(0),
                _ => 0,
            }
        }
        assert!(
            depth(&v) <= shapeshift_core::MAX_RECORD_DEPTH,
            "{}",
            depth(&v)
        );
    }

    #[test]
    fn line_breaks_in_text_are_normalised() {
        assert_eq!(read("<r><a>x\r\ny</a></r>", "a"), vec![Ok(json!("x\ny"))]);
    }

    #[test]
    fn a_record_past_the_ceiling_fails_the_input() {
        let opts = XmlOptions { record: "a".into() };
        let big = format!("<r><a>1</a><a>{}</a></r>", "x".repeat(100_000));
        let got: Vec<_> = XmlReader::with_limit(
            std::io::BufReader::with_capacity(4096, big.as_bytes()),
            &opts,
            16 * 1024,
        )
        .collect();
        assert_eq!(got[0].as_ref().unwrap(), &json!("1"));
        assert!(
            matches!(&got[1], Err(JsonError::Io(e)) if e.to_string().contains("runs past")),
            "{:?}",
            got[1]
        );
        assert_eq!(got.len(), 2);
        // Many small records pass the same ceiling.
        let many = format!("<r>{}</r>", "<a>1</a>".repeat(20_000));
        let n = XmlReader::with_limit(
            std::io::BufReader::with_capacity(4096, many.as_bytes()),
            &opts,
            16 * 1024,
        )
        .filter(|r| r.is_ok())
        .count();
        assert_eq!(n, 20_000);
    }

    #[test]
    fn soap_envelopes_lose_their_prefixes() {
        let doc = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body><m:GetResponse xmlns:m="urn:m"><m:Item><m:Id>7</m:Id></m:Item><m:Item><m:Id>8</m:Id></m:Item></m:GetResponse></soap:Body>
</soap:Envelope>"#;
        assert_eq!(
            read(doc, "/Envelope/Body/GetResponse/Item"),
            vec![Ok(json!({"Id":"7"})), Ok(json!({"Id":"8"}))]
        );
    }

    #[test]
    fn a_doctype_is_refused_before_any_entity_is_read() {
        let doc = r#"<?xml version="1.0"?><!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]><r><a>&x;</a></r>"#;
        let got = read(doc, "a");
        assert_eq!(got.len(), 1);
        assert!(
            got[0].clone().unwrap_err().contains("document type"),
            "{:?}",
            got[0]
        );
    }

    #[test]
    fn an_unknown_entity_and_a_broken_document_stop_with_their_position() {
        let got = read("<r><a>ok</a><a>&nope;</a><a>never</a></r>", "a");
        assert_eq!(got[0], Ok(json!("ok")));
        assert!(
            got[1].clone().unwrap_err().contains("&nope;"),
            "{:?}",
            got[1]
        );
        assert_eq!(got.len(), 2, "reading stops at the error");
        let got = read("<r><a>1</a><a>2</b></r>", "a");
        assert_eq!(got[0], Ok(json!("1")));
        assert!(
            got[1].clone().unwrap_err().contains("at byte"),
            "{:?}",
            got[1]
        );
        let got = read("<r><a>1</a><a>2", "a");
        assert!(
            got.last()
                .unwrap()
                .clone()
                .unwrap_err()
                .contains("ends inside"),
            "{got:?}"
        );
    }

    #[test]
    fn deep_records_are_refused() {
        let doc = format!(
            "<r><a>{}{}</a></r>",
            "<b>".repeat(MAX_DEPTH + 1),
            "</b>".repeat(MAX_DEPTH + 1)
        );
        let got = read(&doc, "a");
        assert!(
            got[0].clone().unwrap_err().contains("deeper than"),
            "{:?}",
            got[0]
        );
    }

    #[test]
    fn cdata_and_an_empty_record_with_attributes() {
        assert_eq!(
            read("<r><a><![CDATA[<x> & y]]></a></r>", "a"),
            vec![Ok(json!("<x> & y"))]
        );
        assert_eq!(
            read(r#"<r><a k="v"/></r>"#, "a"),
            vec![Ok(json!({"@k":"v"}))]
        );
    }
}
