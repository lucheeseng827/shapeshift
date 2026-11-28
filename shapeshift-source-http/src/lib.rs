//! The HTTP source: records pulled from a REST endpoint, a page at a time.
//!
//! A [`HttpSource`] reads `source.http` (see `docs/SPEC.md` §2.6): the first page's URL, how
//! to reach the next ([`HttpPaging`]), where each response's records are, and, when
//! `incremental` is set, the field the position follows. The position rules are
//! [`shapeshift_core::Tracker`]'s; this crate only asks the endpoint for what they need and
//! hands each record to them.
//!
//! What it guarantees about credentials:
//! - They come from environment variables the spec names, and appear in no message: every
//!   URL this crate prints is cut to its scheme, host and path, and every message it builds
//!   is scrubbed of the credential's value.
//! - They go only to the origin (scheme, host, port) of `source.http.url`. A redirect or a
//!   `next` link to another origin, or from https down to http, fails the run.
//! - Over plain http they go only to a loopback address, unless the caller allows it
//!   ([`HttpOptions::insecure`], the CLI's `--insecure-source`).

mod body;
mod client;
mod soap;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use shapeshift_core::{
    Bounds, CsvOptions, HttpPaging, HttpSourceSpec, Position, Progress, Result, ShapeError, Source,
    Tracker, Verdict, XmlOptions,
};
use url::Url;

pub use client::display_url;
use client::{Client, Page};

/// What the caller allows beyond the spec.
#[derive(Debug, Clone, Default)]
pub struct HttpOptions {
    /// Send credentials over plain http to a machine other than this one.
    pub insecure: bool,
    /// The first retry's wait when the endpoint gives no `Retry-After` (doubling after).
    /// Default 500 ms.
    pub backoff: Option<std::time::Duration>,
    /// The only hosts a request may go to (`host` or `host:port`), whatever the spec says: the
    /// first URL, each redirect, each page (the CLI's `--source-hosts`). Empty: no limit.
    pub hosts: Vec<String>,
}

/// The most pages one run fetches before it fails, unless `paging.max_pages` says otherwise:
/// a guard against an endpoint whose paging never ends.
pub const DEFAULT_MAX_PAGES: u64 = 100_000;

type Outcome = Arc<Mutex<Option<(Option<Position>, Progress)>>>;

/// An endpoint, ready to be read from a position.
pub struct HttpSource {
    spec: HttpSourceSpec,
    csv: Option<CsvOptions>,
    xml: Option<XmlOptions>,
    client: Arc<Client>,
    outcome: Outcome,
}

impl HttpSource {
    /// Check the spec against the environment (its credential variables are set) and the
    /// caller's options (plain http with credentials). Nothing is sent yet.
    pub fn new(
        spec: &HttpSourceSpec,
        csv: Option<CsvOptions>,
        xml: Option<XmlOptions>,
        options: HttpOptions,
    ) -> Result<HttpSource> {
        spec.validate()?;
        let client = Client::new(spec, &options)?;
        Ok(HttpSource {
            spec: spec.clone(),
            csv,
            xml,
            client: Arc::new(client),
            outcome: Arc::new(Mutex::new(None)),
        })
    }
}

impl Source for HttpSource {
    fn kind(&self) -> &'static str {
        "http"
    }

    fn identity(&self) -> String {
        let from = self
            .spec
            .incremental
            .as_ref()
            .map_or("", |i| i.from.as_str());
        format!("{} {from}", display_url(self.client.base()))
    }

    fn open(
        &mut self,
        start: Option<Position>,
        bounds: Bounds,
    ) -> Result<Box<dyn Iterator<Item = Result<Value>> + Send>> {
        // A slot of this run's own: an earlier run's iterator, still draining, writes to its own.
        self.outcome = Arc::new(Mutex::new(None));
        let tracker = match &self.spec.incremental {
            Some(inc) => Some(Tracker::new(inc.rules()?, bounds, start)?),
            None if bounds.any() => {
                return Err(ShapeError::Spec(
                    "--max-records, --max-pages and --max-wait bound an incremental source \
                     (`source.http.incremental`), whose next run goes on from where this one \
                     stopped; a source with no position would only be cut short"
                        .into(),
                ))
            }
            None => None,
        };
        let base = self.client.base().clone();
        let first = first_url(&self.spec, &base, tracker.as_ref());
        Ok(Box::new(Pull {
            spec: self.spec.clone(),
            csv: self.csv.clone(),
            xml: self.xml.clone(),
            client: self.client.clone(),
            tracker,
            base,
            next: Some(first),
            page_no: self
                .spec
                .paging
                .as_ref()
                .and_then(|p| p.first_page)
                .unwrap_or(1),
            offset: 0,
            pages: 0,
            last_page: None,
            buffer: VecDeque::new(),
            page_open: false,
            read: 0,
            failed: false,
            outcome: self.outcome.clone(),
        }))
    }

    fn outcome(&self) -> Option<(Option<Position>, Progress)> {
        self.outcome.lock().expect("outcome lock").clone()
    }
}

/// The lower bound a run asks for, as the endpoint's query parameter takes it.
fn bound_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The first request: the spec's URL, its `query`, the incremental lower bound, and the
/// first page's paging parameters.
fn first_url(spec: &HttpSourceSpec, base: &Url, tracker: Option<&Tracker>) -> Url {
    let mut u = base.clone();
    let lower = tracker.and_then(|t| t.lower_bound());
    {
        let mut q = u.query_pairs_mut();
        for (k, v) in &spec.query {
            q.append_pair(k, v);
        }
        if let (Some(inc), Some((v, _))) = (&spec.incremental, &lower) {
            q.append_pair(&inc.param, &bound_text(v));
        }
    }
    if let Some(p) = &spec.paging {
        paging_params(&mut u, p, p.first_page.unwrap_or(1), 0, None);
    }
    if u.query() == Some("") {
        u.set_query(None);
    }
    u
}

/// Set the page / offset / cursor parameters on `u` (which already carries the rest).
fn paging_params(u: &mut Url, p: &HttpPaging, page_no: u64, offset: u64, cursor: Option<&str>) {
    let per_page = p.per_page.unwrap_or(0).to_string();
    let mut q = u.query_pairs_mut();
    if let (Some(page), Some(size)) = (&p.page, &p.size) {
        q.append_pair(page, &page_no.to_string());
        q.append_pair(size, &per_page);
    }
    if let (Some(off), Some(limit)) = (&p.offset, &p.limit) {
        q.append_pair(off, &offset.to_string());
        q.append_pair(limit, &per_page);
    }
    if let (Some(param), Some(c)) = (&p.cursor_param, cursor) {
        q.append_pair(param, c);
    }
}

/// One run's read of the endpoint: a page at a time, each record judged by the position
/// rules on its way out.
struct Pull {
    spec: HttpSourceSpec,
    csv: Option<CsvOptions>,
    xml: Option<XmlOptions>,
    client: Arc<Client>,
    tracker: Option<Tracker>,
    /// The spec's URL; page, offset and cursor paging rebuild each request from it.
    base: Url,
    /// The next page's URL; `None` once paging has ended.
    next: Option<Url>,
    page_no: u64,
    offset: u64,
    pages: u64,
    /// A digest of the last page's body (page and offset paging), to catch an endpoint that
    /// ignores the paging parameter.
    last_page: Option<u64>,
    buffer: VecDeque<Value>,
    /// A page's records are being handed out: when they run out, the page is done.
    page_open: bool,
    read: u64,
    failed: bool,
    outcome: Outcome,
}

impl Pull {
    fn fail(&mut self, e: ShapeError) -> Option<Result<Value>> {
        self.failed = true;
        self.buffer.clear();
        Some(Err(e))
    }

    /// The run is over: record what to commit. `more`: the endpoint had more to give.
    fn finish(&mut self, more: bool) -> Option<Result<Value>> {
        self.next = None;
        self.buffer.clear();
        if let Some(t) = self.tracker.as_mut() {
            t.finish(more);
        }
        let out = match &self.tracker {
            Some(t) => (t.commit(), t.progress().clone()),
            None => (
                None,
                Progress {
                    new: self.read,
                    ..Progress::default()
                },
            ),
        };
        *self.outcome.lock().expect("outcome lock") = Some(out);
        None
    }

    /// Fetch the next page into the buffer, and work out the one after it.
    fn fetch(&mut self, url: Url) -> Result<()> {
        let max_pages = self
            .spec
            .paging
            .as_ref()
            .and_then(|p| p.max_pages)
            .unwrap_or(DEFAULT_MAX_PAGES);
        if self.pages >= max_pages {
            return Err(ShapeError::Source(format!(
                "{} still had pages after {max_pages} (`paging.max_pages`): its paging may not \
                 end. Raise the limit, or bound the run (--max-pages)",
                display_url(&self.base)
            )));
        }
        self.pages += 1;
        let page: Page = self.client.get(&url)?;
        let (records, body) =
            body::records(&self.spec, self.csv.as_ref(), self.xml.as_ref(), &page)?;
        let count = records.len() as u64;
        self.next = None;
        if let Some(p) = self.spec.paging.clone() {
            // Page and offset paging end at an empty page: a server may cap its page below
            // `per_page`, and a short page then is not the last.
            let more = count > 0;
            if (p.page.is_some() || p.offset.is_some()) && more {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                page.body.hash(&mut h);
                let digest = h.finish();
                if self.last_page == Some(digest) {
                    return Err(ShapeError::Source(format!(
                        "{} gave the same page twice: it may not take `paging.{}`, so its paging \
                         would never end",
                        display_url(&page.url),
                        if p.page.is_some() { "page" } else { "offset" }
                    )));
                }
                self.last_page = Some(digest);
            }
            if p.next.is_some() {
                let link = body
                    .as_ref()
                    .and_then(|b| shapeshift_core::select(b, p.next.as_deref().unwrap_or_default()))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                if let Some(link) = link {
                    self.next = Some(self.client.join(&page.url, link)?);
                }
            } else if p.link {
                if let Some(link) = &page.link_next {
                    self.next = Some(self.client.join(&page.url, link)?);
                }
            } else if p.page.is_some() {
                if more {
                    self.page_no += 1;
                    self.next = Some(self.rebuild(&p, None));
                }
            } else if p.offset.is_some() {
                if more {
                    self.offset += count;
                    self.next = Some(self.rebuild(&p, None));
                }
            } else if let Some(path) = &p.cursor {
                let token = body
                    .as_ref()
                    .and_then(|b| shapeshift_core::select(b, path))
                    .and_then(|v| match v {
                        Value::String(s) if !s.is_empty() => Some(s.clone()),
                        Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    });
                if let Some(token) = token {
                    self.next = Some(self.rebuild(&p, Some(&token)));
                }
            }
            if self.next.as_ref() == Some(&page.url) {
                return Err(ShapeError::Source(format!(
                    "{} gave the page it was on as its next page: its paging would never end",
                    display_url(&page.url)
                )));
            }
        }
        self.buffer = records.into();
        self.page_open = true;
        Ok(())
    }

    /// A page, offset or cursor request: the base URL with the run's parameters.
    fn rebuild(&self, p: &HttpPaging, cursor: Option<&str>) -> Url {
        let mut u = first_url(
            &HttpSourceSpec {
                paging: None,
                ..self.spec.clone()
            },
            &self.base,
            self.tracker.as_ref(),
        );
        paging_params(&mut u, p, self.page_no, self.offset, cursor);
        u
    }

    fn judge(&mut self, rec: &Value) -> Result<Verdict> {
        let Some(t) = self.tracker.as_mut() else {
            return Ok(Verdict::Take { replay: false });
        };
        let inc = self
            .spec
            .incremental
            .as_ref()
            .expect("a tracker has a spec");
        let text_format = self.spec.body_format() != shapeshift_core::HttpFormat::Json
            && self.spec.body_format() != shapeshift_core::HttpFormat::Jsonl;
        let field = |path: &str| -> Result<Value> {
            let v = shapeshift_core::select(rec, path)
                .cloned()
                .unwrap_or(Value::Null);
            Ok(if text_format { body::numeric(v) } else { v })
        };
        let cursor = field(&inc.from)?;
        if cursor.is_null() {
            return Err(ShapeError::Source(format!(
                "a record from {} has no `{}` (`incremental.from`): every record needs its \
                 cursor value",
                display_url(&self.base),
                inc.from
            )));
        }
        let key = match &inc.tiebreak {
            Some(path) => {
                let k = field(path)?;
                if k.is_null() {
                    return Err(ShapeError::Source(format!(
                        "a record from {} has no `{path}` (`incremental.tiebreak`)",
                        display_url(&self.base)
                    )));
                }
                Some(k)
            }
            None => None,
        };
        t.record(&cursor, key.as_ref())
    }
}

impl Iterator for Pull {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Result<Value>> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(rec) = self.buffer.pop_front() {
                match self.judge(&rec) {
                    Ok(Verdict::Take { .. }) => {
                        self.read += 1;
                        return Some(Ok(rec));
                    }
                    Ok(Verdict::Skip) => continue,
                    Ok(Verdict::Stop) => return self.finish(true),
                    Err(e) => return self.fail(e),
                }
            }
            if self.page_open {
                self.page_open = false;
                if let Some(t) = self.tracker.as_mut() {
                    if let Err(e) = t.page_done() {
                        return self.fail(e);
                    }
                }
            }
            let wanted = self.tracker.as_ref().map_or(true, |t| t.wants_more());
            let Some(url) = self.next.take() else {
                return self.finish(false);
            };
            if !wanted {
                return self.finish(true);
            }
            if let Err(e) = self.fetch(url) {
                return self.fail(self.client.scrub(e));
            }
        }
    }
}
