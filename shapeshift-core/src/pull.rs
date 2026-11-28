//! Sources that are pulled rather than read from a file: their spec (`source.http`), and the
//! [`Source`] seam every one implements, with the position rules of [`crate::position`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, ShapeError};
use crate::position::{Bounds, Position, Progress};

/// A source the run pulls from (HTTP now, MySQL next), from a position the table recorded.
pub trait Source: Send {
    /// The summary key's suffix: the position is recorded as `shapeshift.source.<kind>`.
    fn kind(&self) -> &'static str;

    /// Start reading from `start` (the table's recorded position; `None` on the first run)
    /// within `bounds`. The records stream: a source holds one page, or one batch of rows,
    /// at a time.
    fn open(
        &mut self,
        start: Option<Position>,
        bounds: Bounds,
    ) -> Result<Box<dyn Iterator<Item = Result<Value>> + Send>>;

    /// Once the records are drained: the position to commit with them (`None` when there
    /// is none to record, as on a first run that read nothing), and what the run did.
    /// `None` before then.
    fn outcome(&self) -> Option<(Option<Position>, Progress)>;

    /// What the position is a position *in*: the endpoint or table and the cursor field, as
    /// text. It is recorded beside the position, and a run whose source says otherwise is
    /// refused, so a table never resumes another source's position.
    fn identity(&self) -> String;
}

/// The position to resume `source` from, out of what the table recorded for its kind: refused
/// when it was recorded for another source (its `"of"`), since resuming there would skip or
/// repeat rows.
pub fn resume_from(recorded: Option<Value>, source: &dyn Source) -> Result<Option<Position>> {
    if let Some(of) = recorded.as_ref().and_then(|r| r["of"].as_str()) {
        let ours = source.identity();
        if of != ours {
            return Err(ShapeError::Source(format!(
                "the table's {} position is for `{of}`, and this run reads `{ours}`: resuming \
                 there would skip or repeat rows. Write this source to a table of its own",
                source.kind()
            )));
        }
    }
    recorded.map(|v| Position::from_json(&v)).transpose()
}

/// What a commit records for `source` at `position`: the position, and which source it is in.
pub fn recorded_position(position: &Position, source: &dyn Source) -> Value {
    let mut v = position.to_json();
    v["of"] = Value::String(source.identity());
    v
}

/// A short, stable digest of `text` (64-bit FNV-1a, hex), for an identity that would be long.
pub fn digest(text: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// `source.http`: a REST endpoint, paged, with credentials from the environment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct HttpSourceSpec {
    /// The first page's URL. Paging and the incremental parameter add to its query.
    pub url: String,

    /// `GET` (the default) or `POST`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<HttpMethod>,

    /// A `POST` body, sent as is on every request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,

    /// Headers sent on every request. Never credentials: those come from `auth`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,

    /// Query parameters added to every request (a sort order, a filter).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,

    /// Credentials, by the names of environment variables that hold them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuth>,

    /// How each response's body reads: `json` (the default), `jsonl`, `csv`, `tsv` or
    /// `xml` (with `source.csv` / `source.xml` as for a file).
    #[serde(default)]
    pub format: HttpFormat,

    /// Where a JSON response's records are, as a path (`data.items`). Absent: an array body
    /// is the records, and an object body is one record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<String>,

    /// How to reach the next page. Absent: one request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paging: Option<HttpPaging>,

    /// Read only what is new since the table's recorded position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental: Option<Incremental>,

    /// One request's timeout, as a duration (`30s`, `2m`). Default `60s`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,

    /// How many times a request is retried on 429, a 5xx or a broken connection. Default 5.
    /// A `POST` is retried too, so it must be a read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// Follow a next-page link or a redirect to another origin (scheme, host, port) than
    /// `url`'s. Off: such a link fails the run. On: it is followed, but neither the credential
    /// nor `headers` are sent there.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_other_origins: bool,

    /// A SOAP service: each request POSTs an envelope shapeshift builds around
    /// `soap.body_template`, and the response is read as XML (`source.xml.record`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soap: Option<SoapSpec>,
}

/// `source.http.soap`: the operation, and the WS-Security username token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SoapSpec {
    /// The `SOAPAction` (1.1), or the `action` of the content type (1.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    /// What goes in the envelope's `Body`: the operation element, like
    /// `<GetOrders xmlns="urn:shop"><since>{{updated_since}}</since></GetOrders>`. A
    /// `{{name}}` is filled with the request parameter of that name (`incremental.param`, a
    /// paging parameter, a `query` entry), XML-escaped; a parameter the template names goes
    /// in the body, not the URL.
    pub body_template: String,
    /// `1.1` (the default) or `1.2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<SoapVersion>,
    /// WS-Security: the environment variables holding the username token's user and
    /// password (`PasswordText`). Both or neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_env: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub enum SoapVersion {
    #[serde(rename = "1.1")]
    V11,
    #[serde(rename = "1.2")]
    V12,
}

/// `1.1` or `1.2`, written as YAML writes it: a number, or a string.
impl<'de> Deserialize<'de> for SoapVersion {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        let text = match &v {
            Value::String(s) => s.trim().to_string(),
            Value::Number(n) => n.to_string(),
            _ => String::new(),
        };
        match text.as_str() {
            "1.1" => Ok(SoapVersion::V11),
            "1.2" => Ok(SoapVersion::V12),
            _ => Err(serde::de::Error::custom(format!(
                "`soap.version` is 1.1 or 1.2, not {v}"
            ))),
        }
    }
}

impl SoapSpec {
    /// The template with each `{{ name }}` replaced by `value(name)`, in one left-to-right
    /// pass: a value is never itself read for placeholders.
    pub fn render(&self, mut value: impl FnMut(&str) -> String) -> String {
        let mut out = String::with_capacity(self.body_template.len());
        let mut rest = self.body_template.as_str();
        while let Some(i) = rest.find("{{") {
            let after = &rest[i + 2..];
            let Some(j) = after.find("}}") else { break };
            out.push_str(&rest[..i]);
            out.push_str(&value(after[..j].trim()));
            rest = &after[j + 2..];
        }
        out.push_str(rest);
        out
    }

    /// The `{{name}}` placeholders the template names, in order.
    pub fn placeholders(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = self.body_template.as_str();
        while let Some(i) = rest.find("{{") {
            let after = &rest[i + 2..];
            let Some(j) = after.find("}}") else { break };
            let name = after[..j].trim().to_string();
            if !out.contains(&name) {
                out.push(name);
            }
            rest = &after[j + 2..];
        }
        out
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HttpFormat {
    #[default]
    Json,
    Jsonl,
    Csv,
    Tsv,
    Xml,
}

/// Credentials. Each names an environment variable; a spec never holds a secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpAuth {
    /// `Authorization: Bearer <value of this variable>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_env: Option<String>,
    /// `Authorization: Basic …` from a variable holding `user:password`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic_env: Option<String>,
    /// A header of the API's own (`X-Api-Key`), its value from a variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_env: Option<HeaderEnv>,
    /// A query parameter of the API's own (`api_key`), its value from a variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_env: Option<HeaderEnv>,
}

/// A name (a header's, or a query parameter's) and the variable holding its value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderEnv {
    pub name: String,
    pub env: String,
}

/// One paging style. Exactly one of `next`, `link`, `page`, `offset` or `cursor`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct HttpPaging {
    /// A path in a JSON body to the next page's URL (absolute, or relative to the page's).
    /// Paging ends where it is absent, null or empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Follow the `Link: <…>; rel="next"` header.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub link: bool,
    /// The page-number parameter (with `size` and `per_page`): pages from `first_page`
    /// (default 1). Paging ends at an empty page (a server may give fewer than `per_page`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<String>,
    /// The offset parameter (with `limit` and `per_page`): offsets from 0, by the records each
    /// page held. Paging ends at an empty page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    /// A path in a JSON body to the next page's token, sent as `cursor_param`. Paging ends
    /// where it is absent, null or empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_param: Option<String>,
    /// The page-size parameter for `page` paging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// The limit parameter for `offset` paging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<String>,
    /// Records asked for per page (`page` and `offset` paging).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_page: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page: Option<u64>,
    /// The most pages a run fetches before it fails (a loop guard). Default 100,000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pages: Option<u64>,
}

/// `incremental`: the field that orders the source, and how a run asks for what is new.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct Incremental {
    /// The query parameter the run's lower bound is sent as (`updated_since`).
    pub param: String,
    /// The path of the cursor field in each record (`updated_at`).
    pub from: String,
    /// `asc`: the source returns records in ascending cursor order (checked, not trusted).
    /// Bounded runs need it. Absent: no order, and a run reads to the end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrder>,
    /// The cursor is unique and only grows (an auto-increment id): no replay.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub strict: bool,
    /// How far below the position the next run re-reads (a duration; a timestamp cursor).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    /// A unique field ordering the records within one cursor value: a bounded run may stop
    /// inside a group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiebreak: Option<String>,
    /// The largest group sharing one value a bounded run without a tiebreak reads to its end.
    /// Default 100,000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tie: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
}

/// `incremental.max_tie`'s default.
pub const DEFAULT_MAX_TIE: u64 = 100_000;

impl Incremental {
    /// The position rules this spec asks for.
    pub fn rules(&self) -> Result<crate::position::Rules> {
        Ok(crate::position::Rules {
            strict: self.strict,
            ordered: self.order == Some(SortOrder::Asc),
            tiebreak: self.tiebreak.is_some(),
            lookback_ms: self.lookback.as_deref().map(duration_ms).transpose()?,
            max_tie: self.max_tie.unwrap_or(DEFAULT_MAX_TIE),
        })
    }
}

/// A duration (`30s`, `5m`), in milliseconds.
pub fn duration_ms(s: &str) -> Result<i64> {
    crate::changes::parse_duration_ms(s).map_err(ShapeError::Spec)
}

/// A header or parameter name that usually carries a credential.
///
/// Matched by word (split at `-`, `_`, `.` and spaces), so `sort_key`, `keyword` and
/// `Idempotency-Key` are plain names, while `api_key`, `X-Api-Key`, `apikey`, `access_token`
/// and `client_secret` are credentials.
fn looks_secret(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    let words: Vec<&str> = n
        .split(['-', '_', '.', ' '])
        .filter(|w| !w.is_empty())
        .collect();
    const SECRET: &[&str] = &[
        "token",
        "secret",
        "password",
        "passwd",
        "signature",
        "apikey",
        "credential",
        "credentials",
        "authorization",
        "cookie",
        "auth",
    ];
    const KEY_OF: &[&str] = &[
        "api",
        "access",
        "secret",
        "private",
        "client",
        "app",
        "auth",
        "subscription",
    ];
    words.iter().any(|w| SECRET.contains(w))
        || words
            .windows(2)
            .any(|p| p[1] == "key" && KEY_OF.contains(&p[0]))
}

/// Whether a pulled source may connect to `host` on `port`, by an allow-list its runner gives
/// (`--source-hosts`, which a worker sets from its own `--allow-source`, not from the spec).
/// An entry is `host` (any port) or `host:port` (`[::1]:5432` for IPv6), compared without case.
/// The first URL, each redirect and each page, and a database's hosts as its driver parsed
/// them, are all held to it. `Ok` when the list is empty: no limit was given.
pub fn check_source_host(allowed: &[String], host: &str, port: u16) -> Result<()> {
    if allowed.is_empty() {
        return Ok(());
    }
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    let ok = allowed.iter().any(|e| {
        let e = e.trim().to_ascii_lowercase();
        let (h, p) = match e.rsplit_once(':') {
            // `host:port`, or `[v6]:port`; a bare IPv6 address has more than one colon.
            Some((h, p)) if !h.contains(':') || h.ends_with(']') => (h.to_string(), Some(p)),
            _ => (e.clone(), None),
        };
        let h = h.trim_start_matches('[').trim_end_matches(']');
        h == host && p.map_or(true, |p| p.parse::<u16>().ok() == Some(port))
    });
    if ok {
        Ok(())
    } else {
        Err(ShapeError::Source(format!(
            "the source would connect to {host}:{port}, which this run may not reach: its runner \
             allows {} (--source-hosts)",
            allowed.join(", ")
        )))
    }
}

fn env_name_ok(n: &str) -> bool {
    !n.is_empty()
        && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !n.as_bytes()[0].is_ascii_digit()
}

/// `source.mysql` and `source.postgres`: rows from a query, or from a table by a watermark
/// column. One shape for both; [`DbKind`] says which database, and each checks it as its own.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct DbSourceSpec {
    /// `mysql://user@host[:port]/database`, or `postgres://…`. The password comes from the
    /// environment (`SHAPESHIFT_MYSQL_PASSWORD` or `SHAPESHIFT_POSTGRES_PASSWORD`, or the file
    /// its `…_FILE` names), never from the URL.
    pub url: String,

    /// A `SELECT`. When incremental it names `:cursor` (bound to the lower bound) and orders
    /// by the cursor column: `select … where updated_at >= :cursor order by updated_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,

    /// A table (`orders`, or `db.orders`), read by `cursor` and its primary key. Instead of
    /// `query`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,

    /// The column the position follows (a watermark: `updated_at`, an id). Set, the source
    /// is incremental. With `query`, the result column; with `table`, the table's column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    /// A first run's lower bound (`2020-01-01`, `0`): bound to a query's `:cursor` when the
    /// table records no position yet, which a query needs; a table's first run reads from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,

    /// A query's unique column ordering the rows within one cursor value (a table uses its
    /// primary key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiebreak: Option<String>,

    /// How far below the position the next run re-reads (a duration; a DATETIME or TIMESTAMP
    /// cursor). It matters here: `updated_at` is set when a row is written, not when its
    /// transaction commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,

    /// The cursor is unique and only grows (an auto-increment id): no replay.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub strict: bool,

    /// The largest group sharing one value a bounded run without a tiebreak reads to its end.
    /// Default 100,000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tie: Option<u64>,

    /// A PEM file of the CA certificates the server's certificate is checked against, beside
    /// the bundled public roots (Amazon RDS, a private CA).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
}

/// A table or column name a source table read quotes: letters, digits, `_` and `$`.
pub fn mysql_identifier_ok(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// The MySQL source's spec (`source.mysql`).
pub type MysqlSourceSpec = DbSourceSpec;
/// The Postgres source's spec (`source.postgres`).
pub type PostgresSourceSpec = DbSourceSpec;

impl DbSourceSpec {
    /// Whether the source reads by a cursor.
    pub fn incremental(&self) -> bool {
        self.cursor.is_some()
    }

    /// The position rules this spec asks for; `tiebreak` is known only once a table's
    /// primary key is.
    pub fn rules(&self, tiebreak: bool) -> Result<crate::position::Rules> {
        Ok(crate::position::Rules {
            strict: self.strict,
            ordered: true,
            tiebreak,
            lookback_ms: self.lookback.as_deref().map(duration_ms).transpose()?,
            max_tie: self.max_tie.unwrap_or(DEFAULT_MAX_TIE),
        })
    }

    /// Check it as a `kind` source: its URL's scheme, and the fields that go together.
    pub fn validate_as(&self, kind: DbKind) -> Result<()> {
        let bad = |m: String| Err(ShapeError::Spec(m));
        let k = kind.name();
        let Some(rest) = kind.schemes().iter().find_map(|s| self.url.strip_prefix(s)) else {
            return bad(format!(
                "`source.{k}.url` is `{}user@host[:port]/database`",
                kind.schemes()[0]
            ));
        };
        if let Some((_, query)) = rest.split_once('?') {
            // Only settings that change neither where it connects, nor how, nor whose
            // credentials it sends: a spec holds no password, and TLS is decided by the host.
            const ALLOWED: &[&str] = &[
                "connect_timeout",
                "application_name",
                "target_session_attrs",
            ];
            for pair in query.split('&').filter(|p| !p.is_empty()) {
                let name = pair.split('=').next().unwrap_or("");
                if !ALLOWED.contains(&name) {
                    return bad(format!(
                        "`source.{k}.url` sets `{name}` in its query: a password comes from {} \
                         and TLS from where the host is, so a URL may set only {}",
                        kind.password_env(),
                        ALLOWED.join(", ")
                    ));
                }
            }
        }
        let authority = rest.split('/').next().unwrap_or("");
        if let Some((user, _)) = authority.rsplit_once('@') {
            if user.contains(':') {
                let env = kind.password_env();
                return bad(format!(
                    "`source.{k}.url` carries a password: a spec and a command line hold none. \
                     Set {env} (or {env}_FILE)"
                ));
            }
        }
        match (&self.query, &self.table) {
            (Some(_), Some(_)) | (None, None) => {
                return bad(format!(
                    "`source.{k}` takes a `query` or a `table`, one of them"
                ))
            }
            (Some(q), None) => {
                let named = !cursor_marks(q, kind).is_empty();
                if self.incremental() && !named {
                    return bad(
                        "an incremental query names `:cursor`, the lower bound each run binds: \
                         `select … where updated_at >= :cursor order by updated_at`"
                            .into(),
                    );
                }
                if !self.incremental() && named {
                    return bad(format!(
                        "the query names `:cursor`, and no `cursor` column says which column it \
                         follows: set `source.{k}.cursor`"
                    ));
                }
            }
            (None, Some(t)) => {
                let parts: Vec<&str> = t.split('.').collect();
                if parts.len() > 2 || !parts.iter().all(|p| mysql_identifier_ok(p)) {
                    return bad(format!(
                        "`source.{k}.table` {t:?} is a table name (`orders` or `{}.orders`): \
                         letters, digits, `_` and `$`",
                        if kind == DbKind::Postgres {
                            "schema"
                        } else {
                            "db"
                        }
                    ));
                }
                if let Some(c) = &self.cursor {
                    if !mysql_identifier_ok(c) {
                        return bad(format!("`source.{k}.cursor` {c:?} is not a column name"));
                    }
                }
                if self.tiebreak.is_some() {
                    return bad(format!(
                        "a table's tiebreak is its primary key: `source.{k}.tiebreak` is for a \
                         query"
                    ));
                }
            }
        }
        let incremental_only = [
            ("start", self.start.is_some()),
            ("tiebreak", self.tiebreak.is_some()),
            ("lookback", self.lookback.is_some()),
            ("strict", self.strict),
            ("max_tie", self.max_tie.is_some()),
        ];
        if !self.incremental() {
            if let Some((name, _)) = incremental_only.iter().find(|(_, set)| *set) {
                return bad(format!(
                    "`source.{k}.{name}` applies to an incremental source: set `cursor`"
                ));
            }
        }
        if self.strict && (self.lookback.is_some() || self.tiebreak.is_some()) {
            return bad(format!(
                "`source.{k}.strict` has no replay and is unique already: no `lookback` or \
                 `tiebreak`"
            ));
        }
        if self.max_tie == Some(0) {
            return bad(format!("`source.{k}.max_tie` must be at least 1"));
        }
        if let Some(l) = &self.lookback {
            duration_ms(l)?;
        }
        Ok(())
    }
}

/// Where a database query names `:cursor` (byte offsets), as the database reads the text: a
/// string literal, a quoted name, a comment, and a Postgres dollar-quoted body name nothing,
/// and neither does a `::` cast or a longer name (`:cursor_x`). Postgres: `''` inside a
/// string, backslash escapes only in an `E'…'` string, nested `/* */`, and a name may hold a
/// `$` (which then starts no dollar quote). MySQL: backslash escapes in `'…'` and `"…"`,
/// backquoted names, and `#` comments.
pub fn cursor_marks(query: &str, kind: DbKind) -> Vec<usize> {
    let b = query.as_bytes();
    let pg = kind == DbKind::Postgres;
    let name_byte = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80;
    // Just past a quoted run opened at `i`: a doubled quote stays inside; a backslash escapes
    // the next byte where `backslash`.
    let quoted = |i: usize, q: u8, backslash: bool| -> usize {
        let mut j = i + 1;
        while j < b.len() {
            if backslash && b[j] == b'\\' {
                j += 2;
                continue;
            }
            if b[j] == q {
                if b.get(j + 1) == Some(&q) {
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j += 1;
        }
        b.len()
    };
    let line_end = |i: usize| {
        b[i..]
            .iter()
            .position(|&c| c == b'\n')
            .map_or(b.len(), |p| i + p + 1)
    };
    let mut marks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' => i = quoted(i, b'\'', !pg),
            b'"' => i = quoted(i, b'"', !pg),
            b'`' if !pg => i = quoted(i, b'`', false),
            b'-' if b.get(i + 1) == Some(&b'-') => i = line_end(i),
            b'#' if !pg => i = line_end(i),
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // Block comments nest in Postgres, and not in MySQL.
                let (mut depth, mut j) = (0usize, i);
                while j < b.len() {
                    if b[j..].starts_with(b"/*") && (pg || depth == 0) {
                        depth += 1;
                        j += 2;
                    } else if b[j..].starts_with(b"*/") {
                        depth -= 1;
                        j += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        j += 1;
                    }
                }
                i = j;
            }
            b'$' if pg && !b.get(i + 1).is_some_and(|c| c.is_ascii_digit()) => {
                // A $tag$ … $tag$ body (`$1`, a parameter, is not one).
                let tag_end = b[i + 1..]
                    .iter()
                    .position(|c| !(c.is_ascii_alphanumeric() || *c == b'_' || *c >= 0x80))
                    .map(|p| i + 1 + p);
                match tag_end {
                    Some(e) if b[e] == b'$' => {
                        let tag = &b[i..=e];
                        i = b[e + 1..]
                            .windows(tag.len())
                            .position(|w| w == tag)
                            .map_or(b.len(), |p| e + 1 + p + tag.len());
                    }
                    _ => i += 1,
                }
            }
            b':' if b[i..].starts_with(b":cursor")
                && (i == 0 || b[i - 1] != b':')
                && !b.get(i + 7).is_some_and(|c| name_byte(*c)) =>
            {
                marks.push(i);
                i += 7;
            }
            c if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 => {
                // A whole name: a `$` inside it starts no dollar quote. `E'…'` is an escape string.
                let start = i;
                while i < b.len() && name_byte(b[i]) {
                    i += 1;
                }
                if pg
                    && i - start == 1
                    && matches!(b[start], b'E' | b'e')
                    && b.get(i) == Some(&b'\'')
                {
                    i = quoted(i, b'\'', true);
                }
            }
            _ => i += 1,
        }
    }
    marks
}

/// Which database a [`DbSourceSpec`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Mysql,
    Postgres,
}

impl DbKind {
    /// Its name: the spec's key (`source.<name>`) and the position's (`shapeshift.source.<name>`).
    pub fn name(self) -> &'static str {
        match self {
            DbKind::Mysql => "mysql",
            DbKind::Postgres => "postgres",
        }
    }

    /// The URL schemes it takes, the first as written in messages.
    pub fn schemes(self) -> &'static [&'static str] {
        match self {
            DbKind::Mysql => &["mysql://"],
            DbKind::Postgres => &["postgres://", "postgresql://"],
        }
    }

    /// The variable its password comes from (`…_FILE` names a file holding it).
    pub fn password_env(self) -> &'static str {
        match self {
            DbKind::Mysql => "SHAPESHIFT_MYSQL_PASSWORD",
            DbKind::Postgres => "SHAPESHIFT_POSTGRES_PASSWORD",
        }
    }

    /// The kind a URL names, by its scheme.
    pub fn of_url(url: &str) -> Option<DbKind> {
        [DbKind::Mysql, DbKind::Postgres]
            .into_iter()
            .find(|k| k.schemes().iter().any(|s| url.starts_with(s)))
    }
}

impl HttpSourceSpec {
    /// How the responses read: a SOAP service's are XML, whatever `format` says by default.
    pub fn body_format(&self) -> HttpFormat {
        if self.soap.is_some() {
            HttpFormat::Xml
        } else {
            self.format
        }
    }

    /// The spec's own consistency; `merge_on` (rule 3) is checked by the dataset.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(ShapeError::Spec(m));
        let scheme = self.url.split("://").next().unwrap_or("");
        if !matches!(scheme, "http" | "https") || !self.url.contains("://") {
            return bad("`source.http.url` must be an http:// or https:// URL".into());
        }
        if self.body.is_some() && self.method != Some(HttpMethod::Post) && self.soap.is_none() {
            return bad("`source.http.body` is sent with `method: POST` only".into());
        }
        for name in self.headers.keys() {
            let lower = name.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "authorization" | "proxy-authorization" | "cookie"
            ) || looks_secret(&lower)
            {
                return bad(format!(
                    "`source.http.headers.{name}` looks like a credential: a spec holds none. \
                     Use `auth.header_env: {{name: {name}, env: <variable>}}`"
                ));
            }
        }
        for name in self.query.keys() {
            if looks_secret(name) {
                return bad(format!(
                    "`source.http.query.{name}` looks like a credential: a spec holds none. Use \
                     `auth.query_env: {{name: {name}, env: <variable>}}`"
                ));
            }
        }
        if let Some(a) = &self.auth {
            let names: Vec<&str> = [
                a.bearer_env.as_deref(),
                a.basic_env.as_deref(),
                a.header_env.as_ref().map(|h| h.env.as_str()),
                a.query_env.as_ref().map(|h| h.env.as_str()),
            ]
            .into_iter()
            .flatten()
            .collect();
            if names.len() != 1 {
                return bad(
                    "`source.http.auth` takes one of `bearer_env`, `basic_env`, `header_env` or \
                     `query_env`"
                        .into(),
                );
            }
            if !env_name_ok(names[0]) {
                return bad(format!(
                    "`source.http.auth` names an environment variable, like ORDERS_API_TOKEN, \
                     not a value: {:?} is not a variable name",
                    names[0]
                ));
            }
            if let Some(h) = &a.header_env {
                if h.name.is_empty() || !h.name.bytes().all(|b| b.is_ascii_graphic() && b != b':') {
                    return bad(format!(
                        "`auth.header_env.name` {:?} is not a header name",
                        h.name
                    ));
                }
            }
            if let Some(q) = &a.query_env {
                if q.name.trim().is_empty() {
                    return bad("`auth.query_env.name` is the query parameter's name".into());
                }
            }
        }
        if self.records.is_some() && self.body_format() != HttpFormat::Json {
            return bad(
                "`source.http.records` is a path in a JSON body: `format: json` only".into(),
            );
        }
        if let Some(t) = &self.timeout {
            duration_ms(t)?;
        }
        if let Some(p) = &self.paging {
            let styles = [
                p.next.is_some(),
                p.link,
                p.page.is_some(),
                p.offset.is_some(),
                p.cursor.is_some(),
            ];
            if styles.iter().filter(|s| **s).count() != 1 {
                return bad(
                    "`source.http.paging` takes one style: `next`, `link`, `page`, `offset` or \
                     `cursor`"
                        .into(),
                );
            }
            let json_only = p.next.is_some() || p.cursor.is_some();
            if json_only && self.body_format() != HttpFormat::Json && self.soap.is_none() {
                return bad(
                    "`paging.next` and `paging.cursor` are paths in a JSON body: `format: json` \
                     only (`link`, `page` and `offset` paging work with any format)"
                        .into(),
                );
            }
            if p.page.is_some() && (p.size.is_none() || p.per_page.is_none()) {
                return bad("`paging.page` needs `size` (the parameter) and `per_page`".into());
            }
            if p.offset.is_some() && (p.limit.is_none() || p.per_page.is_none()) {
                return bad("`paging.offset` needs `limit` (the parameter) and `per_page`".into());
            }
            if p.cursor.is_some() && p.cursor_param.is_none() {
                return bad(
                    "`paging.cursor` needs `cursor_param`: the parameter it is sent as".into(),
                );
            }
            if p.per_page == Some(0) {
                return bad("`paging.per_page` must be at least 1".into());
            }
        }
        if let Some(i) = &self.incremental {
            if i.param.trim().is_empty() || i.from.trim().is_empty() {
                return bad(
                    "`incremental` needs `param` (the query parameter) and `from` (the \
                            record's cursor field)"
                        .into(),
                );
            }
            if i.strict && i.lookback.is_some() {
                return bad("`incremental.strict` has no replay, so no `lookback`".into());
            }
            if i.strict && i.tiebreak.is_some() {
                return bad(
                    "`incremental.strict` is unique already: it needs no `tiebreak`".into(),
                );
            }
            if i.max_tie == Some(0) {
                return bad("`incremental.max_tie` must be at least 1".into());
            }
            if self.query.contains_key(&i.param) {
                return bad(format!(
                    "`source.http.query.{p}` names the incremental parameter: each run sets `{p}` \
                     to the position it resumes from, and a fixed value would replace it. Remove \
                     it from `query`",
                    p = i.param
                ));
            }
            i.rules()?;
        }
        if let Some(s) = &self.soap {
            self.validate_soap(s)?;
        }
        Ok(())
    }

    fn validate_soap(&self, s: &SoapSpec) -> Result<()> {
        let bad = |m: String| Err(ShapeError::Spec(m));
        if self.method == Some(HttpMethod::Get) || self.body.is_some() {
            return bad(
                "a SOAP request is a POST of the envelope shapeshift builds: no `method: GET` \
                 and no `body` beside `soap`"
                    .into(),
            );
        }
        if !matches!(self.format, HttpFormat::Json | HttpFormat::Xml) {
            return bad("a SOAP response is XML: `format` is `xml` (or left out)".into());
        }
        if s.body_template.trim().is_empty() {
            return bad("`soap.body_template` is the operation element the Body holds".into());
        }
        for h in self.headers.keys() {
            if h.eq_ignore_ascii_case("content-type") || h.eq_ignore_ascii_case("soapaction") {
                return bad(format!(
                    "`source.http.headers.{h}` is set from `soap`: its `version` and `action`"
                ));
            }
        }
        match (&s.username_env, &s.password_env) {
            (Some(u), Some(p)) => {
                for n in [u, p] {
                    if !env_name_ok(n) {
                        return bad(format!(
                            "`soap.username_env` and `soap.password_env` name environment \
                             variables, not values: {n:?} is not a variable name"
                        ));
                    }
                }
            }
            (None, None) => {}
            _ => return bad("`soap.username_env` and `soap.password_env` go together".into()),
        }
        if let Some(p) = &self.paging {
            if p.next.is_some() || p.cursor.is_some() {
                return bad(
                    "`paging.next` and `paging.cursor` read a JSON body: a SOAP service pages by \
                     `page` or `offset` (the parameters in the template), or `link`"
                        .into(),
                );
            }
        }
        // Every placeholder has a parameter to fill it, and the cursor's is in the body.
        let mut known: Vec<&str> = self.query.keys().map(String::as_str).collect();
        if let Some(i) = &self.incremental {
            known.push(&i.param);
        }
        if let Some(p) = &self.paging {
            for n in [&p.page, &p.size, &p.offset, &p.limit]
                .into_iter()
                .flatten()
            {
                known.push(n);
            }
        }
        let named = s.placeholders();
        if let Some(unknown) = named.iter().find(|n| !known.contains(&n.as_str())) {
            return bad(format!(
                "`soap.body_template` names `{{{{{unknown}}}}}`, which no parameter fills: it is \
                 `incremental.param`, a paging parameter, or a `query` entry"
            ));
        }
        if let Some(i) = &self.incremental {
            if !named.contains(&i.param) {
                return bad(format!(
                    "an incremental SOAP source sends its lower bound in the body: name \
                     `{{{{{}}}}}` (`incremental.param`) in `soap.body_template`",
                    i.param
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_source_host_is_checked_against_the_runners_list() {
        use super::check_source_host as check;
        let list: Vec<String> = ["db.internal:5432", "API.example.com", "[::1]:8443"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(check(&[], "anywhere", 1).is_ok(), "no list, no limit");
        assert!(check(&list, "db.internal", 5432).is_ok());
        assert!(
            check(&list, "api.example.com", 443).is_ok(),
            "a host alone: any port"
        );
        assert!(check(&list, "[::1]", 8443).is_ok());
        for (host, port) in [
            ("db.internal", 6432),
            ("api.example.com.evil.net", 443),
            ("169.254.169.254", 80),
            ("::1", 5432),
        ] {
            let e = check(&list, host, port).unwrap_err().to_string();
            assert!(
                e.contains("may not reach") && e.contains("--source-hosts"),
                "{host}: {e}"
            );
        }
    }

    #[test]
    fn a_cursor_mark_is_found_only_where_the_database_reads_a_name() {
        use super::{cursor_marks, DbKind::*};
        let n = |q: &str, k| cursor_marks(q, k).len();
        assert_eq!(n("select * from t where u >= :cursor", Postgres), 1);
        assert_eq!(n("select ':cursor' as label", Postgres), 0);
        assert_eq!(n("select \"a:cursor\" from t", Postgres), 0);
        assert_eq!(n("select 1 -- :cursor\n, 2", Postgres), 0);
        assert_eq!(n("select /* a /* :cursor */ b */ 1", Postgres), 0);
        assert_eq!(n("select $q$ :cursor $q$, :cursor", Postgres), 1);
        // An escape string: \' does not end it.
        assert_eq!(
            n("select E'it\\'s :cursor' as l, :cursor::bigint", Postgres),
            1
        );
        // A `$` inside a name starts no dollar quote.
        assert_eq!(
            n("select 1 as foo$tag$, :cursor::bigint as id", Postgres),
            1
        );
        assert_eq!(
            n(
                "select :cursor::timestamptz, :cursor_x, x::cursor",
                Postgres
            ),
            1
        );
        // MySQL: backslash escapes, backquotes and # comments.
        assert_eq!(
            n(
                "select 'it\\'s :cursor', `:cursor` # :cursor\n where u >= :cursor",
                Mysql
            ),
            1
        );
    }

    #[test]
    fn a_marker_inside_a_literal_does_not_need_a_cursor() {
        let spec = super::DbSourceSpec {
            url: "postgres://u@h/db".into(),
            query: Some("select ':cursor' as label".into()),
            ..super::DbSourceSpec::default()
        };
        spec.validate_as(super::DbKind::Postgres).unwrap();
    }

    #[test]
    fn a_query_entry_named_like_the_incremental_parameter_is_refused() {
        let spec: super::HttpSourceSpec = serde_yaml::from_str(
            "url: https://api.example.com/x\nquery: { since: '2020-01-01' }\n\
             incremental: { param: since, from: updated_at, order: asc }\n",
        )
        .unwrap();
        let e = spec.validate().unwrap_err().to_string();
        assert!(e.contains("names the incremental parameter"), "{e}");
    }

    use super::{looks_secret, DbKind, DbSourceSpec, SoapVersion};

    #[test]
    fn a_soap_version_reads_as_yaml_writes_it() {
        for (yaml, want) in [
            ("1.2", SoapVersion::V12),
            ("'1.1'", SoapVersion::V11),
            ("1.1", SoapVersion::V11),
        ] {
            let text = format!(
                "dataset: d\nsource:\n  http:\n    url: https://svc/ws\n    soap:\n      body_template: <Get/>\n      version: {yaml}\n  xml:\n    record: Order\noutput: {{ format: parquet, path: o.parquet }}\n"
            );
            let spec = crate::DatasetSpec::parse(&text).unwrap_or_else(|e| panic!("{yaml}: {e}"));
            assert_eq!(spec.source.http.unwrap().soap.unwrap().version, Some(want));
        }
        let bad = "dataset: d\nsource:\n  http:\n    url: https://svc/ws\n    soap:\n      body_template: <Get/>\n      version: 2\n  xml:\n    record: Order\noutput: { format: parquet, path: o.parquet }\n";
        assert!(crate::DatasetSpec::parse(bad).is_err());
    }

    #[test]
    fn a_database_url_carries_no_secret_and_is_checked_as_its_own_kind() {
        let spec = |url: &str| DbSourceSpec {
            url: url.into(),
            table: Some("t".into()),
            ..DbSourceSpec::default()
        };
        assert!(spec("postgres://app@db/shop")
            .validate_as(DbKind::Postgres)
            .is_ok());
        assert!(spec("postgres://app@db/shop")
            .validate_as(DbKind::Mysql)
            .is_err());
        for q in [
            "password=x",
            "passfile=/p",
            "hostaddr=10.0.0.1",
            "sslmode=disable",
            "options=-c%20x",
        ] {
            let e = spec(&format!("postgres://app@db/shop?{q}"))
                .validate_as(DbKind::Postgres)
                .unwrap_err()
                .to_string();
            assert!(e.contains("SHAPESHIFT_POSTGRES_PASSWORD"), "{q}: {e}");
        }
        assert!(spec("postgres://app@db/shop?connect_timeout=5")
            .validate_as(DbKind::Postgres)
            .is_ok());
    }

    #[test]
    fn credential_names_are_matched_by_word() {
        for n in [
            "api_key",
            "X-Api-Key",
            "apikey",
            "access_token",
            "client_secret",
            "Authorization",
            "password",
            "x-auth-key",
        ] {
            assert!(looks_secret(n), "{n}");
        }
        for n in [
            "sort_key",
            "keyword",
            "Idempotency-Key",
            "partition_key",
            "monkey",
            "sort",
            "X-Trace-Id",
        ] {
            assert!(!looks_secret(n), "{n}");
        }
    }
}
