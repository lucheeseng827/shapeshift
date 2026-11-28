//! The MySQL source: rows from a query, or from a table by a watermark column.
//!
//! A [`MysqlSource`] reads `source.mysql` (`docs/SPEC.md` §2.7). Each run opens one
//! connection, starts a read-only transaction with a consistent snapshot (so one run sees one
//! state of the database), and streams the rows: a thread reads them into a bounded queue, so
//! memory holds a few hundred rows whatever the result's size. The position rules are
//! [`shapeshift_core::Tracker`]'s, as for every pulled source: this crate only writes the
//! statement they need and hands each row to them.
//!
//! - A **query** binds `:cursor` to the run's lower bound and orders its rows itself (the order
//!   is checked: a row out of order fails the run).
//! - A **table** is read by its cursor column and its primary key: with a one-column key that
//!   key is the tiebreak, so a bounded run stops `inside v, after k` and the next resumes with
//!   `cursor > v or (cursor = v and key > k)`.
//!
//! The password comes from `SHAPESHIFT_MYSQL_PASSWORD`, or the file
//! `SHAPESHIFT_MYSQL_PASSWORD_FILE` names; TLS is required to any host but this one, unless
//! [`MysqlOptions::insecure`].

mod sql;
mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Params, SslOpts};
use serde_json::{Map, Value};
use shapeshift_core::{
    Bounds, MysqlSourceSpec, Position, Progress, Result, ShapeError, Source, Tracker, Verdict,
};

use types::Meta;

/// The environment variables the password comes from.
pub const PASSWORD_ENV: &str = "SHAPESHIFT_MYSQL_PASSWORD";
pub const PASSWORD_FILE_ENV: &str = "SHAPESHIFT_MYSQL_PASSWORD_FILE";

/// Rows the reader thread may be ahead of the run by.
const QUEUE_ROWS: usize = 512;

/// What the caller allows beyond the spec.
#[derive(Debug, Clone, Default)]
pub struct MysqlOptions {
    /// Connect to another machine without TLS.
    pub insecure: bool,
    /// The only hosts it may connect to (`host` or `host:port`), checked on the host the
    /// driver parsed from the URL (the CLI's `--source-hosts`). Empty: no limit.
    pub hosts: Vec<String>,
}

type Outcome = Arc<Mutex<Option<(Option<Position>, Progress)>>>;

/// A database, ready to be read from a position.
pub struct MysqlSource {
    spec: MysqlSourceSpec,
    opts: Opts,
    /// The password, kept to take it out of any message.
    password: Option<String>,
    outcome: Outcome,
}

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Source(format!("mysql: {e}"))
}

/// The mysql crate builds its TLS client from rustls's process-wide provider. A build that also
/// links aws-lc-rs (object_store's, so any build with `object_store` and `mysql`) has two
/// providers and no default, and rustls panics at the first TLS connection. Install ring, the
/// one this crate is built for, unless the process has chosen a provider already: a provider
/// installed first is kept, and installing twice is refused, not an error.
fn ensure_tls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `mysql://host/db` as messages show it: no user, no query.
pub fn display_url(url: &str) -> String {
    let rest = url.strip_prefix("mysql://").unwrap_or(url);
    let rest = rest.split('?').next().unwrap_or(rest);
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    format!("mysql://{host}/{path}")
}

fn password() -> Result<Option<String>> {
    if let Ok(p) = std::env::var(PASSWORD_ENV) {
        return Ok(Some(p));
    }
    match std::env::var(PASSWORD_FILE_ENV) {
        Ok(path) => {
            let text = std::fs::read_to_string(&path).map_err(|e| {
                ShapeError::Spec(format!(
                    "{PASSWORD_FILE_ENV} names {path}, which cannot be read ({e})"
                ))
            })?;
            Ok(Some(text.trim_end_matches(['\r', '\n']).to_string()))
        }
        Err(_) => Ok(None),
    }
}

impl MysqlSource {
    /// Check the spec and work out the connection: the password from the environment, and
    /// TLS to any host but this one. Nothing connects yet.
    pub fn new(spec: &MysqlSourceSpec, options: MysqlOptions) -> Result<MysqlSource> {
        spec.validate_as(shapeshift_core::DbKind::Mysql)?;
        let parsed = Opts::from_url(&spec.url)
            .map_err(|e| ShapeError::Spec(format!("`source.mysql.url` does not parse ({e})")))?;
        if parsed.get_db_name().is_none() {
            return Err(ShapeError::Spec(
                "`source.mysql.url` names no database: `mysql://user@host/database`".into(),
            ));
        }
        let host = parsed.get_ip_or_hostname().into_owned();
        shapeshift_core::check_source_host(&options.hosts, &host, parsed.get_tcp_port())?;
        let password = password()?;
        let mut b = OptsBuilder::from_opts(parsed)
            .pass(password.clone())
            .prefer_socket(false)
            .tcp_connect_timeout(Some(Duration::from_secs(30)))
            // A connection that went quiet (a peer gone, a half-open socket) is found, rather
            // than waited on forever. A slow query is not cut: there is no read timeout.
            .tcp_keepalive_time_ms(Some(60_000))
            .tcp_keepalive_probe_interval_secs(Some(10))
            .tcp_keepalive_probe_count(Some(6))
            .stmt_cache_size(Some(0));
        // TLS to another machine (unless insecure), and wherever a `ca_file` is given: a CA
        // named is a CA to check, so it is used even on loopback and even when insecure.
        let tls = !is_loopback(&host) || spec.ca_file.is_some();
        if tls && !(options.insecure && spec.ca_file.is_none()) {
            let mut ssl = SslOpts::default();
            if let Some(ca) = &spec.ca_file {
                ssl = ssl.with_root_cert_path(Some(std::path::PathBuf::from(ca)));
            }
            b = b.ssl_opts(Some(ssl));
        }
        Ok(MysqlSource {
            spec: spec.clone(),
            opts: Opts::from(b),
            password,
            outcome: Arc::new(Mutex::new(None)),
        })
    }

    fn scrub(&self, e: ShapeError) -> ShapeError {
        let Some(p) = self.password.as_deref().filter(|p| p.len() >= 4) else {
            return e;
        };
        match e {
            ShapeError::Source(m) => ShapeError::Source(m.replace(p, "***")),
            ShapeError::Spec(m) => ShapeError::Spec(m.replace(p, "***")),
            other => other,
        }
    }

    /// Connect, and begin the run's transaction: UTC, repeatable read, read only, one snapshot.
    fn connect(&self) -> Result<Conn> {
        if self.opts.get_ssl_opts().is_some() {
            ensure_tls_provider();
        }
        let mut conn = Conn::new(self.opts.clone()).map_err(|e| {
            ShapeError::Source(format!(
                "mysql: connecting to {}: {e}",
                display_url(&self.spec.url)
            ))
        })?;
        for stmt in [
            "SET time_zone = '+00:00'",
            // The server gives up on a client that stops reading for this long; a run's sink
            // may pause between rows (a row group flushed, an upload), so allow an hour.
            "SET SESSION net_write_timeout = 3600",
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        ] {
            conn.query_drop(stmt).map_err(err)?;
        }
        Ok(conn)
    }

    /// A table's primary key columns, in order.
    fn primary_key(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
        let (db, name) = match table.split_once('.') {
            Some((d, t)) => (Some(d.to_string()), t.to_string()),
            None => (None, table.to_string()),
        };
        let rows: Vec<String> = conn
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
                 WHERE TABLE_SCHEMA = COALESCE(:db, DATABASE()) AND TABLE_NAME = :t \
                 AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
                Params::Named(HashMap::from([
                    (
                        b"db".to_vec(),
                        db.map_or(mysql::Value::NULL, |d| mysql::Value::Bytes(d.into_bytes())),
                    ),
                    (b"t".to_vec(), mysql::Value::Bytes(name.into_bytes())),
                ])),
            )
            .map_err(err)?;
        Ok(rows)
    }
}

impl Source for MysqlSource {
    fn kind(&self) -> &'static str {
        "mysql"
    }

    fn open(
        &mut self,
        start: Option<Position>,
        bounds: Bounds,
    ) -> Result<Box<dyn Iterator<Item = Result<Value>> + Send>> {
        // A slot of this run's own: an earlier run's iterator, still draining, writes to its own.
        self.outcome = Arc::new(Mutex::new(None));
        let spec = self.spec.clone();
        if bounds.max_pages.is_some() {
            return Err(ShapeError::Spec(
                "a database's rows come in no pages: bound the run with --max-records or \
                 --max-wait"
                    .into(),
            ));
        }
        if !spec.incremental() && bounds.any() {
            return Err(ShapeError::Spec(
                "--max-records and --max-wait bound an incremental source (`source.mysql.cursor`), \
                 whose next run goes on from where this one stopped"
                    .into(),
            ));
        }
        let mut conn = self.connect().map_err(|e| self.scrub(e))?;

        // The statement, its parameters, and the tiebreak it orders by.
        let (statement, tiebreak, params) = match (&spec.query, &spec.table) {
            (Some(q), _) => {
                let mut params = HashMap::new();
                if spec.incremental() {
                    let rules = spec.rules(spec.tiebreak.is_some())?;
                    let t = Tracker::new(rules, bounds.clone(), start.clone())?;
                    let bound =
                        match (t.lower_bound(), &spec.start) {
                            (Some((v, _)), _) => types::param_of(&v),
                            (None, Some(s)) => types::start_param(s),
                            (None, None) => return Err(ShapeError::Spec(
                                "a query's first run binds `:cursor` to `source.mysql.start` (the \
                                 lowest value to read, like `1970-01-01` or `0`), and none is set"
                                    .into(),
                            )),
                        };
                    params.insert(b"cursor".to_vec(), bound);
                }
                (q.clone(), spec.tiebreak.clone(), params)
            }
            (None, Some(table)) => {
                let pk = Self::primary_key(&mut conn, table)?;
                // The key is a tiebreak only when MySQL orders it as the position rules compare
                // it (a number or an instant): a text key is ordered by its collation, so the
                // table is read without one (groups are finished; `max_tie` applies).
                let probe = conn
                    .prep(format!("SELECT * FROM {} LIMIT 0", sql::quote(table)))
                    .map_err(err)?;
                let key = (pk.len() == 1).then(|| pk[0].clone()).filter(|k| {
                    probe
                        .columns()
                        .iter()
                        .any(|c| c.name_str() == k.as_str() && Meta::of(c).orderable(true))
                });
                match &spec.cursor {
                    None => {
                        let order = if pk.is_empty() {
                            String::new()
                        } else {
                            format!(
                                " ORDER BY {}",
                                pk.iter()
                                    .map(|c| sql::column(c))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        };
                        (
                            format!("SELECT * FROM {}{order}", sql::quote(table)),
                            None,
                            HashMap::new(),
                        )
                    }
                    Some(cursor) => {
                        // The key is no tiebreak when it is the cursor itself.
                        let key = key.filter(|k| k != cursor && !spec.strict);
                        let rules = spec.rules(key.is_some())?;
                        let t = Tracker::new(rules, bounds.clone(), start.clone())?;
                        let from = sql::from_position(
                            start.as_ref(),
                            spec.strict,
                            spec.start.is_some(),
                            key.as_deref(),
                            spec.lookback.is_some() && !spec.strict,
                        );
                        let mut params = HashMap::new();
                        match (&start, t.lower_bound(), &spec.start) {
                            (Some(Position::Inside(v, k)), lower, _) if key.is_some() => {
                                params.insert(b"v".to_vec(), types::param_of(v));
                                params.insert(b"k".to_vec(), types::param_of(k));
                                // With a lookback, the replay below `v` starts at the cutoff.
                                if let (Some((c, _)), true) =
                                    (lower, spec.lookback.is_some() && !spec.strict)
                                {
                                    params.insert(b"c".to_vec(), types::param_of(&c));
                                }
                            }
                            (_, Some((v, _)), _) => {
                                params.insert(b"v".to_vec(), types::param_of(&v));
                            }
                            (None, None, Some(s)) => {
                                params.insert(b"v".to_vec(), types::start_param(s));
                            }
                            _ => {}
                        }
                        let sql = sql::table_read(table, cursor, &pk, from);
                        (sql, key, params)
                    }
                }
            }
            (None, None) => unreachable!("validated: a query or a table"),
        };

        let stmt = conn.prep(&statement).map_err(|e| {
            self.scrub(ShapeError::Source(format!(
                "mysql: preparing the read: {e}"
            )))
        })?;
        let metas: Vec<Meta> = stmt.columns().iter().map(Meta::of).collect();
        for m in &metas {
            m.check()?;
        }
        let mut seen = std::collections::HashSet::new();
        if let Some(dup) = metas.iter().find(|m| !seen.insert(m.name.clone())) {
            return Err(ShapeError::Source(format!(
                "the read returns two columns named `{}`: alias one in the query",
                dup.name
            )));
        }
        for (what, col) in [
            ("cursor", spec.cursor.as_ref()),
            ("tiebreak", tiebreak.as_ref()),
        ] {
            if let Some(c) = col {
                let Some(m) = metas.iter().find(|m| &m.name == c) else {
                    return Err(ShapeError::Source(format!(
                        "the read returns no column `{c}` (`source.mysql.{what}`)"
                    )));
                };
                // The rows come in MySQL's order and are judged in the tracker's: the two must
                // agree, or a run would fail on a row out of order or, worse, skip one.
                if !m.orderable(what == "tiebreak") {
                    return Err(ShapeError::Source(format!(
                        "`{c}` (`source.mysql.{what}`) is not a column MySQL orders as the \
                         position compares it: a {what} is an integer or a DATE, DATETIME or \
                         TIMESTAMP{} (text sorts by its collation, a DECIMAL arrives as text)",
                        if what == "cursor" {
                            ", or a FLOAT or DOUBLE"
                        } else {
                            ""
                        }
                    )));
                }
            }
        }
        let tracker = match &spec.cursor {
            Some(_) => Some(Tracker::new(
                spec.rules(tiebreak.is_some())?,
                bounds,
                start,
            )?),
            None => None,
        };

        let conn_id = conn.connection_id();
        let done = Arc::new(AtomicBool::new(false));
        let (tx, rx) = sync_channel::<Msg>(QUEUE_ROWS);
        let finished = done.clone();
        let params = if params.is_empty() {
            Params::Empty
        } else {
            Params::Named(params)
        };
        std::thread::spawn(move || {
            let mut conn = conn;
            let run = (|| -> Result<()> {
                let result = conn.exec_iter(&stmt, params).map_err(err)?;
                for row in result {
                    let values = row.map_err(err)?.unwrap();
                    let mut rec = Map::with_capacity(values.len());
                    for (m, v) in metas.iter().zip(values) {
                        rec.insert(m.name.clone(), types::json_of(m, v)?);
                    }
                    if tx.send(Msg::Row(Value::Object(rec))).is_err() {
                        return Ok(()); // the run stopped reading
                    }
                }
                Ok(())
            })();
            // The end is said, never implied: a reader that dies without saying it is an
            // error, not a complete read.
            let _ = tx.send(match run {
                Ok(()) => Msg::Done,
                Err(e) => Msg::Failed(e),
            });
            let _ = conn.query_drop("COMMIT");
            finished.store(true, Ordering::SeqCst);
        });

        Ok(Box::new(Pull {
            rx: Some(rx),
            tracker,
            spec,
            tiebreak,
            read: 0,
            failed: false,
            outcome: self.outcome.clone(),
            kill: Some(Kill {
                opts: self.opts.clone(),
                conn_id,
                done,
            }),
        }))
    }

    fn outcome(&self) -> Option<(Option<Position>, Progress)> {
        self.outcome.lock().expect("outcome lock").clone()
    }

    fn identity(&self) -> String {
        let what = match (&self.spec.table, &self.spec.query) {
            (Some(t), _) => format!("table {t}"),
            (_, Some(q)) => format!("query {}", shapeshift_core::digest(q.trim())),
            _ => String::new(),
        };
        let cursor = self.spec.cursor.as_deref().unwrap_or("");
        format!("{} {what} {cursor}", display_url(&self.spec.url))
    }
}

/// What the reader thread sends.
enum Msg {
    Row(Value),
    /// Every row was read.
    Done,
    Failed(ShapeError),
}

/// How long a receive waits before the run checks its `--max-wait`.
const WAIT_STEP: Duration = Duration::from_millis(250);

/// What stops the server's side of a read the run no longer wants: its rows would otherwise
/// all be sent, and drained, before the connection could close.
struct Kill {
    opts: Opts,
    conn_id: u32,
    done: Arc<AtomicBool>,
}

impl Kill {
    fn now(self) {
        if self.done.load(Ordering::SeqCst) {
            return;
        }
        // Quickly, or not at all: a run that is ending does not wait on a server that is gone.
        let opts = OptsBuilder::from_opts(self.opts)
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(5)));
        if let Ok(mut c) = Conn::new(opts) {
            let _ = c.query_drop(format!("KILL QUERY {}", self.conn_id));
        }
    }
}

struct Pull {
    rx: Option<Receiver<Msg>>,
    tracker: Option<Tracker>,
    spec: MysqlSourceSpec,
    tiebreak: Option<String>,
    read: u64,
    failed: bool,
    outcome: Outcome,
    kill: Option<Kill>,
}

impl Pull {
    fn stop_reading(&mut self) {
        self.rx = None;
        if let Some(k) = self.kill.take() {
            k.now();
        }
    }

    fn finish(&mut self, more: bool) -> Option<Result<Value>> {
        self.stop_reading();
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

    fn fail(&mut self, e: ShapeError) -> Option<Result<Value>> {
        self.failed = true;
        self.stop_reading();
        Some(Err(e))
    }

    fn judge(&mut self, rec: &Value) -> Result<Verdict> {
        let Some(t) = self.tracker.as_mut() else {
            return Ok(Verdict::Take { replay: false });
        };
        let cursor_col = self.spec.cursor.as_deref().expect("a tracker has a cursor");
        let cursor = rec.get(cursor_col).cloned().unwrap_or(Value::Null);
        if cursor.is_null() {
            return Err(ShapeError::Source(format!(
                "a row has a NULL `{cursor_col}` (`source.mysql.cursor`): every row needs its \
                 cursor value (filter them out, or COALESCE it in the query)"
            )));
        }
        let key = match &self.tiebreak {
            Some(k) => {
                let v = rec.get(k).cloned().unwrap_or(Value::Null);
                if v.is_null() {
                    return Err(ShapeError::Source(format!(
                        "a row has a NULL tiebreak `{k}`"
                    )));
                }
                Some(v)
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
            let got = self.rx.as_ref()?.recv_timeout(WAIT_STEP);
            let rec =
                match got {
                    Ok(Msg::Row(rec)) => rec,
                    Ok(Msg::Done) => return self.finish(false),
                    Ok(Msg::Failed(e)) => return self.fail(e),
                    // Nothing yet: a server that is slow to send still ends a run at --max-wait.
                    Err(RecvTimeoutError::Timeout) => {
                        if self.tracker.as_mut().is_some_and(|t| t.time_up()) {
                            return self.finish(true);
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return self.fail(ShapeError::Source(
                        "mysql: the reader stopped without finishing the read: the run commits \
                         nothing"
                            .into(),
                    )),
                };
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
    }
}

impl Drop for Pull {
    fn drop(&mut self) {
        self.stop_reading();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tls_connection_finds_a_process_provider() {
        // Whatever the build links, the mysql crate's TLS client must find a default provider.
        ensure_tls_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        // A second call keeps the provider installed first, rather than failing.
        ensure_tls_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn urls_print_without_the_user_and_tls_is_required_off_loopback() {
        assert_eq!(
            display_url("mysql://app@db.example.com:3306/shop"),
            "mysql://db.example.com:3306/shop"
        );
        assert!(is_loopback("127.0.0.1") && is_loopback("localhost") && is_loopback("[::1]"));
        assert!(!is_loopback("10.0.0.5") && !is_loopback("db.example.com"));
        let spec = MysqlSourceSpec {
            url: "mysql://app@db.example.com/shop".into(),
            table: Some("orders".into()),
            ..MysqlSourceSpec::default()
        };
        let s = MysqlSource::new(&spec, MysqlOptions::default()).unwrap();
        assert!(s.opts.get_ssl_opts().is_some());
        let s = MysqlSource::new(
            &spec,
            MysqlOptions {
                insecure: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(s.opts.get_ssl_opts().is_none());
        let local = MysqlSourceSpec {
            url: "mysql://app@127.0.0.1/shop".into(),
            ..spec.clone()
        };
        assert!(MysqlSource::new(&local, MysqlOptions::default())
            .unwrap()
            .opts
            .get_ssl_opts()
            .is_none());
        let pw = MysqlSourceSpec {
            url: "mysql://app:hunter2@db/shop".into(),
            ..spec
        };
        let e = MysqlSource::new(&pw, MysqlOptions::default())
            .err()
            .unwrap()
            .to_string();
        assert!(
            e.contains("SHAPESHIFT_MYSQL_PASSWORD") && !e.contains("hunter2"),
            "{e}"
        );
    }
}
