//! The Postgres source: rows from a query, or from a table by a watermark column.
//!
//! A [`PostgresSource`] reads `source.postgres` (`docs/SPEC.md` §2.8), as the MySQL source reads
//! `source.mysql`, by the same position rules ([`shapeshift_core::Tracker`]). Each run opens one
//! connection and reads in a `REPEATABLE READ`, `READ ONLY` transaction (one snapshot of the
//! database) through a server-side portal, 500 rows a fetch: a thread reads them into a bounded
//! queue, and a run that stops early simply stops fetching, so the rest is never sent.
//!
//! The password comes from `SHAPESHIFT_POSTGRES_PASSWORD`, or the file
//! `SHAPESHIFT_POSTGRES_PASSWORD_FILE` names; TLS is required to any host but this one, unless
//! [`PostgresOptions::insecure`].

mod types;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use postgres::config::{Host, SslMode};
use postgres::{CancelToken, Client, Config, IsolationLevel, NoTls};
use serde_json::{Map, Value};
use shapeshift_core::{
    Bounds, DbKind, Position, PostgresSourceSpec, Progress, Result, ShapeError, Source, Tracker,
    Verdict,
};

/// The environment variable the password comes from (`…_FILE`: a file holding it).
pub const PASSWORD_ENV: &str = "SHAPESHIFT_POSTGRES_PASSWORD";
pub const PASSWORD_FILE_ENV: &str = "SHAPESHIFT_POSTGRES_PASSWORD_FILE";

/// Rows one fetch reads from the portal.
const FETCH_ROWS: i32 = 500;
/// Rows the reader thread may be ahead of the run by.
const QUEUE_ROWS: usize = 1000;
/// How long a receive waits before the run checks its `--max-wait`.
const WAIT_STEP: Duration = Duration::from_millis(250);
/// How often a stopped run's cancel is sent again while its read has not ended, and how many
/// times (two minutes in all).
const CANCEL_STEP: Duration = Duration::from_millis(250);
const CANCEL_TRIES: u32 = 480;

/// What the caller allows beyond the spec.
#[derive(Debug, Clone, Default)]
pub struct PostgresOptions {
    /// Connect to another machine without TLS.
    pub insecure: bool,
    /// The only hosts it may connect to (`host` or `host:port`), checked on the hosts the
    /// driver parsed from the URL, whatever form they took (the CLI's `--source-hosts`).
    /// Empty: no limit.
    pub hosts: Vec<String>,
}

type Outcome = Arc<Mutex<Option<(Option<Position>, Progress)>>>;

/// A database, ready to be read from a position.
pub struct PostgresSource {
    spec: PostgresSourceSpec,
    config: Config,
    tls: bool,
    password: Option<String>,
    outcome: Outcome,
    /// How the names the read uses resolve for this role, asked once: the identity names it,
    /// and the read is bound to it.
    resolved: std::sync::OnceLock<std::result::Result<Resolved, String>>,
}

/// The role's search path as the database resolves it (`current_schemas`), and an unqualified
/// table's schema on it.
#[derive(Clone, Debug)]
struct Resolved {
    path: Vec<String>,
    /// `(schema, table)` for a table named without its schema.
    table: Option<(String, String)>,
}

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Source(format!("postgres: {e}"))
}

/// A connection that failed, with its causes: the driver's own message is only its kind
/// ("invalid configuration", "error performing TLS handshake"), and what went wrong is in its
/// source chain. The two a user can fix get a hint: a server that asked for a password no
/// variable gave, and a certificate no trusted CA signed.
fn connect_err(e: &(dyn std::error::Error + 'static), has_password: bool, tls: bool) -> ShapeError {
    let mut msg = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        let text = c.to_string();
        if !msg.ends_with(&text) {
            msg = format!("{msg}: {text}");
        }
        cause = c.source();
    }
    if !has_password && msg.contains("password missing") {
        msg.push_str(&format!(
            " (the server asked for a password: set {PASSWORD_ENV}, or {PASSWORD_FILE_ENV} to a \
             file holding it)"
        ));
    } else if tls && msg.contains("TLS handshake") {
        msg.push_str(
            " (a server whose certificate a private CA signed needs that CA in \
             `source.postgres.ca_file`)",
        );
    }
    err(msg)
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `postgres://host/db` as messages show it: no user, no query.
pub fn display_url(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").unwrap_or(("postgres", url));
    let rest = rest.split('?').next().unwrap_or(rest);
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    format!("{scheme}://{host}/{path}")
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

/// A name quoted as one identifier, a `"` in it doubled.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A table name (`orders`, `schema.orders`) quoted per part.
fn quote_table(name: &str) -> String {
    name.split('.').map(quote).collect::<Vec<_>>().join(".")
}

/// The query with its `:cursor` as the statement's first parameter (`$1`). Text in a string
/// literal, a quoted name, a comment or a dollar-quoted body is left alone, as are a `::` cast
/// and a longer name (`:cursor_x`).
fn number_cursor(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut last = 0;
    for m in shapeshift_core::cursor_marks(query, DbKind::Postgres) {
        out.push_str(&query[last..m]);
        out.push_str("$1");
        last = m + ":cursor".len();
    }
    out.push_str(&query[last..]);
    out
}

/// What the reader thread sends.
enum Msg {
    /// The read is set up: the tiebreak it orders by, if any, and how to cancel its fetch.
    Ready(Option<String>, CancelToken),
    Row(Value),
    Done,
    Failed(ShapeError),
}

/// What the reader needs to set the read up.
struct Plan {
    spec: PostgresSourceSpec,
    start: Option<Position>,
    bounds: Bounds,
    resolved: Resolved,
}

impl PostgresSource {
    /// Check the spec and work out the connection: the password from the environment, and
    /// TLS to any host but this one. Nothing connects yet.
    pub fn new(spec: &PostgresSourceSpec, options: PostgresOptions) -> Result<PostgresSource> {
        spec.validate_as(DbKind::Postgres)?;
        let mut config: Config = spec
            .url
            .parse()
            .map_err(|e| ShapeError::Spec(format!("`source.postgres.url` does not parse ({e})")))?;
        let password = password()?;
        if let Some(p) = &password {
            config.password(p);
        }
        // A URL's own `connect_timeout` stands.
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(Duration::from_secs(30));
        }
        config
            .keepalives(true)
            .keepalives_idle(Duration::from_secs(60));
        if config.get_application_name().is_none() {
            config.application_name("shapeshift");
        }
        if !options.hosts.is_empty() {
            // Every host the URL names, with its port (one port for all, or one each).
            let ports = config.get_ports();
            for (i, h) in config.get_hosts().iter().enumerate() {
                let port = ports.get(i).or(ports.first()).copied().unwrap_or(5432);
                match h {
                    Host::Tcp(name) => shapeshift_core::check_source_host(&options.hosts, name, port)?,
                    #[cfg(unix)]
                    Host::Unix(_) => {
                        return Err(ShapeError::Source(
                            "the source names a Unix socket, which a run held to --source-hosts \n                             may not use"
                                .into(),
                        ))
                    }
                }
            }
        }
        let remote = config.get_hosts().iter().any(|h| match h {
            Host::Tcp(name) => !is_loopback(name),
            #[cfg(unix)]
            Host::Unix(_) => false,
        });
        // TLS to another machine (unless insecure), and wherever a CA is named.
        let tls = (remote && !options.insecure) || spec.ca_file.is_some();
        if tls {
            config.ssl_mode(SslMode::Require);
        }
        Ok(PostgresSource {
            spec: spec.clone(),
            config,
            tls,
            password,
            outcome: Arc::new(Mutex::new(None)),
            resolved: std::sync::OnceLock::new(),
        })
    }

    /// How names resolve for this role now: its search path (`search_path` decides) and, for a
    /// table named without its schema, the schema it is found in. The read then runs with that
    /// path and that schema, so the relation it reads is the one the identity names.
    fn resolve(&self) -> std::result::Result<Resolved, String> {
        let mut c = Self::connect(&self.config, self.tls, self.spec.ca_file.as_deref())
            .map_err(|e| self.scrub(e).to_string())?;
        let path: Vec<String> = c
            .query_one("SELECT current_schemas(false)::text[]", &[])
            .map_err(|e| format!("postgres: reading the search path: {e}"))?
            .get(0);
        let table = match &self.spec.table {
            Some(t) if !t.contains('.') => {
                let row = c
                    .query_opt(
                        "SELECT n.nspname::text FROM pg_class c \
                         JOIN pg_namespace n ON n.oid = c.relnamespace \
                         WHERE c.oid = to_regclass($1)",
                        &[&quote(t)],
                    )
                    .map_err(|e| format!("postgres: resolving `{t}`: {e}"))?
                    .ok_or_else(|| {
                        format!(
                            "postgres: no table `{t}` on the role's search path ({}): name its \
                             schema (`schema.{t}`)",
                            path.join(", ")
                        )
                    })?;
                Some((row.get::<_, String>(0), t.clone()))
            }
            _ => None,
        };
        Ok(Resolved { path, table })
    }

    fn resolved(&self) -> &std::result::Result<Resolved, String> {
        self.resolved.get_or_init(|| self.resolve())
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

    fn connect(config: &Config, tls: bool, ca_file: Option<&str>) -> Result<Client> {
        let connected = if tls {
            config.connect(Self::tls_connector(ca_file)?)
        } else {
            config.connect(NoTls)
        };
        connected.map_err(|e| connect_err(&e, config.get_password().is_some(), tls))
    }

    fn tls_connector(ca_file: Option<&str>) -> Result<tokio_postgres_rustls::MakeRustlsConnect> {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = ca_file {
            let file = std::fs::File::open(path)
                .map_err(|e| ShapeError::Spec(format!("`source.postgres.ca_file` {path}: {e}")))?;
            for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(file)) {
                let cert = cert.map_err(|e| {
                    ShapeError::Spec(format!("`source.postgres.ca_file` {path}: {e}"))
                })?;
                roots.add(cert).map_err(|e| {
                    ShapeError::Spec(format!("`source.postgres.ca_file` {path}: {e}"))
                })?;
            }
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(err)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(tokio_postgres_rustls::MakeRustlsConnect::new(tls_config))
    }

    /// The reader: connect, set the read up, report it, then send the rows a fetch at a time.
    fn read(
        config: Config,
        tls: bool,
        plan: Plan,
        tx: SyncSender<Msg>,
        done: Arc<AtomicBool>,
        stop: Arc<AtomicBool>,
    ) {
        // However the read ends, there is nothing left to cancel.
        struct Done(Arc<AtomicBool>);
        impl Drop for Done {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let _done = Done(done);
        let send_failure = |tx: &SyncSender<Msg>, e: ShapeError| {
            let _ = tx.send(Msg::Failed(e));
        };
        let mut client = match Self::connect(&config, tls, plan.spec.ca_file.as_deref()) {
            Ok(c) => c,
            Err(e) => return send_failure(&tx, e),
        };
        let token = client.cancel_token();
        let run = (|| -> Result<bool> {
            let mut t = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .map_err(err)?;
            t.batch_execute("SET LOCAL TIME ZONE 'UTC'").map_err(err)?;
            // The path the identity was resolved under, so a query's names (and the types and
            // functions it uses) resolve as they did then.
            let path = if plan.resolved.path.is_empty() {
                "''".to_string()
            } else {
                plan.resolved
                    .path
                    .iter()
                    .map(|s| quote(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            t.batch_execute(&format!("SET LOCAL search_path TO {path}"))
                .map_err(err)?;
            let spec = &plan.spec;
            // A table named without its schema is read in the schema it resolved to.
            let table_sql = |table: &str| match &plan.resolved.table {
                Some((schema, t)) => format!("{}.{}", quote(schema), quote(t)),
                None => quote_table(table),
            };
            // The statement, the tiebreak it orders by, and its parameters as JSON.
            let (sql, tiebreak, bound): (String, Option<String>, Vec<Value>) = match (
                &spec.query,
                &spec.table,
            ) {
                (Some(q), _) => {
                    let mut bound = Vec::new();
                    if spec.incremental() {
                        let rules = spec.rules(spec.tiebreak.is_some())?;
                        let tr = Tracker::new(rules, plan.bounds.clone(), plan.start.clone())?;
                        bound.push(match (tr.lower_bound(), &spec.start) {
                            (Some((v, _)), _) => v,
                            (None, Some(s)) => Value::String(s.clone()),
                            (None, None) => {
                                return Err(ShapeError::Spec(
                                    "a query's first run binds `:cursor` to \
                                         `source.postgres.start` (the lowest value to read, like \
                                         `1970-01-01` or `0`), and none is set"
                                        .into(),
                                ))
                            }
                        });
                    }
                    (number_cursor(q), spec.tiebreak.clone(), bound)
                }
                (None, Some(table)) => {
                    let pk: Vec<String> = t
                        .query(
                            "SELECT a.attname::text FROM pg_index i JOIN pg_attribute a \
                                 ON a.attrelid = i.indrelid \
                                 AND a.attnum = ANY((i.indkey::int2[])[0:i.indnkeyatts - 1]) \
                                 WHERE i.indrelid = $1::text::regclass AND i.indisprimary \
                                 ORDER BY array_position(i.indkey::int2[], a.attnum)",
                            &[&table_sql(table)],
                        )
                        .map_err(err)?
                        .iter()
                        .map(|r| r.get(0))
                        .collect();
                    let probe = t
                        .prepare(&format!("SELECT * FROM {} LIMIT 0", table_sql(table)))
                        .map_err(err)?;
                    // A one-column key MySQL or Postgres orders as the rules compare it is
                    // the tiebreak; a text or composite key only orders the rows.
                    let key = (pk.len() == 1)
                        .then(|| pk[0].clone())
                        .filter(|k| {
                            probe
                                .columns()
                                .iter()
                                .any(|c| c.name() == k && types::orderable(c.type_(), true))
                        })
                        .filter(|k| Some(k) != spec.cursor.as_ref() && !spec.strict);
                    match &spec.cursor {
                        None => {
                            let order = if pk.is_empty() {
                                String::new()
                            } else {
                                format!(
                                    " ORDER BY {}",
                                    pk.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ")
                                )
                            };
                            (
                                format!("SELECT * FROM {}{order}", table_sql(table)),
                                None,
                                Vec::new(),
                            )
                        }
                        Some(cursor) => {
                            let rules = spec.rules(key.is_some())?;
                            let tr = Tracker::new(rules, plan.bounds.clone(), plan.start.clone())?;
                            let w = quote(cursor);
                            let mut bound = Vec::new();
                            let cond = match (&plan.start, &key, tr.lower_bound(), &spec.start) {
                                (Some(Position::Inside(v, k)), Some(kc), lower, _) => {
                                    let k_col = quote(kc);
                                    match lower.filter(|_| spec.lookback.is_some() && !spec.strict)
                                    {
                                        // From the lookback's cutoff, but not what the
                                        // saved key already covers.
                                        Some((c, _)) => {
                                            bound.push(c);
                                            bound.push(v.clone());
                                            bound.push(k.clone());
                                            format!(" AND {w} >= $1 AND NOT ({w} = $2 AND {k_col} <= $3)")
                                        }
                                        None => {
                                            bound.push(v.clone());
                                            bound.push(k.clone());
                                            format!(
                                                " AND ({w} > $1 OR ({w} = $1 AND {k_col} > $2))"
                                            )
                                        }
                                    }
                                }
                                (_, _, Some((v, exclusive)), _) => {
                                    bound.push(v);
                                    format!(" AND {w} {} $1", if exclusive { ">" } else { ">=" })
                                }
                                (None, _, None, Some(s)) => {
                                    bound.push(Value::String(s.clone()));
                                    format!(" AND {w} >= $1")
                                }
                                _ => String::new(),
                            };
                            let mut order = vec![w.clone()];
                            order.extend(pk.iter().filter(|c| *c != cursor).map(|c| quote(c)));
                            (
                                format!(
                                    "SELECT * FROM {} WHERE {w} IS NOT NULL{cond} ORDER BY {}",
                                    table_sql(table),
                                    order.join(", ")
                                ),
                                key,
                                bound,
                            )
                        }
                    }
                }
                (None, None) => unreachable!("validated: a query or a table"),
            };
            let stmt = t
                .prepare(&sql)
                .map_err(|e| ShapeError::Source(format!("postgres: preparing the read: {e}")))?;
            let cols: Vec<(String, postgres::types::Type)> = stmt
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_().clone()))
                .collect();
            for (name, ty) in &cols {
                types::check(name, ty)?;
            }
            let mut seen = std::collections::HashSet::new();
            if let Some((dup, _)) = cols.iter().find(|(n, _)| !seen.insert(n.clone())) {
                return Err(ShapeError::Source(format!(
                    "the read returns two columns named `{dup}`: alias one in the query"
                )));
            }
            for (what, col) in [
                ("cursor", spec.cursor.as_ref()),
                ("tiebreak", tiebreak.as_ref()),
            ] {
                if let Some(c) = col {
                    let Some((_, ty)) = cols.iter().find(|(n, _)| n == c) else {
                        return Err(ShapeError::Source(format!(
                            "the read returns no column `{c}` (`source.postgres.{what}`)"
                        )));
                    };
                    if !types::orderable(ty, what == "tiebreak") {
                        return Err(ShapeError::Source(format!(
                            "`{c}` (`source.postgres.{what}`) is of type {}, which Postgres does \
                             not order as the position compares it: a {what} is an integer or a \
                             date, timestamp or timestamptz{} (text sorts by its collation, a \
                             numeric arrives as text)",
                            ty.name(),
                            if what == "cursor" {
                                ", or a float"
                            } else {
                                ", or a uuid"
                            }
                        )));
                    }
                }
            }
            let params: Vec<Box<dyn postgres::types::ToSql + Sync + Send>> = stmt
                .params()
                .iter()
                .zip(&bound)
                .map(|(ty, v)| types::param_of(ty, v))
                .collect::<Result<_>>()?;
            let refs: Vec<&(dyn postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|p| p.as_ref() as &(dyn postgres::types::ToSql + Sync))
                .collect();
            let portal = t.bind(&stmt, &refs).map_err(err)?;
            if tx.send(Msg::Ready(tiebreak, token)).is_err() {
                return Ok(false);
            }
            loop {
                // A run that has stopped starts no fetch; one already under way is cancelled.
                if stop.load(Ordering::SeqCst) {
                    return Ok(false);
                }
                let rows = t.query_portal(&portal, FETCH_ROWS).map_err(err)?;
                let last = rows.len() < FETCH_ROWS as usize;
                for row in &rows {
                    let mut rec = Map::with_capacity(cols.len());
                    for (i, (name, ty)) in cols.iter().enumerate() {
                        rec.insert(name.clone(), types::json_of(row, i, name, ty)?);
                    }
                    if tx.send(Msg::Row(Value::Object(rec))).is_err() {
                        return Ok(false); // the run stopped reading: fetch no more
                    }
                }
                if last {
                    break;
                }
            }
            drop(portal);
            t.commit().map_err(err)?;
            Ok(true)
        })();
        match run {
            Ok(true) => {
                let _ = tx.send(Msg::Done);
            }
            Ok(false) => {}
            Err(e) => send_failure(&tx, e),
        }
    }
}

impl Source for PostgresSource {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    fn open(
        &mut self,
        start: Option<Position>,
        bounds: Bounds,
    ) -> Result<Box<dyn Iterator<Item = Result<Value>> + Send>> {
        // A slot of this run's own: an earlier run's iterator, still draining, writes to its own.
        self.outcome = Arc::new(Mutex::new(None));
        if bounds.max_pages.is_some() {
            return Err(ShapeError::Spec(
                "a database's rows come in no pages: bound the run with --max-records or \
                 --max-wait"
                    .into(),
            ));
        }
        if !self.spec.incremental() && bounds.any() {
            return Err(ShapeError::Spec(
                "--max-records and --max-wait bound an incremental source \
                 (`source.postgres.cursor`), whose next run goes on from where this one stopped"
                    .into(),
            ));
        }
        // Not resolved, not read: the position this run would record names that resolution.
        let resolved = self.resolved().clone().map_err(ShapeError::Source)?;
        let (tx, rx) = sync_channel::<Msg>(QUEUE_ROWS);
        let plan = Plan {
            spec: self.spec.clone(),
            start: start.clone(),
            bounds: bounds.clone(),
            resolved,
        };
        let (config, tls) = (self.config.clone(), self.tls);
        let done = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let (reader_done, reader_stop) = (done.clone(), stop.clone());
        std::thread::spawn(move || Self::read(config, tls, plan, tx, reader_done, reader_stop));
        // The read is set up, or why not, before any row.
        let (tiebreak, token) = match rx.recv() {
            Ok(Msg::Ready(t, token)) => (t, token),
            Ok(Msg::Failed(e)) => return Err(self.scrub(e)),
            _ => return Err(err("the reader stopped before the read was set up")),
        };
        let tracker = match &self.spec.cursor {
            Some(_) => Some(Tracker::new(
                self.spec.rules(tiebreak.is_some())?,
                bounds,
                start,
            )?),
            None => None,
        };
        Ok(Box::new(Pull {
            rx: Some(rx),
            tracker,
            cursor: self.spec.cursor.clone(),
            tiebreak,
            read: 0,
            failed: false,
            outcome: self.outcome.clone(),
            password: self.password.clone(),
            cancel: Some(Cancel {
                token,
                tls: self.tls,
                ca_file: self.spec.ca_file.clone(),
                done,
                stop,
            }),
        }))
    }

    fn outcome(&self) -> Option<(Option<Position>, Progress)> {
        self.outcome.lock().expect("outcome lock").clone()
    }

    fn identity(&self) -> String {
        // Names resolve through the role's search path, which can change between runs: the
        // identity names the resolution the read is bound to. One that could not be resolved
        // names none, matches no recorded position, and its run reads nothing.
        let resolved = self.resolved().as_ref().ok();
        let what = match (&self.spec.table, &self.spec.query) {
            (Some(t), _) => match resolved.and_then(|r| r.table.as_ref()) {
                Some((schema, table)) => format!("table {schema}.{table}"),
                None if t.contains('.') => format!("table {t}"),
                None => format!("table {t} unresolved"),
            },
            (_, Some(q)) => format!(
                "query {} path {}",
                shapeshift_core::digest(q.trim()),
                // As a JSON array: a schema named `a,b` is not the two schemas `a` and `b`.
                resolved.map_or("unresolved".to_string(), |r| {
                    serde_json::to_string(&r.path).expect("a list of strings serializes")
                })
            ),
            _ => String::new(),
        };
        let cursor = self.spec.cursor.as_deref().unwrap_or("");
        // The role too: its search path is what the names resolve through.
        let role = self.config.get_user().unwrap_or("");
        format!("{} as {role} {what} {cursor}", display_url(&self.spec.url))
    }
}

/// Cancels the read's fetch in flight when a run stops before the read ends: dropping the
/// queue stops the reader only between fetches, and a fetch the server is slow to answer
/// would otherwise hold the connection and its snapshot open until it returns.
struct Cancel {
    token: CancelToken,
    tls: bool,
    ca_file: Option<String>,
    done: Arc<AtomicBool>,
    /// Set first: the reader starts no fetch after it.
    stop: Arc<AtomicBool>,
}

impl Cancel {
    fn now(self) {
        self.stop.store(true, Ordering::SeqCst);
        if self.done.load(Ordering::SeqCst) {
            return;
        }
        // On its own thread: a run that is ending does not wait on the server. A cancel that
        // lands between two fetches is ignored, and the reader may have passed its check of
        // `stop` just before it was set: so it is sent again until the read has ended.
        std::thread::spawn(move || {
            let tls = match self.tls {
                true => match PostgresSource::tls_connector(self.ca_file.as_deref()) {
                    Ok(c) => Some(c),
                    Err(_) => return,
                },
                false => None,
            };
            for _ in 0..CANCEL_TRIES {
                if self.done.load(Ordering::SeqCst) {
                    return;
                }
                let _ = match &tls {
                    Some(c) => self.token.cancel_query(c.clone()),
                    None => self.token.cancel_query(NoTls),
                };
                std::thread::sleep(CANCEL_STEP);
            }
        });
    }
}

struct Pull {
    rx: Option<Receiver<Msg>>,
    tracker: Option<Tracker>,
    cursor: Option<String>,
    tiebreak: Option<String>,
    read: u64,
    failed: bool,
    outcome: Outcome,
    password: Option<String>,
    cancel: Option<Cancel>,
}

impl Drop for Pull {
    fn drop(&mut self) {
        // A run dropped before its read ended (an early stop, a failure, an abandoned
        // iterator): end the fetch, so the transaction and connection go with it.
        self.rx = None;
        if let Some(c) = self.cancel.take() {
            c.now();
        }
    }
}

impl Pull {
    fn finish(&mut self, more: bool) -> Option<Result<Value>> {
        // Dropping the queue stops the reader at its next row: nothing more is fetched; a
        // fetch still in flight is cancelled.
        self.rx = None;
        match self.cancel.take() {
            Some(c) if more => c.now(),
            // The read ended by itself: nothing is in flight.
            _ => {}
        }
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
        self.rx = None;
        let e = match self.password.as_deref().filter(|p| p.len() >= 4) {
            Some(p) => ShapeError::Source(e.to_string().replace(p, "***")),
            None => e,
        };
        Some(Err(e))
    }

    fn judge(&mut self, rec: &Value) -> Result<Verdict> {
        let Some(t) = self.tracker.as_mut() else {
            return Ok(Verdict::Take { replay: false });
        };
        let col = self.cursor.as_deref().expect("a tracker has a cursor");
        let cursor = rec.get(col).cloned().unwrap_or(Value::Null);
        if cursor.is_null() {
            return Err(ShapeError::Source(format!(
                "a row has a NULL `{col}` (`source.postgres.cursor`): every row needs its cursor \
                 value (filter them out, or COALESCE it in the query)"
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
            let rec =
                match self.rx.as_ref()?.recv_timeout(WAIT_STEP) {
                    Ok(Msg::Row(r)) => r,
                    Ok(Msg::Done) => return self.finish(false),
                    Ok(Msg::Failed(e)) => return self.fail(e),
                    Ok(Msg::Ready(..)) => continue,
                    Err(RecvTimeoutError::Timeout) => {
                        if self.tracker.as_mut().is_some_and(|t| t.time_up()) {
                            return self.finish(true);
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return self.fail(err(
                        "the reader stopped without finishing the read: the run commits nothing",
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

#[cfg(test)]
mod tests {
    use super::*;

    /// An error of the driver's shape: a kind, with the cause underneath.
    #[derive(Debug)]
    struct Kind(&'static str, Option<Box<Kind>>);
    impl std::fmt::Display for Kind {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for Kind {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|k| k as _)
        }
    }

    #[test]
    fn a_failed_connection_names_its_cause_and_the_fix() {
        let no_pass = Kind(
            "invalid configuration",
            Some(Box::new(Kind("password missing", None))),
        );
        let m = connect_err(&no_pass, false, true).to_string();
        assert!(m.contains("invalid configuration: password missing"), "{m}");
        assert!(
            m.contains(PASSWORD_ENV) && m.contains(PASSWORD_FILE_ENV),
            "{m}"
        );
        // With a password given, the hint would mislead: none.
        let m = connect_err(&no_pass, true, true).to_string();
        assert!(!m.contains(PASSWORD_ENV), "{m}");

        let tls = Kind(
            "error performing TLS handshake",
            Some(Box::new(Kind(
                "invalid peer certificate: UnknownIssuer",
                None,
            ))),
        );
        let m = connect_err(&tls, true, true).to_string();
        assert!(m.contains("UnknownIssuer") && m.contains("ca_file"), "{m}");
        let plain = Kind("connection refused", None);
        assert_eq!(
            connect_err(&plain, true, false).to_string(),
            "source error: postgres: connection refused"
        );
    }

    #[test]
    fn escape_strings_and_names_holding_a_dollar_bind_the_real_cursor() {
        assert_eq!(
            number_cursor("select E'it\\'s :cursor' as l, :cursor::bigint as id"),
            "select E'it\\'s :cursor' as l, $1::bigint as id"
        );
        assert_eq!(
            number_cursor("select 1 as foo$tag$, :cursor::bigint as id"),
            "select 1 as foo$tag$, $1::bigint as id"
        );
    }

    #[test]
    fn a_query_names_its_cursor_as_the_first_parameter() {
        assert_eq!(
            number_cursor("select * from t where u >= :cursor and x = ':c' order by u"),
            "select * from t where u >= $1 and x = ':c' order by u"
        );
        assert_eq!(
            number_cursor("select :cursor::timestamptz, :cursor_x"),
            "select $1::timestamptz, :cursor_x"
        );
        assert_eq!(number_cursor("select 1::cursor"), "select 1::cursor");
        assert_eq!(
            number_cursor("select ':cursor', \"x:cursor\" -- :cursor\n/* :cursor /* :cursor */ */ $q$ :cursor $q$ where a >= :cursor"),
            "select ':cursor', \"x:cursor\" -- :cursor\n/* :cursor /* :cursor */ */ $q$ :cursor $q$ where a >= $1"
        );
        assert_eq!(
            number_cursor("select 'it''s :cursor' where b = :cursor"),
            "select 'it''s :cursor' where b = $1"
        );
    }

    #[test]
    fn a_runners_host_list_holds_every_host_the_driver_reads() {
        let held = |url: &str, hosts: &[&str]| {
            let spec = PostgresSourceSpec {
                url: url.into(),
                table: Some("orders".into()),
                ..PostgresSourceSpec::default()
            };
            let options = PostgresOptions {
                hosts: hosts.iter().map(|h| h.to_string()).collect(),
                ..Default::default()
            };
            PostgresSource::new(&spec, options)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        assert!(held("postgres://app@db.internal/shop", &["db.internal:5432"]).is_ok());
        assert!(held("postgres://app@DB.Internal:6432/shop", &["db.internal"]).is_ok());
        assert!(
            held("postgres://app@db.internal/shop", &[]).is_ok(),
            "no list, no limit"
        );
        // A second host, a wrong port: each refused, as the driver would have connected to it.
        // (A host given as a parameter is refused by the spec's own check, before this one.)
        assert!(held(
            "postgres://app@db.internal/shop?host=evil.example",
            &["db.internal"]
        )
        .is_err());
        for (url, to) in [
            (
                "postgres://app@db.internal,evil.example/shop",
                "evil.example",
            ),
            ("postgres://app@db.internal:6432/shop", "db.internal:6432"),
        ] {
            let e = held(url, &["db.internal:5432"]).unwrap_err();
            assert!(e.contains(to) && e.contains("--source-hosts"), "{url}: {e}");
        }
    }

    #[test]
    fn names_are_quoted_and_tls_is_required_off_loopback() {
        assert_eq!(quote("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_table("shop.orders"), "\"shop\".\"orders\"");
        assert_eq!(
            display_url("postgres://app@db.example.com:5432/shop"),
            "postgres://db.example.com:5432/shop"
        );
        let spec = PostgresSourceSpec {
            url: "postgres://app@db.example.com/shop".into(),
            table: Some("orders".into()),
            ..PostgresSourceSpec::default()
        };
        assert!(
            PostgresSource::new(&spec, PostgresOptions::default())
                .unwrap()
                .tls
        );
        assert!(
            !PostgresSource::new(
                &spec,
                PostgresOptions {
                    insecure: true,
                    ..Default::default()
                }
            )
            .unwrap()
            .tls
        );
        let local = PostgresSourceSpec {
            url: "postgresql://app@127.0.0.1/shop".into(),
            ..spec.clone()
        };
        assert!(
            !PostgresSource::new(&local, PostgresOptions::default())
                .unwrap()
                .tls
        );
        let pw = PostgresSourceSpec {
            url: "postgres://app:hunter2@db/shop".into(),
            ..spec
        };
        let e = PostgresSource::new(&pw, PostgresOptions::default())
            .err()
            .unwrap()
            .to_string();
        assert!(
            e.contains("SHAPESHIFT_POSTGRES_PASSWORD") && !e.contains("hunter2"),
            "{e}"
        );
    }
}
