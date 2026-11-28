//! # shapeshift-serve — a local web console for the OSS shaper
//!
//! `shapeshift serve` starts a tiny HTTP server that puts a browser UI in front of
//! three of the CLI's verbs — **infer** a spec from sample JSON, **shape**
//! JSON/JSONL into Parquet or an Apache Iceberg v2 table, and **inspect** the output
//! (`shapeshift cost` stays on the CLI; the website has the calculator). Two read-only views show what those
//! runs did: **Runs**, every run this console started (its phases, outcome and counts,
//! kept in `runs.jsonl` in the data dir), and **Tables**, a table's history read from its
//! own Iceberg metadata, every snapshot an event linked to the run that wrote it.
//! **Datasets** keeps named specs (`datasets/` in the data dir) that run on demand, here, and
//! the **Overview** says which of them need a look: a failed run, a lost race, or no
//! successful run within a dataset's freshness window. **Alerts** tell a webhook (Slack, Teams,
//! a paging service) when one starts needing a look, and when it stops. It is meant to be run on your own
//! machine (or a box in your own infra) — a friendlier on-ramp than the CLI for
//! trying shapeshift and iterating on a spec.
//!
//! ## Ethos
//! This crate stays inside shapeshift's single-static-binary discipline: the HTTP
//! server is [hand-rolled on `std::net`](crate::http) (no async runtime, no web
//! framework) and the UI is three static files [embedded](Assets) into the binary at
//! compile time. `shapeshift serve` therefore adds **no runtime dependency** — no
//! Node, no JVM, no Python — and the whole thing is one relocatable file.
//!
//! ## Scope (deliberately small)
//! It is a *shaper* console, not a control plane: single-user, keeping nothing beyond the
//! files it writes, its datasets, alert rules, push connectors and the log of its own runs, with
//! **no scheduler, run queue, catalog server, pulled sources, metering, or auth**. A dataset runs
//! when you press Run now, or when a connector's batch is ready, one run of it at a time. Those
//! are orchestration concerns, out of scope here. With `ingest_addr`, a second listener takes
//! pushed records (`POST /v1/ingest/<connector>`, a token on every post) and serves nothing else.
//! The
//! server binds to `127.0.0.1` by
//! default and — as a local tool — has no application-level authentication; keep it
//! bound to loopback, or put your own auth in front of it if you expose it.
//!
//! ```no_run
//! let cfg = shapeshift_serve::ServeConfig::new("127.0.0.1:8087", "shapeshift-data");
//! shapeshift_serve::serve(cfg).unwrap();
//! ```

mod alerts;
mod catalog;
mod connectors;
mod datasets;
mod handlers;
mod http;
mod metrics;
mod pulled;
mod runs;

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};

use crate::handlers::Ctx;
use crate::http::{Request, Response};

/// The embedded single-page UI (three static files, no build step, no CDN).
struct Assets;
impl Assets {
    const INDEX_HTML: &'static str = include_str!("../ui/index.html");
    const STYLES_CSS: &'static str = include_str!("../ui/styles.css");
    const APP_JS: &'static str = include_str!("../ui/app.js");
}

/// A strict, self-contained Content-Security-Policy for the app shell: everything
/// loads from this origin, nothing reaches the network off-box. Reinforces the
/// no-telemetry / no-external-dependency ethos at the browser layer. `script-src`
/// stays strict (`'self'` only — no inline JS); `style-src` allows inline `style=`
/// attributes (the console styles layout inline, and inline CSS cannot execute code).
const CSP: &str = "default-src 'none'; base-uri 'none'; form-action 'none'; \
    style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; \
    img-src 'self' data:; font-src 'self'";

/// Configuration for [`serve`].
pub struct ServeConfig {
    /// Address to bind, e.g. `127.0.0.1:8087` or `0.0.0.0:8087`.
    pub addr: String,
    /// Working directory for shaped output, reject sidecars, and temp input.
    pub data_dir: PathBuf,
    /// Extra `Host` header values to accept, beyond the loopback defaults and the
    /// bind IP (needed when reaching the server by hostname behind a proxy).
    pub allowed_hosts: Vec<String>,
    /// Disable the `Host`-header allow-list entirely. Only sensible when the server
    /// sits behind a trusted reverse proxy that sets `Host` itself.
    pub allow_any_host: bool,
    /// Where push connectors take records (`POST /v1/ingest/<connector>`, that path only),
    /// e.g. `0.0.0.0:8088`. None: connectors keep their settings but take nothing.
    pub ingest_addr: Option<String>,
    /// A REST catalog for the Catalog page to browse (read-only), with its warehouse and
    /// whether credentials may go over plain http to another machine. Its credentials come
    /// from the environment. Needs a build with `object_store`.
    pub catalog_uri: Option<String>,
    pub catalog_warehouse: Option<String>,
    pub insecure_catalog: bool,
    /// Let datasets pull from the sources their specs name (`source.http`, `source.mysql`,
    /// `source.postgres`). Off: a spec could name any host, and any environment variable to
    /// send it, and the console has no login, so whoever reaches it decides.
    pub allow_sources: bool,
    /// Where `GET /metrics` is served (that path only), in the Prometheus text format, e.g.
    /// `127.0.0.1:9187`. None: no listener opens and no counter is kept.
    pub metrics_addr: Option<String>,
}

impl ServeConfig {
    /// A config with sensible defaults (loopback host allow-list, no `allow_any_host`).
    pub fn new(addr: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        ServeConfig {
            addr: addr.into(),
            data_dir: data_dir.into(),
            allowed_hosts: Vec::new(),
            allow_any_host: false,
            ingest_addr: None,
            catalog_uri: None,
            catalog_warehouse: None,
            insecure_catalog: false,
            allow_sources: false,
            metrics_addr: None,
        }
    }
}

/// Resolved, immutable server state handed to every request.
struct Server {
    ctx: Arc<Ctx>,
    allowed_hosts: Vec<String>,
    allow_any_host: bool,
}

/// Start the console and serve until the process is stopped. Blocks the caller.
pub fn serve(cfg: ServeConfig) -> Result<()> {
    let addr: SocketAddr = cfg
        .addr
        .to_socket_addrs()
        .with_context(|| format!("resolving bind address `{}`", cfg.addr))?
        .next()
        .ok_or_else(|| anyhow!("`{}` did not resolve to an address", cfg.addr))?;

    let ctx = Arc::new(Ctx::new(cfg.data_dir.clone())?);
    ctx.set_allow_sources(cfg.allow_sources);
    if let Some(uri) = &cfg.catalog_uri {
        if !catalog::AVAILABLE {
            return Err(anyhow!(
                "this build cannot reach a catalog: build the CLI with --features object_store"
            ));
        }
        ctx.set_catalog(catalog::CatalogSettings {
            uri: uri.clone(),
            warehouse: cfg.catalog_warehouse.clone(),
            insecure: cfg.insecure_catalog,
        });
    }

    // Build the Host allow-list: loopback names + the bind IP, plus any extras.
    let mut allowed_hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    allowed_hosts.push(addr.ip().to_string());
    for h in &cfg.allowed_hosts {
        let h = h.trim().to_ascii_lowercase();
        if !h.is_empty() && !allowed_hosts.contains(&h) {
            allowed_hosts.push(h);
        }
    }

    let listener = TcpListener::bind(addr).with_context(|| format!("binding {addr}"))?;
    // What was actually bound (a port of 0 is chosen now).
    let addr = listener.local_addr().context("reading the bound address")?;
    // The ingest listener: one path, a connector's token on every request, no console.
    let ingest = match &cfg.ingest_addr {
        Some(a) => {
            let ia: SocketAddr = a
                .to_socket_addrs()
                .with_context(|| format!("resolving the ingest address `{a}`"))?
                .next()
                .ok_or_else(|| anyhow!("`{a}` did not resolve to an address"))?;
            if ia.port() != 0 && ia == addr {
                return Err(anyhow!("the ingest address must differ from the console's"));
            }
            let l = TcpListener::bind(ia)
                .with_context(|| format!("binding the ingest address {ia}"))?;
            let ia = l.local_addr().context("reading the bound ingest address")?;
            ctx.connectors().set_ingest_addr(Some(ia.to_string()));
            let c = Arc::clone(&ctx);
            let opts = http::Options {
                threads: 4,
                max_body: connectors::MAX_POST,
                read_timeout: std::time::Duration::from_secs(30),
            };
            std::thread::Builder::new()
                .name("ingest".into())
                .spawn(move || {
                    if let Err(e) = http::serve(l, opts, move |req| {
                        let resp = connectors::ingest_route(&c, req);
                        if let Some(m) = c.metrics() {
                            m.answered("ingest", resp.status);
                        }
                        resp
                    }) {
                        tracing::error!(error = %e, "the ingest listener stopped");
                    }
                })
                .context("starting the ingest listener")?;
            Some(ia)
        }
        None => None,
    };
    // The metrics listener: `GET /metrics` only, read-only, and no console.
    let metrics_at = match &cfg.metrics_addr {
        Some(a) => {
            let ma: SocketAddr = a
                .to_socket_addrs()
                .with_context(|| format!("resolving the metrics address `{a}`"))?
                .next()
                .ok_or_else(|| anyhow!("`{a}` did not resolve to an address"))?;
            if ma.port() != 0 && (ma == addr || Some(ma) == ingest) {
                return Err(anyhow!(
                    "the metrics address must differ from the console's and the ingest listener's"
                ));
            }
            let l = TcpListener::bind(ma)
                .with_context(|| format!("binding the metrics address {ma}"))?;
            let ma = l
                .local_addr()
                .context("reading the bound metrics address")?;
            ctx.enable_metrics();
            let c = Arc::clone(&ctx);
            // On loopback, only loopback names are answered: a page that rebinds a name of its
            // own to this address carries that name, and reads nothing.
            let loopback_only = ma.ip().is_loopback();
            let opts = http::Options {
                threads: 4,
                // No body: a request carrying one is refused.
                max_body: 0,
                read_timeout: std::time::Duration::from_secs(10),
            };
            std::thread::Builder::new()
                .name("metrics".into())
                .spawn(move || {
                    if let Err(e) =
                        http::serve(l, opts, move |req| metrics_route(&c, req, loopback_only))
                    {
                        tracing::error!(error = %e, "the metrics listener stopped");
                    }
                })
                .context("starting the metrics listener")?;
            Some(ma)
        }
        None => None,
    };
    // The workers start once everything is bound: a bad address stops here, with nothing running.
    // Alert rules are evaluated after every run and once a minute, on a thread of their own.
    alerts::spawn_evaluator(Arc::clone(&ctx));
    // Connectors' spools are sealed into runs on a thread of their own, too.
    connectors::spawn_flusher(Arc::clone(&ctx));

    let server = Arc::new(Server {
        ctx,
        allowed_hosts,
        allow_any_host: cfg.allow_any_host,
    });

    // Friendly startup banner (stderr, so it never pollutes piped output).
    let shown_host = if addr.ip().is_unspecified() {
        format!("127.0.0.1:{}", addr.port())
    } else {
        addr.to_string()
    };
    eprintln!("shapeshift serve — console at http://{shown_host}/");
    eprintln!("  data dir : {}", server.ctx.data_dir().display());
    eprintln!("  bound to : {addr}  (Ctrl-C to stop)");
    if let Some(ia) = ingest {
        eprintln!(
            "  ingest   : http://{ia}/v1/ingest/<connector>  (a connector's token on every post)"
        );
        if !ia.ip().is_loopback() {
            eprintln!(
                "  note     : the ingest listener speaks plain HTTP: tokens and records cross the                  network unencrypted. Put a TLS-terminating proxy in front of it, or keep it on a                  network you trust."
            );
        }
    }
    if let Some(ma) = metrics_at {
        eprintln!("  metrics  : http://{ma}/metrics  (Prometheus text format)");
        if !ma.ip().is_loopback() {
            eprintln!(
                "  note     : the metrics listener has no authentication: anyone who reaches it \
                 reads dataset and connector names and counts. Keep it on a network you trust."
            );
        }
    }
    if addr.ip().is_unspecified() || !addr.ip().is_loopback() {
        eprintln!(
            "  note     : this server has no authentication — do not expose it directly; \
             front it with your own auth/proxy."
        );
    }

    let opts = http::Options::default();
    let server_for_handler = Arc::clone(&server);
    http::serve(listener, opts, move |req| {
        let resp = route(&server_for_handler, req);
        if let Some(m) = server_for_handler.ctx.metrics() {
            m.answered("console", resp.status);
        }
        resp
    })
    .context("HTTP server error")?;
    Ok(())
}

/// The metrics listener: `GET /metrics`, and nothing else.
fn metrics_route(ctx: &Ctx, req: &Request, loopback_only: bool) -> Response {
    let loopback_host = || {
        req.host_name()
            .is_some_and(|h| matches!(h.as_str(), "localhost" | "127.0.0.1" | "::1"))
    };
    let resp = match (req.method.as_str(), req.path.as_str()) {
        _ if loopback_only && !loopback_host() => Response::error(
            403,
            "the metrics listener is on loopback: it answers loopback names only",
        ),
        ("GET", "/metrics") => match ctx.metrics().map(|m| m.render(ctx)) {
            Some(Some(text)) => Response::new(
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                text.into_bytes(),
            ),
            Some(None) => Response::error(500, "the metrics could not be read"),
            None => Response::error(503, "metrics are off"),
        },
        (_, "/metrics") => Response::error(405, "the metrics listener serves GET /metrics only"),
        _ => Response::error(404, "the metrics listener serves GET /metrics only"),
    };
    if let Some(m) = ctx.metrics() {
        m.answered("metrics", resp.status);
    }
    resp
}

/// Top-level router + security gates (Host allow-list, JSON content-type on POST).
fn route(server: &Server, req: &Request) -> Response {
    // DNS-rebinding guard: reject a request whose Host we don't recognise. A browser
    // page on another origin that rebinds to our IP still carries its own Host.
    if !server.allow_any_host {
        let ok = req
            .host_name()
            .map(|h| server.allowed_hosts.contains(&h))
            .unwrap_or(false);
        if !ok {
            return Response::error(
                403,
                "request rejected: unrecognised Host header (pass --allow-host to permit it)",
            );
        }
    }

    match (req.method.as_str(), req.path.as_str()) {
        // ---- static UI ----
        ("GET", "/") | ("GET", "/index.html") => Response::new(
            200,
            "text/html; charset=utf-8",
            Assets::INDEX_HTML.as_bytes().to_vec(),
        )
        .with_header("Content-Security-Policy", CSP),
        ("GET", "/styles.css") => Response::new(
            200,
            "text/css; charset=utf-8",
            Assets::STYLES_CSS.as_bytes().to_vec(),
        ),
        ("GET", "/app.js") => Response::new(
            200,
            "text/javascript; charset=utf-8",
            Assets::APP_JS.as_bytes().to_vec(),
        ),
        ("GET", "/favicon.ico") => Response::new(204, "image/x-icon", Vec::new()),

        // ---- API ----
        ("GET", "/api/health") => handlers::health(&server.ctx),
        ("POST", "/api/infer") => {
            guard_json(req).unwrap_or_else(|| handlers::infer(&server.ctx, req))
        }
        ("POST", "/api/shape") => {
            guard_json(req).unwrap_or_else(|| handlers::shape(&server.ctx, req))
        }
        ("POST", "/api/inspect") => {
            guard_json(req).unwrap_or_else(|| handlers::inspect(&server.ctx, req))
        }
        ("GET", "/api/runs") => handlers::runs(&server.ctx),
        ("GET", "/api/overview") => datasets::overview(&server.ctx),
        ("GET", "/api/datasets") => datasets::list(&server.ctx),
        ("POST", "/api/dataset") => {
            guard_json(req).unwrap_or_else(|| datasets::get(&server.ctx, req))
        }
        ("POST", "/api/dataset/save") => {
            guard_json(req).unwrap_or_else(|| datasets::save(&server.ctx, req))
        }
        ("POST", "/api/dataset/delete") => {
            guard_json(req).unwrap_or_else(|| datasets::delete(&server.ctx, req))
        }
        ("POST", "/api/dataset/run") => {
            guard_json(req).unwrap_or_else(|| datasets::run(&server.ctx, req))
        }
        ("POST", "/api/dataset/rejects") => {
            guard_json(req).unwrap_or_else(|| datasets::rejects(&server.ctx, req))
        }
        ("GET", "/api/alerts") => alerts::list(&server.ctx),
        ("GET", "/api/catalog") => catalog::get(&server.ctx),
        ("POST", "/api/catalog/refresh") => {
            guard_json(req).unwrap_or_else(|| catalog::refresh(&server.ctx))
        }
        ("POST", "/api/catalog/table") => {
            guard_json(req).unwrap_or_else(|| catalog::table(&server.ctx, req))
        }
        ("GET", "/api/connectors") => connectors::list(&server.ctx),
        ("POST", "/api/connector/save") => {
            guard_json(req).unwrap_or_else(|| connectors::save(&server.ctx, req))
        }
        ("POST", "/api/connector/rotate") => {
            guard_json(req).unwrap_or_else(|| connectors::rotate(&server.ctx, req))
        }
        ("POST", "/api/connector/flush") => {
            guard_json(req).unwrap_or_else(|| connectors::flush(&server.ctx, req))
        }
        ("POST", "/api/connector/delete") => {
            guard_json(req).unwrap_or_else(|| connectors::delete(&server.ctx, req))
        }
        ("POST", "/api/alerts/rule/save") => {
            guard_json(req).unwrap_or_else(|| alerts::save_rule(&server.ctx, req))
        }
        ("POST", "/api/alerts/rule/delete") => {
            guard_json(req).unwrap_or_else(|| alerts::delete_rule(&server.ctx, req))
        }
        ("POST", "/api/alerts/target/save") => {
            guard_json(req).unwrap_or_else(|| alerts::save_target(&server.ctx, req))
        }
        ("POST", "/api/alerts/target/delete") => {
            guard_json(req).unwrap_or_else(|| alerts::delete_target(&server.ctx, req))
        }
        ("POST", "/api/alerts/target/test") => {
            guard_json(req).unwrap_or_else(|| alerts::test_target(&server.ctx, req))
        }
        ("POST", "/api/alerts/incident/resolve") => {
            guard_json(req).unwrap_or_else(|| alerts::resolve_incident(&server.ctx, req))
        }
        ("GET", "/api/tables") => handlers::tables(&server.ctx),
        ("POST", "/api/table") => {
            guard_json(req).unwrap_or_else(|| handlers::table(&server.ctx, req))
        }

        // ---- fallbacks ----
        // A known-shaped API path reached with the wrong method is a 405, not a 404.
        (_, p) if p.starts_with("/api/") => Response::error(405, "method not allowed"),
        _ => Response::error(404, "not found"),
    }
}

/// Require `Content-Type: application/json` on mutating POSTs. Combined with the
/// absence of any CORS headers, this blocks cross-origin form/`fetch` CSRF: a
/// browser can't send `application/json` cross-origin without a preflight we never
/// approve. Returns `Some(415)` to short-circuit, `None` to proceed.
fn guard_json(req: &Request) -> Option<Response> {
    let ct = req.header("content-type").unwrap_or("");
    if ct
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("application/json")
    {
        None
    } else {
        Some(Response::error(
            415,
            "expected Content-Type: application/json",
        ))
    }
}
