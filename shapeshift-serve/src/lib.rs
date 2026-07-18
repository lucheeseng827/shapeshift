//! # shapeshift-serve — a local web console for the OSS shaper
//!
//! `shapeshift serve` starts a tiny HTTP server that puts a browser UI in front of
//! the same four verbs the CLI exposes — **infer** a spec from sample JSON,
//! **shape** JSON/JSONL into Parquet or an Apache Iceberg v2 table, **inspect** the
//! output, and **cost** a run against MAR billing. It is meant to be run on your own
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
//! It is a *shaper* console, not a control plane: single-user, stateless beyond the
//! files it writes, with **no scheduler, run queue, catalog server, connectors,
//! metering, or auth**. Those orchestration concerns live in the separately-licensed
//! commercial control plane, never in the OSS core. The server binds to `127.0.0.1` by
//! default and — as a local tool — has no application-level authentication; keep it
//! bound to loopback, or put your own auth in front of it if you expose it.
//!
//! ```no_run
//! let cfg = shapeshift_serve::ServeConfig::new("127.0.0.1:8087", "shapeshift-data");
//! shapeshift_serve::serve(cfg).unwrap();
//! ```

mod handlers;
mod http;

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
}

impl ServeConfig {
    /// A config with sensible defaults (loopback host allow-list, no `allow_any_host`).
    pub fn new(addr: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        ServeConfig {
            addr: addr.into(),
            data_dir: data_dir.into(),
            allowed_hosts: Vec::new(),
            allow_any_host: false,
        }
    }
}

/// Resolved, immutable server state handed to every request.
struct Server {
    ctx: Ctx,
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

    let ctx = Ctx::new(cfg.data_dir.clone())?;

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
    if addr.ip().is_unspecified() || !addr.ip().is_loopback() {
        eprintln!(
            "  note     : this server has no authentication — do not expose it directly; \
             front it with your own auth/proxy."
        );
    }

    let opts = http::Options::default();
    let server_for_handler = Arc::clone(&server);
    http::serve(listener, opts, move |req| route(&server_for_handler, req))
        .context("HTTP server error")?;
    Ok(())
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
        ("POST", "/api/cost") => {
            guard_json(req).unwrap_or_else(|| handlers::cost(&server.ctx, req))
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
