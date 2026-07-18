//! A tiny, hand-rolled blocking HTTP/1.1 server on `std::net`.
//!
//! In the same spirit as shapeshift's hand-rolled Avro encoder: rather than pull in
//! an async runtime and a web framework for what is a local, single-user console, we
//! implement exactly the slice of HTTP we need on the standard library. No tokio, no
//! axum, no TLS — so `shapeshift serve` stays one relocatable static binary.
//!
//! ## Model
//! A fixed pool of worker threads each `accept()`s on a clone of the same listening
//! socket (the kernel serialises the accepts). Every connection is handled to
//! completion and then closed (`Connection: close`) — no keep-alive, no pipelining.
//! A read timeout bounds a slow client so it can't pin a worker forever, and the
//! request body is capped so a large POST can't exhaust memory.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

/// A parsed HTTP request. Only the pieces the console needs are surfaced.
pub struct Request {
    pub method: String,
    /// Path with any `?query` stripped (percent-decoding NOT applied — our routes are ASCII).
    pub path: String,
    /// Header names are lowercased; values are trimmed.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    /// First matching header value (case-insensitive; names are already lowercased).
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The host without any `:port` suffix (and without IPv6 brackets), lowercased.
    pub fn host_name(&self) -> Option<String> {
        let h = self.header("host")?;
        let h = h.trim();
        // `[::1]:8087` → `::1`; `127.0.0.1:8087` → `127.0.0.1`.
        let bare = if let Some(rest) = h.strip_prefix('[') {
            rest.split(']').next().unwrap_or("")
        } else {
            h.split(':').next().unwrap_or("")
        };
        Some(bare.to_ascii_lowercase())
    }
}

/// An HTTP response with an owned body.
pub struct Response {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub extra_headers: Vec<(&'static str, String)>,
}

impl Response {
    pub fn new(status: u16, content_type: &'static str, body: Vec<u8>) -> Self {
        Response {
            status,
            content_type,
            body,
            extra_headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.extra_headers.push((name, value.into()));
        self
    }

    pub fn text(status: u16, body: impl Into<String>) -> Self {
        Response::new(
            status,
            "text/plain; charset=utf-8",
            body.into().into_bytes(),
        )
    }

    /// A JSON response from any Serialize value (falls back to a 500 on the rare
    /// serialisation failure).
    pub fn json(status: u16, value: &impl serde::Serialize) -> Self {
        match serde_json::to_vec(value) {
            Ok(body) => Response::new(status, "application/json; charset=utf-8", body),
            Err(e) => Response::text(500, format!("failed to serialize response: {e}")),
        }
    }

    /// A `{ "error": "…" }` body with the given status — the console's error shape.
    pub fn error(status: u16, message: impl Into<String>) -> Self {
        Response::json(status, &serde_json::json!({ "error": message.into() }))
    }
}

/// Server tuning knobs.
pub struct Options {
    /// Worker threads accepting connections.
    pub threads: usize,
    /// Maximum request-body size accepted (bytes); a larger `Content-Length` → 413.
    pub max_body: usize,
    /// Per-connection read timeout.
    pub read_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 16);
        Options {
            threads,
            max_body: 64 * 1024 * 1024, // 64 MiB — generous for a paste-in playground
            read_timeout: Duration::from_secs(30),
        }
    }
}

/// Bind `listener` and serve every request through `handler` until the process exits.
/// Blocks the calling thread (running one of the workers on it).
pub fn serve<H>(listener: TcpListener, opts: Options, handler: H) -> io::Result<()>
where
    H: Fn(&Request) -> Response + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    let opts = Arc::new(opts);
    let mut workers = Vec::new();

    // Spawn N-1 workers; run the Nth on this thread so `serve` blocks.
    for _ in 1..opts.threads {
        let l = listener.try_clone()?;
        let h = Arc::clone(&handler);
        let o = Arc::clone(&opts);
        workers.push(std::thread::spawn(move || worker_loop(l, &o, &*h)));
    }
    worker_loop(listener, &opts, &*handler);

    for w in workers {
        let _ = w.join();
    }
    Ok(())
}

fn worker_loop<H>(listener: TcpListener, opts: &Options, handler: &H)
where
    H: Fn(&Request) -> Response,
{
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_conn(stream, opts, handler),
            // A transient accept error shouldn't kill the worker.
            Err(e) => tracing::debug!(error = %e, "accept failed"),
        }
    }
}

fn handle_conn<H>(stream: TcpStream, opts: &Options, handler: &H)
where
    H: Fn(&Request) -> Response,
{
    let _ = stream.set_read_timeout(Some(opts.read_timeout));
    let _ = stream.set_write_timeout(Some(opts.read_timeout));
    let mut write_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    let resp = match read_request(&stream, opts.max_body) {
        Ok(req) => handler(&req),
        Err(ReadError::TooLarge) => Response::error(413, "request body too large"),
        Err(ReadError::BadRequest(m)) => Response::error(400, m),
        // A closed/timed-out connection with nothing to say — just drop it.
        Err(ReadError::Empty) => return,
        Err(ReadError::Io(e)) => {
            tracing::debug!(error = %e, "connection read error");
            return;
        }
    };

    let _ = write_response(&mut write_stream, &resp);
    let _ = write_stream.flush();
}

enum ReadError {
    Empty,
    TooLarge,
    BadRequest(String),
    Io(io::Error),
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

fn read_request(stream: &TcpStream, max_body: usize) -> Result<Request, ReadError> {
    let mut reader = BufReader::new(stream);

    // ---- request line ----
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(ReadError::Empty);
    }
    let line = line.trim_end();
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| ReadError::BadRequest("empty request line".into()))?
        .to_string();
    let target = parts
        .next()
        .ok_or_else(|| ReadError::BadRequest("missing request target".into()))?;
    // (HTTP version is parts.next() — ignored; we always answer HTTP/1.1 + close.)

    // Route on the path only; a query string, if present, is discarded.
    let path = match target.split_once('?') {
        Some((p, _)) => p.to_string(),
        None => target.to_string(),
    };

    // ---- headers ----
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut h = String::new();
        let n = reader.read_line(&mut h)?;
        if n == 0 {
            return Err(ReadError::BadRequest("unexpected EOF in headers".into()));
        }
        let h = h.trim_end();
        if h.is_empty() {
            break; // end of headers
        }
        if let Some((k, v)) = h.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_string();
            if key == "content-length" {
                content_length = val
                    .parse::<usize>()
                    .map_err(|_| ReadError::BadRequest("invalid Content-Length".into()))?;
            }
            headers.push((key, val));
        }
        // Header lines without a colon are ignored (lenient).
        if headers.len() > 100 {
            return Err(ReadError::BadRequest("too many headers".into()));
        }
    }

    if content_length > max_body {
        return Err(ReadError::TooLarge);
    }

    // ---- body ----
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

fn write_response(stream: &mut TcpStream, resp: &Response) -> io::Result<()> {
    let reason = reason_phrase(resp.status);
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\n",
        resp.status,
        reason,
        resp.content_type,
        resp.body.len(),
    );
    for (k, v) in &resp.extra_headers {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(&resp.body)?;
    Ok(())
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "OK",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with_host(host: &str) -> Request {
        Request {
            method: "GET".into(),
            path: "/".into(),
            headers: vec![("host".into(), host.into())],
            body: Vec::new(),
        }
    }

    #[test]
    fn host_name_strips_port_and_lowercases() {
        assert_eq!(
            req_with_host("127.0.0.1:8087").host_name().unwrap(),
            "127.0.0.1"
        );
        assert_eq!(
            req_with_host("LocalHost:3000").host_name().unwrap(),
            "localhost"
        );
        assert_eq!(req_with_host("[::1]:8087").host_name().unwrap(), "::1");
        assert_eq!(
            req_with_host("example.com").host_name().unwrap(),
            "example.com"
        );
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let r = Request {
            method: "POST".into(),
            path: "/api/shape".into(),
            headers: vec![("content-type".into(), "application/json".into())],
            body: Vec::new(),
        };
        assert_eq!(r.header("Content-Type"), Some("application/json"));
        assert_eq!(r.header("CONTENT-TYPE"), Some("application/json"));
        assert_eq!(r.header("x-missing"), None);
    }
}
