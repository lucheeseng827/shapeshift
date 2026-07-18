//! End-to-end test: start the real server on an ephemeral port and drive the whole
//! console API over raw TCP — no HTTP-client dependency, in keeping with the crate's
//! hand-rolled-on-`std` ethos. Proves the four verbs land real output through the
//! same engine the CLI uses.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use shapeshift_serve::{serve, ServeConfig};

/// Grab a currently-free port, then release it for the server to rebind.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Minimal HTTP/1.1 request over a fresh connection; returns (status, body).
fn request(port: u16, method: &str, path: &str, json_body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let body = json_body.unwrap_or("");
    let extra = if method == "POST" {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    } else {
        String::new()
    };
    let raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{extra}Connection: close\r\n\r\n{body}"
    );
    stream.write_all(raw.as_bytes()).unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).unwrap();
    let status = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let body = resp
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or("")
        .to_string();
    (status, body)
}

#[test]
fn full_console_flow_over_tcp() {
    let port = free_port();
    let data_dir = std::env::temp_dir().join(format!("ss-serve-it-{}-{port}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);

    let cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });

    // Wait for the server to come up.
    let mut up = false;
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let (s, _) = request(port, "GET", "/api/health", None);
            if s == 200 {
                up = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(up, "server did not become healthy");

    // Static shell is served.
    let (s, body) = request(port, "GET", "/", None);
    assert_eq!(s, 200);
    assert!(body.contains("shapeshift"));

    // Security gate: an unknown Host is rejected.
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .write_all(
                b"GET /api/health HTTP/1.1\r\nHost: evil.example\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(
            resp.starts_with("HTTP/1.1 403"),
            "bad-host not rejected: {resp}"
        );
    }

    // Cost arithmetic.
    let (s, body) = request(
        port,
        "POST",
        "/api/cost",
        Some(r#"{"rows":50000000,"vendor_per_million":600,"self_host_cost":2}"#),
    );
    assert_eq!(s, 200);
    assert!(body.contains("\"vendor_cost\":30000"), "cost body: {body}");
    assert!(body.contains("\"saved\":29998"), "cost body: {body}");

    // Infer a spec from a sample (one deliberately-bad line is tolerated).
    let sample = "{\"id\":1,\"amount\":12.5}\n{\"id\":2,\"amount\":3.0}\nnot-json";
    let infer_body = serde_json::json!({ "input": sample, "format": "jsonl", "dataset": "t" });
    let (s, body) = request(port, "POST", "/api/infer", Some(&infer_body.to_string()));
    assert_eq!(s, 200, "infer: {body}");
    assert!(body.contains("\"sampled\":2"), "infer body: {body}");
    assert!(body.contains("dataset: t"), "spec yaml missing: {body}");

    // Shape to Parquet on the fly; the bad line becomes a counted parse error.
    let shape_body = serde_json::json!({
        "input": sample, "format": "jsonl", "to": "parquet",
        "compression": "snappy", "dataset": "t"
    });
    let (s, body) = request(port, "POST", "/api/shape", Some(&shape_body.to_string()));
    assert_eq!(s, 200, "shape: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["rows_out"], 2);
    assert_eq!(v["parse_errors"], 1);
    assert_eq!(v["rejects_total"], 1);

    // Inspect the Parquet output written above.
    let (s, body) = request(
        port,
        "POST",
        "/api/inspect",
        Some(r#"{"path":"outputs/t.parquet"}"#),
    );
    assert_eq!(s, 200, "inspect: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["kind"], "parquet");
    assert_eq!(v["rows"], 2);

    let _ = std::fs::remove_dir_all(&data_dir);
}
