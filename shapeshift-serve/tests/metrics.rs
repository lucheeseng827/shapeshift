//! `serve --metrics-addr`: `GET /metrics` on a listener of its own, counting the console's runs
//! and requests and reading its datasets as they are. Without the flag there is no listener.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use shapeshift_serve::{serve, ServeConfig};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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

fn wait_for(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("nothing listens on {port}");
}

fn sample(text: &str, series: &str) -> Option<f64> {
    let prefix = format!("{series} ");
    text.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .map(|v| v.trim().parse().unwrap())
}

#[test]
fn metrics_count_runs_and_requests_and_read_the_datasets() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-metrics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let (port, mport) = (free_port(), free_port());
    let mut cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    cfg.metrics_addr = Some(format!("127.0.0.1:{mport}"));
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    wait_for(mport);
    wait_for(port);

    let (s, text) = request(mport, "GET", "/metrics", None);
    assert_eq!(s, 200);
    assert!(
        text.contains("# TYPE shapeshift_runs_total counter\n"),
        "{text}"
    );
    assert_eq!(sample(&text, "shapeshift_datasets"), Some(0.0), "{text}");
    for kind in ["run_failed", "run_conflict", "table_stale"] {
        assert_eq!(
            sample(&text, &format!("shapeshift_attention{{kind=\"{kind}\"}}")),
            Some(0.0),
            "{text}"
        );
    }
    assert!(text.contains("shapeshift_build_info{version=\""), "{text}");

    // A run through the console, then a dataset with a freshness window.
    let shape = serde_json::json!({
        "input": "{\"id\":1}\n{\"id\":2}\nnot json", "format": "jsonl", "to": "parquet",
        "compression": "snappy", "dataset": "orders"
    });
    let (s, body) = request(port, "POST", "/api/shape", Some(&shape.to_string()));
    assert_eq!(s, 200, "{body}");
    let save = serde_json::json!({
        "name": "orders", "input_path": "in.jsonl", "freshness_minutes": 30
    });
    let (s, body) = request(port, "POST", "/api/dataset/save", Some(&save.to_string()));
    assert_eq!(s, 200, "{body}");

    let (_, text) = request(mport, "GET", "/metrics", None);
    assert_eq!(
        sample(
            &text,
            "shapeshift_runs_total{dataset=\"(adhoc)\",via=\"shape\",outcome=\"ok\"}"
        ),
        Some(1.0),
        "{text}"
    );
    assert_eq!(
        sample(
            &text,
            "shapeshift_rows_total{dataset=\"(adhoc)\",kind=\"out\"}"
        ),
        Some(2.0),
        "{text}"
    );
    assert_eq!(
        sample(
            &text,
            "shapeshift_run_duration_seconds_count{via=\"shape\"}"
        ),
        Some(1.0),
        "{text}"
    );
    assert_eq!(sample(&text, "shapeshift_datasets"), Some(1.0), "{text}");
    assert_eq!(
        sample(
            &text,
            "shapeshift_dataset_freshness_window_seconds{dataset=\"orders\"}"
        ),
        Some(1800.0),
        "{text}"
    );
    assert!(
        sample(
            &text,
            "shapeshift_dataset_last_success_timestamp_seconds{dataset=\"orders\"}"
        )
        .unwrap()
            > 1.7e9,
        "{text}"
    );
    assert!(
        sample(
            &text,
            "shapeshift_http_requests_total{listener=\"console\",class=\"2xx\"}"
        )
        .unwrap()
            >= 2.0,
        "{text}"
    );
    assert!(
        sample(
            &text,
            "shapeshift_http_requests_total{listener=\"metrics\",class=\"2xx\"}"
        )
        .unwrap()
            >= 1.0,
        "{text}"
    );

    // The listener serves `GET /metrics` and nothing else; the console has no /metrics.
    assert_eq!(request(mport, "POST", "/metrics", None).0, 405);
    assert_eq!(request(mport, "GET", "/api/datasets", None).0, 404);
    assert_eq!(request(port, "GET", "/metrics", None).0, 404);
    // On loopback it answers loopback names only: a rebinding page carries a name of its own.
    let mut s = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    s.write_all(
        b"GET /metrics HTTP/1.1
Host: rebound.example
Connection: close

",
    )
    .unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
    std::fs::remove_dir_all(&data_dir).ok();
}

#[test]
fn the_metrics_address_must_be_its_own() {
    let data_dir =
        std::env::temp_dir().join(format!("ss-serve-metrics-same-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let port = free_port();
    let mut cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    cfg.metrics_addr = Some(format!("127.0.0.1:{port}"));
    let e = serve(cfg).unwrap_err().to_string();
    assert!(e.contains("must differ"), "{e}");
    std::fs::remove_dir_all(&data_dir).ok();
}

fn post_with(port: u16, path: &str, headers: &[(&str, &str)], body: &str) -> u16 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut extra = String::new();
    for (k, v) in headers {
        extra.push_str(&format!("{k}: {v}\r\n"));
    }
    let raw = format!(
        "POST {path} HTTP/1.1\r\nHost: producer.example\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(raw.as_bytes()).unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).unwrap();
    resp.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

#[test]
fn a_connectors_posts_are_counted_accepted_duplicate_and_refused() {
    let data_dir =
        std::env::temp_dir().join(format!("ss-serve-metrics-ingest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let (port, iport, mport) = (free_port(), free_port(), free_port());
    let mut cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    cfg.ingest_addr = Some(format!("127.0.0.1:{iport}"));
    cfg.metrics_addr = Some(format!("127.0.0.1:{mport}"));
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    wait_for(mport);
    wait_for(iport);
    wait_for(port);
    let (s, body) = request(
        port,
        "POST",
        "/api/dataset/save",
        Some(r#"{"name":"events","input_path":""}"#),
    );
    assert_eq!(s, 200, "{body}");
    let (s, body) = request(
        port,
        "POST",
        "/api/connector/save",
        Some(r#"{"name":"web","dataset":"events"}"#),
    );
    assert_eq!(s, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let auth = format!("Bearer {}", v["token"].as_str().unwrap());
    let path = "/v1/ingest/web";
    let posts = [
        post_with(
            iport,
            path,
            &[("Authorization", &auth), ("Idempotency-Key", "k1")],
            "{\"a\":1}\n{\"a\":2}\n",
        ),
        // The same post again: a duplicate.
        post_with(
            iport,
            path,
            &[("Authorization", &auth), ("Idempotency-Key", "k1")],
            "{\"a\":1}\n{\"a\":2}\n",
        ),
        // No token: refused.
        post_with(iport, path, &[], "{\"a\":3}\n"),
    ];
    assert_eq!(posts[0] / 100, 2, "{posts:?}");
    assert_eq!(posts[1] / 100, 2, "{posts:?}");
    assert_eq!(posts[2], 401, "{posts:?}");
    let (_, text) = request(mport, "GET", "/metrics", None);
    for (series, want) in [
        (
            "shapeshift_ingest_posts_total{connector=\"web\",result=\"accepted\"}",
            1.0,
        ),
        (
            "shapeshift_ingest_posts_total{connector=\"web\",result=\"duplicate\"}",
            1.0,
        ),
        ("shapeshift_ingest_records_total{connector=\"web\"}", 2.0),
        ("shapeshift_ingest_waiting_batches{connector=\"web\"}", 0.0),
    ] {
        assert_eq!(sample(&text, series), Some(want), "{series}\n{text}");
    }
    assert!(sample(&text, "shapeshift_ingest_spool_bytes{connector=\"web\"}").unwrap() > 0.0);
    std::fs::remove_dir_all(&data_dir).ok();
}
