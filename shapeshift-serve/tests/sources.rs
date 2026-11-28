//! A dataset whose input is a source (`source.http` in its spec): Run now pulls from the
//! position its table records, and the dataset page shows that position and the last pull.
#![cfg(feature = "http")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use shapeshift_serve::{serve, ServeConfig};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn request(port: u16, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(raw.as_bytes()).unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    let status = resp
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = resp
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// `(id, v)` rows, served `?since=` inclusive in `(v, id)` order, as one JSON array.
fn endpoint(rows: Arc<Mutex<Vec<(i64, i64)>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut r = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            if r.read_line(&mut line).is_err() {
                continue;
            }
            loop {
                let mut h = String::new();
                if r.read_line(&mut h).unwrap_or(0) == 0 || h.trim_end().is_empty() {
                    break;
                }
            }
            let target = line.split_whitespace().nth(1).unwrap_or("/");
            let since: i64 = target
                .split_once("since=")
                .and_then(|(_, v)| v.split('&').next())
                .and_then(|v| v.parse().ok())
                .unwrap_or(i64::MIN);
            let mut all: Vec<(i64, i64)> = rows
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|(_, v)| *v >= since)
                .collect();
            all.sort_by_key(|(id, v)| (*v, *id));
            let body = json!(all
                .iter()
                .map(|(id, v)| json!({"id": id, "v": v}))
                .collect::<Vec<_>>())
            .to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    base
}

#[test]
fn a_dataset_pulls_from_its_source_and_its_page_shows_the_position() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-src-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let port = free_port();
    let mut cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    cfg.allow_sources = true;
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/api/health", "").0 == 200
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let post = |path: &str, body: Value| {
        let (s, b) = request(port, "POST", path, &body.to_string());
        (
            s,
            serde_json::from_str::<Value>(&b).unwrap_or(Value::String(b)),
        )
    };

    let rows = Arc::new(Mutex::new((0..30).map(|i| (i, i / 3)).collect::<Vec<_>>()));
    let base = endpoint(rows.clone());
    let spec = format!(
        "dataset: orders\nsource:\n  http:\n    url: {base}/orders?key=k\n    \
         incremental: {{ param: since, from: v, order: asc }}\noutput:\n  format: iceberg\n  \
         path: ignored\n  merge_on: [id]\n"
    );
    let (s, b) = post(
        "/api/dataset/save",
        json!({ "name": "orders", "input_path": "", "spec_yaml": spec }),
    );
    assert_eq!(s, 200, "{b}");

    // The first pull reads everything.
    let (s, b) = post("/api/dataset/run", json!({ "name": "orders" }));
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["source"]["new"], 30, "{b}");
    assert_eq!(b["rows_out"], 30);

    // The page says what it reads, where its table is, and what the pull read.
    let (_, page) = post("/api/dataset", json!({ "name": "orders" }));
    let src = &page["source"];
    assert_eq!(src["kind"], "http");
    assert_eq!(
        src["from"],
        format!("{base}/orders"),
        "no query string shown"
    );
    assert_eq!(src["position"]["at"], 9, "{src}");
    assert_eq!(src["last"]["new"], 30);

    // New rows: the next pull reads those, re-reading the position's own value.
    rows.lock().unwrap().extend((30..35).map(|i| (i, 10)));
    let (s, b) = post("/api/dataset/run", json!({ "name": "orders" }));
    assert_eq!(s, 200, "{b}");
    assert_eq!(
        (
            b["source"]["new"].as_u64(),
            b["source"]["replayed"].as_u64()
        ),
        (Some(5), Some(3)),
        "{b}"
    );
    let (_, page) = post("/api/dataset", json!({ "name": "orders" }));
    assert_eq!(page["source"]["position"]["at"], 10);
    assert_eq!(page["runs"][0]["summary"]["source"]["new"], 5);

    // Replacing the table would drop its position: refused. (Without a merge key the cursor
    // must be strict, or the spec itself is refused.)
    let strict = spec
        .replace("  merge_on: [id]\n", "")
        .replace("order: asc }", "order: asc, strict: true }");
    let (s, b) = post(
        "/api/dataset/save",
        json!({ "name": "orders", "input_path": "", "append": false, "spec_yaml": strict }),
    );
    assert_eq!(s, 200, "{b}");
    let (s, b) = post("/api/dataset/run", json!({ "name": "orders" }));
    assert_eq!(s, 400);
    assert!(b.to_string().contains("Append"), "{b}");

    // A file and a source together are refused when saved.
    let (s, b) = post(
        "/api/dataset/save",
        json!({ "name": "both", "input_path": "in.jsonl", "spec_yaml": spec }),
    );
    assert_eq!(s, 400, "{b}");

    // A dataset with neither a file nor a source says what it needs.
    let (_, _) = post(
        "/api/dataset/save",
        json!({ "name": "nothing", "input_path": "" }),
    );
    let (s, b) = post("/api/dataset/run", json!({ "name": "nothing" }));
    assert_eq!(s, 400);
    assert!(b.to_string().contains("source.http"), "{b}");
    std::fs::remove_dir_all(&data_dir).ok();
}

#[test]
fn a_console_started_without_allow_sources_pulls_nothing() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-nosrc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let port = free_port();
    let cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/api/health", "").0 == 200
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let base = endpoint(Arc::new(Mutex::new(vec![(1, 1)])));
    let spec = format!("dataset: o\nsource:\n  http:\n    url: {base}/orders\noutput: {{ format: iceberg, path: x }}\n");
    let body = json!({ "name": "o", "input_path": "", "spec_yaml": spec }).to_string();
    assert_eq!(request(port, "POST", "/api/dataset/save", &body).0, 200);
    let (s, b) = request(
        port,
        "POST",
        "/api/dataset/run",
        &json!({ "name": "o" }).to_string(),
    );
    assert_eq!(s, 400);
    assert!(b.contains("--allow-sources"), "{b}");
    std::fs::remove_dir_all(&data_dir).ok();
}

#[test]
fn a_filter_that_keeps_none_of_the_sample_stops_the_run_before_it_commits() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-nokeep-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let port = free_port();
    let mut cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    cfg.allow_sources = true;
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/api/health", "").0 == 200
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // The sampling window is `infer_sample + 10,000` records read; the first record the
    // filter keeps comes after it.
    let rows = Arc::new(Mutex::new((0..10_020).map(|i| (i, i)).collect::<Vec<_>>()));
    let base = endpoint(rows);
    let spec = format!(
        "dataset: late\nsource:\n  http:\n    url: {base}/orders\n    \
         incremental: {{ param: since, from: v, order: asc }}\nwhere: \"v >= 10010\"\n\
         schema: infer\ncolumns:\n  - {{ name: id, type: int64 }}\n\
         options:\n  infer_sample: 5\noutput:\n  format: iceberg\n  path: ignored\n  merge_on: [id]\n"
    );
    let body = json!({ "name": "late", "input_path": "", "spec_yaml": spec }).to_string();
    let (s, b) = request(port, "POST", "/api/dataset/save", &body);
    assert_eq!(s, 200, "{b}");
    let (s, b) = request(
        port,
        "POST",
        "/api/dataset/run",
        &json!({ "name": "late" }).to_string(),
    );
    assert_eq!(s, 400, "{b}");
    assert!(b.contains("kept none"), "{b}");
    // Nothing was committed: the dataset records no position.
    let (_, page) = request(
        port,
        "POST",
        "/api/dataset",
        &json!({ "name": "late" }).to_string(),
    );
    let page: Value = serde_json::from_str(&page).unwrap();
    assert!(page["source"]["position"].is_null(), "{page}");
    std::fs::remove_dir_all(&data_dir).ok();
}
