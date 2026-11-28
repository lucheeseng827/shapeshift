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

    // A run whose source outgrew its spec: the console reports what the schema drops
    // rather than writing a clean-looking run over quietly-lost data.
    let drift_spec = "dataset: t\noutput:\n  format: parquet\n  path: t.parquet\n\
                      schema: strict\ncolumns:\n- name: id\n  type: int64\n";
    let drift_body = serde_json::json!({
        "input": "{\"id\":1}\n{\"id\":2,\"currency\":\"usd\"}",
        "spec_yaml": drift_spec,
        "format": "jsonl", "to": "parquet", "compression": "snappy", "dataset": "drifted"
    });
    let (s, body) = request(port, "POST", "/api/shape", Some(&drift_body.to_string()));
    assert_eq!(s, 200, "shape: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["rows_out"], 2,
        "drift `warn` still writes every row: {body}"
    );
    assert_eq!(v["drift"]["policy"], "warn");
    assert_eq!(v["drift"]["new_fields"][0]["path"], "currency");
    assert_eq!(v["drift"]["new_fields"][0]["suggested_type"], "string");
    assert!(
        v["drift"]["suggested_columns_yaml"]
            .as_str()
            .unwrap_or_default()
            .contains("name: currency"),
        "drift body: {body}"
    );

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

    // A spec with `output.merge_on` upserts: the second run replaces id 2 and keeps
    // id 1, rather than clearing the table the first run wrote.
    let merge_spec = "dataset: m\noutput:\n  format: iceberg\n  path: m\n  merge_on: [id]\n\
                      schema: strict\ncolumns:\n- name: id\n  type: int64\n\
                      - name: status\n  type: string\n";
    for input in [
        "{\"id\":1,\"status\":\"new\"}\n{\"id\":2,\"status\":\"new\"}",
        "{\"id\":2,\"status\":\"paid\"}\n{\"id\":3,\"status\":\"new\"}",
    ] {
        let merge_body = serde_json::json!({
            "input": input, "spec_yaml": merge_spec,
            "format": "jsonl", "to": "iceberg", "compression": "snappy", "dataset": "m"
        });
        let (s, body) = request(port, "POST", "/api/shape", Some(&merge_body.to_string()));
        assert_eq!(s, 200, "merge shape: {body}");
    }
    let (s, body) = request(
        port,
        "POST",
        "/api/inspect",
        Some(r#"{"path":"outputs/m"}"#),
    );
    assert_eq!(s, 200, "inspect: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["kind"], "iceberg");
    assert_eq!(v["total_records"], 4, "inspect body: {body}");
    assert_eq!(v["position_deletes"], 1, "inspect body: {body}");
    assert_eq!(v["live_records"], 3, "inspect body: {body}");
    // The snapshot id is a string, as everywhere in this API: a 64-bit id in a JSON number
    // reaches a browser rounded (…521819 shown as …521800).
    let snap = v["current_snapshot_id"]
        .as_str()
        .expect("snapshot id as a string");
    assert!(snap.parse::<i64>().is_ok(), "inspect body: {body}");
    let meta: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(data_dir.join("outputs/m/metadata/v2.metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        snap,
        meta["current-snapshot-id"].to_string(),
        "inspect body: {body}"
    );

    // Every shape above is in the run log, newest first, each with its phases and outcome.
    let (s, body) = request(port, "GET", "/api/runs", None);
    assert_eq!(s, 200, "runs: {body}");
    let runs: serde_json::Value = serde_json::from_str(&body).unwrap();
    let runs = runs["runs"].as_array().unwrap().clone();
    assert_eq!(runs.len(), 4, "two Parquet shapes and two merges: {body}");
    assert!(runs.iter().all(|r| r["outcome"] == "ok" && r["exit"] == 0));
    let merges: Vec<&serde_json::Value> = runs.iter().filter(|r| r["dataset"] == "m").collect();
    assert_eq!(merges.len(), 2);
    assert_eq!(
        merges[0]["phases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["shaping", "committing"]
    );
    assert_eq!(merges[0]["output"], "outputs/m");
    assert!(merges[0]["summary"]["snapshot_id"].is_string());

    // The table list, and the merge table's history: each snapshot names the run that
    // wrote it, so the two views link up.
    let (s, body) = request(port, "GET", "/api/tables", None);
    assert_eq!(s, 200, "tables: {body}");
    let tables: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(tables["tables"][0]["path"], "outputs/m");
    assert_eq!(tables["tables"][0]["rows"], 3);
    let (s, body) = request(port, "POST", "/api/table", Some(r#"{"path":"outputs/m"}"#));
    assert_eq!(s, 200, "table: {body}");
    let t: serde_json::Value = serde_json::from_str(&body).unwrap();
    let events = t["events"].as_array().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|e| e["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["append", "overwrite"]
    );
    for (event, run) in events.iter().zip(merges.iter().rev()) {
        assert_eq!(event["writer"]["run_id"], run["id"], "{body}");
        assert_eq!(event["snapshot_id"], run["summary"]["snapshot_id"]);
    }
    assert_eq!(t["schema"][1]["name"], "status");
    let (s, _) = request(
        port,
        "POST",
        "/api/table",
        Some(r#"{"path":"outputs/none"}"#),
    );
    assert_eq!(s, 404);

    // A run that fails is in the log too, with what went wrong.
    let bad = serde_json::json!({
        "input": "{\"id\":1}", "spec_yaml": "dataset: x\noutput: {format: iceberg, path: x}\n\
                  schema: strict\ncolumns: [{name: id, type: nonsense}]\n",
        "format": "jsonl", "to": "iceberg", "compression": "snappy", "dataset": "x"
    });
    let (s, _) = request(port, "POST", "/api/shape", Some(&bad.to_string()));
    assert_eq!(s, 400);
    let (_, body) = request(port, "GET", "/api/runs", None);
    let runs: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(runs["runs"][0]["outcome"], "failed", "{body}");
    assert_eq!(runs["runs"][0]["exit"], 1);
    assert!(runs["runs"][0]["error"]["message"].is_string());

    let _ = std::fs::remove_dir_all(&data_dir);
}

fn start(data_dir: &std::path::Path) -> u16 {
    let port = free_port();
    let cfg = ServeConfig::new(format!("127.0.0.1:{port}"), data_dir);
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/api/health", None).0 == 200
        {
            return port;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not become healthy");
}

#[test]
fn a_fresh_run_log_never_takes_a_new_append_for_an_earlier_run() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-ids-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let append = |port: u16, id: u32| {
        let body = serde_json::json!({
            "input": format!("{{\"id\":{id}}}"), "format": "jsonl", "to": "iceberg",
            "compression": "snappy", "dataset": "a", "append": true,
        });
        let (s, body) = request(port, "POST", "/api/shape", Some(&body.to_string()));
        assert_eq!(s, 200, "shape: {body}");
        serde_json::from_str::<serde_json::Value>(&body).unwrap()
    };
    let records = |port: u16| {
        let (_, body) = request(
            port,
            "POST",
            "/api/inspect",
            Some(r#"{"path":"outputs/a"}"#),
        );
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["total_records"].clone()
    };
    let first = start(&data_dir);
    for id in 1..=3 {
        assert_eq!(append(first, id)["earlier_attempt"], false);
    }
    // The run log goes (deleted, or another console with its own data dir writes the same
    // table): the next console numbers its runs from 1 again. Its appends still land.
    std::fs::remove_file(data_dir.join("runs.jsonl")).unwrap();
    let second = start(&data_dir);
    for id in 4..=6 {
        let v = append(second, id);
        assert_eq!(v["earlier_attempt"], false, "{v}");
    }
    assert_eq!(records(second), 6);
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn a_where_in_the_spec_filters_records_and_reports_mismatched_comparisons() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-where-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let port = start(&data_dir);
    let input =
        "{\"kind\":\"gate\",\"n\":1}\n{\"kind\":\"queue\",\"n\":2}\n{\"kind\":\"gate\",\"n\":3}";
    let shape = |spec: &str| {
        let body = serde_json::json!({
            "input": input, "spec_yaml": spec, "format": "jsonl", "to": "parquet",
            "compression": "snappy", "dataset": "w",
        });
        let (s, body) = request(port, "POST", "/api/shape", Some(&body.to_string()));
        (
            s,
            serde_json::from_str::<serde_json::Value>(&body).unwrap_or_default(),
            body,
        )
    };
    // Inferred from the kept records; the other kind is counted, not rejected.
    let (s, v, body) =
        shape("dataset: w\nwhere: kind = 'gate'\noutput: { format: parquet, path: w.parquet }\n");
    assert_eq!(s, 200, "{body}");
    assert_eq!(
        (v["rows_out"].as_u64(), v["filtered"].as_u64()),
        (Some(2), Some(1)),
        "{body}"
    );
    assert_eq!(v["rejects_total"], 0, "{body}");
    assert_eq!(v["type_mismatches"], 0, "{body}");
    // A comparison of different kinds is reported, not silent.
    let spec = "dataset: w\nwhere: n = '1'\nschema: strict\noutput: { format: parquet, path: w.parquet }\n\
                columns:\n- { name: n, type: int64 }\n";
    let (s, v, body) = shape(spec);
    assert_eq!(s, 200, "{body}");
    assert_eq!(
        (v["rows_out"].as_u64(), v["type_mismatches"].as_u64()),
        (Some(0), Some(3)),
        "{body}"
    );
    assert!(
        v["first_mismatch"]
            .as_str()
            .unwrap_or_default()
            .contains("compares a number with a string"),
        "{body}"
    );
    // Inferring from a sample the filter emptied is refused with the reason.
    let (s, _, body) =
        shape("dataset: w\nwhere: kind = 'none'\noutput: { format: parquet, path: w.parquet }\n");
    assert_eq!(s, 400, "{body}");
    assert!(body.contains("kept none of the records sampled"), "{body}");
    // Without a `where`, the mismatch fields are absent.
    let (_, v, body) = shape("dataset: w\noutput: { format: parquet, path: w.parquet }\n");
    assert!(v.get("type_mismatches").is_none(), "{body}");
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn a_dataset_is_kept_run_now_appends_and_the_overview_says_what_needs_a_look() {
    use serde_json::{json, Value};
    let data_dir = std::env::temp_dir().join(format!("ss-serve-ds-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("in.jsonl"),
        "{\"id\":1,\"v\":\"a\"}\n{\"id\":2,\"v\":\"b\"}\nnot-json\n",
    )
    .unwrap();
    let port = start(&data_dir);
    let post = |path: &str, body: Value| request(port, "POST", path, Some(&body.to_string()));
    let parse = |s: &str| serde_json::from_str::<Value>(s).unwrap();

    // Kept, and refused when it could not run.
    let (s, b) = post(
        "/api/dataset/save",
        json!({ "name": "events", "input_path": "in.jsonl", "freshness_minutes": 60 }),
    );
    assert_eq!(s, 200, "{b}");
    assert_eq!(parse(&b)["output"], "outputs/events");
    assert!(data_dir.join("datasets/events.json").is_file());
    let (s, _) = post(
        "/api/dataset/save",
        json!({ "name": "../x", "input_path": "in.jsonl" }),
    );
    assert_eq!(s, 400);
    let (s, _) = post(
        "/api/dataset/save",
        json!({ "name": "p", "input_path": "in.jsonl", "to": "parquet", "partition_by": ["v"] }),
    );
    assert_eq!(s, 400);

    // Run now, twice: an Iceberg dataset appends by default.
    for _ in 0..2 {
        let (s, b) = post("/api/dataset/run", json!({ "name": "events" }));
        assert_eq!(s, 200, "{b}");
        assert_eq!(parse(&b)["rows_out"], 2);
    }
    let (_, b) = post("/api/inspect", json!({ "path": "outputs/events" }));
    assert_eq!(parse(&b)["total_records"], 4, "{b}");
    let (_, b) = post("/api/dataset/rejects", json!({ "name": "events" }));
    assert_eq!(
        parse(&b)["total"],
        1,
        "the bad line, from the latest run's sidecar: {b}"
    );

    // Its page: the runs are its own, marked as started by Run now.
    let (_, b) = post("/api/dataset", json!({ "name": "events" }));
    let page = parse(&b);
    assert_eq!(page["runs"].as_array().unwrap().len(), 2);
    assert_eq!(page["runs"][0]["via"], "dataset");
    assert_eq!(page["freshness"], "fresh");
    assert_eq!(page["table"], true);

    // A dataset whose run fails needs a look; the healthy one does not.
    let (s, _) = post(
        "/api/dataset/save",
        json!({ "name": "broken", "input_path": "missing.jsonl" }),
    );
    assert_eq!(s, 200);
    let (s, _) = post("/api/dataset/run", json!({ "name": "broken" }));
    assert_eq!(s, 400);
    let (_, b) = request(port, "GET", "/api/overview", None);
    let ov = parse(&b);
    let att: Vec<(String, String)> = ov["attention"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["kind"].as_str().unwrap().to_string(),
                c["subject"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(att, vec![("run_failed".to_string(), "broken".to_string())]);
    assert_eq!(ov["figures"]["datasets"], 2);
    assert_eq!(ov["figures"]["failed_24h"], 1);

    // Forgetting a dataset keeps its output and runs.
    let (s, _) = post("/api/dataset/delete", json!({ "name": "events" }));
    assert_eq!(s, 200);
    let (s, _) = post("/api/dataset", json!({ "name": "events" }));
    assert_eq!(s, 404);
    assert!(data_dir.join("outputs/events/metadata").is_dir());
    let _ = std::fs::remove_dir_all(&data_dir);
}

/// A raw POST with extra headers, for the ingest listener.
fn post_with(port: u16, path: &str, headers: &[(&str, &str)], body: &str) -> (u16, String) {
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
fn a_connector_takes_pushed_records_with_its_token_and_its_dataset_lands_them() {
    use serde_json::{json, Value};
    // Relative, as the default `--data-dir` is: a batch's path must not get it prefixed twice.
    // A relative path lands in the crate's directory, so it is removed when the test ends,
    // passed or failed, and is not named `target` (the source mirror refuses to publish one).
    struct RemoveOnDrop(std::path::PathBuf);
    impl Drop for RemoveOnDrop {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let data_dir = std::path::PathBuf::from(format!(".ss-serve-ingest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let _cleanup = RemoveOnDrop(data_dir.clone());
    let port = free_port();
    let ingest = free_port();
    let mut cfg = ServeConfig::new(format!("127.0.0.1:{port}"), &data_dir);
    cfg.ingest_addr = Some(format!("127.0.0.1:{ingest}"));
    std::thread::spawn(move || {
        let _ = serve(cfg);
    });
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/api/health", None).0 == 200
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let post = |path: &str, body: Value| request(port, "POST", path, Some(&body.to_string()));
    let parse = |s: &str| serde_json::from_str::<Value>(s).unwrap();

    // A connector needs an Iceberg dataset.
    let (s, _) = post(
        "/api/dataset/save",
        json!({ "name": "pq", "to": "parquet" }),
    );
    assert_eq!(s, 200);
    let (s, b) = post(
        "/api/connector/save",
        json!({ "name": "c", "dataset": "pq" }),
    );
    assert_eq!(s, 400, "{b}");
    let (s, _) = post("/api/dataset/save", json!({ "name": "events" }));
    assert_eq!(s, 200);
    let (s, b) = post(
        "/api/connector/save",
        // Never sealed by time in this test: it flushes by hand, so nothing races the clock.
        json!({ "name": "logs", "dataset": "events", "flush_seconds": 3600 }),
    );
    assert_eq!(s, 200, "{b}");
    let token = parse(&b)["token"].as_str().unwrap().to_string();
    let (_, b) = post(
        "/api/connector/save",
        json!({ "name": "other", "dataset": "events" }),
    );
    let other = parse(&b)["token"].as_str().unwrap().to_string();
    let (_, b) = request(port, "GET", "/api/connectors", None);
    assert!(!b.contains(&token), "a token is shown once, never listed");

    let auth = format!("Bearer {token}");
    let path = "/v1/ingest/logs";
    // Refused: no token, another connector's token, a half-bad batch, the console's own API.
    assert_eq!(post_with(ingest, path, &[], "{\"id\":1}").0, 401);
    let wrong = format!("Bearer {other}");
    assert_eq!(
        post_with(ingest, path, &[("Authorization", &wrong)], "{\"id\":1}").0,
        401
    );
    let (s, b) = post_with(
        ingest,
        path,
        &[("Authorization", &auth)],
        "{\"id\":1}\nnope\n",
    );
    assert_eq!(s, 400);
    assert!(b.contains("line 2"), "{b}");
    assert_eq!(
        post_with(ingest, "/api/datasets", &[("Authorization", &auth)], "").0,
        404
    );
    assert_eq!(
        post_with(ingest, "/v1/ingest/nope", &[("Authorization", &auth)], "{}").0,
        404
    );

    // Taken: NDJSON, then an array; a retry of a named post is acknowledged, not added.
    let (s, b) = post_with(
        ingest,
        path,
        &[("Authorization", &auth), ("Idempotency-Key", "k1")],
        "{\"id\":1,\"v\":\"a\"}\n{\"id\":2,\"v\":\"b\"}\n",
    );
    assert_eq!((s, parse(&b)["accepted"].clone()), (202, json!(2)), "{b}");
    let (s, b) = post_with(
        ingest,
        path,
        &[("Authorization", &auth), ("Idempotency-Key", "k1")],
        "{\"id\":1,\"v\":\"a\"}\n{\"id\":2,\"v\":\"b\"}\n",
    );
    assert_eq!(
        (s, parse(&b)["duplicate"].clone()),
        (200, json!(true)),
        "{b}"
    );
    let (s, _) = post_with(
        ingest,
        path,
        &[("Authorization", &auth)],
        "[{\"id\":3,\"v\":\"c\"}]",
    );
    assert_eq!(s, 202);

    // Sealed by hand, run by the dataset: three rows land, in one run.
    let (s, b) = post("/api/connector/flush", json!({ "name": "logs" }));
    assert_eq!((s, parse(&b)["sealed"].clone()), (200, json!(true)), "{b}");
    let mut rows = Value::Null;
    for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(100));
        let (s, b) = post("/api/inspect", json!({ "path": "outputs/events" }));
        if s == 200 {
            rows = parse(&b)["total_records"].clone();
            if rows == 3 {
                break;
            }
        }
    }
    assert_eq!(rows, 3);
    // The counts follow the run's end: wait for the batch to be done with.
    let logs_now = || {
        let (_, b) = request(port, "GET", "/api/connectors", None);
        let list = parse(&b);
        let logs = list["connectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "logs")
            .unwrap()
            .clone();
        (list, logs)
    };
    let (mut list, mut logs) = logs_now();
    for _ in 0..100 {
        if logs["stats"]["runs_ok"] == 1 && logs["waiting_batches"] == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
        (list, logs) = logs_now();
    }
    let (_, b) = post("/api/dataset", json!({ "name": "events" }));
    let page = parse(&b);
    assert_eq!(page["runs"][0]["via"], "connector");
    assert_eq!(page["runs"][0]["connector"], "logs");
    assert_eq!(logs["stats"]["records"], 3);
    assert_eq!(logs["stats"]["duplicates"], 1);
    assert_eq!(logs["stats"]["runs_ok"], 1);
    assert_eq!(logs["waiting_batches"], 0);
    assert_eq!(list["ingest_addr"], format!("127.0.0.1:{ingest}"));

    // A batch run again after it landed (its file could not be removed, say) is found by its
    // run id and not appended twice.
    let batch = data_dir.join("spool/logs/batch-0000000000000001-replay.jsonl");
    let runs_ok = |n: u64| {
        for _ in 0..100 {
            if logs_now().1["stats"]["runs_ok"] == n && logs_now().1["waiting_batches"] == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("the batch did not land: {}", logs_now().1);
    };
    std::fs::write(
        &batch,
        "{\"id\":10,\"v\":\"x\"}
{\"id\":11,\"v\":\"y\"}
",
    )
    .unwrap();
    runs_ok(2);
    let rows = |_: ()| {
        let (_, b) = post("/api/inspect", json!({ "path": "outputs/events" }));
        parse(&b)["total_records"].clone()
    };
    assert_eq!(rows(()), 5);
    std::fs::write(
        &batch,
        "{\"id\":10,\"v\":\"x\"}
{\"id\":11,\"v\":\"y\"}
",
    )
    .unwrap();
    runs_ok(3);
    assert_eq!(
        rows(()),
        5,
        "the same batch again is the same run: nothing added"
    );

    // A rotated token replaces the old one.
    let (_, b) = post("/api/connector/rotate", json!({ "name": "logs" }));
    let fresh = format!("Bearer {}", parse(&b)["token"].as_str().unwrap());
    assert_eq!(
        post_with(ingest, path, &[("Authorization", &auth)], "{\"id\":4}").0,
        401
    );
    assert_eq!(
        post_with(ingest, path, &[("Authorization", &fresh)], "{\"id\":4}").0,
        202
    );
    // Records waiting: delete is refused unless they are discarded.
    let (s, _) = post("/api/connector/delete", json!({ "name": "logs" }));
    assert_eq!(s, 409);
    let (s, _) = post(
        "/api/connector/delete",
        json!({ "name": "logs", "discard": true }),
    );
    assert_eq!(s, 200);
    assert!(!data_dir.join("spool/logs").exists());
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn the_catalog_page_says_when_there_is_no_catalog() {
    let data_dir = std::env::temp_dir().join(format!("ss-serve-cat-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let port = start(&data_dir);
    let parse = |s: &str| serde_json::from_str::<serde_json::Value>(s).unwrap();
    let (s, b) = request(port, "GET", "/api/catalog", None);
    assert_eq!(s, 200);
    let v = parse(&b);
    assert_eq!(v["configured"], false);
    // This crate's tests build without `catalog` unless asked.
    assert_eq!(v["available"], cfg!(feature = "catalog"));
    let (s, b) = request(port, "POST", "/api/catalog/refresh", Some("{}"));
    assert_eq!(
        (s, parse(&b)["configured"].clone()),
        (200, serde_json::json!(false))
    );
    let (s, _) = request(
        port,
        "POST",
        "/api/catalog/table",
        Some(r#"{"levels":["a"],"name":"t"}"#),
    );
    assert_eq!(s, 404, "no catalog configured");
    let _ = std::fs::remove_dir_all(&data_dir);
}
