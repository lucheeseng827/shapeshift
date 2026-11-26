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
