//! `shape --from` / `source.http` through the binary, against a stand-in server: bounded
//! incremental runs repeated until one reads nothing new land every record once, the
//! position is in the table, and the credential is in no output.
#![cfg(feature = "http")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

const TOKEN: &str = "e2e-tok-5c1d-not-for-output";

fn workdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-http-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_shapeshift"))
        .args(args)
        .env("SS_E2E_TOKEN", TOKEN)
        .output()
        .unwrap()
}

fn text(o: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// `(id, v)` rows served `since`-inclusive in `(v, id)` order, 25 a page; the bearer token
/// is required.
fn serve(rows: Arc<Mutex<Vec<(i64, i64)>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut r = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            if r.read_line(&mut line).is_err() || line.is_empty() {
                continue;
            }
            let target = line.split_whitespace().nth(1).unwrap_or("/").to_string();
            let mut auth = String::new();
            let mut len = 0;
            loop {
                let mut h = String::new();
                r.read_line(&mut h).unwrap();
                let h = h.trim_end().to_string();
                if h.is_empty() {
                    break;
                }
                let (n, v) = h.split_once(':').unwrap();
                match n.trim().to_ascii_lowercase().as_str() {
                    "authorization" => auth = v.trim().to_string(),
                    "content-length" => len = v.trim().parse().unwrap(),
                    _ => {}
                }
            }
            let mut body = vec![0; len];
            r.read_exact(&mut body).unwrap();
            let u = url::Url::parse(&format!("http://x{target}")).unwrap();
            let q = |k: &str| {
                u.query_pairs()
                    .find(|(n, _)| n == k)
                    .map(|(_, v)| v.into_owned())
            };
            let (status, body) = if auth != format!("Bearer {TOKEN}") {
                (401, "{}".to_string())
            } else {
                let since: i64 = q("since").map_or(i64::MIN, |s| s.parse().unwrap());
                let page: usize = q("page").unwrap().parse().unwrap();
                let mut all: Vec<(i64, i64)> = rows
                    .lock()
                    .unwrap()
                    .iter()
                    .copied()
                    .filter(|(_, v)| *v >= since)
                    .collect();
                all.sort_by_key(|(id, v)| (*v, *id));
                let items: Vec<Value> = all
                    .iter()
                    .skip((page - 1) * 25)
                    .take(25)
                    .map(|(id, v)| json!({"id": id, "v": v, "name": format!("n{id}")}))
                    .collect();
                (200, json!({"data": items}).to_string())
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    base
}

fn spec(base: &str, table: &str, extra: &str) -> String {
    format!(
        "dataset: orders\n\
         source:\n  http:\n    url: {base}/orders\n    auth: {{ bearer_env: SS_E2E_TOKEN }}\n    \
         records: data\n    paging: {{ page: page, size: size, per_page: 25 }}\n    \
         incremental: {{ param: since, from: v, order: asc }}\n\
         output:\n  format: iceberg\n  path: {table}\n{extra}\
         options:\n  row_group_rows: 10\n"
    )
}

#[test]
fn bounded_runs_until_nothing_is_new_land_every_record_once() {
    let d = workdir("gate");
    // 130 rows, two to a cursor value, and a group of 30 sharing one value.
    let mut rows: Vec<(i64, i64)> = (0..100).map(|i| (i, i / 2)).collect();
    rows.extend((100..130).map(|i| (i, 50)));
    let rows = Arc::new(Mutex::new(rows));
    let base = serve(rows.clone());
    let table = d.join("t");
    let t = table.to_str().unwrap();
    let path = d.join("orders.yaml");
    std::fs::write(&path, spec(&base, t, "  merge_on: [id]\n")).unwrap();
    let p = path.to_str().unwrap();

    let mut news = Vec::new();
    for _ in 0..20 {
        let o = run(&[
            "shape",
            "--spec",
            p,
            "--max-records",
            "40",
            "--summary-json",
        ]);
        let all = text(&o);
        assert!(o.status.success(), "{all}");
        assert!(
            !all.contains(TOKEN),
            "the credential is in no output: {all}"
        );
        let doc: Value = serde_json::from_slice(&o.stdout).unwrap();
        let n = doc["source"]["new"].as_u64().unwrap();
        news.push(n);
        if n == 0 {
            assert_eq!(
                doc["source"]["position"], doc["source"]["from"],
                "nothing new moves nothing"
            );
            break;
        }
    }
    assert_eq!(
        news.iter().sum::<u64>(),
        130,
        "each record new once: {news:?}"
    );
    let inspect = text(&run(&["inspect", t]));
    assert!(inspect.contains("live-records: 130"), "{inspect}");

    // The position is in the snapshot, as JSON.
    let meta = std::fs::read_dir(table.join("metadata"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .max()
        .unwrap();
    let m: Value = serde_json::from_str(&std::fs::read_to_string(meta).unwrap()).unwrap();
    let head = m["current-snapshot-id"].clone();
    let snap = m["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["snapshot-id"] == head)
        .unwrap();
    let pos: Value =
        serde_json::from_str(snap["summary"]["shapeshift.source.http"].as_str().unwrap()).unwrap();
    assert_eq!(pos["at"], json!(50));
    let of = pos["of"].as_str().unwrap();
    assert!(of.ends_with("/orders v") && !of.contains(TOKEN), "{of}");

    // Another source written into the same table is refused: its rows are not where this
    // position is.
    let other = d.join("other.yaml");
    std::fs::write(
        &other,
        spec(&base, t, "  merge_on: [id]\n").replace("/orders", "/orders-v2"),
    )
    .unwrap();
    let o = run(&["shape", "--spec", other.to_str().unwrap()]);
    let out = text(&o);
    assert!(
        !o.status.success() && out.contains("position is for"),
        "{out}"
    );

    // A row added late at the position's value is picked up by the re-read; the text report
    // says what happened.
    rows.lock().unwrap().push((500, 50));
    let o = run(&["shape", "--spec", p, "--max-records", "40"]);
    let out = text(&o);
    assert!(o.status.success(), "{out}");
    assert!(out.contains("source http: 0 new, 31 re-read"), "{out}");
    let inspect = text(&run(&["inspect", t]));
    assert!(inspect.contains("live-records: 131"), "{inspect}");
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn an_incremental_source_is_refused_where_a_record_could_land_twice() {
    let d = workdir("refuse");
    let base = serve(Arc::default());
    let t = d.join("t");
    let path = d.join("s.yaml");
    std::fs::write(&path, spec(&base, t.to_str().unwrap(), "")).unwrap();
    let out = text(&run(&["shape", "--spec", path.to_str().unwrap()]));
    assert!(out.contains("merge_on") && out.contains("strict"), "{out}");
    // A Parquet file has nowhere to keep the position.
    let s = spec(&base, d.join("o.parquet").to_str().unwrap(), "")
        .replace("format: iceberg", "format: parquet")
        .replace("order: asc }", "order: asc, strict: true }");
    std::fs::write(&path, s).unwrap();
    let out = text(&run(&["shape", "--spec", path.to_str().unwrap()]));
    assert!(out.contains("output.format: iceberg"), "{out}");
    // Bounds on a file source are refused.
    let out = text(&run(&[
        "shape",
        "-i",
        "x.jsonl",
        "-o",
        "o.parquet",
        "--max-records",
        "3",
    ]));
    assert!(out.contains("apply to an endpoint"), "{out}");
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn from_reads_one_page_into_parquet() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut r = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut h = String::new();
                if r.read_line(&mut h).unwrap_or(0) == 0 || h.trim_end().is_empty() {
                    break;
                }
            }
            let body = json!({"result": {"items": [{"id": 1, "a": "x"}, {"id": 2, "a": "y"}]}})
                .to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let d = workdir("from");
    let out = d.join("o.parquet");
    let o = run(&[
        "shape",
        "--from",
        &format!("{base}/v1/items?api=1"),
        "--records",
        "result.items",
        "-o",
        out.to_str().unwrap(),
    ]);
    let all = text(&o);
    assert!(o.status.success(), "{all}");
    assert!(all.contains("rows_in=2 rows_out=2"), "{all}");
    std::fs::remove_dir_all(&d).ok();
}
