//! The HTTP source against a stand-in server: paging, retries, credentials, and the position
//! rules' progress gate (RELEASE-0.6 M4), run as a loop until a run reads nothing new.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use shapeshift_core::{
    Bounds, HeaderEnv, HttpAuth, HttpPaging, HttpSourceSpec, Incremental, Position, SoapSpec,
    SoapVersion, SortOrder, Source, XmlOptions,
};
use shapeshift_source_http::{HttpOptions, HttpSource};

#[derive(Debug, Clone)]
struct Req {
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: String,
}

impl Req {
    fn q(&self, k: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
    }
    fn h(&self, k: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
    }
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

fn ok(body: Value) -> Resp {
    Resp {
        status: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: body.to_string(),
    }
}

type Log = Arc<Mutex<Vec<Req>>>;

/// A one-thread HTTP/1.1 server on a free loopback port; `Connection: close` on every answer.
fn serve(handler: impl Fn(&Req) -> Resp + Send + Sync + 'static) -> (String, Log) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let log: Log = Arc::default();
    let seen = log.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut r = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            if r.read_line(&mut line).is_err() || line.is_empty() {
                continue;
            }
            let target = line.split_whitespace().nth(1).unwrap_or("/").to_string();
            let mut headers = Vec::new();
            let mut len = 0usize;
            loop {
                let mut h = String::new();
                r.read_line(&mut h).unwrap();
                let h = h.trim_end();
                if h.is_empty() {
                    break;
                }
                let (n, v) = h.split_once(':').unwrap();
                let (n, v) = (n.trim().to_ascii_lowercase(), v.trim().to_string());
                if n == "content-length" {
                    len = v.parse().unwrap();
                }
                headers.push((n, v));
            }
            let mut body = vec![0; len];
            r.read_exact(&mut body).unwrap();
            let u = url::Url::parse(&format!("http://x{target}")).unwrap();
            let req = Req {
                body: String::from_utf8_lossy(&body).into_owned(),
                path: u.path().to_string(),
                query: u
                    .query_pairs()
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect(),
                headers,
            };
            seen.lock().unwrap().push(req.clone());
            let resp = handler(&req);
            let mut out = format!(
                "HTTP/1.1 {} X\r\nContent-Length: {}\r\nConnection: close\r\n",
                resp.status,
                resp.body.len()
            );
            for (k, v) in &resp.headers {
                out.push_str(&format!("{k}: {v}\r\n"));
            }
            out.push_str("\r\n");
            out.push_str(&resp.body);
            let _ = stream.write_all(out.as_bytes());
        }
    });
    (base, log)
}

fn spec(url: &str) -> HttpSourceSpec {
    HttpSourceSpec {
        url: url.to_string(),
        ..HttpSourceSpec::default()
    }
}

fn quick() -> HttpOptions {
    HttpOptions {
        insecure: false,
        backoff: Some(Duration::from_millis(1)),
        hosts: Vec::new(),
    }
}

fn read_all(s: &HttpSourceSpec) -> shapeshift_core::Result<Vec<Value>> {
    let mut src = HttpSource::new(s, None, None, quick())?;
    src.open(None, Bounds::default())?.collect()
}

fn ids(v: &[Value]) -> Vec<i64> {
    v.iter().map(|r| r["id"].as_i64().unwrap()).collect()
}

fn items(from: usize, to: usize) -> Vec<Value> {
    (from..to).map(|i| json!({"id": i})).collect()
}

#[test]
fn every_paging_style_reads_every_record_once() {
    const N: usize = 25;
    // `next`, relative to the page.
    let (base, _) = serve(|r| {
        let at: usize = r.q("at").and_then(|a| a.parse().ok()).unwrap_or(0);
        let end = (at + 10).min(N);
        let next = (end < N).then(|| format!("items?at={end}"));
        ok(json!({"data": {"items": items(at, end)}, "links": {"next": next}}))
    });
    let mut s = spec(&format!("{base}/v1/items"));
    s.records = Some("data.items".into());
    s.paging = Some(HttpPaging {
        next: Some("links.next".into()),
        ..HttpPaging::default()
    });
    assert_eq!(
        ids(&read_all(&s).unwrap()),
        (0..N as i64).collect::<Vec<_>>()
    );

    // `Link` headers.
    let (base, _) = serve(|r| {
        let at: usize = r.q("at").and_then(|a| a.parse().ok()).unwrap_or(0);
        let end = (at + 10).min(N);
        let mut resp = ok(json!(items(at, end)));
        if end < N {
            resp.headers
                .push(("Link".into(), format!("</items?at={end}>; rel=\"next\"")));
        }
        resp
    });
    let mut s = spec(&format!("{base}/items"));
    s.paging = Some(HttpPaging {
        link: true,
        ..HttpPaging::default()
    });
    assert_eq!(read_all(&s).unwrap().len(), N);

    // Page numbers, from 1.
    let (base, _) = serve(|r| {
        let page: usize = r.q("page").unwrap().parse().unwrap();
        let size: usize = r.q("per").unwrap().parse().unwrap();
        let at = (page - 1) * size;
        ok(json!(items(at.min(N), (at + size).min(N))))
    });
    let mut s = spec(&format!("{base}/items"));
    s.paging = Some(HttpPaging {
        page: Some("page".into()),
        size: Some("per".into()),
        per_page: Some(10),
        ..HttpPaging::default()
    });
    assert_eq!(
        ids(&read_all(&s).unwrap()),
        (0..N as i64).collect::<Vec<_>>()
    );

    // Offsets: a page exactly full at the end asks once more and gets none.
    let (base, log) = serve(|r| {
        let off: usize = r.q("offset").unwrap().parse().unwrap();
        let lim: usize = r.q("limit").unwrap().parse().unwrap();
        ok(json!({"rows": items(off.min(20), (off + lim).min(20))}))
    });
    let mut s = spec(&format!("{base}/items"));
    s.records = Some("rows".into());
    s.paging = Some(HttpPaging {
        offset: Some("offset".into()),
        limit: Some("limit".into()),
        per_page: Some(10),
        ..HttpPaging::default()
    });
    assert_eq!(read_all(&s).unwrap().len(), 20);
    assert_eq!(log.lock().unwrap().len(), 3);

    // A cursor token.
    let (base, _) = serve(|r| {
        let at: usize = r.q("after").and_then(|a| a.parse().ok()).unwrap_or(0);
        let end = (at + 10).min(N);
        let token = if end < N {
            json!(end.to_string())
        } else {
            json!(null)
        };
        ok(json!({"items": items(at, end), "meta": {"next_cursor": token}}))
    });
    let mut s = spec(&format!("{base}/items"));
    s.records = Some("items".into());
    s.paging = Some(HttpPaging {
        cursor: Some("meta.next_cursor".into()),
        cursor_param: Some("after".into()),
        ..HttpPaging::default()
    });
    assert_eq!(read_all(&s).unwrap().len(), N);
}

#[test]
fn csv_pages_read_and_a_next_link_to_the_same_page_fails() {
    let (base, _) = serve(|_| Resp {
        status: 200,
        headers: vec![],
        body: "id,name\n1,a\n2,b\n".into(),
    });
    let mut s = spec(&format!("{base}/x.csv"));
    s.format = shapeshift_core::HttpFormat::Csv;
    assert_eq!(read_all(&s).unwrap().len(), 2);

    let (base, _) = serve(|_| ok(json!({"items": [{"id": 1}], "next": "/loop"})));
    let mut s = spec(&format!("{base}/loop"));
    s.records = Some("items".into());
    s.paging = Some(HttpPaging {
        next: Some("next".into()),
        ..HttpPaging::default()
    });
    let e = read_all(&s).unwrap_err().to_string();
    assert!(e.contains("never end"), "{e}");
}

#[test]
fn a_429_storm_is_waited_out_and_too_long_a_storm_fails() {
    let n = Arc::new(Mutex::new(0));
    let count = n.clone();
    let (base, _) = serve(move |_| {
        let mut c = count.lock().unwrap();
        *c += 1;
        if *c <= 4 {
            Resp {
                status: if *c % 2 == 0 { 503 } else { 429 },
                headers: vec![("Retry-After".into(), "0".into())],
                body: String::new(),
            }
        } else {
            ok(json!([{"id": 1}]))
        }
    });
    let s = spec(&format!("{base}/items"));
    assert_eq!(read_all(&s).unwrap().len(), 1);
    *n.lock().unwrap() = 0;
    let mut s = spec(&format!("{base}/items"));
    s.retries = Some(2);
    let e = read_all(&s).unwrap_err().to_string();
    assert!(e.contains("429") && e.contains("3 attempts"), "{e}");

    // A Retry-After longer than a run waits fails at once, saying so.
    let (base, _) = serve(|_| Resp {
        status: 429,
        headers: vec![("Retry-After".into(), "3600".into())],
        body: String::new(),
    });
    let e = read_all(&spec(&format!("{base}/items")))
        .unwrap_err()
        .to_string();
    assert!(e.contains("3600s"), "{e}");
}

#[test]
fn credentials_stay_on_their_origin_and_out_of_every_message() {
    const TOKEN: &str = "tok-zq81-secret-value";
    std::env::set_var("SS_TEST_HTTP_TOKEN", TOKEN);
    // The other origin: another port on the same host.
    let (other, other_log) = serve(|_| ok(json!([{"id": 9}])));
    let to_other = other.clone();
    let (base, log) = serve(move |r| match r.path.as_str() {
        "/redirect" => Resp {
            status: 302,
            headers: vec![("Location".into(), format!("{to_other}/x"))],
            body: String::new(),
        },
        "/same" => Resp {
            status: 307,
            headers: vec![("Location".into(), "/items".into())],
            body: String::new(),
        },
        "/leak" => ok(json!({"items": [{"id": 1}], "next": format!("{to_other}/x")})),
        "/echo" => Resp {
            status: 401,
            headers: vec![],
            body: format!("bad token {}", r.h("authorization").unwrap_or("")),
        },
        _ => ok(json!([{"id": 1}])),
    });
    let auth = HttpAuth {
        bearer_env: Some("SS_TEST_HTTP_TOKEN".into()),
        basic_env: None,
        header_env: None,
        query_env: None,
    };
    let with_auth = |path: &str| {
        let mut s = spec(&format!("{base}{path}"));
        s.auth = Some(auth.clone());
        s
    };
    // Sent to its own origin, through a redirect that stays on it.
    assert_eq!(read_all(&with_auth("/same")).unwrap().len(), 1);
    assert_eq!(
        log.lock().unwrap().last().unwrap().h("authorization"),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    // A redirect, or a next link, to another origin fails before anything is sent there.
    let e = read_all(&with_auth("/redirect")).unwrap_err().to_string();
    assert!(e.contains("another origin"), "{e}");
    let mut s = with_auth("/leak");
    s.records = Some("items".into());
    s.paging = Some(HttpPaging {
        next: Some("next".into()),
        ..HttpPaging::default()
    });
    let e = read_all(&s).unwrap_err().to_string();
    assert!(e.contains("another origin"), "{e}");
    assert!(
        other_log.lock().unwrap().is_empty(),
        "nothing reached the other origin"
    );
    // An error page that echoes the credential is scrubbed.
    let e = read_all(&with_auth("/echo")).unwrap_err().to_string();
    assert!(e.contains("401") && !e.contains(TOKEN), "{e}");
    // A key in a header of the API's own, and in a query parameter.
    std::env::set_var("SS_TEST_HTTP_KEY", "key-77aa-secret");
    let mut s = spec(&format!("{base}/items"));
    s.auth = Some(HttpAuth {
        header_env: Some(HeaderEnv {
            name: "X-Api-Key".into(),
            env: "SS_TEST_HTTP_KEY".into(),
        }),
        ..auth.clone()
    });
    s.auth.as_mut().unwrap().bearer_env = None;
    read_all(&s).unwrap();
    assert_eq!(
        log.lock().unwrap().last().unwrap().h("x-api-key"),
        Some("key-77aa-secret")
    );
    // Plain http to another machine with a credential is refused before connecting.
    let mut s = spec("http://192.0.2.10/items");
    s.auth = Some(auth.clone());
    let e = HttpSource::new(&s, None, None, quick())
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("--insecure-source") && !e.contains(TOKEN), "{e}");
    let insecure = HttpOptions {
        insecure: true,
        ..quick()
    };
    assert!(HttpSource::new(&s, None, None, insecure).is_ok());
    // A variable that is not set is named; its (absent) value cannot be.
    let mut s = spec(&format!("{base}/items"));
    s.auth = Some(HttpAuth {
        bearer_env: Some("SS_TEST_HTTP_UNSET".into()),
        ..auth
    });
    let e = HttpSource::new(&s, None, None, quick())
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("SS_TEST_HTTP_UNSET"), "{e}");
}

// ---- the progress gate ----

/// The source's data: `(id, cursor)`, served `since`-inclusive in `(cursor, id)` order, a page
/// of 40 at a time (or newest first, unordered).
#[derive(Default)]
struct Data {
    rows: Vec<(i64, Value)>,
    newest_first: bool,
}

fn cursor_key(v: &Value) -> (i64, String) {
    match v {
        Value::Number(n) => (n.as_i64().unwrap(), String::new()),
        Value::String(s) => (
            chrono::DateTime::parse_from_rfc3339(s).unwrap().timestamp(),
            String::new(),
        ),
        _ => unreachable!(),
    }
}

fn data_server(data: Arc<Mutex<Data>>) -> String {
    let (base, _) = serve(move |r| {
        let d = data.lock().unwrap();
        let since = r.q("since").map(|s| {
            s.parse::<i64>()
                .map(Value::from)
                .unwrap_or_else(|_| Value::String(s.to_string()))
        });
        let mut rows: Vec<&(i64, Value)> = d
            .rows
            .iter()
            .filter(|(_, c)| {
                since
                    .as_ref()
                    .map_or(true, |s| cursor_key(c) >= cursor_key(s))
            })
            .collect();
        rows.sort_by_key(|(id, c)| (cursor_key(c), *id));
        if d.newest_first {
            rows.reverse();
        }
        let page: usize = r.q("page").unwrap().parse().unwrap();
        let at = (page - 1) * 40;
        let items: Vec<Value> = rows
            .iter()
            .skip(at)
            .take(40)
            .map(|(id, c)| json!({"id": id, "v": c}))
            .collect();
        ok(json!(items))
    });
    base
}

fn gate_spec(base: &str, inc: Incremental) -> HttpSourceSpec {
    let mut s = spec(&format!("{base}/items"));
    s.paging = Some(HttpPaging {
        page: Some("page".into()),
        size: Some("size".into()),
        per_page: Some(40),
        ..HttpPaging::default()
    });
    s.incremental = Some(inc);
    s
}

fn asc() -> Incremental {
    Incremental {
        param: "since".into(),
        from: "v".into(),
        order: Some(SortOrder::Asc),
        ..Incremental::default()
    }
}

/// One run: what it landed (as an upsert by id would), each run's new count, the commit.
struct Table {
    rows: BTreeMap<i64, Value>,
    position: Option<Position>,
    runs: Vec<u64>,
}

impl Table {
    fn new() -> Self {
        Table {
            rows: BTreeMap::new(),
            position: None,
            runs: Vec::new(),
        }
    }

    fn run(&mut self, s: &HttpSourceSpec, bounds: &Bounds) -> shapeshift_core::Result<u64> {
        let mut src = HttpSource::new(s, None, None, quick())?;
        let recs: Vec<Value> = src
            .open(self.position.clone(), bounds.clone())?
            .collect::<Result<_, _>>()?;
        for r in recs {
            self.rows.insert(r["id"].as_i64().unwrap(), r);
        }
        let (position, progress) = src.outcome().expect("drained");
        self.position = position;
        self.runs.push(progress.new);
        Ok(progress.new)
    }

    /// Run until a run reads nothing new.
    fn drain(&mut self, s: &HttpSourceSpec, bounds: &Bounds) -> shapeshift_core::Result<()> {
        for _ in 0..200 {
            if self.run(s, bounds)? == 0 {
                return Ok(());
            }
        }
        panic!(
            "the runs never stopped finding new records: {:?}",
            self.runs
        );
    }
}

fn limit(n: u64) -> Bounds {
    Bounds {
        max_records: Some(n),
        ..Bounds::default()
    }
}

#[test]
fn the_loop_reaches_records_past_a_tie_group_larger_than_its_limit() {
    let data = Arc::new(Mutex::new(Data::default()));
    {
        let mut d = data.lock().unwrap();
        d.rows.extend((0..101).map(|i| (i, json!(1))));
        d.rows.extend((101..160).map(|i| (i, json!(i))));
    }
    let base = data_server(data.clone());
    let s = gate_spec(&base, asc());
    let mut t = Table::new();
    t.drain(&s, &limit(100)).unwrap();
    assert_eq!(t.rows.len(), 160, "runs {:?}", t.runs);

    // A record added late at the cursor's own value is read by the inclusive re-read.
    data.lock().unwrap().rows.push((500, json!(159)));
    t.drain(&s, &limit(100)).unwrap();
    assert!(t.rows.contains_key(&500));
}

#[test]
fn a_late_record_is_landed_at_or_above_the_cutoff_and_missed_below_it() {
    let ts = |m: i64| json!(format!("2026-01-01T00:{m:02}:00Z"));
    let data = Arc::new(Mutex::new(Data::default()));
    data.lock()
        .unwrap()
        .rows
        .extend((0..20).map(|i| (i, ts(i + 10))));
    let base = data_server(data.clone());
    let mut inc = asc();
    inc.lookback = Some("5m".into());
    let s = gate_spec(&base, inc);
    let mut t = Table::new();
    t.drain(&s, &limit(7)).unwrap();
    assert_eq!(t.position, Some(Position::At(ts(29))));
    // 3 minutes behind the cursor (29): inside the lookback, landed however late.
    data.lock().unwrap().rows.push((900, ts(26)));
    // 8 minutes behind: below the cutoff (24), missed, the documented limit.
    data.lock().unwrap().rows.push((901, ts(21)));
    t.drain(&s, &limit(7)).unwrap();
    assert!(t.rows.contains_key(&900));
    assert!(
        !t.rows.contains_key(&901),
        "a record below the replay cutoff is missed"
    );
}

#[test]
fn a_tie_group_past_max_tie_fails_and_with_a_tiebreak_spans_bounded_runs() {
    let data = Arc::new(Mutex::new(Data::default()));
    {
        let mut d = data.lock().unwrap();
        d.rows.extend((0..250).map(|i| (i, json!(7))));
        d.rows.extend((250..260).map(|i| (i, json!(8))));
    }
    let base = data_server(data);
    let mut inc = asc();
    inc.max_tie = Some(200);
    let e = Table::new()
        .run(&gate_spec(&base, inc), &limit(50))
        .unwrap_err()
        .to_string();
    assert!(e.contains("more than 200") && e.contains("tiebreak"), "{e}");

    let mut inc = asc();
    inc.max_tie = Some(200);
    inc.tiebreak = Some("id".into());
    let mut t = Table::new();
    t.drain(&gate_spec(&base, inc), &limit(50)).unwrap();
    assert_eq!(t.rows.len(), 260);
    assert!(
        t.runs.iter().all(|n| *n <= 50),
        "each run within its limit: {:?}",
        t.runs
    );
    assert!(
        t.runs.len() >= 6,
        "the group spans several runs: {:?}",
        t.runs
    );
}

#[test]
fn a_newest_first_api_refuses_bounds_and_lands_everything_unbounded() {
    let data = Arc::new(Mutex::new(Data {
        newest_first: true,
        ..Data::default()
    }));
    data.lock()
        .unwrap()
        .rows
        .extend((0..90).map(|i| (i, json!(i / 3))));
    let base = data_server(data);
    let mut inc = asc();
    inc.order = None;
    let s = gate_spec(&base, inc);
    let e = Table::new().run(&s, &limit(10)).unwrap_err().to_string();
    assert!(e.contains("order: asc"), "{e}");
    let mut t = Table::new();
    t.drain(&s, &Bounds::default()).unwrap();
    assert_eq!(t.rows.len(), 90);
    assert_eq!(t.position, Some(Position::At(json!(29))));
    // An ascending order declared over a newest-first API is caught, not trusted.
    let e = Table::new()
        .run(
            &gate_spec(
                &data_server(Arc::new(Mutex::new(Data {
                    newest_first: true,
                    rows: vec![(1, json!(1)), (2, json!(2))],
                }))),
                asc(),
            ),
            &Bounds::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("not in the ascending order"), "{e}");
}

#[test]
fn a_killed_run_resumes_with_no_duplicate() {
    // A run that fails mid-way commits nothing: the next starts from the last commit, and an
    // upsert by id lands each record once.
    let fail_at = Arc::new(Mutex::new(Some(3usize)));
    let flag = fail_at.clone();
    let rows: Vec<(i64, Value)> = (0..200).map(|i| (i, json!(i))).collect();
    let (base, _) = serve(move |r| {
        let page: usize = r.q("page").unwrap().parse().unwrap();
        if *flag.lock().unwrap() == Some(page) {
            return Resp {
                status: 400,
                headers: vec![],
                body: "killed".into(),
            };
        }
        let since: i64 = r.q("since").map_or(i64::MIN, |s| s.parse().unwrap());
        let items: Vec<Value> = rows
            .iter()
            .filter(|(_, v)| v.as_i64().unwrap() >= since)
            .skip((page - 1) * 40)
            .take(40)
            .map(|(id, v)| json!({"id": id, "v": v}))
            .collect();
        ok(json!(items))
    });
    let s = gate_spec(&base, asc());
    let mut t = Table::new();
    assert!(t.run(&s, &Bounds::default()).is_err());
    assert_eq!(t.position, None, "nothing committed");
    *fail_at.lock().unwrap() = None;
    t.drain(&s, &limit(70)).unwrap();
    assert_eq!(t.rows.len(), 200);
    let total_new: u64 = t.runs.iter().sum();
    assert_eq!(total_new, 200, "each record new exactly once: {:?}", t.runs);
}

#[test]
fn another_origin_is_refused_unless_allowed_and_then_gets_no_headers() {
    let (other, other_log) = serve(|_| ok(json!({"items": [{"id": 9}]})));
    let to_other = other.clone();
    let (base, _) =
        serve(move |_| ok(json!({"items": [{"id": 1}], "next": format!("{to_other}/x")})));
    let mut s = spec(&format!("{base}/items"));
    s.records = Some("items".into());
    s.headers.insert("X-Trace-Id".into(), "t-1".into());
    s.paging = Some(HttpPaging {
        next: Some("next".into()),
        ..HttpPaging::default()
    });
    // No credential, and still not followed: a link from the endpoint's data could name an
    // address on this machine's network.
    let e = read_all(&s).unwrap_err().to_string();
    assert!(e.contains("allow_other_origins"), "{e}");
    assert!(other_log.lock().unwrap().is_empty());
    s.allow_other_origins = true;
    assert_eq!(ids(&read_all(&s).unwrap()), vec![1, 9]);
    let there = other_log.lock().unwrap();
    assert_eq!(there.len(), 1);
    assert_eq!(
        there[0].h("x-trace-id"),
        None,
        "the spec's headers stay home"
    );
}

#[test]
fn a_runners_host_list_holds_every_request_whatever_the_spec_allows() {
    // Another host the endpoint sends the run to: by a redirect, and by a page link.
    let (other, other_log) = serve(|_| ok(json!({"items": [{"id": 9}]})));
    let to_other = other.clone();
    let (base, _) = serve(move |r| match r.path.as_str() {
        "/redirect" => Resp {
            status: 302,
            headers: vec![("Location".into(), format!("{to_other}/x"))],
            body: String::new(),
        },
        _ => ok(json!({"items": [{"id": 1}], "next": format!("{to_other}/x")})),
    });
    let port = |u: &str| url::Url::parse(u).unwrap().port().unwrap();
    let held = |s: &HttpSourceSpec, hosts: Vec<String>| {
        let opts = HttpOptions { hosts, ..quick() };
        HttpSource::new(s, None, None, opts).and_then(|mut src| {
            src.open(None, Bounds::default())?
                .collect::<shapeshift_core::Result<Vec<Value>>>()
        })
    };
    let only_base = vec![format!("127.0.0.1:{}", port(&base))];
    // The spec allows other origins; the runner does not: refused at the redirect.
    let mut s = spec(&format!("{base}/redirect"));
    s.records = Some("items".into());
    s.allow_other_origins = true;
    let e = held(&s, only_base.clone()).unwrap_err().to_string();
    assert!(
        e.contains("may not reach") && e.contains("--source-hosts"),
        "{e}"
    );
    // And at a page link.
    let mut s = spec(&format!("{base}/items"));
    s.records = Some("items".into());
    s.allow_other_origins = true;
    s.paging = Some(HttpPaging {
        next: Some("next".into()),
        ..HttpPaging::default()
    });
    let e = held(&s, only_base.clone()).unwrap_err().to_string();
    assert!(e.contains("may not reach"), "{e}");
    assert!(
        other_log.lock().unwrap().is_empty(),
        "the other host is never asked"
    );
    // A first URL off the list is refused before anything is asked.
    let e = held(&s, vec!["api.example.com".into()])
        .unwrap_err()
        .to_string();
    assert!(e.contains("may not reach"), "{e}");
    // Both hosts listed (a host alone takes any port): followed.
    let both = vec!["127.0.0.1".into()];
    assert_eq!(ids(&held(&s, both).unwrap()), vec![1, 9]);
}

#[test]
fn a_server_that_caps_its_page_is_read_to_an_empty_page_and_one_ignoring_it_fails() {
    // Asked for 10 a page, it gives 7: a short page is not the last one.
    let (base, _) = serve(|r| {
        let page: usize = r.q("page").unwrap().parse().unwrap();
        let at = (page - 1) * 7;
        ok(json!(items(at.min(30), (at + 7).min(30))))
    });
    let mut s = spec(&format!("{base}/items"));
    s.paging = Some(HttpPaging {
        page: Some("page".into()),
        size: Some("size".into()),
        per_page: Some(10),
        ..HttpPaging::default()
    });
    assert_eq!(ids(&read_all(&s).unwrap()), (0..30).collect::<Vec<_>>());
    // One that ignores the page parameter gives the same page again.
    let (base, _) = serve(|_| ok(json!(items(0, 10))));
    s.url = format!("{base}/items");
    let e = read_all(&s).unwrap_err().to_string();
    assert!(e.contains("same page twice"), "{e}");
}

#[test]
fn a_tiebreak_bound_at_a_page_end_commits_inside_the_group() {
    let data = Arc::new(Mutex::new(Data::default()));
    data.lock()
        .unwrap()
        .rows
        .extend((0..100).map(|i| (i, json!(1))));
    let base = data_server(data);
    let mut inc = asc();
    inc.tiebreak = Some("id".into());
    let s = gate_spec(&base, inc);
    let mut t = Table::new();
    // The bound is the page size (40): the run stops at the page's end, inside the group.
    t.run(&s, &limit(40)).unwrap();
    assert_eq!(t.position, Some(Position::Inside(json!(1), json!(39))));
    t.drain(&s, &limit(40)).unwrap();
    assert_eq!(t.rows.len(), 100);
    assert!(t.runs.iter().all(|n| *n <= 40), "{:?}", t.runs);
}

// ---- SOAP ----

/// The text of the first `<name>…</name>` in `xml`, prefixes ignored.
fn element<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    // The start tag, with a prefix or without, with attributes or without.
    let open = [
        format!("<{name}>"),
        format!("<{name} "),
        format!(":{name}>"),
        format!(":{name} "),
    ];
    let tag = open.iter().filter_map(|o| xml.find(o.as_str())).min()?;
    let at = tag + xml[tag..].find('>')? + 1;
    let end = xml[at..].find('<')?;
    Some(&xml[at..at + end])
}

fn envelope(inner: &str) -> String {
    format!(
        "<?xml version=\"1.0\"?><soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\
         <soap:Body>{inner}</soap:Body></soap:Envelope>"
    )
}

fn fault_resp(status: u16, text: &str) -> Resp {
    Resp {
        status,
        headers: vec![("Content-Type".into(), "text/xml".into())],
        body: envelope(&format!(
            "<soap:Fault><faultcode>soap:Client</faultcode><faultstring>{text}</faultstring></soap:Fault>"
        )),
    }
}

/// A SOAP service over orders `(id, minute)`: `GetOrders(since, pageNumber, pageSize)`
/// returns those updated at or after `since`, oldest first, behind a WS-Security token.
fn soap_service(rows: Arc<Mutex<Vec<(i64, i64)>>>) -> (String, Log) {
    serve(move |r| {
        if r.h("soapaction") != Some("\"urn:shop#GetOrders\"")
            || !r.h("content-type").unwrap_or("").starts_with("text/xml")
        {
            return fault_resp(500, "Unknown operation");
        }
        if element(&r.body, "Username") != Some("ada")
            || element(&r.body, "Password") != Some("s3cr&amp;t-pw")
        {
            return fault_resp(500, "Invalid credentials");
        }
        let since = element(&r.body, "since").unwrap_or("");
        let page: usize = element(&r.body, "pageNumber").unwrap().parse().unwrap();
        let size: usize = element(&r.body, "pageSize").unwrap().parse().unwrap();
        let ts = |m: i64| format!("2026-11-01T{:02}:{:02}:00Z", m / 60, m % 60);
        let mut all: Vec<(i64, i64)> = rows
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|(_, m)| since.is_empty() || ts(*m).as_str() >= since)
            .collect();
        all.sort_by_key(|(id, m)| (*m, *id));
        let orders: String = all
            .iter()
            .skip((page - 1) * size)
            .take(size)
            .map(|(id, m)| {
                format!(
                    "<Order><id>{id}</id><updatedAt>{}</updatedAt></Order>",
                    ts(*m)
                )
            })
            .collect();
        Resp {
            status: 200,
            headers: vec![("Content-Type".into(), "text/xml".into())],
            body: envelope(&format!(
                "<GetOrdersResponse xmlns=\"urn:shop\">{orders}</GetOrdersResponse>"
            )),
        }
    })
}

fn soap_spec(base: &str) -> HttpSourceSpec {
    let mut s = spec(&format!("{base}/ws/orders"));
    s.soap = Some(SoapSpec {
        action: Some("urn:shop#GetOrders".into()),
        body_template: "<GetOrders xmlns=\"urn:shop\"><since>{{since}}</since>\
                        <pageNumber>{{pageNumber}}</pageNumber><pageSize>{{pageSize}}</pageSize></GetOrders>"
            .into(),
        version: None,
        username_env: Some("SS_TEST_SOAP_USER".into()),
        password_env: Some("SS_TEST_SOAP_PASSWORD".into()),
    });
    s.paging = Some(HttpPaging {
        page: Some("pageNumber".into()),
        size: Some("pageSize".into()),
        per_page: Some(25),
        ..HttpPaging::default()
    });
    s.incremental = Some(Incremental {
        param: "since".into(),
        from: "updatedAt".into(),
        order: Some(SortOrder::Asc),
        tiebreak: Some("id".into()),
        ..Incremental::default()
    });
    s
}

fn xml() -> Option<XmlOptions> {
    Some(XmlOptions {
        record: "/Envelope/Body/GetOrdersResponse/Order".into(),
    })
}

#[test]
fn a_soap_service_is_paged_incrementally_with_a_username_token() {
    std::env::set_var("SS_TEST_SOAP_USER", "ada");
    std::env::set_var("SS_TEST_SOAP_PASSWORD", "s3cr&t-pw");
    let rows = Arc::new(Mutex::new((0..130).map(|i| (i, i / 7)).collect::<Vec<_>>()));
    let (base, log) = soap_service(rows.clone());
    let s = soap_spec(&base);
    s.validate().unwrap();
    let mut landed = BTreeMap::new();
    let mut position = None;
    let mut news = Vec::new();
    for _ in 0..50 {
        let mut src = HttpSource::new(&s, None, xml(), quick()).unwrap();
        let recs: Vec<Value> = src
            .open(position.clone(), limit(40))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for r in recs {
            landed.insert(r["id"].as_str().unwrap().parse::<i64>().unwrap(), r);
        }
        let (p, progress) = src.outcome().unwrap();
        position = p;
        news.push(progress.new);
        if progress.new == 0 {
            break;
        }
    }
    assert_eq!(landed.len(), 130, "runs {news:?}");
    assert!(news.iter().all(|n| *n <= 40), "{news:?}");
    // The parameters went in the envelope, not the URL.
    let seen = log.lock().unwrap();
    assert!(seen
        .iter()
        .all(|r| r.query.is_empty() && r.path == "/ws/orders"));
    assert!(
        seen.last().unwrap().body.contains("<since>2026-11-01T"),
        "{}",
        seen.last().unwrap().body
    );
}

#[test]
fn a_soap_fault_fails_the_run_once_naming_its_fault_string() {
    std::env::set_var("SS_TEST_SOAP_USER", "ada");
    std::env::set_var("SS_TEST_SOAP_PASSWORD_WRONG", "not-the-password-77");
    let (base, log) = soap_service(Arc::default());
    let mut s = soap_spec(&base);
    s.soap.as_mut().unwrap().password_env = Some("SS_TEST_SOAP_PASSWORD_WRONG".into());
    let mut src = HttpSource::new(&s, None, xml(), quick()).unwrap();
    let e = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("SOAP fault") && e.contains("Invalid credentials"),
        "{e}"
    );
    assert!(!e.contains("not-the-password-77"), "{e}");
    assert_eq!(log.lock().unwrap().len(), 1, "a fault is not retried");

    // SOAP 1.2, and a fault on a 200.
    let (base, _) = serve(|r| {
        assert!(
            r.h("content-type")
                .unwrap()
                .contains("application/soap+xml"),
            "{:?}",
            r.headers
        );
        assert!(r.body.contains("http://www.w3.org/2003/05/soap-envelope"));
        Resp {
            status: 200,
            headers: vec![],
            body: "<env:Envelope xmlns:env=\"http://www.w3.org/2003/05/soap-envelope\"><env:Body><env:Fault>\
                   <env:Code><env:Value>env:Sender</env:Value></env:Code><env:Reason><env:Text xml:lang=\"en\">\
                   No such shop</env:Text></env:Reason></env:Fault></env:Body></env:Envelope>"
                .into(),
        }
    });
    let mut s = spec(&format!("{base}/ws"));
    s.soap = Some(SoapSpec {
        body_template: "<GetShop/>".into(),
        version: Some(SoapVersion::V12),
        ..SoapSpec::default()
    });
    let mut src = HttpSource::new(
        &s,
        None,
        Some(XmlOptions {
            record: "Shop".into(),
        }),
        quick(),
    )
    .unwrap();
    let e = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err()
        .to_string();
    assert!(e.contains("env:Sender: No such shop"), "{e}");
}

#[test]
fn a_username_token_is_refused_over_plain_http_to_another_machine_and_templates_are_checked() {
    std::env::set_var("SS_TEST_SOAP_USER", "ada");
    std::env::set_var("SS_TEST_SOAP_PASSWORD", "s3cr&t-pw");
    let s = soap_spec("http://192.0.2.10");
    let e = HttpSource::new(&s, None, xml(), quick())
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("--insecure-source"), "{e}");
    // A placeholder nothing fills, and an incremental template without its parameter.
    let mut t = soap_spec("https://svc.example");
    t.soap.as_mut().unwrap().body_template =
        "<Get>{{since}}{{region}}{{pageNumber}}{{pageSize}}</Get>".into();
    let e = t.validate().unwrap_err().to_string();
    assert!(e.contains("{{region}}"), "{e}");
    t.soap.as_mut().unwrap().body_template = "<Get>{{pageNumber}}{{pageSize}}</Get>".into();
    let e = t.validate().unwrap_err().to_string();
    assert!(e.contains("{{since}}"), "{e}");
}

#[test]
fn a_fault_that_echoes_the_password_is_scrubbed() {
    std::env::set_var("SS_TEST_SOAP_USER", "ada");
    std::env::set_var("SS_TEST_SOAP_PASSWORD_ECHO", "echo-me-9f2c-secret");
    let (base, _) = serve(|r| {
        let pw = element(&r.body, "Password").unwrap_or("").to_string();
        fault_resp(500, &format!("Invalid user ada / password {pw}"))
    });
    let mut s = soap_spec(&base);
    s.soap.as_mut().unwrap().password_env = Some("SS_TEST_SOAP_PASSWORD_ECHO".into());
    let mut src = HttpSource::new(&s, None, xml(), quick()).unwrap();
    let e = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("Invalid user ada") && !e.contains("echo-me-9f2c-secret"),
        "{e}"
    );
}

#[test]
fn a_redirect_that_keeps_the_post_keeps_the_envelopes_parameters() {
    std::env::set_var("SS_TEST_SOAP_USER", "ada");
    std::env::set_var("SS_TEST_SOAP_PASSWORD", "s3cr&t-pw");
    // The service moved within its own origin: `/old` answers 307 (the POST, and its body,
    // go on) to `/ws/orders`, which must still see the page it is asked for.
    let (base, log) = serve(|r| {
        if r.path == "/old" {
            return Resp {
                status: 307,
                headers: vec![("Location".into(), "/ws/orders".into())],
                body: String::new(),
            };
        }
        let page: usize = match element(&r.body, "pageNumber").and_then(|p| p.parse().ok()) {
            Some(p) => p,
            None => return fault_resp(500, "pageNumber missing"),
        };
        let orders: String = if page == 1 {
            (0..3)
                .map(|i| {
                    format!(
                        "<Order><id>{i}</id><updatedAt>2026-11-01T00:0{i}:00Z</updatedAt></Order>"
                    )
                })
                .collect()
        } else {
            String::new()
        };
        Resp {
            status: 200,
            headers: vec![],
            body: envelope(&format!("<GetOrdersResponse>{orders}</GetOrdersResponse>")),
        }
    });
    let mut s = soap_spec(&format!("{base}/old"));
    s.url = format!("{base}/old");
    let mut src = HttpSource::new(&s, None, xml(), quick()).unwrap();
    let rows: Vec<Value> = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 3);
    let seen = log.lock().unwrap();
    assert!(seen
        .iter()
        .any(|r| r.path == "/ws/orders" && r.body.contains("<pageNumber>2</pageNumber>")));
}
