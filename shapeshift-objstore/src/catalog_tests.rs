//! Catalog commits against a stand-in REST catalog: a small HTTP server that keeps real
//! table metadata, checks a commit's requirements and applies its updates the way the
//! protocol says, and can be told to fail one commit in each way a real catalog can be
//! seen to fail (a refusal, a 5xx before or after applying, a dropped connection, an
//! answer that comes too late). The reference server is the check that the requests are
//! right; this is the check that every outcome is settled right.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{json, Value};
use shapeshift_core::{RecordBatch, ShapeError, Sink, SinkSummary};

use super::*;
use shapeshift_iceberg::RUN_ID_KEY;

/// How the stand-in treats the next commit.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Apply it and answer 200.
    None,
    /// Refuse it: 409, nothing applied, the table unchanged.
    Refuse,
    /// Another writer's commit lands on `main` first, and this one is refused (409): the
    /// race an append rebases past.
    MoveThenRefuse,
    /// Another writer adds a column and commits, and this one is refused: a table the
    /// append's files no longer fit.
    ChangeSchemaThenRefuse,
    /// Reject it as malformed: 400, nothing applied. Not a conflict, so not rebased.
    Reject400,
    /// Apply it, then answer 409: what a copy sent again sees when the first copy landed
    /// after all.
    ApplyThen409,
    /// Apply it, then answer 500 (the answer says nothing; the table says committed).
    ApplyThen500,
    /// Answer 500 without applying it; the table has not moved.
    Fail500,
    /// Another writer's commit lands on `main` first, then this one gets a 500.
    MoveThen500,
    /// Apply it, then drop the connection without answering.
    ApplyThenDrop,
    /// Answer nothing for this long, then apply it: the commit lands after the client
    /// has given up on it.
    LandLate(Duration),
    /// An earlier attempt of the run named here commits first (a snapshot carrying that
    /// run id), and this one is refused.
    RunLandsThenRefuse(&'static str),
    /// Answer 303, redirecting to the table's own URL (a GET of it answers 200 with the
    /// table): what a gateway in front of a catalog may do, nothing applied.
    Redirect303,
    /// Apply it, let another writer commit on top, then answer 500: the table, loaded
    /// again, holds the snapshot under a head (and a metadata file) that is not this one's.
    ApplyMoveThen500,
    /// Change the table as another writer would (move a ref, add a tag), then check this
    /// commit against it as usual.
    Then(fn(&mut Value)),
}

#[derive(Default)]
struct State {
    /// `file:/…` location under which tables are created.
    warehouse: String,
    namespaces: Vec<String>,
    /// Keyed by the namespace path segment (levels joined by 0x1F) and name.
    tables: HashMap<(String, String), Value>,
    /// The metadata file a registered table was registered at, until a commit replaces it.
    registered: HashMap<(String, String), String>,
    metadata_files: u32,
    faults: VecDeque<Fault>,
    /// How the next creates are treated: `ApplyThen500` or `Fail500`, else normally.
    create_faults: VecDeque<Fault>,
    /// `(method, path, body)` of every request.
    requests: Vec<(String, String, Value)>,
    /// When set, a request must carry a token this rule accepts, and the token endpoint
    /// is served.
    auth: Option<AuthRule>,
    /// When set, `GET /v1/config` must name one of these warehouses (each is served
    /// under prefix `p1`).
    warehouses: Option<Vec<String>>,
    /// `(path, Authorization header)` of every request.
    authorizations: Vec<(String, Option<String>)>,
    /// The raw form body of every token request.
    token_requests: Vec<String>,
    /// The connection the tests' writes use (`StandIn::conn`): no credentials unless set.
    client: Option<CatalogConnection>,
}

/// What the stand-in asks of a request's credentials.
struct AuthRule {
    /// A token taken as is.
    token: Option<String>,
    /// `(client_id, client_secret, scope)` the token endpoint issues tokens for.
    client: Option<(String, String, String)>,
    /// Where the token endpoint is served (its path segments).
    token_path: Vec<&'static str>,
    /// How long an issued token is good for, and so the `expires_in` it is issued with.
    lifetime: Duration,
    /// Tokens issued so far, with when each expires.
    issued: Vec<(String, std::time::Instant)>,
}

impl AuthRule {
    fn accepts(&self, header: Option<&str>) -> bool {
        let Some(token) = header.and_then(|h| h.strip_prefix("Bearer ")) else {
            return false;
        };
        self.token.as_deref() == Some(token)
            || self
                .issued
                .iter()
                .any(|(t, until)| t == token && std::time::Instant::now() < *until)
    }
}

/// A stand-in catalog serving `/v1/` with prefix `p1`.
struct StandIn {
    uri: String,
    state: Arc<Mutex<State>>,
}

impl StandIn {
    fn start(warehouse: &Path, namespaces: &[&str]) -> StandIn {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            warehouse: format!("file:{}", warehouse.display()),
            namespaces: namespaces
                .iter()
                .map(|n| n.replace('.', "\u{1f}"))
                .collect(),
            ..State::default()
        }));
        let shared = state.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                let state = shared.clone();
                std::thread::spawn(move || serve(conn, &state));
            }
        });
        StandIn { uri, state }
    }

    /// This catalog, as the tests' writes reach it: with no credentials, unless
    /// [`StandIn::client`] set some.
    fn conn(&self) -> CatalogConnection {
        let st = self.state.lock().unwrap();
        st.client
            .clone()
            .unwrap_or_else(|| CatalogConnection::new(&self.uri))
    }

    /// Reach this catalog with `auth` and `warehouse` from now on.
    fn client(&self, auth: CatalogAuth, warehouse: Option<&str>) {
        self.state.lock().unwrap().client = Some(CatalogConnection {
            uri: self.uri.clone(),
            warehouse: warehouse.map(str::to_string),
            auth,
            allow_plain_http: false,
        });
    }

    /// Ask every request for credentials by `rule`.
    fn require(&self, rule: AuthRule) {
        self.state.lock().unwrap().auth = Some(rule);
    }

    fn fault_next_commit(&self, fault: Fault) {
        self.state.lock().unwrap().faults.push_back(fault);
    }

    fn table(&self, ns: &str, name: &str) -> Value {
        let key = (ns.replace('.', "\u{1f}"), name.to_string());
        self.state.lock().unwrap().tables[&key].clone()
    }

    /// Commit bodies sent so far.
    fn commits(&self) -> Vec<Value> {
        let st = self.state.lock().unwrap();
        st.requests
            .iter()
            .filter(|(m, p, _)| m == "POST" && p.contains("/tables/"))
            .map(|(_, _, b)| b.clone())
            .collect()
    }

    /// Commit to `main` as another writer would: a snapshot of its own on the head.
    fn commit_foreign(&self, ns: &str, name: &str, id: i64) {
        let key = (ns.replace('.', "\u{1f}"), name.to_string());
        let mut st = self.state.lock().unwrap();
        let meta = st.tables.get_mut(&key).unwrap();
        foreign(meta, id);
    }
}

/// Another writer's snapshot on `main`. It reads the same files as the head (its manifest
/// list is the head's), so a write built on it has a real manifest list to carry forward.
fn foreign(meta: &mut Value, id: i64) {
    foreign_run(meta, id, None);
}

/// [`foreign`], written by an attempt of run `run` (or by a writer that names no run). The
/// summary is the head's counts, without the head's shapeshift properties.
fn foreign_run(meta: &mut Value, id: i64, run: Option<&str>) {
    // A snapshot id is never reused in a table: a fault met twice gets a fresh one.
    let taken = |id: i64| {
        meta["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["snapshot-id"] == id)
    };
    let mut id = id;
    while taken(id) {
        id += 1000;
    }
    let seq = meta["last-sequence-number"].as_i64().unwrap() + 1;
    let parent = main_snapshot(meta).expect("another writer appends onto a head");
    let head = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["snapshot-id"] == parent)
        .unwrap()
        .clone();
    let mut summary = head["summary"].clone();
    if let Some(m) = summary.as_object_mut() {
        m.retain(|k, _| !k.starts_with("shapeshift."));
        if let Some(run) = run {
            m.insert(RUN_ID_KEY.into(), json!(run));
        }
    }
    let snap = json!({ "snapshot-id": id, "parent-snapshot-id": parent, "sequence-number": seq,
                       "timestamp-ms": 1, "manifest-list": head["manifest-list"],
                       "summary": summary, "schema-id": meta["current-schema-id"] });
    meta["snapshots"].as_array_mut().unwrap().push(snap);
    meta["last-sequence-number"] = json!(seq);
    meta["refs"]["main"] = json!({ "snapshot-id": id, "type": "branch" });
    meta["current-snapshot-id"] = json!(id);
}

fn serve(conn: TcpStream, state: &Mutex<State>) {
    let mut reader = BufReader::new(conn.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || line.is_empty() {
        return;
    }
    let mut parts = line.split_whitespace();
    let (method, path) = (
        parts.next().unwrap().to_string(),
        parts.next().unwrap().to_string(),
    );
    let mut length = 0;
    let mut authorization = None;
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).unwrap();
        if h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                length = v.trim().parse().unwrap();
            } else if k.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_string());
            }
        }
    }
    let mut raw = vec![0; length];
    reader.read_exact(&mut raw).unwrap();
    let body: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    let (path, query) = match path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (path, String::new()),
    };
    {
        let mut st = state.lock().unwrap();
        st.requests
            .push((method.clone(), path.clone(), body.clone()));
        st.authorizations
            .push((path.clone(), authorization.clone()));
    }

    let segs: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(decode)
        .collect();
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    if let Some(answer) = authenticate(state, &method, &segs, authorization.as_deref(), &raw) {
        return respond(conn, answer.0, &answer.1);
    }
    let (code, answer) = match (method.as_str(), segs.as_slice()) {
        ("GET", ["v1", "config"]) => {
            let st = state.lock().unwrap();
            let asked = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("warehouse="))
                .map(decode);
            match (&st.warehouses, asked) {
                (None, _) => (
                    200,
                    json!({ "defaults": {}, "overrides": { "prefix": "p1" } }),
                ),
                (Some(_), None) => (400, error("BadRequestException", 400)),
                (Some(known), Some(w)) if known.contains(&w) => (
                    200,
                    json!({ "defaults": {}, "overrides": { "prefix": "p1" } }),
                ),
                (Some(_), Some(_)) => (404, error("NoSuchWarehouseException", 404)),
            }
        }
        ("GET", ["v1", "p1", "namespaces", ns, "tables", name]) => {
            let st = state.lock().unwrap();
            let key = (ns.to_string(), name.to_string());
            match (st.tables.get(&key), st.registered.get(&key)) {
                (Some(m), Some(at)) => (200, json!({ "metadata-location": at, "metadata": m })),
                (Some(m), None) => (200, loaded(m, st.metadata_files)),
                (None, _) => (404, error("NoSuchTableException", 404)),
            }
        }
        ("POST", ["v1", "p1", "namespaces", ns, "register"]) => register(state, ns, &body),
        ("POST", ["v1", "p1", "namespaces", ns, "tables"]) => {
            let fault = state.lock().unwrap().create_faults.pop_front();
            match fault {
                Some(Fault::Fail500) => (500, error("ServiceFailureException", 500)),
                Some(Fault::ApplyThen500) => {
                    create(state, ns, &body);
                    (500, error("ServiceFailureException", 500))
                }
                _ => create(state, ns, &body),
            }
        }
        ("POST", ["v1", "p1", "namespaces", ns, "tables", name]) => {
            let fault = state
                .lock()
                .unwrap()
                .faults
                .pop_front()
                .unwrap_or(Fault::None);
            let key = (ns.to_string(), name.to_string());
            match fault {
                Fault::Refuse => (409, error("CommitFailedException", 409)),
                Fault::Reject400 => (400, error("BadRequestException", 400)),
                Fault::ApplyThen409 => {
                    commit(state, &key, &body);
                    (409, error("CommitFailedException", 409))
                }
                Fault::MoveThenRefuse => {
                    let mut st = state.lock().unwrap();
                    foreign(st.tables.get_mut(&key).unwrap(), 43);
                    (409, error("CommitFailedException", 409))
                }
                Fault::RunLandsThenRefuse(run) => {
                    let mut st = state.lock().unwrap();
                    foreign_run(st.tables.get_mut(&key).unwrap(), 45, Some(run));
                    (409, error("CommitFailedException", 409))
                }
                Fault::ChangeSchemaThenRefuse => {
                    let mut st = state.lock().unwrap();
                    let meta = st.tables.get_mut(&key).unwrap();
                    let id = meta["last-column-id"].as_i64().unwrap() + 1;
                    let schema_id = meta["schemas"].as_array().unwrap().len();
                    let mut schema = meta["schemas"].as_array().unwrap().last().unwrap().clone();
                    schema["schema-id"] = json!(schema_id);
                    schema["fields"].as_array_mut().unwrap().push(
                        json!({ "id": id, "name": "added_elsewhere", "required": false,
                                "type": "string" }),
                    );
                    meta["schemas"].as_array_mut().unwrap().push(schema);
                    meta["current-schema-id"] = json!(schema_id);
                    meta["last-column-id"] = json!(id);
                    foreign(meta, 44);
                    (409, error("CommitFailedException", 409))
                }
                Fault::Fail500 => (500, error("ServiceFailureException", 500)),
                Fault::MoveThen500 => {
                    let mut st = state.lock().unwrap();
                    foreign(st.tables.get_mut(&key).unwrap(), 42);
                    (500, error("ServiceFailureException", 500))
                }
                Fault::ApplyThen500 => {
                    commit(state, &key, &body);
                    (500, error("ServiceFailureException", 500))
                }
                Fault::ApplyThenDrop => {
                    commit(state, &key, &body);
                    return; // drop the connection unanswered
                }
                Fault::LandLate(d) => {
                    std::thread::sleep(d);
                    commit(state, &key, &body);
                    return;
                }
                Fault::Redirect303 => {
                    let mut out = conn;
                    let _ = write!(
                        out,
                        "HTTP/1.1 303 See Other\r\nLocation: {path}\r\nContent-Length: 0\r\n\
                         Connection: close\r\n\r\n"
                    );
                    return;
                }
                Fault::ApplyMoveThen500 => {
                    commit(state, &key, &body);
                    let mut st = state.lock().unwrap();
                    foreign(st.tables.get_mut(&key).unwrap(), 46);
                    st.metadata_files += 1;
                    (500, error("ServiceFailureException", 500))
                }
                Fault::Then(change) => {
                    change(state.lock().unwrap().tables.get_mut(&key).unwrap());
                    commit(state, &key, &body)
                }
                Fault::None => commit(state, &key, &body),
            }
        }
        _ => (404, error("NotFound", 404)),
    };
    respond(conn, code, &answer)
}

fn respond(conn: TcpStream, code: u16, answer: &Value) {
    let text = answer.to_string();
    let mut out = conn;
    let _ = write!(
        out,
        "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{text}",
        text.len()
    );
}

/// The answer to a request the stand-in's [`AuthRule`] decides: a token issued (or refused)
/// at the token endpoint, or a 401 for a request without a token it takes. `None` lets the
/// request through.
fn authenticate(
    state: &Mutex<State>,
    method: &str,
    segs: &[&str],
    header: Option<&str>,
    raw: &[u8],
) -> Option<(u16, Value)> {
    let mut st = state.lock().unwrap();
    let token_endpoint = st.auth.as_ref()?.token_path.as_slice() == segs;
    if method == "POST" && token_endpoint {
        let form = String::from_utf8_lossy(raw).into_owned();
        st.token_requests.push(form.clone());
        let fields: HashMap<String, String> = form
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.to_string(), decode(v)))
            .collect();
        let rule = st.auth.as_mut().unwrap();
        let field = |k: &str| fields.get(k).map(String::as_str);
        let known = rule.client.as_ref().is_some_and(|(id, secret, scope)| {
            field("grant_type") == Some("client_credentials")
                && field("client_id") == Some(id.as_str())
                && field("client_secret") == Some(secret.as_str())
                && field("scope") == Some(scope.as_str())
        });
        if !known {
            return Some((
                401,
                json!({ "error": "invalid_client", "error_description": "stand-in: no such client" }),
            ));
        }
        let token = format!("issued-{}", rule.issued.len() + 1);
        rule.issued
            .push((token.clone(), std::time::Instant::now() + rule.lifetime));
        return Some((
            200,
            json!({ "access_token": token, "token_type": "bearer",
                    "expires_in": rule.lifetime.as_secs(),
                    "issued_token_type": "urn:ietf:params:oauth:token-type:access_token" }),
        ));
    }
    if st.auth.as_ref()?.accepts(header) {
        None
    } else {
        Some((401, error("NotAuthorizedException", 401)))
    }
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn error(kind: &str, code: u16) -> Value {
    json!({ "error": { "message": format!("stand-in {kind}"), "type": kind, "code": code } })
}

fn loaded(meta: &Value, n: u32) -> Value {
    json!({ "metadata-location": format!("{}/metadata/{n:05}.metadata.json",
                                         meta["location"].as_str().unwrap()),
            "metadata": meta })
}

fn create(state: &Mutex<State>, ns: &str, req: &Value) -> (u16, Value) {
    let mut st = state.lock().unwrap();
    if !st.namespaces.iter().any(|n| n == ns) {
        return (404, error("NoSuchNamespaceException", 404));
    }
    let name = req["name"].as_str().unwrap().to_string();
    if st.tables.contains_key(&(ns.to_string(), name.clone())) {
        return (409, error("AlreadyExistsException", 409));
    }
    let last_col = req["schema"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_i64().unwrap())
        .max()
        .unwrap();
    let location = format!("{}/{}/{name}", st.warehouse, ns.replace('\u{1f}', "/"));
    let mut spec = req["partition-spec"].clone();
    spec["spec-id"] = json!(0);
    let meta = json!({
        "format-version": 2, "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": location, "last-sequence-number": 0, "last-updated-ms": 0,
        "last-column-id": last_col, "current-schema-id": 0, "schemas": [req["schema"]],
        "default-spec-id": 0, "partition-specs": [spec],
        "last-partition-id": spec["fields"].as_array().unwrap().iter()
            .map(|f| f["field-id"].as_i64().unwrap()).max().unwrap_or(999),
        "default-sort-order-id": 0, "sort-orders": [{ "order-id": 0, "fields": [] }],
        "properties": {}, "current-snapshot-id": -1, "refs": {}, "snapshots": [],
        "snapshot-log": [], "metadata-log": [],
    });
    st.tables.insert((ns.to_string(), name), meta.clone());
    (200, loaded(&meta, 0))
}

/// Check `body`'s requirements against the table and apply its updates.
fn commit(state: &Mutex<State>, key: &(String, String), body: &Value) -> (u16, Value) {
    let mut st = state.lock().unwrap();
    // The metadata file this commit replaces: where it was registered, or the stand-in's own.
    let previous = match st.registered.get(key) {
        Some(at) => at.clone(),
        None => loaded(&st.tables[key], st.metadata_files)["metadata-location"]
            .as_str()
            .unwrap()
            .to_string(),
    };
    let meta = st.tables.get_mut(key).unwrap();
    for r in body["requirements"].as_array().unwrap() {
        let ok = match r["type"].as_str().unwrap() {
            "assert-table-uuid" => r["uuid"] == meta["table-uuid"],
            "assert-ref-snapshot-id" => {
                r["snapshot-id"].as_i64() == ref_of(meta, r["ref"].as_str().unwrap())
            }
            "assert-current-schema-id" => r["current-schema-id"] == meta["current-schema-id"],
            "assert-last-assigned-field-id" => {
                r["last-assigned-field-id"] == meta["last-column-id"]
            }
            other => panic!("requirement {other} not expected"),
        };
        if !ok {
            return (409, error("CommitFailedException", 409));
        }
    }
    // Applied to a copy, so a refused update leaves the table as it was.
    let mut next = meta.clone();
    for u in body["updates"].as_array().unwrap() {
        match u["action"].as_str().unwrap() {
            "add-schema" => {
                next["schemas"]
                    .as_array_mut()
                    .unwrap()
                    .push(u["schema"].clone());
                next["last-column-id"] = u["last-column-id"].clone();
            }
            "set-current-schema" => {
                assert_eq!(u["schema-id"], -1);
                let last = next["schemas"].as_array().unwrap().last().unwrap()["schema-id"].clone();
                next["current-schema-id"] = last;
            }
            // Only what a catalog checks: a v2 snapshot's sequence number must be new. (Its
            // parent is not checked here, as a catalog does not; the tests read it back.)
            "add-snapshot" => {
                let snap = &u["snapshot"];
                let seq = snap["sequence-number"].as_i64().unwrap();
                if seq <= next["last-sequence-number"].as_i64().unwrap() {
                    return (400, error("BadRequestException", 400));
                }
                next["snapshots"].as_array_mut().unwrap().push(snap.clone());
                next["last-sequence-number"] = json!(seq);
            }
            "set-snapshot-ref" => {
                assert_eq!(
                    (&u["ref-name"], &u["type"]),
                    (&json!("main"), &json!("branch"))
                );
                next["refs"]["main"] = json!({ "snapshot-id": u["snapshot-id"], "type": "branch" });
                next["current-snapshot-id"] = u["snapshot-id"].clone();
            }
            // As the Java catalog applies it: the snapshots go, with any ref left pointing at
            // one of them and their snapshot-log entries. Expiring `main`'s own is refused.
            "remove-snapshots" => {
                let ids: Vec<i64> = u["snapshot-ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| i.as_i64().unwrap())
                    .collect();
                if ids.iter().any(|id| Some(*id) == main_snapshot(&next)) {
                    return (400, error("BadRequestException", 400));
                }
                let gone = |v: &Value| ids.iter().any(|id| v["snapshot-id"] == *id);
                next["snapshots"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|s| !gone(s));
                next["refs"]
                    .as_object_mut()
                    .unwrap()
                    .retain(|_, r| !gone(r));
                if let Some(log) = next["snapshot-log"].as_array_mut() {
                    log.retain(|e| !gone(e));
                }
            }
            other => panic!("update {other} not expected"),
        }
    }
    // As the Java catalog does, the file this commit replaces joins the metadata log.
    next["metadata-log"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "timestamp-ms": 0, "metadata-file": previous }));
    *meta = next.clone();
    let meta = next;
    st.registered.remove(key);
    st.metadata_files += 1;
    (200, loaded(&meta, st.metadata_files))
}

/// Register a table as the Java catalog does: the name must be free in an existing
/// namespace; the metadata file is read where it is named.
fn register(state: &Mutex<State>, ns: &str, req: &Value) -> (u16, Value) {
    let mut st = state.lock().unwrap();
    if !st.namespaces.iter().any(|n| n == ns) {
        return (404, error("NoSuchNamespaceException", 404));
    }
    assert!(
        req.get("overwrite").is_none() || req["overwrite"] == false,
        "{req}"
    );
    let key = (ns.to_string(), req["name"].as_str().unwrap().to_string());
    if st.tables.contains_key(&key) {
        return (409, error("AlreadyExistsException", 409));
    }
    let at = req["metadata-location"].as_str().unwrap().to_string();
    let path = at
        .strip_prefix("file://")
        .or_else(|| at.strip_prefix("file:"))
        .unwrap_or(&at);
    let meta: Value = match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap(),
        Err(_) => return (400, error("NotFoundException", 400)),
    };
    st.tables.insert(key.clone(), meta.clone());
    st.registered.insert(key, at.clone());
    (200, json!({ "metadata-location": at, "metadata": meta }))
}

/// The snapshot the ref `name` points at.
fn ref_of(meta: &Value, name: &str) -> Option<i64> {
    if name == "main" {
        main_snapshot(meta)
    } else {
        meta["refs"][name]["snapshot-id"].as_i64()
    }
}

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-catalog-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Write `ids` (and, with `channel`, a column the table may not have yet) to `table`.
fn write(
    cat: &StandIn,
    table: &str,
    append: bool,
    ids: std::ops::Range<i64>,
    channel: bool,
    options: CatalogOptions,
) -> shapeshift_core::Result<SinkSummary> {
    write_run(cat, table, append, ids, channel, None, options)
}

/// [`write`] as an attempt of run `run`.
fn write_as(
    cat: &StandIn,
    table: &str,
    ids: std::ops::Range<i64>,
    run: &str,
    options: CatalogOptions,
) -> shapeshift_core::Result<SinkSummary> {
    write_run(cat, table, true, ids, false, Some(run), options)
}

fn write_run(
    cat: &StandIn,
    table: &str,
    append: bool,
    ids: std::ops::Range<i64>,
    channel: bool,
    run: Option<&str>,
    options: CatalogOptions,
) -> shapeshift_core::Result<SinkSummary> {
    let mut sink = open_with(cat, table, append, ids, channel, run, options)?;
    sink.finish()
}

/// A sink opened on `table` (the table loaded or created now) with `ids` written but not
/// committed.
fn open_with(
    cat: &StandIn,
    table: &str,
    append: bool,
    ids: std::ops::Range<i64>,
    channel: bool,
    run: Option<&str>,
    options: CatalogOptions,
) -> shapeshift_core::Result<ObjectStoreIcebergSink> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ];
    if channel {
        fields.push(Field::new("channel", DataType::Utf8, true));
    }
    let schema = Arc::new(Schema::new(fields));
    let region: Vec<&str> = ids.clone().map(|i| ["eu", "us"][i as usize % 2]).collect();
    let mut cols: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(Int64Array::from(ids.clone().collect::<Vec<_>>())),
        Arc::new(StringArray::from(region)),
    ];
    if channel {
        cols.push(Arc::new(StringArray::from(vec![
            "web";
            ids.clone().count()
        ])));
    }
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let mut sink = ObjectStoreIcebergSink::open_in_catalog(
        &cat.conn(),
        table,
        schema,
        Compression::Snappy,
        append,
        &["region".to_string()],
        &[],
        options,
    )?;
    if let Some(run) = run {
        sink.set_run_id(run)?;
    }
    sink.write_batch(&batch)?;
    Ok(sink)
}

/// The snapshot `id` in a table's metadata.
fn snapshot(meta: &Value, id: i64) -> Value {
    meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["snapshot-id"] == id)
        .unwrap()
        .clone()
}

/// Every file under `dir`.
fn files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(files(&p));
            } else {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// The live rows of the snapshot `main` points at, read through the manifests it names;
/// every file it names must be there.
fn live_rows(meta: &Value) -> i64 {
    let main = main_snapshot(meta).unwrap();
    let snap = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["snapshot-id"] == main)
        .unwrap();
    let local = |uri: &str| PathBuf::from(uri.strip_prefix("file:").unwrap());
    let list = std::fs::read(local(snap["manifest-list"].as_str().unwrap())).unwrap();
    let mut rows = 0;
    for m in read_manifest_list(&list).unwrap() {
        let bytes = std::fs::read(local(&m.path)).unwrap();
        for f in shapeshift_iceberg::read_manifest_data_files(&bytes).unwrap() {
            if f.status == 2 {
                continue; // deleted
            }
            assert!(local(&f.file_path).exists(), "{} is missing", f.file_path);
            rows += f.record_count;
        }
    }
    rows
}

/// A catalog call may take 10 s; a refused append is rebuilt at most 3 times, and an
/// unanswered commit sent again at most twice.
const QUICK: CatalogOptions = CatalogOptions {
    timeout: Duration::from_secs(10),
    max_rebases: 3,
    max_resends: 2,
};

/// A write of five rows over two regions adds exactly this many files, however many times
/// it was rebuilt: two data files, one manifest, one manifest list.
const FILES_PER_WRITE: usize = 4;

#[test]
fn a_catalog_write_creates_the_table_then_appends_onto_it_through_the_catalog() {
    let wh = tmp("append");
    let cat = StandIn::start(&wh, &["lake.raw"]);

    // First write: no table, so it is created empty (in the multi-level namespace), then
    // the snapshot is committed onto it asserting `main` does not exist yet.
    let first = write(&cat, "lake.raw.events", false, 0..10, false, QUICK).unwrap();
    let id1 = first.snapshot_id.unwrap();
    assert_eq!(
        first.table_version, None,
        "the catalog numbers its metadata files"
    );
    assert!(first.files[0]
        .to_string_lossy()
        .ends_with("00001.metadata.json"));
    {
        let st = cat.state.lock().unwrap();
        let paths: Vec<_> = st
            .requests
            .iter()
            .map(|(m, p, _)| format!("{m} {p}"))
            .collect();
        assert_eq!(
            paths,
            [
                "GET /v1/config",
                "GET /v1/p1/namespaces/lake%1Fraw/tables/events",
                "POST /v1/p1/namespaces/lake%1Fraw/tables",
                "POST /v1/p1/namespaces/lake%1Fraw/tables/events",
            ]
        );
    }
    let meta = cat.table("lake.raw", "events");
    let body = &cat.commits()[0];
    assert_eq!(
        body["requirements"],
        json!([
            { "type": "assert-table-uuid", "uuid": meta["table-uuid"] },
            { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null },
        ])
    );
    let actions: Vec<_> = body["updates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["action"].clone())
        .collect();
    assert_eq!(actions, [json!("add-snapshot"), json!("set-snapshot-ref")]);
    assert_eq!(main_snapshot(&meta), Some(id1));
    // The table was created empty (`current-snapshot-id: -1`): the first snapshot has no
    // parent, not a parent of -1.
    assert_eq!(meta["snapshots"][0].get("parent-snapshot-id"), None);
    assert_eq!(live_rows(&meta), 10);

    // A table holding data is written to only with --append.
    let before = files(&wh);
    let err = write(&cat, "lake.raw.events", false, 10..20, false, QUICK).unwrap_err();
    assert!(err.to_string().contains("--append"), "{err}");
    assert_eq!(files(&wh), before, "a refused open stages nothing");

    // An append chains onto `main` and asserts it; a new column adds a schema, asserting
    // the schema and the last field id it was evolved from.
    let second = write(&cat, "lake.raw.events", true, 10..20, true, QUICK).unwrap();
    let meta = cat.table("lake.raw", "events");
    let body = cat.commits().pop().unwrap();
    assert_eq!(body["requirements"][1]["snapshot-id"], id1);
    let types: Vec<_> = body["requirements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["type"].clone())
        .collect();
    assert_eq!(
        types,
        [
            json!("assert-table-uuid"),
            json!("assert-ref-snapshot-id"),
            json!("assert-current-schema-id"),
            json!("assert-last-assigned-field-id"),
        ]
    );
    let actions: Vec<_> = body["updates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["action"].clone())
        .collect();
    assert_eq!(
        actions,
        [
            json!("add-schema"),
            json!("set-current-schema"),
            json!("add-snapshot"),
            json!("set-snapshot-ref")
        ]
    );
    assert_eq!(meta["current-schema-id"], 1);
    assert_eq!(meta["last-column-id"], 3);
    let snap = meta["snapshots"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(
        (
            snap["parent-snapshot-id"].as_i64(),
            snap["sequence-number"].as_i64()
        ),
        (Some(id1), Some(2))
    );
    assert_eq!(main_snapshot(&meta), second.snapshot_id);
    assert_eq!(live_rows(&meta), 20);

    // A namespace the catalog lacks is reported, not created.
    let err = write(&cat, "nowhere.events", false, 0..1, false, QUICK).unwrap_err();
    assert!(err.to_string().contains("no namespace `nowhere`"), "{err}");
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn a_commit_is_settled_by_what_the_catalog_holds_and_files_go_only_with_a_refusal() {
    use Fault::*;
    let wh = tmp("outcomes");
    let cat = StandIn::start(&wh, &["lake"]);
    write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    let dir = table_dir(&cat, "lake", "t");
    let mut rows = 10;

    // Each case: the faults met by the commit and the copies of it sent again, whether the
    // write succeeds, and whether its files stay. None of these is a conflict, so none is
    // rebuilt.
    let cases: [(&[Fault], bool, bool); 7] = [
        (&[ApplyThen500], true, true),
        (&[ApplyThenDrop], true, true),
        // Unanswered and not taken: sent again, and the copy commits.
        (&[Fail500], true, true),
        (&[Fail500, Fail500], true, true),
        // The copy is refused because the first attempt landed after all: committed, once.
        (&[Fail500, ApplyThen409], true, true),
        // Unanswered past the resends: unknown, and the files stay.
        (&[Fail500, Fail500, Fail500], false, true),
        // Malformed: refused for good, not sent again, and the files go.
        (&[Reject400], false, false),
    ];
    for (faults, ok, kept) in cases {
        let before = files(&dir);
        for &f in faults {
            cat.fault_next_commit(f);
        }
        let result = write(&cat, "lake.t", true, rows..rows + 5, false, QUICK);
        let added = files(&dir).len() - before.len();
        assert_eq!(result.is_ok(), ok, "{faults:?}: {result:?}");
        match &result {
            Ok(summary) => {
                rows += 5;
                assert_eq!(summary.rebases, 0, "{faults:?}");
                let meta = cat.table("lake", "t");
                assert_eq!(main_snapshot(&meta), summary.snapshot_id, "{faults:?}");
            }
            Err(e) => assert!(!matches!(e, ShapeError::Conflict(_)), "{faults:?}: {e}"),
        }
        let expected = if kept { FILES_PER_WRITE } else { 0 };
        assert_eq!(added, expected, "{faults:?}: files added");
        assert_eq!(live_rows(&cat.table("lake", "t")), rows, "{faults:?}");
        assert!(
            cat.state.lock().unwrap().faults.is_empty(),
            "{faults:?}: every fault met"
        );
    }

    // An answer that comes too late, sent again: the copy commits, and the late original,
    // asserting a head that has moved, can no longer apply — the rows land once.
    let before = files(&dir);
    cat.fault_next_commit(LandLate(Duration::from_millis(800)));
    let impatient = CatalogOptions {
        timeout: Duration::from_millis(300),
        ..QUICK
    };
    let summary = write(&cat, "lake.t", true, rows..rows + 5, false, impatient).unwrap();
    rows += 5;
    assert_eq!(files(&dir).len() - before.len(), FILES_PER_WRITE);
    std::thread::sleep(Duration::from_millis(1200));
    let meta = cat.table("lake", "t");
    assert_eq!(main_snapshot(&meta), summary.snapshot_id);
    assert_eq!(
        live_rows(&meta),
        rows,
        "the late original did not apply again"
    );

    // The same, with no copy sent: unknown (not a conflict, so not retried blindly), and the
    // files stay — the commit lands after the client gave up, and reads them.
    let before = files(&dir);
    cat.fault_next_commit(LandLate(Duration::from_millis(1500)));
    let silent = CatalogOptions {
        max_resends: 0,
        ..impatient
    };
    let err = write(&cat, "lake.t", true, rows..rows + 5, false, silent).unwrap_err();
    assert!(
        !matches!(err, ShapeError::Conflict(_)) && err.to_string().contains("may or may not"),
        "{err}"
    );
    assert_eq!(files(&dir).len() - before.len(), FILES_PER_WRITE);
    let landed = (0..100).any(|_| {
        std::thread::sleep(Duration::from_millis(50));
        live_rows(&cat.table("lake", "t")) == rows + 5
    });
    assert!(
        landed,
        "the late commit landed and every file it names is there"
    );
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn an_append_another_writer_beat_is_rebuilt_onto_the_new_head() {
    let wh = tmp("rebase");
    let cat = StandIn::start(&wh, &["lake"]);
    write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    let dir = table_dir(&cat, "lake", "t");
    let mut rows = 10;

    // Each refusal another writer caused is rebuilt onto the head it left, and committed:
    // the snapshot's parent is that head, and the write leaves one attempt's files.
    // Each case: the fault, the head the write is rebuilt onto (`None`: the same one), and
    // the requests from its commit on — a 409 is followed by the load the rebuild needs; a
    // 5xx is first settled by a load, then the rebuild loads the head it builds on.
    let cases = [
        (Fault::Refuse, None, &["POST", "GET", "POST"][..]),
        (
            Fault::MoveThenRefuse,
            Some(43),
            &["POST", "GET", "POST"][..],
        ),
        (
            Fault::MoveThen500,
            Some(42),
            &["POST", "GET", "GET", "POST"][..],
        ),
    ];
    for (fault, parent, sequence) in cases {
        let before = files(&dir);
        let head = main_snapshot(&cat.table("lake", "t"));
        let requests = cat.state.lock().unwrap().requests.len();
        cat.fault_next_commit(fault);
        let summary = write(&cat, "lake.t", true, rows..rows + 5, false, QUICK).unwrap();
        rows += 5;
        assert_eq!(summary.rebases, 1, "{fault:?}");
        let meta = cat.table("lake", "t");
        assert_eq!(main_snapshot(&meta), summary.snapshot_id, "{fault:?}");
        let snap = meta["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        assert_eq!(
            snap["parent-snapshot-id"].as_i64(),
            parent.or(head),
            "{fault:?}"
        );
        assert_eq!(
            files(&dir).len() - before.len(),
            FILES_PER_WRITE,
            "{fault:?}"
        );
        assert_eq!(live_rows(&meta), rows, "{fault:?}");
        // The table was loaded again before the second commit, and every request after the
        // first commit was to the table.
        let st = cat.state.lock().unwrap();
        let sent: Vec<_> = st.requests[requests..]
            .iter()
            .map(|(m, p, _)| (m.as_str(), p.rsplit('/').next().unwrap()))
            .collect();
        let first_commit = sent.iter().position(|&r| r == ("POST", "t")).unwrap();
        let after: Vec<_> = sent[first_commit..].iter().map(|&(m, _)| m).collect();
        assert_eq!(after, sequence, "{fault:?}: {sent:?}");
        assert!(
            sent[first_commit..].iter().all(|&(_, p)| p == "t"),
            "{sent:?}"
        );
    }

    // Another writer commits between this one's load and its commit.
    let before = files(&dir);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let mut sink = ObjectStoreIcebergSink::open_in_catalog(
        &cat.conn(),
        "lake.t",
        schema.clone(),
        Compression::Snappy,
        true,
        &["region".to_string()],
        &[],
        QUICK,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_000, 1_001])),
            Arc::new(StringArray::from(vec!["eu", "us"])),
        ],
    )
    .unwrap();
    sink.write_batch(&batch).unwrap();
    cat.commit_foreign("lake", "t", 77);
    let summary = sink.finish().unwrap();
    rows += 2;
    let meta = cat.table("lake", "t");
    let snap = meta["snapshots"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(
        (summary.rebases, snap["parent-snapshot-id"].as_i64()),
        (1, Some(77))
    );
    assert_eq!(files(&dir).len() - before.len(), FILES_PER_WRITE);
    assert_eq!(live_rows(&meta), rows);

    // Refused past the bound: a conflict that says so, and nothing left behind.
    let before = files(&dir);
    for _ in 0..=QUICK.max_rebases {
        cat.fault_next_commit(Fault::Refuse);
    }
    let err = write(&cat, "lake.t", true, rows..rows + 5, false, QUICK).unwrap_err();
    assert!(matches!(err, ShapeError::Conflict(_)), "{err}");
    assert!(
        err.to_string().contains("rebuilt onto 3 newer snapshot(s)"),
        "{err}"
    );
    assert_eq!(files(&dir), before, "every attempt's files are removed");

    // Another writer changed the schema the files were written for: not rebuilt onto.
    cat.fault_next_commit(Fault::ChangeSchemaThenRefuse);
    let err = write(&cat, "lake.t", true, rows..rows + 5, false, QUICK).unwrap_err();
    assert!(matches!(err, ShapeError::Conflict(_)), "{err}");
    assert!(err.to_string().contains("its schema changed"), "{err}");
    assert_eq!(files(&dir), before, "the files are removed");
    assert_eq!(live_rows(&cat.table("lake", "t")), rows);
    std::fs::remove_dir_all(&wh).ok();
}

/// The local directory of a stand-in table.
fn table_dir(cat: &StandIn, ns: &str, name: &str) -> PathBuf {
    let loc = cat.table(ns, name)["location"]
        .as_str()
        .unwrap()
        .to_string();
    PathBuf::from(loc.strip_prefix("file:").unwrap())
}

#[test]
fn a_create_whose_answer_was_lost_is_settled_by_loading_the_table() {
    let wh = tmp("create");
    let cat = StandIn::start(&wh, &["lake"]);
    // Created, but answered 500: the table is there, so the write goes to it.
    cat.state
        .lock()
        .unwrap()
        .create_faults
        .push_back(Fault::ApplyThen500);
    let summary = write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    let meta = cat.table("lake", "t");
    assert_eq!(main_snapshot(&meta), summary.snapshot_id);
    assert_eq!(live_rows(&meta), 10);
    // Not created, and answered 500: nothing to write to, and the error says why.
    cat.state
        .lock()
        .unwrap()
        .create_faults
        .push_back(Fault::Fail500);
    let err = write(&cat, "lake.u", false, 0..10, false, QUICK).unwrap_err();
    assert!(
        err.to_string().contains("could not be created") && err.to_string().contains("500"),
        "{err}"
    );
    assert!(files(&wh.join("lake"))
        .iter()
        .all(|f| !f.starts_with(wh.join("lake/u"))));
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn a_catalog_commit_records_its_run_and_an_earlier_attempt_is_not_appended_again() {
    let wh = tmp("run");
    let cat = StandIn::start(&wh, &["lake"]);

    // A commit carries its run id and the engine version; it did not rebase.
    let first = write_as(&cat, "lake.t", 0..5, "r-1", QUICK).unwrap();
    let head = snapshot(&cat.table("lake", "t"), first.snapshot_id.unwrap());
    assert_eq!(head["summary"][RUN_ID_KEY], "r-1");
    assert_eq!(
        head["summary"]["shapeshift.version"],
        env!("CARGO_PKG_VERSION")
    );
    assert!(head["summary"].get("shapeshift.rebases").is_none());
    let found = find_run_in_catalog(&cat.conn(), "lake.t", "r-1")
        .unwrap()
        .unwrap();
    assert_eq!(Some(found.snapshot_id), first.snapshot_id);
    assert_eq!(
        find_run_in_catalog(&cat.conn(), "lake.t", "r-2").unwrap(),
        None
    );
    assert_eq!(
        find_run_in_catalog(&cat.conn(), "lake.gone", "r-1").unwrap(),
        None
    );

    // An attempt that begins once the run has committed finds it in the table it loaded,
    // and sends no commit.
    let (before, sent) = (files(&wh), cat.commits().len());
    let late = write_as(&cat, "lake.t", 0..5, "r-1", QUICK).unwrap();
    assert!(late.earlier_attempt);
    assert_eq!(late.snapshot_id, first.snapshot_id);
    assert_eq!((cat.commits().len(), files(&wh)), (sent, before));

    // An earlier attempt of r-2 lands while this attempt runs: this one is refused, finds
    // the run in the table, and reports that snapshot. It is not rebuilt and sent again,
    // and nothing of it is left.
    let before = files(&wh);
    let sent = cat.commits().len();
    cat.fault_next_commit(Fault::RunLandsThenRefuse("r-2"));
    let lost = write_as(&cat, "lake.t", 5..10, "r-2", QUICK).unwrap();
    assert!(lost.earlier_attempt);
    assert_eq!((lost.snapshot_id, lost.rebases), (Some(45), 0));
    assert_eq!(
        (lost.rows, lost.bytes),
        (0, 0),
        "this attempt wrote nothing"
    );
    assert!(lost.files.is_empty());
    assert_eq!(cat.commits().len(), sent + 1, "one commit, not rebased");
    assert_eq!(files(&wh), before, "none of its files were left");

    // The run is looked for even when no rebuild is allowed...
    let no_rebases = CatalogOptions {
        max_rebases: 0,
        ..QUICK
    };
    cat.fault_next_commit(Fault::RunLandsThenRefuse("r-3"));
    let lost = write_as(&cat, "lake.t", 10..15, "r-3", no_rebases).unwrap();
    assert!(lost.earlier_attempt);
    // r-3's attempt moved `main` again, to a snapshot of its own (not r-2's).
    assert_eq!(lost.snapshot_id, Some(1045));
    assert_eq!(main_snapshot(&cat.table("lake", "t")), Some(1045));
    assert_eq!(files(&wh), before);
    // ...and another run's commit is still a conflict there, with nothing left behind.
    let wh2 = tmp("run-other");
    let other = StandIn::start(&wh2, &["lake"]);
    write_as(&other, "lake.u", 0..5, "r-1", QUICK).unwrap();
    let before = files(&wh2);
    other.fault_next_commit(Fault::MoveThenRefuse);
    let err = write_as(&other, "lake.u", 5..10, "r-4", no_rebases).unwrap_err();
    assert!(matches!(err, ShapeError::Conflict(_)), "{err}");
    assert_eq!(files(&wh2), before);

    // An append rebuilt onto another writer's commit records how many times.
    other.fault_next_commit(Fault::MoveThenRefuse);
    let rebased = write_as(&other, "lake.u", 10..15, "r-5", QUICK).unwrap();
    assert_eq!((rebased.rebases, rebased.earlier_attempt), (1, false));
    let head = snapshot(&other.table("lake", "u"), rebased.snapshot_id.unwrap());
    assert_eq!(head["summary"][RUN_ID_KEY], "r-5");
    assert_eq!(head["summary"]["shapeshift.rebases"], "1");
    let _ = std::fs::remove_dir_all(&wh);
    let _ = std::fs::remove_dir_all(&wh2);
}

#[test]
fn a_commit_answered_with_a_redirect_is_not_followed_and_does_not_count_as_committed() {
    let wh = tmp("redirect");
    let cat = StandIn::start(&wh, &["lake"]);
    write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    let dir = table_dir(&cat, "lake", "t");
    let (head, before, asked) = (
        main_snapshot(&cat.table("lake", "t")),
        files(&dir),
        cat.state.lock().unwrap().requests.len(),
    );
    // Followed, the 303 would turn the commit into a GET of the table, whose 200 would read
    // as a commit that never happened.
    cat.fault_next_commit(Fault::Redirect303);
    let err = write(&cat, "lake.t", true, 10..15, false, QUICK).unwrap_err();
    assert!(
        err.to_string().contains("redirect (303 See Other to /v1/"),
        "{err}"
    );
    assert!(!matches!(err, ShapeError::Conflict(_)), "{err}");
    assert_eq!(
        main_snapshot(&cat.table("lake", "t")),
        head,
        "nothing committed"
    );
    assert_eq!(files(&dir), before, "and nothing left behind");
    let after: Vec<String> = cat.state.lock().unwrap().requests[asked..]
        .iter()
        .map(|(m, _, _)| m.clone())
        .collect();
    assert_eq!(
        after,
        ["GET", "GET", "POST"],
        "config, load, the commit: no GET after it"
    );
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn a_write_without_append_that_another_writer_beat_is_not_rebuilt_onto_their_rows() {
    let wh = tmp("no-append");
    let cat = StandIn::start(&wh, &["lake"]);
    // Two writers without --append open the same new table while it is empty; the first
    // commits. The second is refused, and must not add its rows to a table that now holds
    // data (a write with --append would be rebuilt onto it).
    let mut second = open_with(&cat, "lake.t", false, 100..105, false, None, QUICK).unwrap();
    let first = write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    let dir = table_dir(&cat, "lake", "t");
    let before = files(&dir);
    let err = second.finish().unwrap_err();
    assert!(matches!(err, ShapeError::Conflict(_)), "{err}");
    let meta = cat.table("lake", "t");
    assert_eq!(main_snapshot(&meta), first.snapshot_id);
    assert_eq!(live_rows(&meta), 10);
    assert_eq!(
        files(&dir),
        before,
        "none of the second write's files were left"
    );
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn a_commit_settled_by_loading_the_table_reports_only_a_metadata_file_it_wrote() {
    let wh = tmp("own-location");
    let cat = StandIn::start(&wh, &["lake"]);
    write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    // Applied, answer lost, and still the head: the table's current metadata file is ours.
    cat.fault_next_commit(Fault::ApplyThen500);
    let ours = write(&cat, "lake.t", true, 10..15, false, QUICK).unwrap();
    assert_eq!(ours.files.len(), 1, "{:?}", ours.files);
    // Applied, then another writer committed on top before the table was loaded again:
    // the current metadata file is theirs, so none is reported.
    cat.fault_next_commit(Fault::ApplyMoveThen500);
    let under = write(&cat, "lake.t", true, 15..20, false, QUICK).unwrap();
    assert!(under.files.is_empty(), "{:?}", under.files);
    let meta = cat.table("lake", "t");
    assert_ne!(main_snapshot(&meta), under.snapshot_id);
    assert_eq!(
        live_rows(&meta),
        20,
        "its rows are committed under the other writer's"
    );
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn a_manifest_list_outside_the_table_location_is_read_where_the_catalog_says() {
    let wh = tmp("metadata-path");
    let cat = StandIn::start(&wh, &["lake"]);
    write(&cat, "lake.t", false, 0..10, false, QUICK).unwrap();
    // The table's metadata lives apart from its data (as `write.metadata.path` makes it):
    // move the head's manifest list elsewhere in the same store, and name it there.
    let shared = wh.join("shared-metadata");
    std::fs::create_dir_all(&shared).unwrap();
    {
        let mut st = cat.state.lock().unwrap();
        let meta = st
            .tables
            .get_mut(&("lake".to_string(), "t".to_string()))
            .unwrap();
        let head = main_snapshot(meta).unwrap();
        let snap = meta["snapshots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|s| s["snapshot-id"] == head)
            .unwrap();
        let old = PathBuf::from(
            snap["manifest-list"]
                .as_str()
                .unwrap()
                .strip_prefix("file:")
                .unwrap(),
        );
        let new = shared.join(old.file_name().unwrap());
        std::fs::rename(&old, &new).unwrap();
        snap["manifest-list"] = json!(format!("file:{}", new.display()));
    }
    write(&cat, "lake.t", true, 10..15, false, QUICK).unwrap();
    assert_eq!(live_rows(&cat.table("lake", "t")), 15);
    // A manifest list in another store is refused, not read from this one.
    let elsewhere = json!({
        "current-snapshot-id": 1, "location": "s3://lake/t",
        "snapshots": [{ "snapshot-id": 1, "manifest-list": "s3://other/t/metadata/snap-1.avro" }],
    });
    let err =
        catalog_manifest_list_key(&elsewhere, "s3://lake/t", &ObjPath::from("t")).unwrap_err();
    assert!(err.to_string().contains("not in the store"), "{err}");
    assert_eq!(
        catalog_manifest_list_key(
            &json!({ "current-snapshot-id": 1, "snapshots": [
                { "snapshot-id": 1, "manifest-list": "s3://lake/meta/snap-1.avro" }] }),
            "s3://lake/t",
            &ObjPath::from("t"),
        )
        .unwrap(),
        ObjPath::from("meta/snap-1.avro")
    );
    std::fs::remove_dir_all(&wh).ok();
}

/// A merge sink on `table`, keyed on `id`, with `rows` (`(id, region, v)`) written but not
/// committed; an attempt of `run` when one is named.
fn merge_sink(
    cat: &StandIn,
    table: &str,
    rows: &[(i64, &str, i64)],
    run: Option<&str>,
) -> shapeshift_core::Result<ObjectStoreIcebergSink> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut sink = ObjectStoreIcebergSink::open_in_catalog(
        &cat.conn(),
        table,
        schema,
        Compression::Snappy,
        true,
        &["region".to_string()],
        &["id".to_string()],
        QUICK,
    )?;
    if let Some(run) = run {
        sink.set_run_id(run)?;
    }
    sink.write_batch(&batch)?;
    Ok(sink)
}

fn merge(
    cat: &StandIn,
    table: &str,
    rows: &[(i64, &str, i64)],
    run: Option<&str>,
) -> shapeshift_core::Result<SinkSummary> {
    merge_sink(cat, table, rows, run)?.finish()
}

fn parquet_batches(path: &Path) -> Vec<RecordBatch> {
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    .map(|b| b.unwrap())
    .collect()
}

/// The rows `main` reads, `(id, region, v)` sorted: its live data files, less the positions
/// its delete files remove. Every file it names must be there.
fn table_rows(meta: &Value) -> Vec<(i64, String, i64)> {
    let snap = snapshot(meta, main_snapshot(meta).unwrap());
    let local = |uri: &str| PathBuf::from(uri.strip_prefix("file:").unwrap());
    let list = std::fs::read(local(snap["manifest-list"].as_str().unwrap())).unwrap();
    let mut data = Vec::new();
    let mut deleted: HashMap<String, Vec<i64>> = HashMap::new();
    for m in read_manifest_list(&list).unwrap() {
        let bytes = std::fs::read(local(&m.path)).unwrap();
        for e in shapeshift_iceberg::read_manifest_entries(&bytes).unwrap() {
            if e.status == 2 {
                continue; // removed in this snapshot
            }
            if e.content == 0 {
                data.push(e.file_path);
                continue;
            }
            assert_eq!(e.content, 1, "shapeshift writes position deletes only");
            for b in parquet_batches(&local(&e.file_path)) {
                let col = |name: &str| b.column(b.schema().index_of(name).unwrap()).clone();
                let (path, pos) = (col("file_path"), col("pos"));
                let path = path.as_any().downcast_ref::<StringArray>().unwrap();
                let pos = pos.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    deleted
                        .entry(path.value(i).to_string())
                        .or_default()
                        .push(pos.value(i));
                }
            }
        }
    }
    let mut rows = Vec::new();
    for f in data {
        let gone = deleted.remove(&f).unwrap_or_default();
        let mut at = 0i64;
        for b in parquet_batches(&local(&f)) {
            let col = |name: &str| b.column(b.schema().index_of(name).unwrap()).clone();
            let (id, region, v) = (col("id"), col("region"), col("v"));
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            let region = region.as_any().downcast_ref::<StringArray>().unwrap();
            let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                if !gone.contains(&at) {
                    rows.push((id.value(i), region.value(i).to_string(), v.value(i)));
                }
                at += 1;
            }
        }
    }
    assert!(
        deleted.is_empty(),
        "a delete names no live file: {deleted:?}"
    );
    rows.sort();
    rows
}

fn rows(r: &[(i64, &str, i64)]) -> Vec<(i64, String, i64)> {
    r.iter().map(|&(i, g, v)| (i, g.to_string(), v)).collect()
}

/// The table holds three merges into both partitions: small files, and delete files.
fn merged_three_times(cat: &StandIn, table: &str) {
    merge(
        cat,
        table,
        &[(1, "eu", 1), (2, "us", 2), (3, "eu", 3)],
        None,
    )
    .unwrap();
    merge(
        cat,
        table,
        &[(1, "us", 10), (2, "us", 20), (4, "eu", 40)],
        None,
    )
    .unwrap();
    merge(cat, table, &[(3, "eu", 30), (5, "us", 50)], None).unwrap();
}

#[test]
fn a_merge_through_the_catalog_replaces_rows_and_is_never_rebuilt() {
    let wh = tmp("merge");
    let cat = StandIn::start(&wh, &["lake"]);

    // No table: the merge creates it, and finds nothing to replace.
    let first = merge(
        &cat,
        "lake.m",
        &[(1, "eu", 1), (2, "us", 2), (3, "eu", 3)],
        None,
    )
    .unwrap();
    assert_eq!(first.merge.as_ref().unwrap().keys_inserted, 3);
    // Id 1 moves eu → us, id 2 is updated, id 4 is new: the snapshot deletes the rows it
    // replaces, and asserts the head they were found in.
    let second = merge(
        &cat,
        "lake.m",
        &[(1, "us", 10), (2, "us", 20), (4, "eu", 40)],
        None,
    )
    .unwrap();
    let m = second.merge.as_ref().unwrap();
    assert_eq!(
        (m.keys_updated, m.keys_inserted, m.prior_rows_replaced),
        (2, 1, 2)
    );
    let meta = cat.table("lake", "m");
    assert_eq!(main_snapshot(&meta), second.snapshot_id);
    assert_eq!(second.table_version, None);
    assert_eq!(
        table_rows(&meta),
        rows(&[(1, "us", 10), (2, "us", 20), (3, "eu", 3), (4, "eu", 40)])
    );
    let body = cat.commits().pop().unwrap();
    assert_eq!(
        body["requirements"][1]["snapshot-id"].as_i64(),
        first.snapshot_id
    );
    assert_eq!(body["updates"][0]["action"], "add-snapshot");
    assert_eq!(
        body["updates"][0]["snapshot"]["summary"]["added-position-deletes"],
        "2"
    );

    // Another writer commits first: refused (exit 75), sent once and not rebuilt onto
    // their rows, and nothing of it left behind: data, delete and metadata files.
    let dir = table_dir(&cat, "lake", "m");
    let before = files(&dir);
    let commits = cat.commits().len();
    cat.fault_next_commit(Fault::MoveThenRefuse);
    let err = merge(&cat, "lake.m", &[(3, "eu", 30)], None).unwrap_err();
    assert!(matches!(err, ShapeError::Conflict(_)), "{err}");
    assert_eq!(cat.commits().len(), commits + 1, "sent once");
    assert_eq!(files(&dir), before);

    // An earlier attempt of the run landed first: this one reports it and leaves nothing.
    cat.fault_next_commit(Fault::RunLandsThenRefuse("nightly-7"));
    let again = merge(&cat, "lake.m", &[(3, "eu", 30)], Some("nightly-7")).unwrap();
    assert!(again.earlier_attempt);
    assert_eq!(again.snapshot_id, main_snapshot(&cat.table("lake", "m")));
    assert_eq!(files(&dir), before);

    // A lost answer settles as for an append: the table loaded again says it landed.
    cat.fault_next_commit(Fault::ApplyThen500);
    let landed = merge(&cat, "lake.m", &[(3, "eu", 30)], None).unwrap();
    let meta = cat.table("lake", "m");
    assert_eq!(main_snapshot(&meta), landed.snapshot_id);
    assert_eq!(
        table_rows(&meta),
        rows(&[(1, "us", 10), (2, "us", 20), (3, "eu", 30), (4, "eu", 40)])
    );

    // A file the merge must read is gone. While the table has not moved, that is the
    // failure it is; once another writer has moved `main` (a compaction and an expiry
    // remove files), it is a conflict, and running again plans against the new head.
    for f in files(&dir) {
        if f.extension().is_some_and(|e| e == "parquet") {
            std::fs::remove_file(f).unwrap();
        }
    }
    let err = merge(&cat, "lake.m", &[(4, "eu", 41)], None).unwrap_err();
    assert!(!matches!(err, ShapeError::Conflict(_)), "{err}");
    let sink = merge_sink(&cat, "lake.m", &[(4, "eu", 41)], None).unwrap();
    cat.commit_foreign("lake", "m", 88);
    let mut sink = sink;
    let err = sink.finish().unwrap_err();
    assert!(
        matches!(err, ShapeError::Conflict(_))
            && err
                .to_string()
                .contains("another writer committed to lake.m"),
        "{err}"
    );
    std::fs::remove_dir_all(&wh).ok();
}

#[test]
fn a_compaction_through_the_catalog_commits_a_replace_and_is_never_rebuilt() {
    let wh = tmp("compact");
    let cat = StandIn::start(&wh, &["lake"]);
    merged_three_times(&cat, "lake.c");
    let want = table_rows(&cat.table("lake", "c"));
    let dir = table_dir(&cat, "lake", "c");
    let options = CompactOptions::default();

    // Another writer commits first: refused, not rebuilt (a merge may have deleted rows
    // from the very files it rewrote), and the files it wrote are removed.
    let before = files(&dir);
    let commits = cat.commits().len();
    cat.fault_next_commit(Fault::MoveThenRefuse);
    let err = compact_through_catalog(&cat.conn(), "lake.c", &options, QUICK).unwrap_err();
    assert!(matches!(err, ShapeError::Conflict(_)), "{err}");
    assert_eq!(cat.commits().len(), commits + 1, "sent once");
    assert_eq!(files(&dir), before);

    // A `replace` snapshot on `main`, asserting the head it read: the same rows, every
    // delete applied, one file per partition.
    let head = main_snapshot(&cat.table("lake", "c"));
    let s = compact_through_catalog(&cat.conn(), "lake.c", &options, QUICK).unwrap();
    assert_eq!(s.version, None, "the catalog numbers its metadata files");
    // Six small files become one per partition; the three delete files (one per data file
    // a merge deleted from) are applied and gone.
    assert_eq!(
        (
            s.data_files_rewritten,
            s.data_files_written,
            s.delete_files_removed
        ),
        (6, 2, 3),
        "{s:?}"
    );
    let meta = cat.table("lake", "c");
    assert_eq!(main_snapshot(&meta), s.snapshot_id);
    assert_eq!(
        snapshot(&meta, s.snapshot_id.unwrap())["summary"]["operation"],
        "replace"
    );
    let body = cat.commits().pop().unwrap();
    assert_eq!(body["requirements"][1]["snapshot-id"].as_i64(), head);
    assert_eq!(table_rows(&meta), want);

    // Nothing left to compact: nothing sent.
    let commits = cat.commits().len();
    let again = compact_through_catalog(&cat.conn(), "lake.c", &options, QUICK).unwrap();
    assert_eq!(again.snapshot_id, None);
    assert_eq!(cat.commits().len(), commits);

    // A table the catalog does not have, and one it has with no snapshot yet.
    let err = compact_through_catalog(&cat.conn(), "lake.nope", &options, QUICK).unwrap_err();
    assert!(err.to_string().contains("has no table lake.nope"), "{err}");
    drop(merge_sink(&cat, "lake.empty", &[(1, "eu", 1)], None).unwrap());
    let empty = compact_through_catalog(&cat.conn(), "lake.empty", &options, QUICK).unwrap();
    assert_eq!(empty, CompactSummary::default());
    std::fs::remove_dir_all(&wh).ok();
}

/// Move the tag `v1` onto `main`'s parent, as another writer resetting it would.
fn move_tag(meta: &mut Value) {
    let head = snapshot(meta, main_snapshot(meta).unwrap());
    meta["refs"]["v1"]["snapshot-id"] = head["parent-snapshot-id"].clone();
}

#[test]
fn an_expiry_through_the_catalog_removes_snapshots_then_deletes_only_their_files() {
    let wh = tmp("expire");
    let cat = StandIn::start(&wh, &["lake"]);
    merged_three_times(&cat, "lake.e");
    compact_through_catalog(&cat.conn(), "lake.e", &CompactOptions::default(), QUICK).unwrap();
    let want = table_rows(&cat.table("lake", "e"));
    let dir = table_dir(&cat, "lake", "e");
    // A tag on the first snapshot: kept, with the files it reads.
    let first = {
        let mut st = cat.state.lock().unwrap();
        let meta = st.tables.get_mut(&("lake".into(), "e".into())).unwrap();
        let first = meta["snapshots"][0]["snapshot-id"].clone();
        meta["refs"]["v1"] = json!({ "snapshot-id": first, "type": "tag" });
        first
    };
    let all = ExpireOptions {
        older_than_ms: snapshot_timestamp_ms() + 60_000,
        retain_last: 1,
        dry_run: false,
    };
    let dry = ExpireOptions {
        dry_run: true,
        ..all.clone()
    };

    // A dry run plans, and changes nothing.
    let (before, commits) = (files(&dir), cat.commits().len());
    let plan = expire_through_catalog(&cat.conn(), "lake.e", &dry, QUICK).unwrap();
    assert_eq!(
        plan.expired_snapshots.len(),
        2,
        "the tagged and the current stay"
    );
    assert!(!plan.committed);
    assert_eq!(
        (files(&dir), cat.commits().len()),
        (before.clone(), commits)
    );

    // Refused, or a lost answer the table settles as refused: a conflict, nothing deleted.
    // `main` moved, or the tag it asserted did (onto a snapshot this would expire), or the
    // answer was lost and `main` has moved since.
    for fault in [
        Fault::MoveThenRefuse,
        Fault::Then(move_tag),
        Fault::MoveThen500,
    ] {
        // Each case starts from the table as it now is, the tag back on the first snapshot.
        {
            let mut st = cat.state.lock().unwrap();
            let meta = st.tables.get_mut(&("lake".into(), "e".into())).unwrap();
            meta["refs"]["v1"]["snapshot-id"] = first.clone();
        }
        cat.fault_next_commit(fault);
        let err = expire_through_catalog(&cat.conn(), "lake.e", &all, QUICK).unwrap_err();
        assert!(matches!(err, ShapeError::Conflict(_)), "{fault:?}: {err}");
        assert!(err.to_string().contains("no snapshot was expired"), "{err}");
        assert_eq!(files(&dir), before, "{fault:?}");
    }
    assert_eq!(table_rows(&cat.table("lake", "e")), want);

    // Unanswered, and the table neither moved nor took it, every time it was sent: unknown
    // (exit 1, not 75), and nothing deleted.
    for _ in 0..=QUICK.max_resends {
        cat.fault_next_commit(Fault::Fail500);
    }
    let err = expire_through_catalog(&cat.conn(), "lake.e", &all, QUICK).unwrap_err();
    assert!(
        !matches!(err, ShapeError::Conflict(_)) && err.to_string().contains("may or may not"),
        "{err}"
    );
    assert!(
        err.to_string().contains("Running the expiry again is safe"),
        "{err}"
    );
    assert_eq!(files(&dir), before);

    // Applied, then answered 500: the table loaded again no longer holds the snapshots, so
    // it committed, and only now are their files deleted.
    let plan = expire_through_catalog(&cat.conn(), "lake.e", &dry, QUICK).unwrap();
    let meta = cat.table("lake", "e");
    cat.fault_next_commit(Fault::ApplyThen500);
    let done = expire_through_catalog(&cat.conn(), "lake.e", &all, QUICK).unwrap();
    assert!(done.committed);
    assert_eq!(done.version, None);
    assert_eq!(done.expired_snapshots, plan.expired_snapshots);
    let body = cat.commits().pop().unwrap();
    let mut refs: Vec<_> = vec![json!({ "type": "assert-table-uuid", "uuid": meta["table-uuid"] })];
    refs.push(json!({ "type": "assert-ref-snapshot-id", "ref": "main",
                      "snapshot-id": main_snapshot(&meta) }));
    refs.push(json!({ "type": "assert-ref-snapshot-id", "ref": "v1", "snapshot-id": first }));
    assert_eq!(body["requirements"], json!(refs));
    assert_eq!(
        body["updates"],
        json!([{ "action": "remove-snapshots", "snapshot-ids": plan.expired_snapshots }])
    );
    let meta = cat.table("lake", "e");
    let left: Vec<_> = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["snapshot-id"].clone())
        .collect();
    assert!(left.contains(&first), "the tagged snapshot stays");
    assert!(plan
        .expired_snapshots
        .iter()
        .all(|id| !left.contains(&json!(id))));
    let deleted = done.data_files_deleted
        + done.delete_files_deleted
        + done.manifests_deleted
        + done.manifest_lists_deleted;
    assert!(done.data_files_deleted > 0, "{done:?}");
    assert_eq!(
        (before.len() - files(&dir).len()) as u64,
        deleted,
        "{done:?}"
    );
    assert_eq!(done.files_not_deleted, 0);
    // Every file `main` and the tag read is still there.
    assert_eq!(table_rows(&meta), want);
    let mut tagged = meta.clone();
    tagged["refs"]["main"]["snapshot-id"] = first.clone();
    assert_eq!(
        table_rows(&tagged),
        rows(&[(1, "eu", 1), (2, "us", 2), (3, "eu", 3)])
    );

    // Nothing left to expire, a table with no snapshot, and one the catalog lacks.
    let commits = cat.commits().len();
    let none = expire_through_catalog(&cat.conn(), "lake.e", &all, QUICK).unwrap();
    assert!(none.expired_snapshots.is_empty() && !none.committed);
    assert_eq!(cat.commits().len(), commits);
    drop(merge_sink(&cat, "lake.empty", &[(1, "eu", 1)], None).unwrap());
    let empty = expire_through_catalog(&cat.conn(), "lake.empty", &all, QUICK).unwrap();
    assert_eq!(empty, ExpireSummary::default());
    let err = expire_through_catalog(&cat.conn(), "lake.nope", &all, QUICK).unwrap_err();
    assert!(err.to_string().contains("has no table lake.nope"), "{err}");
    std::fs::remove_dir_all(&wh).ok();
}

// ---------------------------------------------------------------------------
// Authentication (0.5 S1)
// ---------------------------------------------------------------------------

/// The token endpoint Iceberg's REST spec gives a catalog.
const OWN_TOKEN_PATH: [&str; 3] = ["v1", "oauth", "tokens"];

fn token_rule(token: &str) -> AuthRule {
    AuthRule {
        token: Some(token.to_string()),
        client: None,
        token_path: OWN_TOKEN_PATH.to_vec(),
        lifetime: Duration::from_secs(3600),
        issued: Vec::new(),
    }
}

fn client_rule(
    id: &str,
    secret: &str,
    scope: &str,
    path: &[&'static str],
    lifetime: Duration,
) -> AuthRule {
    AuthRule {
        token: None,
        client: Some((id.to_string(), secret.to_string(), scope.to_string())),
        token_path: path.to_vec(),
        lifetime,
        issued: Vec::new(),
    }
}

fn credentials(id: &str, secret: &str, server_uri: Option<String>, scope: &str) -> CatalogAuth {
    CatalogAuth::ClientCredentials {
        client_id: Some(id.to_string()),
        client_secret: secret.to_string(),
        server_uri,
        scope: scope.to_string(),
    }
}

/// The `Authorization` header of every request since the last call, token requests aside.
fn authorizations(cat: &StandIn) -> Vec<Option<String>> {
    let mut st = cat.state.lock().unwrap();
    st.authorizations
        .drain(..)
        .filter(|(path, _)| !path.contains("/oauth/tokens") && !path.starts_with("/idp/"))
        .map(|(_, a)| a)
        .collect()
}

#[test]
fn a_bearer_token_goes_with_every_request_and_a_missing_one_says_what_to_set() {
    let wh = tmp("auth-token");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.require(token_rule("s3cr3t-token"));

    // No credentials: refused at the config, naming what to set.
    let err = write(&cat, "lake.t", false, 0..5, false, QUICK)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("401") && err.contains(TOKEN_ENV) && err.contains(CREDENTIAL_ENV),
        "{err}"
    );

    // A wrong token: refused, and never repeated back.
    cat.client(CatalogAuth::Token("wrong-token-value".into()), None);
    let err = write(&cat, "lake.t", false, 0..5, false, QUICK)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!("refused the token in {TOKEN_ENV}")),
        "{err}"
    );
    assert!(!err.contains("wrong-token-value"), "{err}");

    // The right one goes with the config, the load, the create and the commit of a first
    // write, and with every request of an append after it.
    cat.client(CatalogAuth::Token("s3cr3t-token".into()), None);
    authorizations(&cat);
    write(&cat, "lake.t", false, 0..5, false, QUICK).unwrap();
    write(&cat, "lake.t", true, 5..10, false, QUICK).unwrap();
    let sent = authorizations(&cat);
    assert_eq!(
        sent.len(),
        7,
        "config, load, create, commit; config, load, commit"
    );
    assert!(
        sent.iter()
            .all(|a| a.as_deref() == Some("Bearer s3cr3t-token")),
        "{sent:?}"
    );
    assert_eq!(live_rows(&cat.table("lake", "t")), 10);
}

#[test]
fn client_credentials_are_exchanged_for_a_token_and_again_before_it_expires() {
    let wh = tmp("auth-client");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.require(client_rule(
        "shapeshift",
        "pa55:w&rd",
        "catalog",
        &OWN_TOKEN_PATH,
        Duration::from_secs(2),
    ));
    cat.client(
        credentials("shapeshift", "pa55:w&rd", None, "catalog"),
        None,
    );

    // A write: one token, fetched with the client_credentials grant (the secret's `:`
    // and `&` encoded), then carried by every catalog request.
    write(&cat, "lake.t", false, 0..5, false, QUICK).unwrap();
    {
        let st = cat.state.lock().unwrap();
        assert_eq!(st.token_requests.len(), 1);
        let form = &st.token_requests[0];
        for field in [
            "grant_type=client_credentials",
            "client_id=shapeshift",
            "client_secret=pa55%3Aw%26rd",
            "scope=catalog",
        ] {
            assert!(form.split('&').any(|f| f == field), "{field} in {form}");
        }
    }
    let sent = authorizations(&cat);
    assert!(
        sent.iter().all(|a| a.as_deref() == Some("Bearer issued-1")),
        "{sent:?}"
    );

    // One connection outliving its token: past nine tenths of the token's life the next
    // call fetches another first. The stand-in refuses an expired token, so a client that
    // kept the old one would fail the second load.
    let conn = cat.conn();
    let ident = TableIdent::parse("lake.t").unwrap();
    block_on(async move {
        let catalog = RestCatalog::connect(&conn, QUICK.timeout).await?;
        assert!(catalog.load(&ident).await?.is_some());
        tokio::time::sleep(Duration::from_millis(1900)).await;
        assert!(catalog.load(&ident).await?.is_some());
        Ok::<_, ShapeError>(())
    })
    .unwrap();
    assert_eq!(cat.state.lock().unwrap().token_requests.len(), 3);
    assert_eq!(
        authorizations(&cat),
        [
            Some("Bearer issued-2".to_string()),
            Some("Bearer issued-2".to_string()),
            Some("Bearer issued-3".to_string()),
        ],
        "config and the first load on one token, the late load on a fresh one"
    );
}

#[test]
fn refused_client_credentials_are_reported_without_the_secret() {
    let wh = tmp("auth-refused");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.require(client_rule(
        "shapeshift",
        "right",
        "catalog",
        &OWN_TOKEN_PATH,
        Duration::from_secs(60),
    ));
    cat.client(
        credentials("shapeshift", "the-wrong-secret", None, "catalog"),
        None,
    );
    let err = write(&cat, "lake.t", false, 0..5, false, QUICK)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("invalid_client")
            && err.contains(CREDENTIAL_ENV)
            && err.contains("/v1/oauth/tokens"),
        "{err}"
    );
    assert!(!err.contains("the-wrong-secret"), "{err}");
    // Nothing but the token request was sent.
    let st = cat.state.lock().unwrap();
    assert_eq!(st.requests.len(), 1, "{:?}", st.requests);
}

#[test]
fn the_secret_is_not_sent_on_through_a_redirect() {
    // A token endpoint that answers 307 to another address: following it would send the
    // form, secret and all, to wherever the redirect points.
    let trap = TcpListener::bind("127.0.0.1:0").unwrap();
    let trap_uri = format!("http://{}", trap.local_addr().unwrap());
    let redirector = TcpListener::bind("127.0.0.1:0").unwrap();
    let redirect_uri = format!("http://{}/token", redirector.local_addr().unwrap());
    let to = format!("{trap_uri}/stolen");
    std::thread::spawn(move || {
        for conn in redirector.incoming().flatten() {
            let mut r = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            while r.read_line(&mut line).is_ok_and(|n| n > 2) {
                line.clear();
            }
            let mut out = conn;
            let _ = write!(
                out,
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {to}\r\nContent-Length: 0\r\n\
                 Connection: close\r\n\r\n"
            );
        }
    });
    trap.set_nonblocking(true).unwrap();
    let wh = tmp("auth-redirect");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.client(
        credentials(
            "shapeshift",
            "never-forwarded",
            Some(redirect_uri),
            "catalog",
        ),
        None,
    );
    let err = write(&cat, "lake.t", false, 0..5, false, QUICK)
        .unwrap_err()
        .to_string();
    assert!(err.contains("307"), "{err}");
    assert!(!err.contains("never-forwarded"), "{err}");
    assert!(
        trap.accept().is_err(),
        "the credentials were sent on to the redirect's target"
    );
}

#[test]
fn a_token_endpoint_and_scope_of_its_own_are_used_as_named() {
    let wh = tmp("auth-idp");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.require(client_rule(
        "shapeshift",
        "s",
        "PRINCIPAL_ROLE:ALL",
        &["idp", "token"],
        Duration::from_secs(60),
    ));
    let idp = format!("{}/idp/token", cat.uri);
    cat.client(
        credentials("shapeshift", "s", Some(idp), "PRINCIPAL_ROLE:ALL"),
        None,
    );
    write(&cat, "lake.t", false, 0..5, false, QUICK).unwrap();
    let st = cat.state.lock().unwrap();
    assert_eq!(st.token_requests.len(), 1);
    assert!(st.token_requests[0].contains("scope=PRINCIPAL_ROLE%3AALL"));
    assert!(
        !st.requests
            .iter()
            .any(|(_, p, _)| p.contains("/oauth/tokens")),
        "the catalog's own endpoint is not asked"
    );
}

#[test]
fn the_warehouse_is_named_to_the_catalog_and_a_wrong_one_is_refused() {
    let wh = tmp("auth-warehouse");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.state.lock().unwrap().warehouses = Some(vec!["prod lake".to_string()]);

    let err = write(&cat, "lake.t", false, 0..5, false, QUICK)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("GET /v1/config") && err.contains("400"),
        "{err}"
    );

    cat.client(CatalogAuth::None, Some("staging"));
    let err = write(&cat, "lake.t", false, 0..5, false, QUICK)
        .unwrap_err()
        .to_string();
    assert!(err.contains("NoSuchWarehouseException"), "{err}");

    // Encoded in the query, so a name with a space reaches the catalog whole.
    cat.client(CatalogAuth::None, Some("prod lake"));
    write(&cat, "lake.t", false, 0..5, false, QUICK).unwrap();
    assert_eq!(live_rows(&cat.table("lake", "t")), 5);
}

#[test]
fn a_catalog_uri_carrying_credentials_is_refused_without_showing_them() {
    let wh = tmp("auth-userinfo");
    let cat = StandIn::start(&wh, &["lake"]);
    let leaky = cat.uri.replacen("http://", "http://who:pa55word@", 1);
    let conn = CatalogConnection::new(leaky);
    let err = match block_on(async move { RestCatalog::connect(&conn, QUICK.timeout).await }) {
        Ok(_) => panic!("a URI with credentials in it was used"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("carries credentials") && err.contains(TOKEN_ENV),
        "{err}"
    );
    assert!(!err.contains("pa55word"), "{err}");
    assert!(
        cat.state.lock().unwrap().requests.is_empty(),
        "nothing was sent"
    );
}

#[test]
fn credentials_are_not_sent_over_plain_http_to_another_machine_unless_allowed() {
    let away = |uri: &str, server_uri: Option<&str>, allow: bool| CatalogConnection {
        uri: uri.to_string(),
        warehouse: None,
        auth: match server_uri {
            None => CatalogAuth::Token("tok-value".into()),
            Some(s) => credentials("id", "secret-value", Some(s.to_string()), "catalog"),
        },
        allow_plain_http: allow,
    };
    let refusal = |conn: CatalogConnection| match block_on(async move {
        RestCatalog::connect(&conn, QUICK.timeout).await
    }) {
        Ok(_) => String::new(),
        Err(e) => e.to_string(),
    };
    // The catalog, or only its token endpoint, on plain http elsewhere: refused before a
    // single request, naming the way out.
    for conn in [
        away("http://catalog.invalid:8181", None, false),
        away(
            "https://catalog.invalid",
            Some("http://idp.invalid/token"),
            false,
        ),
    ] {
        let err = refusal(conn);
        assert!(
            err.contains("refusing to send catalog credentials over plain http")
                && err.contains("--insecure-catalog"),
            "{err}"
        );
        assert!(
            !err.contains("tok-value") && !err.contains("secret-value"),
            "{err}"
        );
    }
    // Allowed, it goes ahead (and here finds no such host).
    let err = refusal(away("http://catalog.invalid:8181", None, true));
    assert!(!err.contains("refusing"), "{err}");
    // Without credentials there is nothing to protect (plain http is the reference fixture's
    // own address), and on this machine credentials may go over plain http.
    let wh = tmp("auth-plain-http");
    let cat = StandIn::start(&wh, &["lake"]);
    cat.require(token_rule("t"));
    cat.client(CatalogAuth::Token("t".into()), None);
    write(&cat, "lake.t", false, 0..5, false, QUICK).unwrap();
}

/// A table at a path, written there by the object-store sink, partitioned by `region` as
/// [`write`]'s tables are.
fn path_table(url: &str, ids: std::ops::Range<i64>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let region: Vec<&str> = ids.clone().map(|i| ["eu", "us"][i as usize % 2]).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.collect::<Vec<_>>())),
            Arc::new(StringArray::from(region)),
        ],
    )
    .unwrap();
    let mut sink = ObjectStoreIcebergSink::create(
        url,
        schema,
        shapeshift_core::Compression::Snappy,
        false,
        &["region".to_string()],
    )
    .unwrap();
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap();
}

/// A relocated table registered with the catalog (RELOCATE-PLAN R3): the catalog names it at
/// its metadata file, registering it again at that file is already done, and it takes writes
/// through the catalog that land where it is. Another table under the name is refused and the
/// catalog keeps the first; a namespace the catalog lacks is reported.
#[test]
fn a_relocated_table_is_registered_and_written_through_the_catalog() {
    let wh = tmp("register");
    let cat = StandIn::start(&wh, &["lake"]);
    let url = |name: &str| format!("file://{}", wh.join(name).display());
    path_table(&url("a"), 0..5);
    // Before the move, the catalog has the table at its old place as `lake.old`.
    let conn = cat.conn();
    let at_a = format!("{}/metadata/v1.metadata.json", url("a"));
    register_in_catalog(&conn, "lake.old", &at_a).unwrap();
    std::fs::rename(wh.join("a"), wh.join("b")).unwrap();
    let r = relocate_iceberg(&url("b"), &RelocateOptions::default()).unwrap();
    assert!(r.committed, "{r:?}");
    assert!(r.metadata.starts_with(&url("b")), "{r:?}");

    let done = register_in_catalog(&conn, "lake.t", &r.metadata).unwrap();
    assert_eq!(done, Registration::Registered);
    let again = register_in_catalog(&conn, "lake.t", &r.metadata).unwrap();
    assert_eq!(again, Registration::AlreadyRegistered);
    let registers: Vec<Value> = cat
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(m, p, _)| m == "POST" && p.ends_with("/register"))
        .map(|(_, _, b)| b.clone())
        .collect();
    assert_eq!(
        registers[1],
        json!({ "name": "t", "metadata-location": r.metadata })
    );

    // Written through the catalog: the snapshot lands where the table is.
    write(&cat, "lake.t", true, 5..10, false, QUICK).unwrap();
    let meta = cat.table("lake", "t");
    // The same registration again, after that write: the catalog has moved the name on to a
    // later version of this file, so it is still done (a retried pipeline step).
    let later = register_in_catalog(&conn, "lake.t", &r.metadata).unwrap();
    assert_eq!(later, Registration::AlreadyRegistered);
    assert_eq!(meta["location"], url("b").as_str());
    let snapshots = meta["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 2);
    for s in snapshots {
        let list = s["manifest-list"].as_str().unwrap();
        assert!(list.starts_with(&url("b")), "{list}");
    }

    // Another table under the name is refused, and the catalog keeps the first.
    path_table(&url("other"), 0..2);
    let other = format!("{}/metadata/v1.metadata.json", url("other"));
    let e = register_in_catalog(&conn, "lake.t", &other)
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("already has lake.t") && e.contains("never replaces"),
        "{e}"
    );
    assert_eq!(cat.table("lake", "t"), meta);
    // A name that holds this table at its old place (the same uuid, a move keeps it) is not
    // this registration: refused, not taken for done.
    let held = cat.table("lake", "old");
    assert_eq!(held["table-uuid"], meta["table-uuid"]);
    let e = register_in_catalog(&conn, "lake.old", &r.metadata)
        .unwrap_err()
        .to_string();
    assert!(e.contains("nor a later version of it"), "{e}");
    assert_eq!(cat.table("lake", "old"), held);
    // A namespace the catalog lacks.
    let e = register_in_catalog(&conn, "nope.t", &other)
        .unwrap_err()
        .to_string();
    assert!(e.contains("no namespace `nope`"), "{e}");
}

/// A local table, which records plain paths, relocated and registered: the catalog names it
/// at its plain-path metadata file, and writes through the catalog land in its directory.
#[test]
fn a_local_table_registered_by_its_path_takes_writes_through_the_catalog() {
    let wh = tmp("register-local");
    let cat = StandIn::start(&wh, &["lake"]);
    let (a, b) = (wh.join("a"), wh.join("b"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["eu", "us"])),
        ],
    )
    .unwrap();
    let mut sink = shapeshift_iceberg::IcebergSink::create(
        &a,
        schema,
        shapeshift_core::Compression::Snappy,
        false,
        &["region".to_string()],
    )
    .unwrap();
    sink.write_batch(&batch).unwrap();
    sink.finish().unwrap();
    std::fs::rename(&a, &b).unwrap();
    let r = shapeshift_iceberg::relocate_table(&b, &RelocateOptions::default()).unwrap();
    let here = b.canonicalize().unwrap().display().to_string();
    assert!(r.metadata.starts_with(&format!("{here}/")), "{r:?}");

    let conn = cat.conn();
    let done = register_in_catalog(&conn, "lake.t", &r.metadata).unwrap();
    assert_eq!(done, Registration::Registered);
    write(&cat, "lake.t", true, 5..9, false, QUICK).unwrap();
    let meta = cat.table("lake", "t");
    assert_eq!(meta["location"], here.as_str());
    let list = meta["snapshots"].as_array().unwrap()[1]["manifest-list"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(list.starts_with(&format!("{here}/")), "{list}");
    assert!(std::path::Path::new(&list).exists(), "{list}");
}
