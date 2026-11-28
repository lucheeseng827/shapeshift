//! Connectors: records pushed to an ingest URL, landed by a dataset.
//!
//! A producer that already ships JSON over HTTP (eggrd's `[log.ship]`, Mergestro's
//! `--metrics-url`, a webhook) is pointed at `POST /v1/ingest/<connector>` on the console's
//! **ingest listener**, a second listener (`--ingest-addr`) that serves that one path and
//! nothing else. The console itself stays on loopback; only the ingest URL is exposed.
//!
//! **Who may post.** `Authorization: Bearer ssi_…`, the connector's own token. A token is shown
//! once, when the connector is made or its token rotated; the console keeps only its SHA-256.
//! A post with no token, or another connector's, is refused (401).
//!
//! **What is accepted.** Newline-delimited JSON objects, a JSON array of objects, or one JSON
//! object, up to 4 MiB a request. A request is taken whole or not at all: one line that is not a
//! JSON object refuses it (400, naming the line), so a producer's retry never lands half a batch.
//!
//! **Retries.** A post that names itself (`ce-id`, `x-dagron-event-id` or `Idempotency-Key`) is
//! known by that id and its body: sent again with the same id **and** the same body, it is
//! acknowledged and not added (the same id with another body is a new post). The console
//! remembers each connector's last [`DEDUPE_KEEP`] in memory; a restart forgets them.
//!
//! **When it is acknowledged.** Once its records are appended to the connector's spool file and
//! synced (`202`).
//!
//! **How records become rows.** The spool (`spool/<connector>/spool.jsonl`) is sealed when it
//! reaches `flush_bytes` or its oldest record is `flush_seconds` old: renamed to a batch file, then
//! run by the connector's dataset as any run is (one run of a dataset at a time), and removed once
//! it lands. A batch whose run fails is kept and tried again a minute later, before any newer one.
//! A batch is never dropped; a console that stops finds its batches on start.
//!
//! **Backpressure.** Past four times `flush_bytes` waiting (the dataset is gone, or its runs keep
//! failing), the ingest URL answers `503` with `Retry-After`, and a producer that retries loses
//! nothing.
//!
//! What this console does not do, by design: signed producers (HMAC), secrets sealed under a
//! key, pulling from a source (Kafka, CDC), or running connectors on other machines.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use shapeshift_core::{OutputFormat, SourceFormat};

use crate::datasets::run_dataset;
use crate::handlers::{clean_dataset, parse_body, Ctx};
use crate::http::{Request, Response};
use crate::runs::now_ms;

/// Largest request the ingest listener takes.
pub const MAX_POST: usize = 4 * 1024 * 1024;
/// Ids remembered per connector, for retries.
const DEDUPE_KEEP: usize = 4096;
/// How often the flusher looks.
const TICK: Duration = Duration::from_secs(1);
/// A failed batch waits this long before it is tried again.
const RETRY_AFTER_MS: i64 = 60_000;
/// Backlog past this many times `flush_bytes` refuses posts.
const BACKPRESSURE: u64 = 4;

fn default_flush_bytes() -> u64 {
    1024 * 1024
}
fn default_flush_seconds() -> u64 {
    60
}

/// A connector, as kept in `connectors.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connector {
    pub name: String,
    /// The dataset that lands its records (an Iceberg dataset: each batch appends).
    pub dataset: String,
    /// SHA-256 of its token, hex. The token itself is never kept.
    token_sha256: String,
    /// The token's last four characters, to tell tokens apart.
    pub token_hint: String,
    #[serde(default = "default_flush_bytes")]
    pub flush_bytes: u64,
    #[serde(default = "default_flush_seconds")]
    pub flush_seconds: u64,
    #[serde(default)]
    pub created_ms: i64,
}

/// What a connector did since this console started (not kept across restarts).
#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    pub posts: u64,
    pub records: u64,
    pub duplicates: u64,
    pub refused: u64,
    pub batches: u64,
    pub runs_ok: u64,
    pub runs_failed: u64,
    pub last_post_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Default)]
struct Live {
    stats: Stats,
    seen: VecDeque<String>,
    seen_set: HashSet<String>,
    /// When the oldest record in the open spool arrived.
    oldest_ms: Option<i64>,
    /// A failed batch is not tried again before this.
    retry_at_ms: i64,
}

pub struct Connectors {
    file: PathBuf,
    spool: PathBuf,
    defs: Mutex<Vec<Connector>>,
    live: Mutex<HashMap<String, Live>>,
    /// Appends, seals and deletes of a spool, one at a time (taken before `defs` or `live`).
    spooling: Mutex<()>,
    /// Connectors whose spool holds a partial append that could not be rolled back: nothing is
    /// sealed or taken until it is repaired (or the connector deleted with discard).
    poisoned: Mutex<HashSet<String>>,
    ingest_addr: Mutex<Option<String>>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A new token: `ssi_` and 32 random bytes as hex.
fn new_token() -> Result<String> {
    use ring::rand::SecureRandom as _;
    let mut b = [0u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut b)
        .map_err(|_| anyhow::anyhow!("no randomness for a token"))?;
    Ok(format!(
        "ssi_{}",
        b.iter().map(|x| format!("{x:02x}")).collect::<String>()
    ))
}

impl Connectors {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let file = data_dir.join("connectors.json");
        let defs = match std::fs::read(&file) {
            Ok(b) => serde_json::from_slice(&b)
                .with_context(|| format!("{} does not parse", file.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e).with_context(|| format!("reading {}", file.display())),
        };
        let spool = data_dir.join("spool");
        std::fs::create_dir_all(&spool).with_context(|| format!("creating {}", spool.display()))?;
        let c = Connectors {
            file,
            spool,
            defs: Mutex::new(defs),
            live: Mutex::new(HashMap::new()),
            spooling: Mutex::new(()),
            poisoned: Mutex::new(HashSet::new()),
            ingest_addr: Mutex::new(None),
        };
        // A delete that was interrupted left its spool detached: finish it.
        for e in std::fs::read_dir(&c.spool).into_iter().flatten().flatten() {
            if e.file_name().to_string_lossy().starts_with(".deleted-") {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
        // A spool left by an earlier process counts from now.
        let names: Vec<String> = c.list().into_iter().map(|d| d.name).collect();
        for n in names {
            if c.spool_file(&n)
                .metadata()
                .map(|m| m.len() > 0)
                .unwrap_or(false)
            {
                c.live
                    .lock()
                    .expect("connectors")
                    .entry(n)
                    .or_default()
                    .oldest_ms = Some(now_ms());
            }
        }
        Ok(c)
    }

    pub(crate) fn set_ingest_addr(&self, addr: Option<String>) {
        *self.ingest_addr.lock().expect("connectors") = addr;
    }

    pub fn list(&self) -> Vec<Connector> {
        self.defs.lock().expect("connectors").clone()
    }

    fn get(&self, name: &str) -> Option<Connector> {
        self.list().into_iter().find(|c| c.name == name)
    }

    fn dir(&self, name: &str) -> PathBuf {
        self.spool.join(name)
    }

    fn spool_file(&self, name: &str) -> PathBuf {
        self.dir(name).join("spool.jsonl")
    }

    /// Sealed batches waiting to run, oldest first.
    fn batches(&self, name: &str) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = std::fs::read_dir(self.dir(name))
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("batch-") && n.ends_with(".jsonl"))
            })
            .collect();
        out.sort();
        out
    }

    /// Bytes waiting: the open spool and the sealed batches.
    fn waiting(&self, name: &str) -> (u64, u64, usize) {
        let spool = self
            .spool_file(name)
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        let batches = self.batches(name);
        let sealed = batches
            .iter()
            .map(|p| p.metadata().map(|m| m.len()).unwrap_or(0))
            .sum();
        (spool, sealed, batches.len())
    }

    fn persist(&self, defs: &[Connector]) -> Result<()> {
        let tmp = self.file.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(defs)?)
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.file)
            .with_context(|| format!("replacing {}", self.file.display()))?;
        Ok(())
    }

    /// Seal the open spool into a batch, if it has anything.
    fn seal(&self, name: &str) -> Result<bool> {
        let _s = self.spooling.lock().expect("connectors");
        if self.poisoned.lock().expect("connectors").contains(name) {
            bail!("its spool holds a partial post that could not be rolled back: repair spool/{name}/spool.jsonl, or delete the connector with discard");
        }
        let spool = self.spool_file(name);
        match spool.metadata() {
            Ok(m) if m.len() > 0 => {}
            _ => return Ok(false),
        }
        // Batch names sort by when they were sealed (zero-padded), then a random part.
        let batch = self.dir(name).join(format!(
            "batch-{:016}-{}.jsonl",
            now_ms(),
            &uuid::Uuid::new_v4().simple().to_string()[..6]
        ));
        std::fs::rename(&spool, &batch).with_context(|| format!("sealing {}", spool.display()))?;
        sync_dir(&self.dir(name)).context("syncing the spool directory")?;
        let mut live = self.live.lock().expect("connectors");
        let l = live.entry(name.to_string()).or_default();
        l.oldest_ms = None;
        l.stats.batches += 1;
        Ok(true)
    }
}

impl Connectors {
    /// Append `text` to the open spool and make it durable: the file synced, and on Unix the
    /// directories whose entries it created. On failure, what was written of it is cut off again
    /// (durably), so a refused post never lands; if even that fails, the connector is poisoned.
    /// Called with `spooling` held.
    fn append(&self, name: &str, text: &[u8]) -> Result<()> {
        let dir = self.dir(name);
        let new_dir = !dir.exists();
        std::fs::create_dir_all(&dir)?;
        if new_dir {
            sync_dir(&self.spool)?;
        }
        let path = self.spool_file(name);
        let existed = path.exists();
        let before = path.metadata().map(|m| m.len()).unwrap_or(0);
        let wrote = (|| -> std::io::Result<()> {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            f.write_all(text)?;
            f.sync_all()?;
            if !existed {
                sync_dir(&dir)?;
            }
            Ok(())
        })();
        if let Err(e) = wrote {
            // A fresh handle: one opened to append cannot truncate everywhere (Windows).
            let rolled = (|| -> std::io::Result<()> {
                let f = std::fs::OpenOptions::new().write(true).open(&path)?;
                f.set_len(before)?;
                f.sync_all()
            })();
            if let Err(r) = rolled {
                tracing::error!(connector = %name, error = %r, "could not roll back a partial post");
                self.poisoned
                    .lock()
                    .expect("connectors")
                    .insert(name.to_string());
            }
            return Err(e.into());
        }
        Ok(())
    }
}

/// Make a directory's entries durable (a new file, a rename). Unix only: elsewhere the file
/// system offers no such call.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

// ---- the ingest listener ------------------------------------------------------------

/// Parse a post into records: NDJSON objects, an array of objects, or one object.
fn records(body: &[u8]) -> Result<Vec<Value>, String> {
    let text = std::str::from_utf8(body).map_err(|_| "the body is not UTF-8".to_string())?;
    if text.trim().is_empty() {
        return Err("the body is empty".into());
    }
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        return match v {
            Value::Object(_) => Ok(vec![v]),
            Value::Array(items) => {
                for (i, it) in items.iter().enumerate() {
                    if !it.is_object() {
                        return Err(format!("element {i} of the array is not a JSON object"));
                    }
                }
                Ok(items)
            }
            _ => Err("the body is not a JSON object, an array of them, or NDJSON".into()),
        };
    }
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(v) if v.is_object() => out.push(v),
            _ => return Err(format!("line {} is not a JSON object", n + 1)),
        }
    }
    Ok(out)
}

fn refuse(ctx: &Ctx, name: &str, status: u16, msg: impl Into<String>) -> Response {
    let msg = msg.into();
    let mut live = ctx.connectors().live.lock().expect("connectors");
    let l = live.entry(name.to_string()).or_default();
    l.stats.refused += 1;
    // A bad post is worth showing. A wrong token is not (anyone can send one), and
    // backpressure is a symptom: the cause the flusher recorded stays on screen.
    if status == 400 || status == 500 {
        l.stats.last_error = Some(msg.clone());
    }
    Response::error(status, msg)
}

/// The ingest listener's only route: `POST /v1/ingest/<connector>`.
pub fn ingest_route(ctx: &Ctx, req: &Request) -> Response {
    let Some(name) = req.path.strip_prefix("/v1/ingest/") else {
        return Response::error(404, "not found");
    };
    let name = name.split(['?', '#']).next().unwrap_or("");
    if req.method != "POST" {
        return Response::error(405, "POST records to this URL");
    }
    let cs = ctx.connectors();
    let Some(c) = cs.get(name) else {
        return Response::error(404, "no such connector");
    };
    // The token, checked before anything about the body is said.
    let token = req
        .header("authorization")
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("");
    if token.is_empty() || sha256_hex(token.as_bytes()) != c.token_sha256 {
        return refuse(
            ctx,
            name,
            401,
            "this URL needs its connector's token: Authorization: Bearer ssi_…",
        );
    }
    let recs = match records(&req.body) {
        Ok(r) => r,
        Err(e) => return refuse(ctx, name, 400, e),
    };
    // A retry of a post that named itself is acknowledged and not added.
    let id = ["ce-id", "x-dagron-event-id", "idempotency-key"]
        .iter()
        .find_map(|h| req.header(h))
        .map(|id| format!("{id}/{}", sha256_hex(&req.body)));
    // Check, append and record under one lock, so a concurrent retry, a seal or a delete of
    // this connector waits for this post, and this post sees what they did.
    let _s = cs.spooling.lock().expect("connectors");
    // Deleted, or deleted and made again with another token, since the check above.
    if cs.get(name).map(|now| now.token_sha256) != Some(c.token_sha256.clone()) {
        return Response::error(404, "no such connector");
    }
    if cs.poisoned.lock().expect("connectors").contains(name) {
        return refuse(
            ctx,
            name,
            503,
            "this connector's spool needs repair: try again later",
        )
        .with_header("Retry-After", "30");
    }
    if let Some(key) = &id {
        let mut live = cs.live.lock().expect("connectors");
        let l = live.entry(name.to_string()).or_default();
        if l.seen_set.contains(key) {
            l.stats.duplicates += 1;
            return Response::json(200, &json!({ "accepted": 0, "duplicate": true }));
        }
    }
    let (spool, sealed, _) = cs.waiting(name);
    if spool + sealed > BACKPRESSURE.saturating_mul(c.flush_bytes) {
        return refuse(
            ctx,
            name,
            503,
            "too many records are waiting to land (is the dataset there, and do its runs succeed?): try again later",
        )
        .with_header("Retry-After", "30");
    }
    // Append and sync, then acknowledge.
    let mut text = String::new();
    for r in &recs {
        text.push_str(&r.to_string());
        text.push('\n');
    }
    if let Err(e) = cs.append(name, text.as_bytes()) {
        return refuse(ctx, name, 500, format!("could not spool the records: {e}"));
    }
    let mut live = cs.live.lock().expect("connectors");
    let l = live.entry(name.to_string()).or_default();
    let now = now_ms();
    l.oldest_ms.get_or_insert(now);
    l.stats.posts += 1;
    l.stats.records += recs.len() as u64;
    l.stats.last_post_ms = Some(now);
    if let Some(key) = id {
        l.seen.push_back(key.clone());
        l.seen_set.insert(key);
        while l.seen.len() > DEDUPE_KEEP {
            if let Some(old) = l.seen.pop_front() {
                l.seen_set.remove(&old);
            }
        }
    }
    Response::json(202, &json!({ "accepted": recs.len() }))
}

// ---- the flusher --------------------------------------------------------------------

/// One pass: seal what is due, then run each connector's oldest batch.
pub(crate) fn flush_once(ctx: &Ctx, now: i64) {
    let cs = ctx.connectors();
    for c in cs.list() {
        let due = {
            let live = cs.live.lock().expect("connectors");
            let oldest = live.get(&c.name).and_then(|l| l.oldest_ms);
            let (spool, _, _) = cs.waiting(&c.name);
            spool >= c.flush_bytes
                || oldest.is_some_and(|t| now - t >= (c.flush_seconds as i64) * 1000)
        };
        if due {
            if let Err(e) = cs.seal(&c.name) {
                tracing::warn!(connector = %c.name, error = %e, "could not seal a spool");
            }
        }
        let Some(batch) = cs.batches(&c.name).into_iter().next() else {
            continue;
        };
        let retry_at = cs
            .live
            .lock()
            .expect("connectors")
            .get(&c.name)
            .map_or(0, |l| l.retry_at_ms);
        if now < retry_at {
            continue;
        }
        let fail = |msg: String| {
            let mut live = cs.live.lock().expect("connectors");
            let l = live.entry(c.name.clone()).or_default();
            l.stats.runs_failed += 1;
            l.stats.last_error = Some(msg);
            l.retry_at_ms = now + RETRY_AFTER_MS;
        };
        let Some(d) = ctx.datasets().get(&c.dataset) else {
            fail(format!("its dataset `{}` is gone: records wait", c.dataset));
            continue;
        };
        if d.to != OutputFormat::Iceberg {
            fail(format!(
                "its dataset `{}` writes Parquet, which a batch would replace: make it Iceberg",
                d.name
            ));
            continue;
        }
        // Absolute: a relative data dir must not be prefixed to the batch's path a second time.
        let input = match std::path::absolute(&batch) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(e) => {
                fail(format!("could not resolve a batch's path, kept: {e}"));
                continue;
            }
        };
        // Each batch appends, whatever the dataset does on Run now: a batch never replaces the
        // table. And each has its own run id, recorded in the snapshot it commits, so a batch run
        // again after it landed (its file could not be removed) is found and not appended twice.
        let mut d = d;
        d.append = true;
        let stem = batch
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let run_id = format!("c-{}-{}", c.name, stem.trim_start_matches("batch-"));
        let extra = json!({ "via": "connector", "connector": c.name, "batch_run_id": run_id });
        // The dataset may be running (Run now, or another connector's batch): next tick.
        let Some(resp) = run_dataset(ctx, &d, &input, SourceFormat::Jsonl, Some(run_id), extra)
        else {
            continue;
        };
        if resp.status == 200 {
            let removed = std::fs::remove_file(&batch);
            let mut live = cs.live.lock().expect("connectors");
            let l = live.entry(c.name.clone()).or_default();
            l.stats.runs_ok += 1;
            l.retry_at_ms = 0;
            l.stats.last_error = removed.err().map(|e| {
                format!("a batch landed but its file could not be removed ({e}); it is run again, and found landed")
            });
        } else {
            let msg = serde_json::from_slice::<Value>(&resp.body)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
                .unwrap_or_else(|| format!("the run answered {}", resp.status));
            fail(format!(
                "a batch did not land, kept and tried again in a minute: {msg}"
            ));
        }
    }
}

pub fn spawn_flusher(ctx: Arc<Ctx>) {
    std::thread::Builder::new()
        .name("connectors".into())
        .spawn(move || loop {
            flush_once(&ctx, now_ms());
            std::thread::sleep(TICK);
        })
        .expect("starting the connectors thread");
}

// ---- the console's API --------------------------------------------------------------

/// One connector, as the metrics read it.
pub(crate) struct MetricRow {
    pub name: String,
    pub stats: Stats,
    pub spool_bytes: u64,
    pub waiting_bytes: u64,
    pub waiting_batches: usize,
    pub oldest_ms: Option<i64>,
}

impl Connectors {
    /// Every connector's counts and what it holds, for `/metrics`.
    pub(crate) fn metric_rows(&self) -> Vec<MetricRow> {
        self.list()
            .into_iter()
            .map(|c| {
                let (spool_bytes, waiting_bytes, waiting_batches) = self.waiting(&c.name);
                let live = self.live.lock().expect("connectors");
                let l = live.get(&c.name);
                MetricRow {
                    stats: l.map(|l| l.stats.clone()).unwrap_or_default(),
                    oldest_ms: l.and_then(|l| l.oldest_ms),
                    name: c.name,
                    spool_bytes,
                    waiting_bytes,
                    waiting_batches,
                }
            })
            .collect()
    }
}

fn view(cs: &Connectors, c: &Connector) -> Value {
    let (spool, sealed, batches) = cs.waiting(&c.name);
    let stats = cs
        .live
        .lock()
        .expect("connectors")
        .get(&c.name)
        .map(|l| l.stats.clone())
        .unwrap_or_default();
    json!({
        "name": c.name, "dataset": c.dataset, "token_hint": c.token_hint,
        "flush_bytes": c.flush_bytes, "flush_seconds": c.flush_seconds, "created_ms": c.created_ms,
        "path": format!("/v1/ingest/{}", c.name),
        "spool_bytes": spool, "waiting_bytes": sealed, "waiting_batches": batches,
        "stats": stats,
    })
}

/// `GET /api/connectors`: every connector, and the ingest listener's address (if it is on).
pub fn list(ctx: &Ctx) -> Response {
    let cs = ctx.connectors();
    let out: Vec<Value> = cs.list().iter().map(|c| view(cs, c)).collect();
    let addr = cs.ingest_addr.lock().expect("connectors").clone();
    Response::json(200, &json!({ "connectors": out, "ingest_addr": addr }))
}

#[derive(Deserialize)]
struct SaveReq {
    name: String,
    dataset: String,
    #[serde(default)]
    flush_bytes: Option<u64>,
    #[serde(default)]
    flush_seconds: Option<u64>,
}

fn save_in(ctx: &Ctx, r: SaveReq) -> Result<(Connector, Option<String>)> {
    let name = r.name.trim().to_string();
    if name.is_empty() || name.len() > 100 || clean_dataset(Some(&name)) != name {
        bail!("a connector name is 1-100 letters, digits, `_` or `-`");
    }
    let Some(d) = ctx.datasets().get(r.dataset.trim()) else {
        bail!("no dataset `{}`: make the dataset first", r.dataset.trim());
    };
    if d.to != OutputFormat::Iceberg {
        bail!("`{}` writes Parquet, and each batch would replace the file: a connector needs an Iceberg dataset", d.name);
    }
    let flush_bytes = r.flush_bytes.unwrap_or_else(default_flush_bytes);
    let flush_seconds = r.flush_seconds.unwrap_or_else(default_flush_seconds);
    if !(1024..=(1u64 << 30)).contains(&flush_bytes) {
        bail!("flush_bytes is 1 KiB to 1 GiB");
    }
    if !(1..=86_400).contains(&flush_seconds) {
        bail!("flush_seconds is 1 to 86400");
    }
    let cs = ctx.connectors();
    let mut defs = cs.defs.lock().expect("connectors");
    // Staged, written, and only then published: a failed write leaves the connector as it was.
    let mut next = defs.clone();
    let (c, token) = match next.iter_mut().find(|c| c.name == name) {
        Some(c) => {
            c.dataset = d.name.clone();
            c.flush_bytes = flush_bytes;
            c.flush_seconds = flush_seconds;
            (c.clone(), None)
        }
        None => {
            let token = new_token()?;
            let c = Connector {
                name: name.clone(),
                dataset: d.name.clone(),
                token_sha256: sha256_hex(token.as_bytes()),
                token_hint: token[token.len() - 4..].to_string(),
                flush_bytes,
                flush_seconds,
                created_ms: now_ms(),
            };
            next.push(c.clone());
            (c, Some(token))
        }
    };
    cs.persist(&next)?;
    *defs = next;
    Ok((c, token))
}

/// `POST /api/connector/save`: make a connector (its token is in the answer, once), or change
/// its dataset and flush settings.
pub fn save(ctx: &Ctx, req: &Request) -> Response {
    let r: SaveReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match save_in(ctx, r) {
        Ok((c, token)) => Response::json(
            200,
            &json!({ "connector": view(ctx.connectors(), &c), "token": token }),
        ),
        Err(e) => Response::error(400, format!("{e:#}")),
    }
}

#[derive(Deserialize)]
struct NameReq {
    name: String,
    #[serde(default)]
    discard: bool,
}

/// `POST /api/connector/rotate`: a new token (in the answer, once); the old one stops working.
pub fn rotate(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let cs = ctx.connectors();
    let mut defs = cs.defs.lock().expect("connectors");
    // The old token stays in force until the new one is written.
    let mut next = defs.clone();
    let Some(c) = next.iter_mut().find(|c| c.name == r.name) else {
        return Response::error(404, format!("no connector `{}`", r.name));
    };
    let token = match new_token() {
        Ok(t) => t,
        Err(e) => return Response::error(500, format!("{e:#}")),
    };
    c.token_sha256 = sha256_hex(token.as_bytes());
    c.token_hint = token[token.len() - 4..].to_string();
    let c = c.clone();
    if let Err(e) = cs.persist(&next) {
        return Response::error(500, format!("{e:#}"));
    }
    *defs = next;
    drop(defs);
    Response::json(200, &json!({ "connector": view(cs, &c), "token": token }))
}

/// `POST /api/connector/flush`: seal the spool now (its batch runs on the next tick).
pub fn flush(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let cs = ctx.connectors();
    if cs.get(&r.name).is_none() {
        return Response::error(404, format!("no connector `{}`", r.name));
    }
    match cs.seal(&r.name) {
        Ok(sealed) => {
            // A batch that failed waits a minute; asked by hand, try it now.
            if let Some(l) = cs.live.lock().expect("connectors").get_mut(&r.name) {
                l.retry_at_ms = 0;
            }
            Response::json(200, &json!({ "sealed": sealed }))
        }
        Err(e) => Response::error(500, format!("{e:#}")),
    }
}

/// `POST /api/connector/delete`: refused while records wait, unless `discard` is set.
pub fn delete(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let cs = ctx.connectors();
    // Under the spool lock: no post lands between the check and the delete.
    let _s = cs.spooling.lock().expect("connectors");
    let (spool, sealed, _) = cs.waiting(&r.name);
    if spool + sealed > 0 && !r.discard {
        return Response::error(
            409,
            format!(
                "{} bytes of records are waiting to land: flush it, or delete with discard",
                spool + sealed
            ),
        );
    }
    let mut defs = cs.defs.lock().expect("connectors");
    if !defs.iter().any(|c| c.name == r.name) {
        return Response::error(404, format!("no connector `{}`", r.name));
    }
    // Detach the spool first (a rename), so nothing of it can be picked up again, by this
    // connector or one made later with its name; then write the definitions.
    let dir = cs.dir(&r.name);
    let detached = cs.spool.join(format!(".deleted-{}-{}", r.name, now_ms()));
    if dir.exists() {
        if let Err(e) = std::fs::rename(&dir, &detached) {
            return Response::error(
                500,
                format!("could not detach its spool, nothing changed: {e}"),
            );
        }
    }
    let next: Vec<Connector> = defs.iter().filter(|c| c.name != r.name).cloned().collect();
    if let Err(e) = cs.persist(&next) {
        if detached.exists() {
            let _ = std::fs::rename(&detached, &dir);
        }
        return Response::error(500, format!("{e:#}"));
    }
    *defs = next;
    drop(defs);
    cs.live.lock().expect("connectors").remove(&r.name);
    cs.poisoned.lock().expect("connectors").remove(&r.name);
    if detached.exists() {
        if let Err(e) = std::fs::remove_dir_all(&detached) {
            // Detached already: it can never be read again, and the next start removes it.
            tracing::warn!(dir = %detached.display(), error = %e, "could not remove a deleted connector's spool");
        }
    }
    Response::json(200, &json!({ "deleted": r.name }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_post_is_ndjson_an_array_or_one_object_and_never_half_taken() {
        assert_eq!(records(b"{\"a\":1}\n{\"a\":2}\n").unwrap().len(), 2);
        assert_eq!(
            records(b"[{\"a\":1},{\"a\":2},{\"a\":3}]").unwrap().len(),
            3
        );
        assert_eq!(
            records(b"{\n  \"a\": 1\n}").unwrap().len(),
            1,
            "one pretty object"
        );
        assert_eq!(records(b"{\"a\":1}\r\n\r\n{\"a\":2}").unwrap().len(), 2);
        assert_eq!(
            records(b"{\"a\":1}\nnot json\n{\"a\":2}").unwrap_err(),
            "line 2 is not a JSON object"
        );
        assert!(records(b"[{\"a\":1}, 2]")
            .unwrap_err()
            .contains("element 1"));
        assert!(records(b"42").is_err());
        assert!(records(b"  ").is_err());
        assert!(records(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn tokens_are_random_and_kept_as_hashes() {
        let a = new_token().unwrap();
        let b = new_token().unwrap();
        assert!(a.starts_with("ssi_") && a.len() == 4 + 64);
        assert_ne!(a, b);
        assert_eq!(sha256_hex(b"abc").len(), 64);
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
