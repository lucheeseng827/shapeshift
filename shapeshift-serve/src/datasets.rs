//! Datasets: named specs this console keeps, run on demand, and watches.
//!
//! A dataset is what the Shape form holds, given a name and kept: a spec (YAML), the input it
//! reads (a path on this machine), how it lands (Parquet or Iceberg, appended or merged), and an
//! optional freshness window. Each is one JSON file, `datasets/<name>.json` in the data dir. Its
//! output is the Shape form's for the same name: `outputs/<name>` (Iceberg) or
//! `outputs/<name>.parquet`.
//!
//! **Run now** runs it here, in this process, through the same code as the Shape form, and the
//! run goes in the run log under the dataset's name. A dataset runs once at a time: a second
//! request while one is running is refused (409). There is no queue and no schedule: that is an
//! orchestrator's job.
//!
//! The **overview** reads these and the run log for what needs a look: a dataset whose latest
//! run failed, or lost a race to another writer (exit 75), or that has had no successful run
//! within its freshness window. These are the conditions an alert rule watches.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use shapeshift_core::{Compression, DatasetSpec, DriftPolicy, OutputFormat, SourceFormat};

use crate::handlers::{clean_dataset, parse_body, run_shape, Ctx, ShapeReq};
use crate::http::{Request, Response};
use crate::runs::now_ms;

/// Rejected rows the dataset page shows from a run's sidecar file.
const REJECTS_SHOWN: usize = 200;
/// Runs a dataset's page lists.
const RUNS_SHOWN: usize = 50;
/// Runs the overview lists.
const RECENT_SHOWN: usize = 8;

/// One dataset, as kept in `datasets/<name>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub name: String,
    /// The spec, as typed. Empty means "infer the schema on every run".
    #[serde(default)]
    pub spec_yaml: String,
    /// The file it reads, on this machine (relative to the data dir, or absolute).
    #[serde(default)]
    pub input_path: String,
    #[serde(default)]
    pub format: SourceFormat,
    #[serde(default = "default_to")]
    pub to: OutputFormat,
    #[serde(default)]
    pub compression: Compression,
    /// Iceberg: append a snapshot to the table (the default) rather than replace it.
    #[serde(default = "default_true")]
    pub append: bool,
    #[serde(default)]
    pub partition_by: Vec<String>,
    #[serde(default)]
    pub on_drift: Option<DriftPolicy>,
    /// No successful run within this many minutes is `table_stale`. None: not watched.
    #[serde(default)]
    pub freshness_minutes: Option<u64>,
    #[serde(default)]
    pub created_ms: i64,
    #[serde(default)]
    pub updated_ms: i64,
}

fn default_to() -> OutputFormat {
    OutputFormat::Iceberg
}
fn default_true() -> bool {
    true
}

impl Dataset {
    /// Where its output lands, relative to the data dir (as the run log records it).
    pub fn output(&self) -> String {
        match self.to {
            OutputFormat::Parquet => format!("outputs/{}.parquet", self.name),
            OutputFormat::Iceberg => format!("outputs/{}", self.name),
        }
    }

    /// The source its spec pulls from, if any: its kind and where it is.
    pub fn pulled(&self) -> Option<(&'static str, String)> {
        let spec = DatasetSpec::parse(self.spec_yaml.trim()).ok()?;
        crate::pulled::describe(&spec.source)
    }

    /// The sidecar a run writes its rejected rows to.
    fn rejects_file(&self) -> String {
        format!("{}.rejects.jsonl", self.output())
    }
}

/// The datasets in the data dir, and which of them are running now.
pub struct Datasets {
    dir: PathBuf,
    running: Mutex<HashSet<String>>,
    /// Saves are read-modify-write (`created_ms` is kept): one at a time.
    writing: Mutex<()>,
}

impl Datasets {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let dir = data_dir.join("datasets");
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        Ok(Datasets {
            dir,
            running: Mutex::new(HashSet::new()),
            writing: Mutex::new(()),
        })
    }

    fn file(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.json"))
    }

    /// Every dataset, by name. A file that does not parse is skipped (and logged), never fatal.
    pub fn list(&self) -> Vec<Dataset> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return out;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            match std::fs::read(&p)
                .map_err(anyhow::Error::from)
                .and_then(|b| serde_json::from_slice::<Dataset>(&b).map_err(Into::into))
            {
                Ok(d) => out.push(d),
                Err(e) => {
                    tracing::warn!(file = %p.display(), error = %e, "skipping a dataset file")
                }
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn get(&self, name: &str) -> Option<Dataset> {
        let bytes = std::fs::read(self.file(name)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Validate and keep `d`, replacing a dataset of the same name (its `created_ms` kept).
    pub fn save(&self, mut d: Dataset) -> Result<Dataset> {
        check_name(&d.name)?;
        if !d.spec_yaml.trim().is_empty() {
            let spec = DatasetSpec::parse(&d.spec_yaml).context("the spec does not parse")?;
            if crate::pulled::is_pulled(&spec.source) && !d.input_path.trim().is_empty() {
                bail!(
                    "the spec pulls from a source, and an input file is set too: a dataset reads one \
                     of them, so clear the input file or the spec's source"
                );
            }
            // The console's controls win over the spec's own (as on the Shape form), but a
            // merge is the spec's: say so now rather than at the first run.
            if !spec.output.merge_on.is_empty() && d.to != OutputFormat::Iceberg {
                bail!("the spec merges on a key (`output.merge_on`): that needs Iceberg output");
            }
        }
        if d.to == OutputFormat::Parquet && !d.partition_by.is_empty() {
            bail!("partitioning applies only to Iceberg output");
        }
        if d.freshness_minutes == Some(0) {
            bail!("a freshness window must be at least one minute (or none)");
        }
        let _w = self.writing.lock().expect("datasets");
        let now = now_ms();
        d.created_ms = self.get(&d.name).map_or(now, |old| old.created_ms);
        d.updated_ms = now;
        let file = self.file(&d.name);
        let tmp = file.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&d)?)
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &file).with_context(|| format!("replacing {}", file.display()))?;
        Ok(d)
    }

    /// Forget a dataset. Its output and its runs stay.
    pub fn delete(&self, name: &str) -> Result<bool> {
        let _w = self.writing.lock().expect("datasets");
        match std::fs::remove_file(self.file(name)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).context("removing the dataset"),
        }
    }

    /// Claim `name` for a run; false if it is running already.
    fn claim(&self, name: &str) -> bool {
        self.running
            .lock()
            .expect("datasets")
            .insert(name.to_string())
    }

    fn release(&self, name: &str) {
        self.running.lock().expect("datasets").remove(name);
    }
}

/// A dataset's name is its file name and its output's: letters, digits, `_` and `-` only.
fn check_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 100 || clean_dataset(Some(name)) != name {
        bail!("a dataset name is 1-100 letters, digits, `_` or `-` (and does not start or end with `_` or `-`)");
    }
    Ok(())
}

// ---- what needs a look --------------------------------------------------------

/// A dataset's freshness: `fresh`, `stale`, or `unwatched` (no window set).
fn freshness(d: &Dataset, last_ok_ms: Option<i64>, now: i64) -> &'static str {
    match d.freshness_minutes {
        None => "unwatched",
        Some(m) => {
            let since = last_ok_ms.unwrap_or(d.created_ms);
            if now.saturating_sub(since) > (m as i64).saturating_mul(60_000) {
                "stale"
            } else {
                "fresh"
            }
        }
    }
}

fn succeeded(run: &Value) -> bool {
    matches!(run["outcome"].as_str(), Some("ok" | "earlier"))
}

/// The conditions that need a person now, over every dataset: newest first.
/// Kinds: `run_failed` (critical), `run_conflict` and `table_stale` (warning).
pub fn conditions(datasets: &[Dataset], runs: &[Value], now: i64) -> Vec<Value> {
    let mut out = Vec::new();
    for d in datasets {
        // `runs` is newest first: the first of a dataset's is its latest.
        let mine = runs.iter().filter(|r| r["dataset"] == d.name.as_str());
        let mut latest = None;
        let mut last_ok = None;
        for r in mine {
            if latest.is_none() && r["outcome"] != "running" {
                latest = Some(r);
            }
            if last_ok.is_none() && succeeded(r) {
                last_ok = r["ended_ms"].as_i64();
            }
            if latest.is_some() && last_ok.is_some() {
                break;
            }
        }
        if let Some(r) = latest {
            let at = r["ended_ms"].as_i64().unwrap_or(now);
            let msg = r["error"]["message"].as_str().unwrap_or("");
            match r["outcome"].as_str() {
                Some("failed" | "interrupted") => out.push(json!({
                    "kind": "run_failed", "severity": "critical", "subject": d.name,
                    "message": format!("its latest run {} {}", r["id"].as_str().unwrap_or(""),
                        if msg.is_empty() { "failed".to_string() } else { format!("failed: {msg}") }),
                    "since_ms": at, "link": { "dataset": d.name, "run": r["id"] },
                })),
                Some("conflict") => out.push(json!({
                    "kind": "run_conflict", "severity": "warning", "subject": d.name,
                    "message": format!("its latest run {} lost a race to another writer (exit 75): nothing of it was published; run it again",
                        r["id"].as_str().unwrap_or("")),
                    "since_ms": at, "link": { "dataset": d.name, "run": r["id"] },
                })),
                _ => {}
            }
        }
        if freshness(d, last_ok, now) == "stale" {
            let window = d.freshness_minutes.unwrap_or(0);
            out.push(json!({
                "kind": "table_stale", "severity": "warning", "subject": d.name,
                "message": match last_ok {
                    Some(_) => format!("no successful run in its {window}-minute freshness window"),
                    None => format!("no successful run yet, and its {window}-minute freshness window has passed"),
                },
                "since_ms": last_ok.unwrap_or(d.created_ms) + (window as i64) * 60_000,
                "link": { "dataset": d.name },
            }));
        }
    }
    out.sort_by_key(|c| std::cmp::Reverse(c["since_ms"].as_i64().unwrap_or(0)));
    out
}

// ---- handlers -------------------------------------------------------------------

/// What the list shows of a dataset: its definition, its latest run, and its freshness.
fn card(d: &Dataset, runs: &[Value], running: bool, now: i64) -> Value {
    let mut mine = runs.iter().filter(|r| r["dataset"] == d.name.as_str());
    let latest = mine.clone().next();
    let last_ok = mine
        .find(|r| succeeded(r))
        .and_then(|r| r["ended_ms"].as_i64());
    json!({
        "dataset": d,
        "output": d.output(),
        "running": running,
        "latest_run": latest,
        "last_ok_ms": last_ok,
        "freshness": freshness(d, last_ok, now),
    })
}

/// `GET /api/datasets`: every dataset, with its latest run and freshness.
pub fn list(ctx: &Ctx) -> Response {
    let runs = ctx.runs().list();
    let running = ctx.datasets().running.lock().expect("datasets").clone();
    let now = now_ms();
    let out: Vec<Value> = ctx
        .datasets()
        .list()
        .iter()
        .map(|d| card(d, &runs, running.contains(&d.name), now))
        .collect();
    Response::json(200, &json!({ "datasets": out }))
}

#[derive(Deserialize)]
struct NameReq {
    name: String,
}

/// `POST /api/dataset`: one dataset, with its runs.
pub fn get(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let Some(d) = ctx.datasets().get(&r.name) else {
        return Response::error(404, format!("no dataset `{}`", r.name));
    };
    let runs = ctx.runs().list();
    let running = ctx
        .datasets()
        .running
        .lock()
        .expect("datasets")
        .contains(&d.name);
    let mut out = card(&d, &runs, running, now_ms());
    out["runs"] = json!(runs
        .iter()
        .filter(|r| r["dataset"] == d.name.as_str())
        .take(RUNS_SHOWN)
        .collect::<Vec<_>>());
    out["rejects_file"] = json!(d.rejects_file());
    // A pulled source: what it reads, the position its table records now, and what the
    // latest pull read.
    if let Some((kind, from)) = d.pulled() {
        let spec = DatasetSpec::parse(d.spec_yaml.trim()).ok();
        let position = spec
            .as_ref()
            .and_then(|s| crate::pulled::recorded(&s.source, &ctx.data_dir().join(d.output())).ok())
            .flatten();
        let last = runs
            .iter()
            .filter(|r| r["dataset"] == d.name.as_str())
            .find_map(|r| r["summary"]["source"].as_object().cloned());
        out["source"] = json!({ "kind": kind, "from": from, "position": position, "last": last });
    }
    out["table"] = json!(
        d.to == OutputFormat::Iceberg && ctx.data_dir().join(d.output()).join("metadata").is_dir()
    );
    Response::json(200, &out)
}

/// `POST /api/dataset/save`: create or replace a dataset.
pub fn save(ctx: &Ctx, req: &Request) -> Response {
    let d: Dataset = match parse_body(req) {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    match ctx.datasets().save(d) {
        Ok(d) => Response::json(200, &json!({ "dataset": d, "output": d.output() })),
        Err(e) => Response::error(400, format!("{e:#}")),
    }
}

/// `POST /api/dataset/delete`: forget a dataset (its output and runs stay).
pub fn delete(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if ctx
        .datasets()
        .running
        .lock()
        .expect("datasets")
        .contains(&r.name)
    {
        return Response::error(
            409,
            format!("`{}` is running: delete it when the run ends", r.name),
        );
    }
    match ctx.datasets().delete(&r.name) {
        Ok(true) => Response::json(200, &json!({ "deleted": r.name })),
        Ok(false) => Response::error(404, format!("no dataset `{}`", r.name)),
        Err(e) => Response::error(500, format!("{e:#}")),
    }
}

/// `POST /api/dataset/run`: run it now, here. One run of a dataset at a time (409 otherwise).
pub fn run(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let Some(d) = ctx.datasets().get(&r.name) else {
        return Response::error(404, format!("no dataset `{}`", r.name));
    };
    if d.input_path.trim().is_empty() && d.pulled().is_none() {
        return Response::error(
            400,
            format!(
                "`{}` has no input to read: set an input file, or a source (`source.http`, \
                 `source.mysql`, `source.postgres`) in its spec",
                d.name
            ),
        );
    }
    let input = d.input_path.clone();
    run_dataset(ctx, &d, &input, d.format, None, json!({ "via": "dataset" }))
        .unwrap_or_else(|| Response::error(409, format!("`{}` is running already", d.name)))
}

/// Run `d` on `input` read as `format` (its own input file, or a connector's sealed batch,
/// which is JSONL), as one run in the log with `extra` added to its record. None if the dataset
/// is running already: one run of a dataset at a time, whoever starts it.
pub(crate) fn run_dataset(
    ctx: &Ctx,
    d: &Dataset,
    input: &str,
    format: SourceFormat,
    run_id: Option<String>,
    mut extra: Value,
) -> Option<Response> {
    if !ctx.datasets().claim(&d.name) {
        return None;
    }
    let shape = ShapeReq {
        input: None,
        input_path: Some(input.to_string()),
        spec_yaml: (!d.spec_yaml.trim().is_empty()).then(|| d.spec_yaml.clone()),
        format,
        to: d.to,
        compression: d.compression,
        dataset: Some(d.name.clone()),
        partition_by: d.partition_by.clone(),
        append: d.append && d.to == OutputFormat::Iceberg,
        on_drift: d.on_drift,
        run_id,
    };
    // A pulled dataset's runs record its source's kind (the run log and `/api/runs` show it).
    if let (Some((kind, _)), Some(obj)) = (d.pulled(), extra.as_object_mut()) {
        obj.insert("source_kind".into(), Value::String(kind.to_string()));
    }
    let resp = run_shape(ctx, shape, extra);
    ctx.datasets().release(&d.name);
    Some(resp)
}

/// `POST /api/dataset/rejects`: the rows the latest run rejected, from its sidecar file.
pub fn rejects(ctx: &Ctx, req: &Request) -> Response {
    let r: NameReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let Some(d) = ctx.datasets().get(&r.name) else {
        return Response::error(404, format!("no dataset `{}`", r.name));
    };
    let path = ctx.data_dir().join(d.rejects_file());
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Response::json(
                200,
                &json!({ "file": d.rejects_file(), "total": 0, "rows": [] }),
            )
        }
        Err(e) => return Response::error(500, format!("reading {}: {e}", path.display())),
    };
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let rows: Vec<Value> = lines
        .iter()
        .take(REJECTS_SHOWN)
        .map(|l| serde_json::from_str(l).unwrap_or_else(|_| json!({ "raw": l })))
        .collect();
    Response::json(
        200,
        &json!({ "file": d.rejects_file(), "total": lines.len(), "rows": rows }),
    )
}

/// `GET /api/overview`: figures, what needs a look, and the latest runs.
pub fn overview(ctx: &Ctx) -> Response {
    let runs = ctx.runs().list();
    let datasets = ctx.datasets().list();
    let now = now_ms();
    let day_ago = now - 86_400_000;
    let today: Vec<&Value> = runs
        .iter()
        .filter(|r| r["started_ms"].as_i64().unwrap_or(0) >= day_ago)
        .collect();
    let tables = std::fs::read_dir(ctx.data_dir().join("outputs"))
        .map(|it| {
            it.flatten()
                .filter(|e| e.path().join("metadata").is_dir())
                .count()
        })
        .unwrap_or(0);
    let attention = conditions(&datasets, &runs, now);
    Response::json(
        200,
        &json!({
            "figures": {
                "datasets": datasets.len(),
                "watched": datasets.iter().filter(|d| d.freshness_minutes.is_some()).count(),
                "runs_24h": today.len(),
                "failed_24h": today.iter().filter(|r| matches!(r["outcome"].as_str(),
                    Some("failed" | "interrupted" | "conflict"))).count(),
                "running": runs.iter().filter(|r| r["outcome"] == "running").count(),
                "tables": tables,
            },
            "attention": attention,
            "recent": runs.iter().take(RECENT_SHOWN).collect::<Vec<_>>(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ds(name: &str, window: Option<u64>, created: i64) -> Dataset {
        Dataset {
            name: name.into(),
            spec_yaml: String::new(),
            input_path: "in.jsonl".into(),
            format: SourceFormat::Jsonl,
            to: OutputFormat::Iceberg,
            compression: Compression::default(),
            append: true,
            partition_by: vec![],
            on_drift: None,
            freshness_minutes: window,
            created_ms: created,
            updated_ms: created,
        }
    }

    fn run(id: &str, dataset: &str, outcome: &str, ended: i64) -> Value {
        json!({ "id": id, "dataset": dataset, "outcome": outcome,
                "started_ms": ended - 10, "ended_ms": ended, "error": null })
    }

    #[test]
    fn the_latest_finished_run_decides_failed_or_conflict() {
        let d = [ds("a", None, 0), ds("b", None, 0), ds("c", None, 0)];
        // Newest first: a failed then recovered; b's latest failed; c's latest lost a race.
        let runs = [
            run("l-6", "a", "ok", 600),
            run("l-5", "b", "failed", 500),
            run("l-4", "c", "conflict", 400),
            run("l-3", "a", "failed", 300),
            run("l-2", "b", "ok", 200),
        ];
        let c = conditions(&d, &runs, 1_000);
        let kinds: Vec<(&str, &str)> = c
            .iter()
            .map(|x| (x["kind"].as_str().unwrap(), x["subject"].as_str().unwrap()))
            .collect();
        assert_eq!(kinds, vec![("run_failed", "b"), ("run_conflict", "c")]);
    }

    #[test]
    fn a_running_run_does_not_hide_the_finished_one_before_it() {
        let d = [ds("a", None, 0)];
        let runs = [
            run("l-2", "a", "running", 0),
            run("l-1", "a", "failed", 100),
        ];
        let c = conditions(&d, &runs, 1_000);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0]["kind"], "run_failed");
    }

    #[test]
    fn stale_counts_from_the_last_success_or_from_creation() {
        let min = 60_000;
        let d = [
            ds("ok", Some(10), 0),
            ds("old", Some(10), 0),
            ds("never", Some(10), 0),
            ds("free", None, 0),
        ];
        let now = 100 * min;
        let runs = [
            run("l-3", "ok", "ok", now - 5 * min),
            run("l-2", "old", "ok", now - 20 * min),
            run("l-1", "free", "ok", 0),
        ];
        let c = conditions(&d, &runs, now);
        let stale: Vec<&str> = c
            .iter()
            .filter(|x| x["kind"] == "table_stale")
            .map(|x| x["subject"].as_str().unwrap())
            .collect();
        assert_eq!(stale, vec!["old", "never"]);
        let old = c.iter().find(|x| x["subject"] == "old").unwrap();
        assert_eq!(old["since_ms"], json!(now - 10 * min));
    }

    #[test]
    fn names_are_one_safe_path_segment() {
        for ok in ["events", "a-b_c", "x1"] {
            assert!(check_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "../x", "a/b", "a b", "-x", "x_", "é"] {
            assert!(check_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn save_keeps_created_and_refuses_what_cannot_run() {
        let tmp = std::env::temp_dir().join(format!("ss-ds-{}", uuid::Uuid::new_v4().simple()));
        let store = Datasets::open(&tmp).unwrap();
        let first = store.save(ds("events", Some(5), 0)).unwrap();
        let again = store.save(ds("events", Some(6), 0)).unwrap();
        assert_eq!(again.created_ms, first.created_ms);
        assert_eq!(store.get("events").unwrap().freshness_minutes, Some(6));

        let mut parquet = ds("p", None, 0);
        parquet.to = OutputFormat::Parquet;
        parquet.partition_by = vec!["day".into()];
        assert!(store.save(parquet).is_err());
        assert!(store.save(ds("w", Some(0), 0)).is_err());
        let mut bad = ds("s", None, 0);
        bad.spec_yaml = "dataset: [".into();
        assert!(store.save(bad).is_err());

        assert!(store.claim("events"));
        assert!(!store.claim("events"), "one run of a dataset at a time");
        store.release("events");
        assert!(store.claim("events"));

        assert!(store.delete("events").unwrap());
        assert!(!store.delete("events").unwrap());
        assert_eq!(store.list().len(), 0);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
