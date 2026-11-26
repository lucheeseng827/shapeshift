//! The runs this console started, kept in its data directory (`runs.jsonl`).
//!
//! Each run is one JSON record: what it wrote, when each phase began and ended, how it came
//! out, and the counts it reported. A record is appended to the file every time it changes (when
//! the run starts, between phases, when it ends), so the newest line for an id is the run as it
//! stands; reading the file back keeps that one. The file is rewritten, newest records only, when
//! it holds many more lines than runs or more runs than it keeps (the newest 500, and any still
//! running). Only this process's runs are here: it is a local console's log, not a fleet's.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use shapeshift_core::ShapeError;

/// Runs kept in memory and in the file; older finished ones are dropped when it is rewritten.
const KEEP: usize = 500;
/// Runs past [`KEEP`] before the file is rewritten (so it is not rewritten on every new run).
const SLACK: usize = KEEP / 10;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The run log: every run's newest record, oldest run first.
pub struct RunLog {
    path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    runs: Vec<Value>,
    index: HashMap<String, usize>,
    next: u64,
    lines: usize,
}

impl RunLog {
    /// Open the log in `data_dir`, reading back what an earlier process recorded. A run that
    /// was still going when that process stopped can never finish: it is recorded as
    /// `interrupted`.
    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("runs.jsonl");
        let mut inner = Inner {
            runs: Vec::new(),
            index: HashMap::new(),
            next: 1,
            lines: 0,
        };
        // Read as bytes, a line at a time: a line torn by a crash (even mid-character) is
        // skipped, and every whole record before it kept. A file that cannot be read is an
        // error, never an empty log that would replace it.
        match std::fs::read(&path) {
            Ok(bytes) => {
                for line in bytes.split(|b| *b == b'\n') {
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    inner.lines += 1;
                    let Ok(rec) = serde_json::from_slice::<Value>(line) else {
                        continue;
                    };
                    inner.put(rec);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("reading the run log {}", path.display()))
            }
        }
        for rec in &mut inner.runs {
            if rec["outcome"] == "running" {
                rec["outcome"] = json!("interrupted");
                let ended = last_phase_start(rec)
                    .unwrap_or_else(|| rec["started_ms"].as_i64().unwrap_or(0));
                close_phase(rec, ended);
                rec["ended_ms"] = json!(ended);
                rec["error"] = json!({ "kind": "other",
                    "message": "the console stopped while this run was in progress" });
            }
        }
        let log = RunLog {
            path,
            inner: Mutex::new(inner),
        };
        log.rewrite()?;
        Ok(log)
    }

    /// Start a run: a new id, `running`, its first phase begun now. `fields` are added to the
    /// record (what it writes, and how).
    pub fn start(&self, fields: Value, first_phase: &str) -> Run<'_> {
        let record = {
            let mut inner = self.inner.lock().expect("run log");
            let now = now_ms();
            // The number orders this log's runs; the random part makes the id unique across
            // logs, since it goes into the snapshot: a table already holding a snapshot with
            // this id would take the run for an earlier attempt and write nothing.
            let id = format!(
                "l-{:04}-{}",
                inner.next,
                &uuid::Uuid::new_v4().simple().to_string()[..8]
            );
            inner.next += 1;
            let mut record = json!({
                "id": id,
                "started_ms": now,
                "ended_ms": null,
                "outcome": "running",
                "exit": null,
                "phases": [ { "name": first_phase, "start_ms": now, "end_ms": null } ],
                "summary": null,
                "error": null,
            });
            if let Some(extra) = fields.as_object() {
                for (k, v) in extra {
                    record[k] = v.clone();
                }
            }
            // In the list from the moment its id is taken, so runs list in the order they began.
            inner.put(record.clone());
            record
        };
        let run = Run { log: self, record };
        run.save();
        run
    }

    /// Every run's newest record, newest run first.
    pub fn list(&self) -> Vec<Value> {
        let inner = self.inner.lock().expect("run log");
        inner.runs.iter().rev().cloned().collect()
    }

    fn save(&self, record: &Value) {
        let mut inner = self.inner.lock().expect("run log");
        inner.put(record.clone());
        inner.lines += 1;
        let appended = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .and_then(|mut f| writeln!(f, "{record}"));
        if let Err(e) = appended {
            tracing::warn!(path = %self.path.display(), error = %e, "could not record a run");
        }
        // Rewrite when old records pile up, or when there are more runs than are kept (a run
        // writes only a few records, so the first alone would never bound how many runs).
        let too_many = inner.lines > 4 * KEEP || inner.runs.len() > KEEP + SLACK;
        drop(inner);
        if too_many {
            if let Err(e) = self.rewrite() {
                tracing::warn!(error = %e, "could not compact the run log");
            }
        }
    }

    /// Rewrite the file with each kept run's newest record only.
    fn rewrite(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("run log");
        if inner.runs.len() > KEEP {
            // The oldest finished runs go; a run still going stays, whatever its age.
            let mut excess = inner.runs.len() - KEEP;
            inner.runs.retain(|r| {
                let drop = excess > 0 && r["outcome"] != "running";
                if drop {
                    excess -= 1;
                }
                !drop
            });
            inner.index = inner
                .runs
                .iter()
                .enumerate()
                .filter_map(|(i, r)| Some((r["id"].as_str()?.to_string(), i)))
                .collect();
        }
        let tmp = self.path.with_extension("jsonl.tmp");
        let mut text = String::new();
        for r in &inner.runs {
            text.push_str(&r.to_string());
            text.push('\n');
        }
        std::fs::write(&tmp, text).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("replacing {}", self.path.display()))?;
        inner.lines = inner.runs.len();
        Ok(())
    }
}

impl Inner {
    /// Keep `rec` as its run's newest record (a new run goes last).
    fn put(&mut self, rec: Value) {
        let Some(id) = rec["id"].as_str().map(str::to_string) else {
            return;
        };
        let number = id.strip_prefix("l-").and_then(|n| n.split('-').next());
        if let Some(n) = number.and_then(|n| n.parse::<u64>().ok()) {
            self.next = self.next.max(n + 1);
        }
        match self.index.get(&id) {
            Some(&i) => self.runs[i] = rec,
            None => {
                self.index.insert(id, self.runs.len());
                self.runs.push(rec);
            }
        }
    }
}

/// A run in progress. Each change is recorded at once, so the console shows it while it runs.
pub struct Run<'a> {
    log: &'a RunLog,
    record: Value,
}

impl Run<'_> {
    pub fn id(&self) -> String {
        self.record["id"].as_str().unwrap_or_default().to_string()
    }

    /// Record more of what the run is (once it is known).
    pub fn set(&mut self, key: &str, value: Value) {
        self.record[key] = value;
        self.save();
    }

    /// End the current phase now and begin `name`.
    pub fn phase(&mut self, name: &str) {
        let now = now_ms();
        close_phase(&mut self.record, now);
        if let Some(p) = self.record["phases"].as_array_mut() {
            p.push(json!({ "name": name, "start_ms": now, "end_ms": null }));
        }
        self.save();
    }

    /// The run committed (or wrote its file): `summary` is what it reported.
    pub fn succeed(mut self, summary: Value) {
        let outcome = if summary["earlier_attempt"] == true {
            "earlier"
        } else {
            "ok"
        };
        self.end(outcome, 0, Some(summary), None);
    }

    /// The run failed: a conflict exits 75 (safe to run again), anything else 1.
    pub fn fail(mut self, error: &anyhow::Error) {
        let kind = error_kind(error);
        let (outcome, exit) = if kind == "conflict" {
            ("conflict", 75)
        } else {
            ("failed", 1)
        };
        let error = json!({ "kind": kind, "message": format!("{error:#}") });
        self.end(outcome, exit, None, Some(error));
    }

    fn end(&mut self, outcome: &str, exit: i32, summary: Option<Value>, error: Option<Value>) {
        let now = now_ms();
        close_phase(&mut self.record, now);
        self.record["ended_ms"] = json!(now);
        self.record["outcome"] = json!(outcome);
        self.record["exit"] = json!(exit);
        self.record["summary"] = summary.unwrap_or(Value::Null);
        self.record["error"] = error.unwrap_or(Value::Null);
        self.save();
    }

    fn save(&self) {
        self.log.save(&self.record);
    }
}

fn close_phase(record: &mut Value, at: i64) {
    if let Some(last) = record["phases"].as_array_mut().and_then(|p| p.last_mut()) {
        if last["end_ms"].is_null() {
            last["end_ms"] = json!(at);
        }
    }
}

fn last_phase_start(record: &Value) -> Option<i64> {
    record["phases"].as_array()?.last()?["start_ms"].as_i64()
}

/// The engine error's kind, as `--summary-json` names it, or `other`.
pub fn error_kind(e: &anyhow::Error) -> &'static str {
    match e.downcast_ref::<ShapeError>() {
        Some(ShapeError::Spec(_)) => "spec",
        Some(ShapeError::Schema(_)) => "schema",
        Some(ShapeError::Record(_)) => "record",
        Some(ShapeError::Drift(_)) => "drift",
        Some(ShapeError::Arrow(_)) => "arrow",
        Some(ShapeError::Sink(_)) => "sink",
        Some(ShapeError::Conflict(_)) => "conflict",
        Some(ShapeError::Source(_)) => "source",
        None => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ss-runs-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn a_run_is_recorded_as_it_goes_and_read_back_by_the_next_process() {
        let dir = tmp("log");
        let log = RunLog::open(&dir).unwrap();
        let mut a = log.start(json!({ "dataset": "events" }), "shaping");
        assert!(
            a.id().starts_with("l-0001-") && a.id().len() == 15,
            "{}",
            a.id()
        );
        assert_eq!(log.list()[0]["outcome"], "running", "visible while it runs");
        a.phase("committing");
        a.succeed(json!({ "rows_out": 3, "earlier_attempt": false }));
        let b = log.start(json!({ "dataset": "orders" }), "shaping");
        b.fail(&anyhow::Error::from(ShapeError::Conflict("moved".into())));
        let c = log.start(json!({ "dataset": "x" }), "shaping");
        drop(c); // the process stops mid-run

        let runs = log.list();
        assert_eq!(runs.len(), 3);
        assert_eq!(
            (runs[1]["outcome"].clone(), runs[1]["exit"].clone()),
            (json!("conflict"), json!(75))
        );
        let phases = runs[2]["phases"].as_array().unwrap();
        assert_eq!(phases.len(), 2);
        assert!(phases.iter().all(|p| p["end_ms"].is_i64()));
        assert_eq!(runs[2]["summary"]["rows_out"], 3);

        // The next process reads it back: newest record per run, the unfinished one marked.
        let again = RunLog::open(&dir).unwrap();
        let runs = again.list();
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0]["outcome"], "interrupted");
        assert_eq!(runs[2]["outcome"], "ok");
        assert!(again
            .start(json!({}), "shaping")
            .id()
            .starts_with("l-0004-"));
        // Another log (a fresh data dir, another console) numbers from 1 again, but never
        // repeats an id.
        let other = RunLog::open(&tmp("log-other")).unwrap();
        assert_ne!(other.start(json!({}), "shaping").id(), runs[2]["id"]);
        let lines = std::fs::read_to_string(dir.join("runs.jsonl"))
            .unwrap()
            .lines()
            .count();
        assert_eq!(
            lines, 4,
            "reopening keeps one line per run (plus the new one)"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_log_keeps_its_newest_runs_and_any_still_running() {
        let dir = tmp("keep");
        let log = RunLog::open(&dir).unwrap();
        let first = log.start(json!({ "dataset": "long" }), "shaping");
        for _ in 0..(KEEP + SLACK + 50) {
            log.start(json!({}), "shaping")
                .succeed(json!({ "rows_out": 1 }));
        }
        let runs = log.list();
        assert!(runs.len() <= KEEP + SLACK, "{} runs kept", runs.len());
        assert!(
            runs.iter().any(|r| r["id"] == first.record["id"]),
            "a running run is never dropped"
        );
        let lines = std::fs::read_to_string(dir.join("runs.jsonl"))
            .unwrap()
            .lines()
            .count();
        assert!(lines <= 4 * KEEP + 1, "{lines} lines");
    }

    #[test]
    fn a_line_torn_mid_character_loses_only_itself() {
        let dir = tmp("torn");
        let good = json!({ "id": "l-0001-aaaaaaaa", "outcome": "ok", "dataset": "café" });
        let mut bytes = format!("{good}\n").into_bytes();
        bytes.extend_from_slice(b"{\"id\":\"l-0002-bbbbbbbb\",\"dataset\":\"caf\xc3");
        std::fs::write(dir.join("runs.jsonl"), bytes).unwrap();
        let log = RunLog::open(&dir).unwrap();
        let runs = log.list();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["dataset"], "café");
        assert!(log.start(json!({}), "shaping").id().starts_with("l-0002-"));
    }
}
