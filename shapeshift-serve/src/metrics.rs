//! The console's metrics (`serve --metrics-addr`): `GET /metrics` on a listener of its own, in
//! the Prometheus text format. Off unless the flag is given: no counter is kept otherwise.
//!
//! Counts that only the moment knows (runs as they end, alert deliveries, HTTP requests) are
//! kept here as they happen. Everything the console's stores already hold (datasets, the run
//! log, connectors' spools and counts, incidents) is read when the scrape asks, so it is what
//! the pages show.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use serde_json::Value;
use shapeshift_core::metrics::Registry;

use crate::datasets::conditions;
use crate::handlers::Ctx;
use crate::runs::now_ms;

const RUNS: &str = "shapeshift_runs_total";
const RUN_SECONDS: &str = "shapeshift_run_duration_seconds";
const ROWS: &str = "shapeshift_rows_total";
const PULLS: &str = "shapeshift_source_pulls_total";
const PULLED: &str = "shapeshift_source_records_total";
const DELIVERIES: &str = "shapeshift_alert_deliveries_total";
const EVALUATIONS: &str = "shapeshift_alert_evaluations_total";
const REQUESTS: &str = "shapeshift_http_requests_total";
/// The `dataset` label of a run posted to `/api/shape`.
const ADHOC: &str = "(adhoc)";

/// What the console counts while it runs.
pub struct Live {
    started: Instant,
    reg: Mutex<Registry>,
    /// A pulled dataset's position after its last pull, when the cursor is a time (Unix
    /// seconds).
    positions: Mutex<BTreeMap<String, f64>>,
}

impl Default for Live {
    fn default() -> Self {
        Live::new()
    }
}

impl Live {
    pub fn new() -> Live {
        let mut r = Registry::new();
        // Declared here once, with fixed names: a mistake is a test failure, not a scrape's.
        let declared = (|| -> shapeshift_core::Result<()> {
            r.counter(
                RUNS,
                "Runs that ended, by dataset, how they were started, and outcome.",
                &["dataset", "via", "outcome"],
            )?;
            r.histogram(
                RUN_SECONDS,
                "How long runs took, by how they were started.",
                &["via"],
            )?;
            r.counter(
                ROWS,
                "Rows runs read, wrote and rejected, by dataset.",
                &["dataset", "kind"],
            )?;
            r.counter(
                PULLS,
                "Pulls from a dataset's source, by result.",
                &["dataset", "kind", "result"],
            )?;
            r.counter(
                PULLED,
                "Records pulled from a dataset's source: new, and read again.",
                &["dataset", "kind"],
            )?;
            r.counter(
                DELIVERIES,
                "Alert notices sent to a target, by result.",
                &["result"],
            )?;
            r.counter(
                EVALUATIONS,
                "Times the alert rules were evaluated, by result.",
                &["result"],
            )?;
            r.counter(
                REQUESTS,
                "HTTP requests answered, by listener and status class.",
                &["listener", "class"],
            )?;
            Ok(())
        })();
        declared.expect("the console's metrics are declared once, consistently");
        Live {
            started: Instant::now(),
            reg: Mutex::new(r),
            positions: Mutex::new(BTreeMap::new()),
        }
    }

    fn with(&self, f: impl FnOnce(&mut Registry) -> shapeshift_core::Result<()>) {
        if let Ok(mut r) = self.reg.lock() {
            if let Err(e) = f(&mut r) {
                tracing::warn!(error = %e, "a metric was not recorded");
            }
        }
    }

    /// A run ended: `record` is its run-log record.
    pub fn run_ended(&self, record: &Value) {
        let via = record["via"].as_str().unwrap_or("");
        // A kept dataset's runs (`Run now`, a connector's batches) are labelled by its name. A
        // run posted to `/api/shape` names its dataset in the request, which anyone who reaches
        // the console writes: those share one label, so a caller cannot mint label values.
        let dataset = match via {
            "shape" => ADHOC,
            _ => record["dataset"].as_str().unwrap_or(""),
        };
        let outcome = record["outcome"].as_str().unwrap_or("");
        let took = match (record["started_ms"].as_i64(), record["ended_ms"].as_i64()) {
            (Some(s), Some(e)) if e >= s => Some((e - s) as f64 / 1000.0),
            _ => None,
        };
        let summary = &record["summary"];
        let source = &summary["source"];
        self.with(|r| {
            r.inc(RUNS, &[dataset, via, outcome], 1.0)?;
            if let Some(t) = took {
                r.observe(RUN_SECONDS, &[via], t)?;
            }
            for (field, kind) in [
                ("rows_in", "in"),
                ("rows_out", "out"),
                ("rejected", "rejected"),
            ] {
                if let Some(n) = summary[field].as_f64() {
                    r.inc(ROWS, &[dataset, kind], n)?;
                }
            }
            if let Some(kind) = source["kind"].as_str() {
                r.inc(PULLS, &[dataset, kind, "ok"], 1.0)?;
                for (field, k) in [("new", "new"), ("replayed", "replayed")] {
                    if let Some(n) = source[field].as_f64() {
                        r.inc(PULLED, &[dataset, k], n)?;
                    }
                }
            } else if matches!(outcome, "failed" | "interrupted") {
                // A pulled dataset's run that failed (in its pull, or after it): no position was
                // committed. A conflict is not one: the commit lost a race, not the pull.
                if let Some(kind) = record["source_kind"].as_str() {
                    r.inc(PULLS, &[dataset, kind, "failed"], 1.0)?;
                }
            }
            Ok(())
        });
        let at = source["position"]
            .get("at")
            .or_else(|| source["position"].get("inside"))
            .and_then(Value::as_str)
            .and_then(shapeshift_core::metrics::unix_seconds);
        if let Ok(mut p) = self.positions.lock() {
            match (at, source["kind"].is_string()) {
                (Some(t), _) => {
                    p.insert(dataset.to_string(), t);
                }
                // A pull whose position is no time: no time to show.
                (None, true) => {
                    p.remove(dataset);
                }
                _ => {}
            }
        }
    }

    /// The alert rules were evaluated (`ok`: without error), and these deliveries made
    /// (`true`: delivered).
    pub fn alerts_evaluated(&self, ok: bool, delivered: &[bool]) {
        self.with(|r| {
            r.inc(EVALUATIONS, &[if ok { "ok" } else { "failed" }], 1.0)?;
            for ok in delivered {
                r.inc(DELIVERIES, &[if *ok { "ok" } else { "failed" }], 1.0)?;
            }
            Ok(())
        });
    }

    /// A listener answered a request with `status`.
    pub fn answered(&self, listener: &str, status: u16) {
        let class = match status {
            100..=199 => "1xx",
            200..=299 => "2xx",
            300..=399 => "3xx",
            400..=499 => "4xx",
            _ => "5xx",
        };
        self.with(|r| r.inc(REQUESTS, &[listener, class], 1.0));
    }

    /// The exposition: the counts kept, and the stores read now. `None` when the counts cannot
    /// be read (a thread panicked holding them): a scrape must fail, not look empty.
    pub fn render(&self, ctx: &Ctx) -> Option<String> {
        let mut r = self.reg.lock().ok()?.clone();
        if let Err(e) = self.read_stores(ctx, &mut r) {
            tracing::warn!(error = %e, "the console's state was not all read for /metrics");
        }
        Some(r.render())
    }

    fn read_stores(&self, ctx: &Ctx, r: &mut Registry) -> shapeshift_core::Result<()> {
        let now = now_ms();
        let runs = ctx.runs().list();
        let datasets = ctx.datasets().list();

        r.gauge("shapeshift_runs_running", "Runs in progress.", &[])?;
        let running = runs.iter().filter(|x| x["outcome"] == "running").count();
        r.set("shapeshift_runs_running", &[], running as f64)?;

        r.gauge("shapeshift_datasets", "Datasets the console keeps.", &[])?;
        r.set("shapeshift_datasets", &[], datasets.len() as f64)?;
        r.gauge(
            "shapeshift_dataset_last_success_timestamp_seconds",
            "When a dataset's latest successful run ended, in Unix seconds (of the runs the log keeps).",
            &["dataset"],
        )?;
        r.gauge(
            "shapeshift_dataset_freshness_window_seconds",
            "A dataset's freshness window: no successful run within it is `table_stale`.",
            &["dataset"],
        )?;
        // Each dataset's latest successful end, in one pass (a run that began earlier can end
        // later, so the latest end, not the newest run).
        let mut last_ok: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
        for x in &runs {
            if !matches!(x["outcome"].as_str(), Some("ok" | "earlier")) {
                continue;
            }
            if let (Some(d), Some(t)) = (x["dataset"].as_str(), x["ended_ms"].as_i64()) {
                let e = last_ok.entry(d).or_insert(t);
                *e = (*e).max(t);
            }
        }
        for d in &datasets {
            if let Some(&t) = last_ok.get(d.name.as_str()) {
                r.set(
                    "shapeshift_dataset_last_success_timestamp_seconds",
                    &[&d.name],
                    t as f64 / 1000.0,
                )?;
            }
            if let Some(m) = d.freshness_minutes {
                r.set(
                    "shapeshift_dataset_freshness_window_seconds",
                    &[&d.name],
                    (m * 60) as f64,
                )?;
            }
        }
        r.gauge(
            "shapeshift_attention",
            "Conditions the Overview lists, by kind.",
            &["kind"],
        )?;
        let mut kinds: BTreeMap<String, u64> = ["run_failed", "run_conflict", "table_stale"]
            .iter()
            .map(|k| (k.to_string(), 0))
            .collect();
        for c in conditions(&datasets, &runs, now) {
            if let Some(k) = c["kind"].as_str() {
                *kinds.entry(k.to_string()).or_default() += 1;
            }
        }
        for (k, n) in &kinds {
            r.set("shapeshift_attention", &[k], *n as f64)?;
        }

        r.gauge(
            "shapeshift_alert_incidents_firing",
            "Alert incidents firing, by kind.",
            &["kind"],
        )?;
        for (k, n) in ctx.alerts().firing_by_kind() {
            r.set("shapeshift_alert_incidents_firing", &[&k], n as f64)?;
        }

        r.gauge(
            "shapeshift_source_position_timestamp_seconds",
            "A pulled dataset's position after its last pull, when the cursor is a time, in Unix seconds.",
            &["dataset"],
        )?;
        if let Ok(p) = self.positions.lock() {
            // A deleted dataset's position goes with it.
            for (d, t) in p
                .iter()
                .filter(|(d, _)| datasets.iter().any(|x| x.name == **d))
            {
                r.set("shapeshift_source_position_timestamp_seconds", &[d], *t)?;
            }
        }

        r.counter(
            "shapeshift_ingest_posts_total",
            "Posts to a connector, by result: accepted, duplicate (its Idempotency-Key was seen) or refused.",
            &["connector", "result"],
        )?;
        r.counter(
            "shapeshift_ingest_records_total",
            "Records connectors accepted.",
            &["connector"],
        )?;
        r.counter(
            "shapeshift_ingest_batch_runs_total",
            "Runs of a connector's sealed batches, by result.",
            &["connector", "result"],
        )?;
        r.gauge(
            "shapeshift_ingest_spool_bytes",
            "Bytes a connector holds: its open spool and its sealed batches.",
            &["connector"],
        )?;
        r.gauge(
            "shapeshift_ingest_waiting_batches",
            "Sealed batches a connector has waiting to run.",
            &["connector"],
        )?;
        r.gauge(
            "shapeshift_ingest_oldest_record_age_seconds",
            "How long the oldest record in a connector's open spool has waited.",
            &["connector"],
        )?;
        for c in ctx.connectors().metric_rows() {
            let n = c.name.as_str();
            let s = &c.stats;
            // `posts` counts the accepted ones: a duplicate is answered before it is counted.
            let accepted = s.posts;
            r.inc(
                "shapeshift_ingest_posts_total",
                &[n, "accepted"],
                accepted as f64,
            )?;
            r.inc(
                "shapeshift_ingest_posts_total",
                &[n, "duplicate"],
                s.duplicates as f64,
            )?;
            r.inc(
                "shapeshift_ingest_posts_total",
                &[n, "refused"],
                s.refused as f64,
            )?;
            r.inc("shapeshift_ingest_records_total", &[n], s.records as f64)?;
            r.inc(
                "shapeshift_ingest_batch_runs_total",
                &[n, "ok"],
                s.runs_ok as f64,
            )?;
            r.inc(
                "shapeshift_ingest_batch_runs_total",
                &[n, "failed"],
                s.runs_failed as f64,
            )?;
            r.set(
                "shapeshift_ingest_spool_bytes",
                &[n],
                (c.spool_bytes + c.waiting_bytes) as f64,
            )?;
            r.set(
                "shapeshift_ingest_waiting_batches",
                &[n],
                c.waiting_batches as f64,
            )?;
            let age = c
                .oldest_ms
                .map_or(0.0, |t| (now - t).max(0) as f64 / 1000.0);
            r.set("shapeshift_ingest_oldest_record_age_seconds", &[n], age)?;
        }

        r.gauge(
            "shapeshift_uptime_seconds",
            "How long this console has been up.",
            &[],
        )?;
        r.set(
            "shapeshift_uptime_seconds",
            &[],
            self.started.elapsed().as_secs_f64(),
        )?;
        r.gauge(
            "shapeshift_build_info",
            "The shapeshift build serving these metrics.",
            &["version", "features"],
        )?;
        r.set(
            "shapeshift_build_info",
            &[env!("CARGO_PKG_VERSION"), &features()],
            1.0,
        )?;
        Ok(())
    }
}

/// The source and catalog features this console was built with, comma-separated.
fn features() -> String {
    let all: [(&str, bool); 4] = [
        ("catalog", cfg!(feature = "catalog")),
        ("http", cfg!(feature = "http")),
        ("mysql", cfg!(feature = "mysql")),
        ("postgres", cfg!(feature = "postgres")),
    ];
    all.iter()
        .filter(|(_, on)| *on)
        .map(|(n, _)| *n)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_run_that_ends_is_counted_by_dataset_way_and_outcome() {
        let live = Live::new();
        live.run_ended(&json!({
            "dataset": "orders", "via": "dataset", "outcome": "ok",
            "started_ms": 1_000, "ended_ms": 1_400,
            "summary": { "rows_in": 10, "rows_out": 9, "rejected": 1,
                "source": { "kind": "http", "new": 7, "replayed": 2,
                    "position": { "at": "2026-11-01T00:00:00Z" } } },
        }));
        live.run_ended(&json!({
            "dataset": "orders", "via": "dataset", "outcome": "failed",
            "started_ms": 2_000, "ended_ms": 2_100, "summary": null,
        }));
        let t = live.reg.lock().unwrap().render();
        assert!(
            t.contains(
                "shapeshift_runs_total{dataset=\"orders\",via=\"dataset\",outcome=\"ok\"} 1\n"
            ),
            "{t}"
        );
        assert!(
            t.contains(
                "shapeshift_runs_total{dataset=\"orders\",via=\"dataset\",outcome=\"failed\"} 1\n"
            ),
            "{t}"
        );
        assert!(
            t.contains("shapeshift_rows_total{dataset=\"orders\",kind=\"out\"} 9\n"),
            "{t}"
        );
        assert!(
            t.contains("shapeshift_run_duration_seconds_count{via=\"dataset\"} 2\n"),
            "{t}"
        );
        assert!(
            t.contains("shapeshift_run_duration_seconds_bucket{via=\"dataset\",le=\"0.5\"} 2\n"),
            "{t}"
        );
        assert!(
            t.contains(
                "shapeshift_source_pulls_total{dataset=\"orders\",kind=\"http\",result=\"ok\"} 1\n"
            ),
            "{t}"
        );
        assert!(
            t.contains("shapeshift_source_records_total{dataset=\"orders\",kind=\"new\"} 7\n"),
            "{t}"
        );
        // A run posted to /api/shape is labelled `(adhoc)`, whatever dataset it names.
        live.run_ended(
            &json!({ "dataset": "anything", "via": "shape", "outcome": "ok",
            "started_ms": 1, "ended_ms": 2, "summary": { "rows_out": 1 } }),
        );
        // A conflict is no failed pull; a failure is.
        live.run_ended(
            &json!({ "dataset": "orders", "via": "dataset", "outcome": "conflict",
            "source_kind": "http", "summary": null }),
        );
        live.run_ended(
            &json!({ "dataset": "orders", "via": "dataset", "outcome": "failed",
            "source_kind": "http", "summary": null }),
        );
        let t = live.reg.lock().unwrap().render();
        assert!(
            t.contains(
                "shapeshift_runs_total{dataset=\"(adhoc)\",via=\"shape\",outcome=\"ok\"} 1\n"
            ),
            "{t}"
        );
        assert!(!t.contains("anything"), "{t}");
        assert!(t.contains("shapeshift_source_pulls_total{dataset=\"orders\",kind=\"http\",result=\"failed\"} 1\n"), "{t}");
        assert_eq!(
            live.positions.lock().unwrap().get("orders"),
            Some(&1_793_491_200.0)
        );
    }

    #[test]
    fn the_features_label_is_the_build_s() {
        let f = features();
        for (name, on) in [
            ("catalog", cfg!(feature = "catalog")),
            ("http", cfg!(feature = "http")),
            ("mysql", cfg!(feature = "mysql")),
            ("postgres", cfg!(feature = "postgres")),
        ] {
            assert_eq!(f.split(',').any(|x| x == name), on, "{name} in {f:?}");
        }
    }

    #[test]
    fn requests_are_counted_by_status_class() {
        let live = Live::new();
        live.answered("console", 200);
        live.answered("console", 404);
        live.answered("ingest", 503);
        let t = live.reg.lock().unwrap().render();
        assert!(
            t.contains("shapeshift_http_requests_total{listener=\"console\",class=\"2xx\"} 1\n"),
            "{t}"
        );
        assert!(
            t.contains("shapeshift_http_requests_total{listener=\"console\",class=\"4xx\"} 1\n"),
            "{t}"
        );
        assert!(
            t.contains("shapeshift_http_requests_total{listener=\"ingest\",class=\"5xx\"} 1\n"),
            "{t}"
        );
    }
}
