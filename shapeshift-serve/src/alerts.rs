//! Alerts: rules over what needs a look, and webhooks to tell.
//!
//! An **alert rule** picks a condition, the datasets it watches (all, or one), and the
//! **targets** to tell. The conditions are the overview's ([`crate::datasets::conditions`]):
//!
//! - `run_failed` — a dataset's latest finished run failed (or was interrupted);
//! - `run_conflict` — it lost a race to another writer (exit 75);
//! - `table_stale` — no successful run within the dataset's freshness window;
//! - `rejects_over` — its latest run rejected more rows than the rule's threshold.
//!
//! Each subject (a dataset) that meets a rule's condition is an **incident**. It notifies once
//! when it opens (`firing`) and once when it resolves, never on every evaluation. An incident
//! resolved by hand while its condition still holds stays quiet until the condition clears.
//!
//! A **target** is a webhook. The console POSTs JSON with a `text` field, so a Slack or Teams
//! incoming webhook, or a paging service's, takes it as is. The details are under
//! `shapeshift`. HTTPS uses rustls with Mozilla's roots bundled, so no CA store is needed on
//! the box. Each target records how its last delivery went, and has a test.
//!
//! Rules, targets and incidents are one file, `alerts.json` in the data dir. A webhook URL is
//! a secret (anyone holding a Slack URL can post to the channel): the file is written owner-only
//! (0600) on Unix, it is **not encrypted**, and the API never returns a URL after it is saved.
//!
//! Rules are evaluated after every run and once a minute (for `table_stale`), on a thread of
//! their own; deliveries happen there too, never on a request.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::datasets::{conditions, Dataset};
use crate::handlers::{parse_body, Ctx};
use crate::http::{Request, Response};
use crate::runs::now_ms;

/// The conditions a rule can watch.
pub const KINDS: &[&str] = &["run_failed", "run_conflict", "table_stale", "rejects_over"];
/// Target kinds. Both get the same JSON; `slack` is a label for the list.
pub const TARGET_KINDS: &[&str] = &["webhook", "slack"];
/// Resolved incidents kept (firing ones are always kept).
const RESOLVED_KEPT: usize = 200;
/// How often rules are evaluated when nothing pokes them.
const TICK: Duration = Duration::from_secs(60);
/// A delivery that takes longer than this has failed.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub name: String,
    pub kind: String,
    /// One dataset, or every dataset when absent.
    #[serde(default)]
    pub dataset: Option<String>,
    /// `rejects_over`: more rejected rows than this in one run (default 0: any).
    #[serde(default)]
    pub threshold: Option<u64>,
    #[serde(default)]
    pub targets: Vec<String>,
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default)]
    pub created_ms: i64,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delivery {
    pub at_ms: i64,
    pub ok: bool,
    /// `firing`, `resolved` or `test`.
    pub event: String,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    pub id: String,
    pub name: String,
    pub kind: String,
    /// Never sent back to the browser: see [`Target::view`].
    pub url: String,
    #[serde(default)]
    pub last: Option<Delivery>,
    #[serde(default)]
    pub created_ms: i64,
}

impl Target {
    /// What the console shows: the URL as scheme and host only.
    fn view(&self) -> Value {
        json!({
            "id": self.id, "name": self.name, "kind": self.kind,
            "url": masked(&self.url), "last": self.last, "created_ms": self.created_ms,
        })
    }
}

/// `https://hooks.slack.com/…`: enough to recognise a target, nothing that lets anyone post.
fn masked(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = host.rsplit('@').next().unwrap_or(host);
    format!("{scheme}://{host}/…")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub rule_id: String,
    pub rule_name: String,
    pub kind: String,
    pub subject: String,
    pub severity: String,
    pub message: String,
    /// `firing` or `resolved`.
    pub state: String,
    pub opened_ms: i64,
    #[serde(default)]
    pub resolved_ms: Option<i64>,
    /// `condition` (it cleared), `hand`, or `rule` (the rule was disabled or deleted).
    #[serde(default)]
    pub resolved_by: Option<String>,
    #[serde(default)]
    pub link: Value,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    rules: Vec<Rule>,
    #[serde(default)]
    targets: Vec<Target>,
    #[serde(default)]
    incidents: Vec<Incident>,
    /// `rule_id/subject` resolved by hand while still true: quiet until the condition clears.
    #[serde(default)]
    held: Vec<String>,
}

/// A notice to send: the payload, and the targets to send it to.
struct Notice {
    event: String,
    targets: Vec<String>,
    payload: Value,
}

pub struct Alerts {
    path: PathBuf,
    state: Mutex<State>,
    poke: (Mutex<bool>, Condvar),
}

fn new_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

impl Alerts {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("alerts.json");
        let state = match std::fs::read(&path) {
            Ok(b) => serde_json::from_slice(&b)
                .with_context(|| format!("{} does not parse", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        Ok(Alerts {
            path,
            state: Mutex::new(state),
            poke: (Mutex::new(false), Condvar::new()),
        })
    }

    /// Write the state, owner-only on Unix (it holds webhook URLs).
    fn persist(&self, st: &State) -> Result<()> {
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(st)?;
        {
            use std::io::Write as _;
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                opts.mode(0o600);
            }
            let mut f = opts
                .open(&tmp)
                .with_context(|| format!("writing {}", tmp.display()))?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("replacing {}", self.path.display()))?;
        Ok(())
    }

    /// Ask the evaluator to run now (after a run, or a rule changed).
    pub fn poke(&self) {
        let (m, cv) = &self.poke;
        *m.lock().expect("alerts") = true;
        cv.notify_one();
    }

    /// Wait until poked or [`TICK`] passes.
    fn wait(&self) {
        let (m, cv) = &self.poke;
        let mut poked = m.lock().expect("alerts");
        if !*poked {
            poked = cv.wait_timeout(poked, TICK).expect("alerts").0;
        }
        *poked = false;
    }

    /// Evaluate every rule against `found` (the conditions true now), open and resolve
    /// incidents, and answer the notices to send.
    fn evaluate(&self, datasets: &[Dataset], runs: &[Value], now: i64) -> Result<Vec<Notice>> {
        let base = conditions(datasets, runs, now);
        let mut st = self.state.lock().expect("alerts");
        let mut notices = Vec::new();
        let mut changed = false;
        let rules = st.rules.clone();
        let mut still_held = HashSet::new();
        for rule in &rules {
            let firing: Vec<usize> = st
                .incidents
                .iter()
                .enumerate()
                .filter(|(_, i)| i.rule_id == rule.id && i.state == "firing")
                .map(|(n, _)| n)
                .collect();
            if !rule.enabled {
                for n in firing {
                    resolve(&mut st.incidents[n], now, "rule");
                    changed = true;
                }
                continue;
            }
            let found: Vec<Value> = if rule.kind == "rejects_over" {
                rejects_over(datasets, runs, rule.threshold.unwrap_or(0))
            } else {
                base.iter()
                    .filter(|c| c["kind"] == rule.kind.as_str())
                    .cloned()
                    .collect()
            }
            .into_iter()
            .filter(|c| rule.dataset.as_deref().map_or(true, |d| c["subject"] == d))
            .collect();
            let now_true: HashSet<&str> =
                found.iter().filter_map(|c| c["subject"].as_str()).collect();
            // Resolve what cleared.
            for n in firing {
                if !now_true.contains(st.incidents[n].subject.as_str()) {
                    resolve(&mut st.incidents[n], now, "condition");
                    notices.push(notice(rule, &st.incidents[n]));
                    changed = true;
                }
            }
            // Open what is new.
            for c in &found {
                let subject = c["subject"].as_str().unwrap_or_default();
                let key = format!("{}/{subject}", rule.id);
                if st.held.contains(&key) {
                    still_held.insert(key);
                    continue;
                }
                let open = st
                    .incidents
                    .iter()
                    .any(|i| i.rule_id == rule.id && i.subject == subject && i.state == "firing");
                if open {
                    continue;
                }
                let inc = Incident {
                    id: new_id("i"),
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    kind: rule.kind.clone(),
                    subject: subject.to_string(),
                    severity: c["severity"].as_str().unwrap_or("warning").to_string(),
                    message: c["message"].as_str().unwrap_or_default().to_string(),
                    state: "firing".into(),
                    opened_ms: now,
                    resolved_ms: None,
                    resolved_by: None,
                    link: c["link"].clone(),
                };
                notices.push(notice(rule, &inc));
                st.incidents.push(inc);
                changed = true;
            }
        }
        // A hold ends when its condition clears (or its rule goes).
        let before = st.held.len();
        st.held.retain(|k| still_held.contains(k));
        changed |= st.held.len() != before;
        if changed {
            trim(&mut st.incidents);
            self.persist(&st)?;
        }
        Ok(notices)
    }

    /// Send each notice to its targets; answers how each delivery went (`true`: delivered).
    fn deliver(&self, notices: Vec<Notice>, send: &dyn Fn(&str, &Value) -> Delivery) -> Vec<bool> {
        let mut outcomes = Vec::new();
        for n in notices {
            let targets: Vec<(String, String)> = {
                let st = self.state.lock().expect("alerts");
                st.targets
                    .iter()
                    .filter(|t| n.targets.contains(&t.id))
                    .map(|t| (t.id.clone(), t.url.clone()))
                    .collect()
            };
            for (id, url) in targets {
                let mut d = send(&url, &n.payload);
                d.event = n.event.clone();
                outcomes.push(d.ok);
                self.record(&id, d);
            }
        }
        outcomes
    }

    /// Incidents firing now, by kind.
    pub(crate) fn firing_by_kind(&self) -> std::collections::BTreeMap<String, u64> {
        let st = self.state.lock().expect("alerts");
        let mut out = std::collections::BTreeMap::new();
        for i in st.incidents.iter().filter(|i| i.state == "firing") {
            *out.entry(i.kind.clone()).or_default() += 1;
        }
        out
    }

    fn record(&self, target: &str, d: Delivery) {
        let mut st = self.state.lock().expect("alerts");
        if let Some(t) = st.targets.iter_mut().find(|t| t.id == target) {
            if !d.ok {
                tracing::warn!(target = %t.name, error = ?d.error, status = ?d.status, "alert delivery failed");
            }
            t.last = Some(d);
        }
        if let Err(e) = self.persist(&st) {
            tracing::warn!(error = %e, "could not record an alert delivery");
        }
    }
}

fn resolve(i: &mut Incident, now: i64, by: &str) {
    i.state = "resolved".into();
    i.resolved_ms = Some(now);
    i.resolved_by = Some(by.into());
}

/// Keep every firing incident and the newest [`RESOLVED_KEPT`] resolved ones.
fn trim(incidents: &mut Vec<Incident>) {
    let resolved = incidents.iter().filter(|i| i.state != "firing").count();
    let mut drop = resolved.saturating_sub(RESOLVED_KEPT);
    if drop == 0 {
        return;
    }
    // Oldest first in the vector: drop from the front.
    incidents.retain(|i| {
        if drop > 0 && i.state != "firing" {
            drop -= 1;
            false
        } else {
            true
        }
    });
}

/// Datasets whose latest finished run rejected more than `threshold` rows.
fn rejects_over(datasets: &[Dataset], runs: &[Value], threshold: u64) -> Vec<Value> {
    let mut out = Vec::new();
    for d in datasets {
        let Some(r) = runs
            .iter()
            .find(|r| r["dataset"] == d.name.as_str() && r["outcome"] != "running")
        else {
            continue;
        };
        let s = &r["summary"];
        let n = s["rejected"].as_u64().unwrap_or(0) + s["parse_errors"].as_u64().unwrap_or(0);
        if n > threshold {
            out.push(json!({
                "kind": "rejects_over", "severity": "warning", "subject": d.name,
                "message": format!("its latest run {} rejected {n} row{} (more than {threshold})",
                    r["id"].as_str().unwrap_or(""), if n == 1 { "" } else { "s" }),
                "since_ms": r["ended_ms"], "link": { "dataset": d.name, "run": r["id"] },
            }));
        }
    }
    out
}

/// What a target receives for an incident: `text` for Slack/Teams, details under `shapeshift`.
fn notice(rule: &Rule, i: &Incident) -> Notice {
    let head = if i.state == "firing" {
        "FIRING"
    } else {
        "RESOLVED"
    };
    Notice {
        event: i.state.clone(),
        targets: rule.targets.clone(),
        payload: json!({
            "text": if i.state == "firing" {
                format!("[shapeshift] {head} {}: {} — {}", rule.name, i.subject, i.message)
            } else {
                format!("[shapeshift] {head} {}: {} is fine again (it was: {})", rule.name, i.subject, i.message)
            },
            "shapeshift": {
                "state": i.state,
                "severity": i.severity,
                "rule": { "id": rule.id, "name": rule.name, "kind": rule.kind },
                "subject": i.subject,
                "message": i.message,
                "incident_id": i.id,
                "opened_ms": i.opened_ms,
                "resolved_ms": i.resolved_ms,
                "link": i.link,
            }
        }),
    }
}

/// POST `payload` to `url`. Any 2xx is delivered; anything else, or no answer, is not.
pub fn send(url: &str, payload: &Value) -> Delivery {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(DELIVERY_TIMEOUT))
        .http_status_as_error(false)
        .build()
        .into();
    let at_ms = now_ms();
    match agent
        .post(url)
        .content_type("application/json")
        .send(payload.to_string())
    {
        Ok(resp) => {
            let status = resp.status().as_u16();
            Delivery {
                at_ms,
                ok: (200..300).contains(&status),
                event: String::new(),
                status: Some(status),
                error: (!(200..300).contains(&status)).then(|| format!("answered {status}")),
            }
        }
        Err(e) => Delivery {
            at_ms,
            ok: false,
            event: String::new(),
            status: None,
            error: Some(e.to_string()),
        },
    }
}

/// The evaluator: after every run, and once a minute.
pub fn spawn_evaluator(ctx: Arc<Ctx>) {
    std::thread::Builder::new()
        .name("alerts".into())
        .spawn(move || loop {
            let runs = ctx.runs().list();
            let datasets = ctx.datasets().list();
            match ctx.alerts().evaluate(&datasets, &runs, now_ms()) {
                Ok(n) => {
                    let delivered = ctx.alerts().deliver(n, &send);
                    if let Some(m) = ctx.metrics() {
                        m.alerts_evaluated(true, &delivered);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "alert evaluation failed");
                    if let Some(m) = ctx.metrics() {
                        m.alerts_evaluated(false, &[]);
                    }
                }
            }
            ctx.alerts().wait();
        })
        .expect("starting the alerts thread");
}

// ---- handlers -------------------------------------------------------------------

/// `GET /api/alerts`: rules, targets (URLs masked), incidents (firing first, then newest).
pub fn list(ctx: &Ctx) -> Response {
    let st = ctx.alerts().state.lock().expect("alerts");
    let mut incidents: Vec<&Incident> = st.incidents.iter().collect();
    incidents.sort_by_key(|i| (i.state != "firing", std::cmp::Reverse(i.opened_ms)));
    Response::json(
        200,
        &json!({
            "rules": st.rules,
            "targets": st.targets.iter().map(Target::view).collect::<Vec<_>>(),
            "incidents": incidents,
            "kinds": KINDS,
            "target_kinds": TARGET_KINDS,
        }),
    )
}

#[derive(Deserialize)]
struct RuleReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    kind: String,
    #[serde(default)]
    dataset: Option<String>,
    #[serde(default)]
    threshold: Option<u64>,
    #[serde(default)]
    targets: Vec<String>,
    #[serde(default = "yes")]
    enabled: bool,
}

fn save_rule_in(alerts: &Alerts, r: RuleReq) -> Result<Rule> {
    let name = r.name.trim().to_string();
    if name.is_empty() || name.len() > 100 {
        bail!("a rule needs a name (up to 100 characters)");
    }
    if !KINDS.contains(&r.kind.as_str()) {
        bail!(
            "`{}` is not a condition: one of {}",
            r.kind,
            KINDS.join(", ")
        );
    }
    if r.threshold.is_some() && r.kind != "rejects_over" {
        bail!("a threshold applies only to `rejects_over`");
    }
    let mut st = alerts.state.lock().expect("alerts");
    for t in &r.targets {
        if !st.targets.iter().any(|x| &x.id == t) {
            bail!("no target `{t}`");
        }
    }
    let dataset = r
        .dataset
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty());
    let rule = match r.id {
        Some(id) => {
            let Some(old) = st.rules.iter_mut().find(|x| x.id == id) else {
                bail!("no rule `{id}`");
            };
            // A rule that now watches something else starts again: its incidents resolve.
            if old.kind != r.kind || old.dataset != dataset {
                let rid = old.id.clone();
                for i in st
                    .incidents
                    .iter_mut()
                    .filter(|i| i.rule_id == rid && i.state == "firing")
                {
                    resolve(i, now_ms(), "rule");
                }
                st.held.retain(|k| !k.starts_with(&format!("{rid}/")));
            }
            let old = st
                .rules
                .iter_mut()
                .find(|x| x.id == id)
                .expect("found above");
            old.name = name;
            old.kind = r.kind;
            old.dataset = dataset;
            old.threshold = r.threshold;
            old.targets = r.targets;
            old.enabled = r.enabled;
            old.clone()
        }
        None => {
            let rule = Rule {
                id: new_id("r"),
                name,
                kind: r.kind,
                dataset,
                threshold: r.threshold,
                targets: r.targets,
                enabled: r.enabled,
                created_ms: now_ms(),
            };
            st.rules.push(rule.clone());
            rule
        }
    };
    alerts.persist(&st)?;
    Ok(rule)
}

/// `POST /api/alerts/rule/save`: create (no `id`) or replace a rule.
pub fn save_rule(ctx: &Ctx, req: &Request) -> Response {
    let r: RuleReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match save_rule_in(ctx.alerts(), r) {
        Ok(rule) => {
            ctx.alerts().poke();
            Response::json(200, &json!({ "rule": rule }))
        }
        Err(e) => Response::error(400, format!("{e:#}")),
    }
}

#[derive(Deserialize)]
struct IdReq {
    id: String,
}

/// `POST /api/alerts/rule/delete`: a deleted rule's firing incidents resolve, quietly.
pub fn delete_rule(ctx: &Ctx, req: &Request) -> Response {
    let r: IdReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let alerts = ctx.alerts();
    let mut st = alerts.state.lock().expect("alerts");
    let before = st.rules.len();
    st.rules.retain(|x| x.id != r.id);
    if st.rules.len() == before {
        return Response::error(404, format!("no rule `{}`", r.id));
    }
    let now = now_ms();
    for i in st
        .incidents
        .iter_mut()
        .filter(|i| i.rule_id == r.id && i.state == "firing")
    {
        resolve(i, now, "rule");
    }
    st.held.retain(|k| !k.starts_with(&format!("{}/", r.id)));
    match alerts.persist(&st) {
        Ok(()) => Response::json(200, &json!({ "deleted": r.id })),
        Err(e) => Response::error(500, format!("{e:#}")),
    }
}

#[derive(Deserialize)]
struct TargetReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default = "webhook")]
    kind: String,
    /// Required for a new target; absent on an edit keeps the saved one.
    #[serde(default)]
    url: Option<String>,
}

fn webhook() -> String {
    "webhook".into()
}

fn check_url(url: &str) -> Result<()> {
    let ok_scheme = url.starts_with("https://") || url.starts_with("http://");
    if !ok_scheme || url.len() > 2048 || url.chars().any(char::is_whitespace) {
        bail!("a target is an http:// or https:// URL");
    }
    Ok(())
}

fn save_target_in(alerts: &Alerts, r: TargetReq) -> Result<Target> {
    let name = r.name.trim().to_string();
    if name.is_empty() || name.len() > 100 {
        bail!("a target needs a name (up to 100 characters)");
    }
    if !TARGET_KINDS.contains(&r.kind.as_str()) {
        bail!(
            "`{}` is not a target kind: one of {}",
            r.kind,
            TARGET_KINDS.join(", ")
        );
    }
    let url = r
        .url
        .map(|u| u.trim().to_string())
        .filter(|u| !u.is_empty());
    if let Some(u) = &url {
        check_url(u)?;
    }
    let mut st = alerts.state.lock().expect("alerts");
    let target = match r.id {
        Some(id) => {
            let Some(t) = st.targets.iter_mut().find(|t| t.id == id) else {
                bail!("no target `{id}`");
            };
            t.name = name;
            t.kind = r.kind;
            if let Some(u) = url {
                t.url = u;
                t.last = None;
            }
            t.clone()
        }
        None => {
            let Some(url) = url else {
                bail!("a new target needs its URL");
            };
            let t = Target {
                id: new_id("t"),
                name,
                kind: r.kind,
                url,
                last: None,
                created_ms: now_ms(),
            };
            st.targets.push(t.clone());
            t
        }
    };
    alerts.persist(&st)?;
    Ok(target)
}

/// `POST /api/alerts/target/save`: create (no `id`) or edit a target. The URL is never echoed.
pub fn save_target(ctx: &Ctx, req: &Request) -> Response {
    let r: TargetReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match save_target_in(ctx.alerts(), r) {
        Ok(t) => Response::json(200, &json!({ "target": t.view() })),
        Err(e) => Response::error(400, format!("{e:#}")),
    }
}

/// `POST /api/alerts/target/delete`: refused while a rule still tells it.
pub fn delete_target(ctx: &Ctx, req: &Request) -> Response {
    let r: IdReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let alerts = ctx.alerts();
    let mut st = alerts.state.lock().expect("alerts");
    let users: Vec<String> = st
        .rules
        .iter()
        .filter(|x| x.targets.contains(&r.id))
        .map(|x| x.name.clone())
        .collect();
    if !users.is_empty() {
        return Response::error(
            409,
            format!("rules still tell this target: {}", users.join(", ")),
        );
    }
    let before = st.targets.len();
    st.targets.retain(|t| t.id != r.id);
    if st.targets.len() == before {
        return Response::error(404, format!("no target `{}`", r.id));
    }
    match alerts.persist(&st) {
        Ok(()) => Response::json(200, &json!({ "deleted": r.id })),
        Err(e) => Response::error(500, format!("{e:#}")),
    }
}

/// `POST /api/alerts/target/test`: send a test notice now and answer how it went.
pub fn test_target(ctx: &Ctx, req: &Request) -> Response {
    let r: IdReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let url = {
        let st = ctx.alerts().state.lock().expect("alerts");
        match st.targets.iter().find(|t| t.id == r.id) {
            Some(t) => t.url.clone(),
            None => return Response::error(404, format!("no target `{}`", r.id)),
        }
    };
    let payload = json!({
        "text": "[shapeshift] Test: this target gets shapeshift's alerts.",
        "shapeshift": { "state": "test" },
    });
    let mut d = send(&url, &payload);
    d.event = "test".into();
    ctx.alerts().record(&r.id, d.clone());
    Response::json(200, &json!({ "delivery": d }))
}

/// `POST /api/alerts/incident/resolve`: resolve by hand. If its condition still holds, the
/// incident stays quiet until it clears (then a new one can open).
pub fn resolve_incident(ctx: &Ctx, req: &Request) -> Response {
    let r: IdReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let alerts = ctx.alerts();
    let mut st = alerts.state.lock().expect("alerts");
    let Some(i) = st.incidents.iter_mut().find(|i| i.id == r.id) else {
        return Response::error(404, format!("no incident `{}`", r.id));
    };
    if i.state != "firing" {
        return Response::error(409, "that incident is resolved already");
    }
    resolve(i, now_ms(), "hand");
    let key = format!("{}/{}", i.rule_id, i.subject);
    if !st.held.contains(&key) {
        st.held.push(key);
    }
    match alerts.persist(&st) {
        Ok(()) => {
            drop(st);
            alerts.poke();
            Response::json(200, &json!({ "resolved": r.id }))
        }
        Err(e) => Response::error(500, format!("{e:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shapeshift_core::{Compression, OutputFormat, SourceFormat};

    fn ds(name: &str) -> Dataset {
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
            freshness_minutes: None,
            created_ms: 0,
            updated_ms: 0,
        }
    }

    fn run(id: &str, dataset: &str, outcome: &str, rejected: u64) -> Value {
        json!({ "id": id, "dataset": dataset, "outcome": outcome, "started_ms": 1, "ended_ms": 2,
                "error": null, "summary": { "rejected": rejected, "parse_errors": 0 } })
    }

    fn store() -> (Alerts, PathBuf) {
        let dir = std::env::temp_dir().join(format!("ss-alerts-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        (Alerts::open(&dir).unwrap(), dir)
    }

    fn target(a: &Alerts) -> String {
        save_target_in(
            a,
            TargetReq {
                id: None,
                name: "hook".into(),
                kind: "webhook".into(),
                url: Some("https://example.invalid/x/secret".into()),
            },
        )
        .unwrap()
        .id
    }

    fn rule(a: &Alerts, kind: &str, t: &str, threshold: Option<u64>) -> Rule {
        save_rule_in(
            a,
            RuleReq {
                id: None,
                name: kind.into(),
                kind: kind.into(),
                dataset: None,
                threshold,
                targets: vec![t.into()],
                enabled: true,
            },
        )
        .unwrap()
    }

    fn events(n: &[Notice]) -> Vec<String> {
        n.iter()
            .map(|n| {
                format!(
                    "{} {}",
                    n.event,
                    n.payload["shapeshift"]["subject"].as_str().unwrap()
                )
            })
            .collect()
    }

    #[test]
    fn an_incident_notifies_once_when_it_opens_and_once_when_it_resolves() {
        let (a, dir) = store();
        let t = target(&a);
        rule(&a, "run_failed", &t, None);
        let d = [ds("a"), ds("b")];

        let failed = [run("l-2", "a", "failed", 0), run("l-1", "b", "ok", 0)];
        assert_eq!(
            events(&a.evaluate(&d, &failed, 10).unwrap()),
            vec!["firing a"]
        );
        // Still failing: nothing new.
        assert!(a.evaluate(&d, &failed, 20).unwrap().is_empty());
        // It recovers: resolved, once.
        let recovered = [run("l-3", "a", "ok", 0), run("l-2", "a", "failed", 0)];
        assert_eq!(
            events(&a.evaluate(&d, &recovered, 30).unwrap()),
            vec!["resolved a"]
        );
        assert!(a.evaluate(&d, &recovered, 40).unwrap().is_empty());

        // Survives a restart: the state is the file.
        let again = Alerts::open(&dir).unwrap();
        let st = again.state.lock().unwrap();
        assert_eq!(st.incidents.len(), 1);
        assert_eq!(st.incidents[0].state, "resolved");
        assert_eq!(st.incidents[0].resolved_by.as_deref(), Some("condition"));
        drop(st);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn resolved_by_hand_stays_quiet_until_the_condition_clears() {
        let (a, dir) = store();
        let t = target(&a);
        rule(&a, "run_failed", &t, None);
        let d = [ds("a")];
        let failed = [run("l-1", "a", "failed", 0)];
        a.evaluate(&d, &failed, 10).unwrap();
        let id = a.state.lock().unwrap().incidents[0].id.clone();
        // By hand, as the handler does.
        {
            let mut st = a.state.lock().unwrap();
            let key = format!("{}/a", st.incidents[0].rule_id);
            resolve(&mut st.incidents[0], 11, "hand");
            st.held.push(key);
        }
        assert!(
            a.evaluate(&d, &failed, 20).unwrap().is_empty(),
            "held while still failing"
        );
        let ok = [run("l-2", "a", "ok", 0), run("l-1", "a", "failed", 0)];
        assert!(
            a.evaluate(&d, &ok, 30).unwrap().is_empty(),
            "cleared: the hold ends, nothing to say"
        );
        assert!(a.state.lock().unwrap().held.is_empty());
        let failed_again = [run("l-3", "a", "failed", 0), run("l-2", "a", "ok", 0)];
        let n = a.evaluate(&d, &failed_again, 40).unwrap();
        assert_eq!(events(&n), vec!["firing a"]);
        assert_ne!(
            n[0].payload["shapeshift"]["incident_id"],
            json!(id),
            "a new incident"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_over_uses_the_rules_threshold_and_dataset_filters() {
        let (a, dir) = store();
        let t = target(&a);
        rule(&a, "rejects_over", &t, Some(5));
        save_rule_in(
            &a,
            RuleReq {
                id: None,
                name: "only b".into(),
                kind: "rejects_over".into(),
                dataset: Some("b".into()),
                threshold: None,
                targets: vec![t.clone()],
                enabled: true,
            },
        )
        .unwrap();
        let d = [ds("a"), ds("b")];
        let runs = [run("l-2", "a", "ok", 6), run("l-1", "b", "ok", 5)];
        let mut got = events(&a.evaluate(&d, &runs, 10).unwrap());
        got.sort();
        // a: 6 > 5 for the first rule; b: 5 > 0 for the second (any reject), not for the first.
        assert_eq!(got, vec!["firing a", "firing b"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_disabled_rule_resolves_quietly_and_targets_are_checked() {
        let (a, dir) = store();
        let t = target(&a);
        let r = rule(&a, "run_failed", &t, None);
        let d = [ds("a")];
        let failed = [run("l-1", "a", "failed", 0)];
        a.evaluate(&d, &failed, 10).unwrap();
        save_rule_in(
            &a,
            RuleReq {
                id: Some(r.id.clone()),
                name: r.name.clone(),
                kind: r.kind.clone(),
                dataset: None,
                threshold: None,
                targets: vec![t.clone()],
                enabled: false,
            },
        )
        .unwrap();
        assert!(a.evaluate(&d, &failed, 20).unwrap().is_empty());
        assert_eq!(
            a.state.lock().unwrap().incidents[0].resolved_by.as_deref(),
            Some("rule")
        );

        let bad = RuleReq {
            id: None,
            name: "x".into(),
            kind: "run_failed".into(),
            dataset: None,
            threshold: None,
            targets: vec!["t-nope".into()],
            enabled: true,
        };
        assert!(save_rule_in(&a, bad).is_err());
        let bad = RuleReq {
            id: None,
            name: "x".into(),
            kind: "worker_offline".into(),
            dataset: None,
            threshold: None,
            targets: vec![],
            enabled: true,
        };
        assert!(
            save_rule_in(&a, bad).is_err(),
            "a fleet condition does not exist here"
        );
        let bad = TargetReq {
            id: None,
            name: "x".into(),
            kind: "webhook".into(),
            url: Some("ftp://x".into()),
        };
        assert!(save_target_in(&a, bad).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_url_is_shown_as_its_host_only() {
        assert_eq!(
            masked("https://hooks.slack.com/services/T0/B0/secret"),
            "https://hooks.slack.com/…"
        );
        assert_eq!(
            masked("http://user:pw@host:9000/p?q=1"),
            "http://host:9000/…"
        );
    }

    #[test]
    fn delivery_records_how_it_went() {
        let (a, dir) = store();
        let t = target(&a);
        rule(&a, "run_failed", &t, None);
        let d = [ds("a")];
        let n = a.evaluate(&d, &[run("l-1", "a", "failed", 0)], 10).unwrap();
        let sent = std::cell::RefCell::new(Vec::new());
        a.deliver(n, &|url, payload| {
            sent.borrow_mut().push((
                url.to_string(),
                payload["text"].as_str().unwrap().to_string(),
            ));
            Delivery {
                at_ms: 1,
                ok: false,
                event: String::new(),
                status: Some(500),
                error: Some("answered 500".into()),
            }
        });
        let sent = sent.into_inner();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "https://example.invalid/x/secret");
        assert!(
            sent[0]
                .1
                .starts_with("[shapeshift] FIRING run_failed: a — "),
            "{}",
            sent[0].1
        );
        let st = a.state.lock().unwrap();
        let last = st.targets[0].last.as_ref().unwrap();
        assert_eq!(
            (last.ok, last.status, last.event.as_str()),
            (false, Some(500), "firing")
        );
        drop(st);
        let _ = std::fs::remove_dir_all(dir);
    }
}
