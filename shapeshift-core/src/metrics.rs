//! A small metrics core: counters, gauges and one histogram type, written in the Prometheus
//! text exposition format (version 0.0.4) by hand. Nothing here is on unless a caller builds a
//! [`Registry`]: no counter is kept and nothing is written otherwise.
//!
//! Labels carry names the operator chose (a dataset, a connector) and fixed words (an outcome,
//! a kind), never a path, a URL or anything read from the data. Each metric holds at most
//! [`MAX_LABEL_SETS`] label sets; past that, new ones are counted under `other` and
//! `shapeshift_metrics_dropped_labels_total` says how many were folded in.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;

use crate::error::{Result, ShapeError};

/// The most label sets one metric holds before new ones are folded into `other`.
pub const MAX_LABEL_SETS: usize = 500;

/// The histogram buckets every histogram uses (seconds, for durations): one fixed set.
pub const BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0,
];

const DROPPED: &str = "shapeshift_metrics_dropped_labels_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Histogram,
}

impl Kind {
    fn word(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Histogram => "histogram",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Hist {
    /// Per bucket (not cumulative; summed when written).
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

#[derive(Debug, Clone)]
enum Series {
    Value(f64),
    Hist(Hist),
}

#[derive(Debug, Clone)]
struct Family {
    help: String,
    kind: Kind,
    labels: Vec<String>,
    series: BTreeMap<Vec<String>, Series>,
}

/// A set of metrics, rendered together.
#[derive(Debug, Clone, Default)]
pub struct Registry {
    families: BTreeMap<String, Family>,
    dropped: BTreeMap<String, u64>,
}

fn name_ok(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

fn label_ok(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with("__")
        && name != "le"
}

/// A sample value as the format writes it.
fn number(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "+Inf".into()
    } else if v == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        format!("{v}")
    }
}

/// A label value escaped: `\`, `"` and a newline.
fn escape_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// HELP text escaped: `\` and a newline.
fn escape_help(v: &str) -> String {
    v.replace('\\', "\\\\").replace('\n', "\\n")
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    fn family(&mut self, name: &str, help: &str, kind: Kind, labels: &[&str]) -> Result<()> {
        if !name_ok(name) {
            return Err(ShapeError::Spec(format!("not a metric name: {name:?}")));
        }
        if let Some(bad) = labels.iter().find(|l| !label_ok(l)) {
            return Err(ShapeError::Spec(format!("not a label name: {bad:?}")));
        }
        match self.families.get(name) {
            Some(f) if f.kind != kind || f.labels != labels => Err(ShapeError::Spec(format!(
                "metric {name} is declared twice, differently"
            ))),
            Some(_) => Ok(()),
            None => {
                self.families.insert(
                    name.to_string(),
                    Family {
                        help: help.to_string(),
                        kind,
                        labels: labels.iter().map(|l| l.to_string()).collect(),
                        series: BTreeMap::new(),
                    },
                );
                Ok(())
            }
        }
    }

    /// Declare a counter (only ever goes up).
    pub fn counter(&mut self, name: &str, help: &str, labels: &[&str]) -> Result<()> {
        self.family(name, help, Kind::Counter, labels)
    }

    /// Declare a gauge (a value that is set).
    pub fn gauge(&mut self, name: &str, help: &str, labels: &[&str]) -> Result<()> {
        self.family(name, help, Kind::Gauge, labels)
    }

    /// Declare a histogram over [`BUCKETS`].
    pub fn histogram(&mut self, name: &str, help: &str, labels: &[&str]) -> Result<()> {
        self.family(name, help, Kind::Histogram, labels)
    }

    /// The series for `values`, created if there is room; past [`MAX_LABEL_SETS`] the
    /// `other` series (and a count of it in the dropped counter).
    fn series(&mut self, name: &str, values: &[&str], kind: Kind) -> Result<&mut Series> {
        let f = self
            .families
            .get_mut(name)
            .ok_or_else(|| ShapeError::Spec(format!("metric {name} was never declared")))?;
        if f.kind != kind {
            return Err(ShapeError::Spec(format!(
                "metric {name} is a {}, not a {}",
                f.kind.word(),
                kind.word()
            )));
        }
        if values.len() != f.labels.len() {
            return Err(ShapeError::Spec(format!(
                "metric {name} takes {} label value(s), not {}",
                f.labels.len(),
                values.len()
            )));
        }
        let mut key: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        if !f.series.contains_key(&key) && f.series.len() >= MAX_LABEL_SETS {
            *self.dropped.entry(name.to_string()).or_default() += 1;
            key = vec!["other".to_string(); f.labels.len()];
        }
        let fresh = || match f.kind {
            Kind::Histogram => Series::Hist(Hist {
                counts: vec![0; BUCKETS.len()],
                ..Hist::default()
            }),
            _ => Series::Value(0.0),
        };
        Ok(f.series.entry(key).or_insert_with(fresh))
    }

    /// Add `by` (at least 0) to a counter.
    pub fn inc(&mut self, name: &str, values: &[&str], by: f64) -> Result<()> {
        if by.is_nan() || by < 0.0 {
            return Err(ShapeError::Spec(format!(
                "counter {name} only goes up: not by {by}"
            )));
        }
        if let Series::Value(v) = self.series(name, values, Kind::Counter)? {
            *v += by;
        }
        Ok(())
    }

    /// Set a gauge.
    pub fn set(&mut self, name: &str, values: &[&str], to: f64) -> Result<()> {
        match self.families.get(name).map(|f| f.kind) {
            Some(Kind::Counter) => {
                return Err(ShapeError::Spec(format!(
                    "metric {name} is a counter: it is added to, not set"
                )))
            }
            Some(Kind::Histogram) => {
                return Err(ShapeError::Spec(format!(
                    "metric {name} is a histogram: it observes, it is not set"
                )))
            }
            _ => {}
        }
        if let Series::Value(v) = self.series(name, values, Kind::Gauge)? {
            *v = to;
        }
        Ok(())
    }

    /// Record one observation in a histogram.
    pub fn observe(&mut self, name: &str, values: &[&str], v: f64) -> Result<()> {
        if let Series::Hist(h) = self.series(name, values, Kind::Histogram)? {
            if let Some(i) = BUCKETS.iter().position(|b| v <= *b) {
                h.counts[i] += 1;
            }
            h.sum += v;
            h.count += 1;
        }
        Ok(())
    }

    /// The metrics in the text exposition format, families and series in a stable order.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let labels = |names: &[String], values: &[String], extra: Option<(&str, &str)>| {
            let mut parts: Vec<String> = names
                .iter()
                .zip(values)
                .map(|(n, v)| format!("{n}=\"{}\"", escape_value(v)))
                .collect();
            if let Some((n, v)) = extra {
                parts.push(format!("{n}=\"{v}\""));
            }
            if parts.is_empty() {
                String::new()
            } else {
                format!("{{{}}}", parts.join(","))
            }
        };
        for (name, f) in &self.families {
            let _ = writeln!(out, "# HELP {name} {}", escape_help(&f.help));
            let _ = writeln!(out, "# TYPE {name} {}", f.kind.word());
            for (values, s) in &f.series {
                match s {
                    Series::Value(v) => {
                        let _ = writeln!(
                            out,
                            "{name}{} {}",
                            labels(&f.labels, values, None),
                            number(*v)
                        );
                    }
                    Series::Hist(h) => {
                        let mut cumulative = 0;
                        for (b, c) in BUCKETS.iter().zip(&h.counts) {
                            cumulative += c;
                            let le = number(*b);
                            let _ = writeln!(
                                out,
                                "{name}_bucket{} {cumulative}",
                                labels(&f.labels, values, Some(("le", &le)))
                            );
                        }
                        let _ = writeln!(
                            out,
                            "{name}_bucket{} {}",
                            labels(&f.labels, values, Some(("le", "+Inf"))),
                            h.count
                        );
                        let _ = writeln!(
                            out,
                            "{name}_sum{} {}",
                            labels(&f.labels, values, None),
                            number(h.sum)
                        );
                        let _ = writeln!(
                            out,
                            "{name}_count{} {}",
                            labels(&f.labels, values, None),
                            h.count
                        );
                    }
                }
            }
        }
        if !self.dropped.is_empty() {
            let _ = writeln!(
                out,
                "# HELP {DROPPED} Label sets folded into `other` because a metric held {MAX_LABEL_SETS} already."
            );
            let _ = writeln!(out, "# TYPE {DROPPED} counter");
            for (metric, n) in &self.dropped {
                let _ = writeln!(out, "{DROPPED}{{metric=\"{}\"}} {n}", escape_value(metric));
            }
        }
        out
    }
}

/// Write `text` to `path` whole or not at all: to a temporary file beside it, synced, then
/// renamed over it, so a scraper never reads half a file.
pub fn write_atomic(path: &Path, text: &str) -> Result<()> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| ShapeError::Spec(format!("{} names no file", path.display())))?
        .to_string_lossy();
    let io = |e: std::io::Error| ShapeError::Sink(format!("writing {}: {e}", path.display()));
    // A name of this write's own, opened only if it does not exist yet: a file (or a link)
    // someone left at that name is never written through.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let mut opened = None;
    for attempt in 0..16u32 {
        let tmp = dir.join(format!(
            ".{name}.{}.{nanos:x}{attempt}.tmp",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(f) => {
                opened = Some((tmp, f));
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(io(e)),
        }
    }
    let (tmp, mut f) = opened.ok_or_else(|| {
        ShapeError::Sink(format!(
            "writing {}: no free temporary name beside it",
            path.display()
        ))
    })?;
    f.write_all(text.as_bytes()).map_err(io)?;
    f.sync_all().map_err(io)?;
    drop(f);
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        io(e)
    })?;
    // The rename itself, durable where the platform can sync a directory.
    #[cfg(unix)]
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// A sample's value from text [`Registry::render`] wrote: the sample of `name` with exactly
/// these labels, in this order. `None` when it is not there or does not read as a number.
pub fn read_sample(text: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let labels = if labels.is_empty() {
        String::new()
    } else {
        let parts: Vec<String> = labels
            .iter()
            .map(|(n, v)| format!("{n}=\"{}\"", escape_value(v)))
            .collect();
        format!("{{{}}}", parts.join(","))
    };
    let prefix = format!("{name}{labels} ");
    text.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse().ok())
}

/// The labels of the first sample of `name` in text [`Registry::render`] wrote, unescaped, in
/// their order. `None` when there is no such sample or its labels do not read.
pub fn read_labels(text: &str, name: &str) -> Option<Vec<(String, String)>> {
    let prefix = format!("{name}{{");
    let rest = text.lines().find_map(|l| l.strip_prefix(prefix.as_str()))?;
    let mut out = Vec::new();
    let mut chars = rest.chars().peekable();
    loop {
        if chars.peek() == Some(&'}') {
            return Some(out);
        }
        let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
        if key.is_empty() || chars.next() != Some('"') {
            return None;
        }
        let mut value = String::new();
        loop {
            match chars.next()? {
                '\\' => match chars.next()? {
                    'n' => value.push('\n'),
                    c => value.push(c),
                },
                '"' => break,
                c => value.push(c),
            }
        }
        out.push((key, value));
        match chars.next()? {
            ',' => continue,
            '}' => return Some(out),
            _ => return None,
        }
    }
}

/// An RFC 3339 instant as Unix seconds (a source position that is a time), or `None`.
pub fn unix_seconds(instant: &str) -> Option<f64> {
    chrono::DateTime::parse_from_rfc3339(instant)
        .ok()
        .map(|t| t.timestamp_micros() as f64 / 1e6)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gauges_and_counters_are_written_in_the_text_format() {
        let mut r = Registry::new();
        r.gauge(
            "shapeshift_run_last_exit_status",
            "The run's exit status.",
            &["dataset", "command"],
        )
        .unwrap();
        r.counter("shapeshift_runs_total", "Runs.", &["outcome"])
            .unwrap();
        r.set("shapeshift_run_last_exit_status", &["orders", "shape"], 0.0)
            .unwrap();
        r.inc("shapeshift_runs_total", &["ok"], 2.0).unwrap();
        r.inc("shapeshift_runs_total", &["ok"], 1.0).unwrap();
        assert_eq!(
            r.render(),
            "# HELP shapeshift_run_last_exit_status The run's exit status.\n\
             # TYPE shapeshift_run_last_exit_status gauge\n\
             shapeshift_run_last_exit_status{dataset=\"orders\",command=\"shape\"} 0\n\
             # HELP shapeshift_runs_total Runs.\n\
             # TYPE shapeshift_runs_total counter\n\
             shapeshift_runs_total{outcome=\"ok\"} 3\n"
        );
    }

    #[test]
    fn label_values_and_help_are_escaped() {
        let mut r = Registry::new();
        r.gauge("g", "a \\ b\nc", &["l"]).unwrap();
        r.set("g", &["q\"uote\\back\nline"], 1.5).unwrap();
        let t = r.render();
        assert!(t.contains("# HELP g a \\\\ b\\nc\n"), "{t}");
        assert!(t.contains("g{l=\"q\\\"uote\\\\back\\nline\"} 1.5\n"), "{t}");
    }

    #[test]
    fn a_histogram_writes_cumulative_buckets_a_sum_and_a_count() {
        let mut r = Registry::new();
        r.histogram("d_seconds", "Durations.", &[]).unwrap();
        r.observe("d_seconds", &[], 0.003).unwrap();
        r.observe("d_seconds", &[], 0.2).unwrap();
        r.observe("d_seconds", &[], 7200.0).unwrap();
        let t = r.render();
        assert!(t.contains("d_seconds_bucket{le=\"0.005\"} 1\n"), "{t}");
        assert!(t.contains("d_seconds_bucket{le=\"0.25\"} 2\n"), "{t}");
        assert!(t.contains("d_seconds_bucket{le=\"3600\"} 2\n"), "{t}");
        assert!(t.contains("d_seconds_bucket{le=\"+Inf\"} 3\n"), "{t}");
        assert!(t.contains("d_seconds_sum 7200.203\n"), "{t}");
        assert!(t.contains("d_seconds_count 3\n"), "{t}");
    }

    #[test]
    fn past_the_cap_new_label_sets_are_counted_under_other() {
        let mut r = Registry::new();
        r.counter("c_total", "C.", &["dataset"]).unwrap();
        for i in 0..MAX_LABEL_SETS + 3 {
            r.inc("c_total", &[&format!("d{i}")], 1.0).unwrap();
        }
        let t = r.render();
        assert_eq!(t.matches("c_total{").count(), MAX_LABEL_SETS + 1);
        assert!(t.contains("c_total{dataset=\"other\"} 3\n"), "{t}");
        assert!(
            t.contains("shapeshift_metrics_dropped_labels_total{metric=\"c_total\"} 3\n"),
            "{t}"
        );
    }

    #[test]
    fn misuse_is_refused() {
        let mut r = Registry::new();
        assert!(r.gauge("1bad", "x", &[]).is_err());
        assert!(r.gauge("ok", "x", &["le"]).is_err());
        r.counter("c_total", "x", &["a"]).unwrap();
        assert!(
            r.gauge("c_total", "x", &["a"]).is_err(),
            "declared twice, differently"
        );
        assert!(
            r.inc("c_total", &["x"], -1.0).is_err(),
            "counters only go up"
        );
        assert!(
            r.set("c_total", &["x"], 1.0).is_err(),
            "a counter is not set"
        );
        assert!(r.inc("c_total", &[], 1.0).is_err(), "label count");
        assert!(r.inc("nope", &[], 1.0).is_err(), "undeclared");
    }

    #[test]
    fn values_that_are_not_finite_are_spelled_as_the_format_spells_them() {
        let mut r = Registry::new();
        r.gauge("g", "x", &["k"]).unwrap();
        r.set("g", &["a"], f64::NAN).unwrap();
        r.set("g", &["b"], f64::INFINITY).unwrap();
        r.set("g", &["c"], f64::NEG_INFINITY).unwrap();
        let t = r.render();
        assert!(
            t.contains("g{k=\"a\"} NaN\n")
                && t.contains("g{k=\"b\"} +Inf\n")
                && t.contains("g{k=\"c\"} -Inf\n"),
            "{t}"
        );
    }

    #[test]
    fn a_samples_labels_read_back_as_they_were_given() {
        let mut r = Registry::new();
        r.gauge("g", "x", &["dataset", "command"]).unwrap();
        r.set("g", &["o\"r,d=e}r\\s\nx", "shape"], 1.0).unwrap();
        let labels = read_labels(&r.render(), "g").unwrap();
        assert_eq!(
            labels,
            vec![
                ("dataset".to_string(), "o\"r,d=e}r\\s\nx".to_string()),
                ("command".to_string(), "shape".to_string())
            ]
        );
        assert_eq!(read_labels("h 1\n", "g"), None);
    }

    #[test]
    fn a_file_left_at_the_temporary_name_is_not_written_through() {
        let dir = std::env::temp_dir().join(format!("ss-metrics-tmp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("m.prom");
        write_atomic(&p, "a 1\n").unwrap();
        write_atomic(&p, "a 2\n").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "a 2\n");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_instant_reads_as_unix_seconds() {
        assert_eq!(unix_seconds("1970-01-01T00:01:40.5Z"), Some(100.5));
        assert_eq!(unix_seconds("yesterday"), None);
    }

    #[test]
    fn a_file_is_replaced_whole_and_a_sample_reads_back() {
        let dir = std::env::temp_dir().join(format!("ss-metrics-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("orders.prom");
        write_atomic(&p, "a 1\n").unwrap();
        write_atomic(&p, "a{x=\"y\"} 2\nb 3\n").unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(read_sample(&text, "a", &[("x", "y")]), Some(2.0));
        assert_eq!(read_sample(&text, "b", &[]), Some(3.0));
        assert_eq!(read_sample(&text, "a", &[]), None);
        // No temporary file is left beside it.
        let left: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
        assert_eq!(left.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
