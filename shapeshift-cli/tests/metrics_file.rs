//! `--metrics-file`: a run's numbers in the Prometheus text format, written whole on exit; a
//! failed run writes its status and keeps the last success time; without the flag, no file.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_shapeshift")
}

fn workdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-metrics-{}-{}", std::process::id(), name));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

/// The value of the sample `name{labels}` in the file.
fn sample(text: &str, name: &str, labels: &str) -> Option<f64> {
    let prefix = format!("{name}{labels} ");
    text.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .map(|v| v.trim().parse().unwrap())
}

fn shape(input: &Path, out: &Path, metrics: Option<&Path>) -> std::process::Output {
    let mut c = Command::new(bin());
    c.args([
        "shape",
        "--input",
        input.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
        "--dataset",
        "orders",
    ]);
    if let Some(m) = metrics {
        c.args(["--metrics-file", m.to_str().unwrap()]);
    }
    c.output().unwrap()
}

const L: &str = "{dataset=\"orders\",command=\"shape\"}";

#[test]
fn a_run_writes_its_numbers_and_a_failed_one_keeps_the_last_success() {
    let d = workdir("run");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\nnot json\n{\"id\":2}\n{\"id\":3}\n").unwrap();
    let metrics = d.join("orders.prom");

    let ok = shape(&input, &d.join("o.parquet"), Some(&metrics));
    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(&text, "shapeshift_run_last_exit_status", L),
        Some(0.0),
        "{text}"
    );
    assert_eq!(
        sample(&text, "shapeshift_run_last_rows_in", L),
        Some(3.0),
        "{text}"
    );
    assert_eq!(
        sample(&text, "shapeshift_run_last_rows_out", L),
        Some(3.0),
        "{text}"
    );
    assert!(sample(&text, "shapeshift_run_last_bytes_written", L).unwrap() > 0.0);
    let success = sample(&text, "shapeshift_run_last_success_timestamp_seconds", L).unwrap();
    let end = sample(&text, "shapeshift_run_last_end_timestamp_seconds", L).unwrap();
    assert_eq!(success, end);
    assert!(sample(&text, "shapeshift_run_last_duration_seconds", L).is_some());
    assert!(
        text.contains("# TYPE shapeshift_run_last_exit_status gauge\n"),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "shapeshift_build_info{{version=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        )),
        "{text}"
    );
    // Nothing from the data: no path and no record value appears.
    assert!(!text.contains(d.to_str().unwrap()), "{text}");

    // A failed run (its input is gone) says so, and still knows when the last success was.
    std::thread::sleep(std::time::Duration::from_millis(50));
    fs::remove_file(&input).unwrap();
    let bad = shape(&input, &d.join("o.parquet"), Some(&metrics));
    assert!(!bad.status.success());
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(&text, "shapeshift_run_last_exit_status", L),
        Some(1.0),
        "{text}"
    );
    assert_eq!(
        sample(&text, "shapeshift_run_last_success_timestamp_seconds", L),
        Some(success),
        "{text}"
    );
    assert!(sample(&text, "shapeshift_run_last_end_timestamp_seconds", L).unwrap() > success);
    assert_eq!(
        sample(&text, "shapeshift_run_last_rows_in", L),
        None,
        "a failed run has no counts"
    );
    // No temporary file is left beside it.
    let tmp = fs::read_dir(&d)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
    assert_eq!(tmp, 0);
    fs::remove_dir_all(&d).ok();
}

#[test]
fn without_the_flag_no_file_is_written() {
    let d = workdir("off");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let out = shape(&input, &d.join("o.parquet"), None);
    assert!(out.status.success());
    let proms = fs::read_dir(&d)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".prom"))
        .count();
    assert_eq!(proms, 0);
    fs::remove_dir_all(&d).ok();
}

#[test]
fn a_table_command_writes_its_status_without_a_dataset_label() {
    let d = workdir("table");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let table = d.join("t");
    let made = Command::new(bin())
        .args([
            "shape",
            "--input",
            input.to_str().unwrap(),
            "--output",
            table.to_str().unwrap(),
            "--to",
            "iceberg",
        ])
        .output()
        .unwrap();
    assert!(
        made.status.success(),
        "{}",
        String::from_utf8_lossy(&made.stderr)
    );
    let metrics = d.join("compact.prom");
    let run = Command::new(bin())
        .args([
            "compact",
            table.to_str().unwrap(),
            "--metrics-file",
            metrics.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(
            &text,
            "shapeshift_run_last_exit_status",
            "{command=\"compact\"}"
        ),
        Some(0.0),
        "{text}"
    );
    fs::remove_dir_all(&d).ok();
}

/// `shape -s <spec>` with a metrics file; the spec's output goes in `d`.
fn shape_spec(spec: &Path, input: &Path, metrics: &Path) -> std::process::Output {
    Command::new(bin())
        .args([
            "shape",
            "-s",
            spec.to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--metrics-file",
            metrics.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

#[test]
fn a_run_whose_spec_does_not_parse_reports_under_the_same_dataset() {
    let d = workdir("spec");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let spec = d.join("orders.yaml");
    let good = format!(
        "dataset: orders\nsource:\n  format: jsonl\noutput:\n  format: parquet\n  path: {}\n",
        d.join("o.parquet").display()
    );
    fs::write(&spec, &good).unwrap();
    let metrics = d.join("orders.prom");
    let ok = shape_spec(&spec, &input, &metrics);
    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let text = fs::read_to_string(&metrics).unwrap();
    let success = sample(&text, "shapeshift_run_last_success_timestamp_seconds", L).unwrap();

    // A spec that is YAML but not a valid spec: its `dataset` key is still read.
    fs::write(&spec, format!("{good}no_such_field: 1\n")).unwrap();
    assert!(!shape_spec(&spec, &input, &metrics).status.success());
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(&text, "shapeshift_run_last_exit_status", L),
        Some(1.0),
        "{text}"
    );
    assert_eq!(
        sample(&text, "shapeshift_run_last_success_timestamp_seconds", L),
        Some(success),
        "{text}"
    );

    // A spec that cannot be read at all: the dataset comes from the file it replaces.
    fs::remove_file(&spec).unwrap();
    assert!(!shape_spec(&spec, &input, &metrics).status.success());
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(&text, "shapeshift_run_last_exit_status", L),
        Some(1.0),
        "{text}"
    );
    assert_eq!(
        sample(&text, "shapeshift_run_last_success_timestamp_seconds", L),
        Some(success),
        "{text}"
    );
    fs::remove_dir_all(&d).ok();
}

#[test]
fn summary_json_and_a_metrics_file_go_together() {
    let d = workdir("both");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let metrics = d.join("m.prom");
    let out = Command::new(bin())
        .args([
            "shape",
            "--input",
            input.to_str().unwrap(),
            "--output",
            d.join("o.parquet").to_str().unwrap(),
            "--dataset",
            "orders",
            "--summary-json",
            "--metrics-file",
            metrics.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    let doc: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(doc["rows"]["out"], 2);
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(&text, "shapeshift_run_last_rows_out", L),
        Some(2.0),
        "{text}"
    );
    fs::remove_dir_all(&d).ok();
}

#[test]
fn a_file_that_cannot_be_written_is_a_warning_and_the_run_stands() {
    let d = workdir("unwritable");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let metrics = d.join("no-such-dir").join("m.prom");
    let out = shape(&input, &d.join("o.parquet"), Some(&metrics));
    assert!(out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("warning: --metrics-file"), "{err}");
    fs::remove_dir_all(&d).ok();
}

#[test]
fn expire_writes_its_status() {
    let d = workdir("expire");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let table = d.join("t");
    let made = Command::new(bin())
        .args([
            "shape",
            "--input",
            input.to_str().unwrap(),
            "--output",
            table.to_str().unwrap(),
            "--to",
            "iceberg",
        ])
        .output()
        .unwrap();
    assert!(made.status.success());
    let metrics = d.join("expire.prom");
    let run = Command::new(bin())
        .args([
            "expire",
            table.to_str().unwrap(),
            "--metrics-file",
            metrics.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let text = fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        sample(
            &text,
            "shapeshift_run_last_exit_status",
            "{command=\"expire\"}"
        ),
        Some(0.0),
        "{text}"
    );
    fs::remove_dir_all(&d).ok();
}
