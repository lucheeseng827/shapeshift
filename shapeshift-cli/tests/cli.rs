//! End-to-end CLI test: run the real `shapeshift` binary over a temp JSONL and
//! assert the shape + inspect + cost flows.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_shapeshift")
}

fn workdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-cli-{}-{}", std::process::id(), name));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn shape_to_parquet_then_inspect() {
    let d = workdir("parquet");
    let input = d.join("in.jsonl");
    fs::write(
        &input,
        "{\"id\":1,\"user\":{\"name\":\"Ada\"}}\nbad line here\n{\"id\":2,\"user\":{\"name\":\"Grace\"}}\n",
    )
    .unwrap();
    let out = d.join("events.parquet");

    let shape = Command::new(bin())
        .args([
            "shape",
            "--input",
            input.to_str().unwrap(),
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        shape.status.success(),
        "shape failed: {}",
        String::from_utf8_lossy(&shape.stderr)
    );
    let stdout = String::from_utf8_lossy(&shape.stdout);
    assert!(stdout.contains("rows_in=2"), "got: {stdout}");
    assert!(stdout.contains("parse_errors=1"), "got: {stdout}");
    assert!(out.exists());
    // The bad line was sidecarred.
    assert!(d.join("events.parquet.rejects.jsonl").exists());

    let inspect = Command::new(bin())
        .args(["inspect", out.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(inspect.status.success());
    let out_s = String::from_utf8_lossy(&inspect.stdout);
    assert!(out_s.contains("rows: 2"));
    assert!(out_s.contains("user.name"));

    fs::remove_dir_all(&d).ok();
}

#[test]
fn shape_to_iceberg_produces_table() {
    let d = workdir("iceberg");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n").unwrap();
    let table = d.join("tbl");

    let shape = Command::new(bin())
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
        shape.status.success(),
        "shape failed: {}",
        String::from_utf8_lossy(&shape.stderr)
    );
    assert!(table.join("metadata/v1.metadata.json").exists());

    let inspect = Command::new(bin())
        .args(["inspect", table.to_str().unwrap()])
        .output()
        .unwrap();
    let out_s = String::from_utf8_lossy(&inspect.stdout);
    assert!(out_s.contains("format-version: 2"));
    assert!(out_s.contains("total-records: 3"));

    fs::remove_dir_all(&d).ok();
}

#[test]
fn infer_prints_a_spec() {
    let d = workdir("infer");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1,\"amount\":2.5}\n").unwrap();
    let out = Command::new(bin())
        .args(["infer", "--input", input.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("dataset: in"));
    assert!(s.contains("type: int64"));
    assert!(s.contains("type: float64"));
    fs::remove_dir_all(&d).ok();
}

#[test]
fn cost_prices_a_row_count() {
    let out = Command::new(bin())
        .args([
            "cost",
            "--rows",
            "50000000",
            "--vendor-per-million",
            "600",
            "--self-host-cost",
            "2",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("30000.00")); // 50M @ $600/M
    assert!(s.contains("saved"));
}

/// A spec whose schema was written from a 2-record sample, over an input that grows a
/// field and changes a type after it — the shape the drift policies exist for.
fn drift_fixture(name: &str) -> (PathBuf, PathBuf) {
    let d = workdir(name);
    let input = d.join("in.jsonl");
    fs::write(
        &input,
        "{\"id\":1,\"amount\":1.5}\n\
         {\"id\":2,\"amount\":2.5}\n\
         {\"id\":3,\"amount\":\"n/a\",\"currency\":\"usd\"}\n\
         {\"id\":4,\"amount\":4.5,\"currency\":\"eur\"}\n",
    )
    .unwrap();
    let spec = d.join("spec.yaml");
    fs::write(
        &spec,
        "dataset: events\n\
         source:\n  format: jsonl\n\
         output:\n  format: parquet\n  path: out.parquet\n\
         schema: infer\n\
         columns:\n\
         - name: id\n  type: int64\n\
         - name: amount\n  type: float64\n\
         options:\n  infer_sample: 2\n",
    )
    .unwrap();
    (d, spec)
}

#[test]
fn shape_reports_schema_drift_without_changing_the_output() {
    let (d, spec) = drift_fixture("drift-warn");
    let out = d.join("out.parquet");
    let shape = Command::new(bin())
        .args([
            "shape",
            "--spec",
            spec.to_str().unwrap(),
            "--input",
            d.join("in.jsonl").to_str().unwrap(),
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(shape.status.success(), "shape failed");
    let s = String::from_utf8_lossy(&shape.stdout);
    // Every row still lands — `warn` reports, it does not drop.
    assert!(s.contains("rows_in=4 rows_out=4"), "got: {s}");
    assert!(s.contains("schema drift (policy: warn)"), "got: {s}");
    // The field the spec never had, and the column that stopped coercing.
    assert!(s.contains("new field `currency`"), "got: {s}");
    assert!(s.contains("column `amount` (float64)"), "got: {s}");
    // …and the declaration that would keep the new field.
    assert!(s.contains("name: currency"), "got: {s}");

    let sidecar = d.join("out.parquet.drift.json");
    assert!(sidecar.exists());
    let report: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&sidecar).unwrap()).unwrap();
    assert_eq!(report["drift"]["new_fields"]["currency"]["count"], 2);
    assert_eq!(report["drift"]["new_fields"]["currency"]["first_record"], 3);
    assert_eq!(report["drift"]["type_mismatches"]["amount"]["count"], 1);
    assert_eq!(report["suggested_columns"][0]["type"], "string");

    fs::remove_dir_all(&d).ok();
}

#[test]
fn shape_can_rescue_quarantine_or_fail_on_drift() {
    let (d, spec) = drift_fixture("drift-policies");
    let input = d.join("in.jsonl");
    let run = |policy: &str, out: &PathBuf| {
        Command::new(bin())
            .args([
                "shape",
                "--spec",
                spec.to_str().unwrap(),
                "--input",
                input.to_str().unwrap(),
                "--output",
                out.to_str().unwrap(),
                "--on-drift",
                policy,
            ])
            .output()
            .unwrap()
    };

    // rescue: every row lands, and what the schema would have dropped is kept in a
    // catch-all column appended after the declared ones.
    let res = d.join("rescue.parquet");
    let o = run("rescue", &res);
    assert!(o.status.success());
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(s.contains("rows_out=4"), "got: {s}");
    assert!(
        s.contains("rescued into the catch-all column on 2 row(s)"),
        "got: {s}"
    );
    let inspect = Command::new(bin())
        .args(["inspect", res.to_str().unwrap()])
        .output()
        .unwrap();
    let i = String::from_utf8_lossy(&inspect.stdout);
    assert!(i.contains("_rescued: Utf8"), "got: {i}");

    // quarantine: the drifted rows are rejected instead, so the table stays exactly
    // on-schema — and the rows are still replayable from the sidecar.
    let q = d.join("quarantine.parquet");
    let o = run("quarantine", &q);
    assert!(o.status.success());
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(s.contains("rows_out=2 rejected=2"), "got: {s}");
    let rejects = fs::read_to_string(d.join("quarantine.parquet.rejects.jsonl")).unwrap();
    assert!(rejects.contains("schema drift"), "got: {rejects}");
    assert_eq!(rejects.lines().count(), 2);

    // error: the run fails, and still leaves the report that explains why.
    let e = d.join("error.parquet");
    let o = run("error", &e);
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("schema drift"), "got: {err}");
    assert!(err.contains("record 3"), "got: {err}");
    assert!(d.join("error.parquet.drift.json").exists());

    // ignore: no detection, and nothing said about it.
    let i = d.join("ignore.parquet");
    let o = run("ignore", &i);
    assert!(o.status.success());
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(s.contains("rows_out=4"), "got: {s}");
    assert!(!s.contains("schema drift"), "got: {s}");
    assert!(!d.join("ignore.parquet.drift.json").exists());

    fs::remove_dir_all(&d).ok();
}

/// The `error` policy's contract is that no consumer sees new data — not that no bytes
/// are written. With `row_group_rows: 1` the sink has already flushed twenty row groups
/// before the drifted record aborts the run, so this pins down what a reader then sees:
/// nothing. `finish()` is never called, so the file never gets its footer.
#[test]
fn drift_error_leaves_nothing_a_reader_can_open() {
    // The guarantee has to hold however the row groups reach the sink. With a writer
    // thread, "the queue closed" must not be read as "commit" — an earlier cut of the
    // pipeline finalized the footer on disconnect and handed back a readable file. Both
    // modes are pinned explicitly so the inline and threaded writers are each covered,
    // whatever the default happens to be.
    for pipeline in ["on", "off"] {
        drift_error_fail_closed(pipeline);
    }
}

fn drift_error_fail_closed(pipeline: &str) {
    let d = workdir(&format!("drift-fail-closed-{pipeline}"));
    let input = d.join("in.jsonl");
    let mut lines: Vec<String> = (1..=20)
        .map(|i| format!("{{\"id\":{i},\"amount\":{i}.5}}"))
        .collect();
    lines.push("{\"id\":21,\"amount\":1.5,\"currency\":\"usd\"}".into());
    fs::write(&input, lines.join("\n") + "\n").unwrap();
    let spec = d.join("spec.yaml");
    let out = d.join("out.parquet");
    fs::write(
        &spec,
        format!(
            "dataset: t\nsource:\n  format: jsonl\n\
             output:\n  format: parquet\n  path: {}\n\
             schema: strict\ncolumns:\n- name: id\n  type: int64\n- name: amount\n  type: float64\n\
             options:\n  row_group_rows: 1\n",
            out.display()
        ),
    )
    .unwrap();

    let shape = Command::new(bin())
        .args([
            "shape",
            "--spec",
            spec.to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--on-drift",
            "error",
            "--pipeline",
            pipeline,
        ])
        .output()
        .unwrap();
    assert!(!shape.status.success(), "pipeline={pipeline}");
    assert!(
        String::from_utf8_lossy(&shape.stderr).contains("record 21"),
        "expected the abort at the drifted record"
    );
    // Twenty row groups really were flushed — this is not the trivial empty-file case.
    assert!(
        fs::metadata(&out).unwrap().len() > 1000,
        "expected a partially written file, so the assertion below means something"
    );
    // …and it is unreadable: no footer, so no reader will open it.
    let inspect = Command::new(bin())
        .args(["inspect", out.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        !inspect.status.success(),
        "an aborted run must not leave a readable file (pipeline={pipeline})"
    );
    assert!(
        String::from_utf8_lossy(&inspect.stderr).contains("footer"),
        "expected a footer complaint, got: {}",
        String::from_utf8_lossy(&inspect.stderr)
    );

    fs::remove_dir_all(&d).ok();
}

/// A piped source must shape **every** record, not just the inference sample.
///
/// `shape` used to read the input twice — once to infer, once to shape — which silently
/// truncated any non-seekable input: the sampling pass ate bytes the shaping pass never
/// saw, and the run still exited 0. With `infer_sample` well below the record count, a
/// regression here shows up immediately as `rows_in` == the sample size.
#[test]
fn stdin_shapes_every_record_not_just_the_sample() {
    use std::io::Write;
    use std::process::Stdio;

    let d = workdir("stdin");
    const ROWS: usize = 500;
    let mut input = String::new();
    for i in 0..ROWS {
        input.push_str(&format!("{{\"id\":{i},\"user\":{{\"name\":\"Ada\"}}}}\n"));
    }
    // A spec whose sample window is a small fraction of the input, so a re-read would
    // lose most of it.
    let spec = d.join("spec.yaml");
    fs::write(
        &spec,
        "dataset: piped\nsource: { format: jsonl }\noutput: { format: parquet, path: out.parquet }\nschema: infer\noptions: { infer_sample: 10 }\n",
    )
    .unwrap();
    let out = d.join("piped.parquet");

    let mut child = Command::new(bin())
        .args([
            "shape",
            "--spec",
            spec.to_str().unwrap(),
            "--input",
            "-",
            "--output",
            out.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let shape = child.wait_with_output().unwrap();
    assert!(
        shape.status.success(),
        "shape failed: {}",
        String::from_utf8_lossy(&shape.stderr)
    );
    let stdout = String::from_utf8_lossy(&shape.stdout);
    assert!(stdout.contains(&format!("rows_in={ROWS}")), "got: {stdout}");
    assert!(
        stdout.contains(&format!("rows_out={ROWS}")),
        "got: {stdout}"
    );

    let inspect = Command::new(bin())
        .args(["inspect", out.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(inspect.status.success());
    assert!(String::from_utf8_lossy(&inspect.stdout).contains(&format!("rows: {ROWS}")));
}

/// A key the spec model does not know is a typo, not a comment: it must fail loudly
/// rather than silently dropping the block it was meant to configure.
#[test]
fn unknown_spec_key_is_rejected() {
    let d = workdir("unknown-key");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let spec = d.join("spec.yaml");
    // `colums` — the classic typo. Before, this parsed and the run inferred instead.
    fs::write(
        &spec,
        "dataset: t\nsource: { format: jsonl }\noutput: { format: parquet, path: out.parquet }\nschema: strict\ncolums:\n  - name: id\n    type: int64\n",
    )
    .unwrap();

    let shape = Command::new(bin())
        .args([
            "shape",
            "--spec",
            spec.to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--output",
            d.join("o.parquet").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        !shape.status.success(),
        "a typo'd spec key must not succeed"
    );
    let err = String::from_utf8_lossy(&shape.stderr);
    assert!(err.contains("unknown field `colums`"), "got: {err}");
    // And it points at the valid names, so the fix is obvious.
    assert!(err.contains("columns"), "got: {err}");
}

/// `infer_sample: 0` means "sample every record", which cannot be served from a stream
/// without buffering the whole thing — the one case the single-pass replay buffer cannot
/// bound. It must be refused with a message that names the fix, not silently accepted and
/// then held in memory.
#[test]
fn sample_everything_is_refused_on_a_stream() {
    use std::io::Write;
    use std::process::Stdio;

    let d = workdir("sample-zero");
    let spec = d.join("spec.yaml");
    fs::write(
        &spec,
        "dataset: t\nsource: { format: jsonl }\noutput: { format: parquet, path: out.parquet }\nschema: infer\noptions: { infer_sample: 0 }\n",
    )
    .unwrap();
    let input = "{\"id\":1}\n{\"id\":2}\n";

    let mut child = Command::new(bin())
        .args([
            "shape",
            "--spec",
            spec.to_str().unwrap(),
            "--input",
            "-",
            "--output",
            d.join("o.parquet").to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        !out.status.success(),
        "sampling a whole stream must not succeed"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("infer_sample: 0"), "got: {err}");
    assert!(err.contains("stream is read once"), "got: {err}");

    // The same spec over a real file is fine — it can simply be read twice.
    let file = d.join("in.jsonl");
    fs::write(&file, input).unwrap();
    let ok = Command::new(bin())
        .args([
            "shape",
            "--spec",
            spec.to_str().unwrap(),
            "--input",
            file.to_str().unwrap(),
            "--output",
            d.join("ok.parquet").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        ok.status.success(),
        "file failed: {}",
        String::from_utf8_lossy(&ok.stderr)
    );
    assert!(String::from_utf8_lossy(&ok.stdout).contains("rows_out=2"));
}
