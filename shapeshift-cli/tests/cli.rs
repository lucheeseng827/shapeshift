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

fn run(args: &[&str]) -> std::process::Output {
    Command::new(bin()).args(args).output().unwrap()
}

#[test]
fn merge_on_upserts_into_an_iceberg_table() {
    let d = workdir("merge");
    let day1 = d.join("day1.jsonl");
    let day2 = d.join("day2.jsonl");
    fs::write(
        &day1,
        "{\"id\":1,\"status\":\"new\"}\n{\"id\":2,\"status\":\"new\"}\n",
    )
    .unwrap();
    fs::write(
        &day2,
        "{\"id\":2,\"status\":\"paid\"}\n{\"id\":3,\"status\":\"new\"}\n{\"id\":3,\"status\":\"paid\"}\n",
    )
    .unwrap();
    let table = d.join("orders");
    let t = table.to_str().unwrap();

    for input in [&day1, &day2] {
        let out = run(&[
            "shape",
            "-i",
            input.to_str().unwrap(),
            "-o",
            t,
            "--to",
            "iceberg",
            "--merge-on",
            "id",
        ]);
        assert!(
            out.status.success(),
            "merge failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("merged on (id)"), "got: {stdout}");
        if input == &day2 {
            assert!(
                stdout.contains("2 key(s) — 1 updated, 1 inserted; 1 prior row(s) replaced"),
                "got: {stdout}"
            );
            assert!(stdout.contains("1 earlier occurrence(s)"), "got: {stdout}");
        }
    }

    let inspect = run(&["inspect", t]);
    let out = String::from_utf8_lossy(&inspect.stdout);
    assert!(
        out.contains("live-records: 3 (5 written, 2 removed by position deletes)"),
        "got: {out}"
    );

    // Compaction applies the deletes: same live rows, no deletes left.
    let compact = run(&["compact", t]);
    let out = String::from_utf8_lossy(&compact.stdout);
    assert!(compact.status.success(), "compact failed: {out}");
    assert!(out.contains("(replace)"), "got: {out}");
    assert!(
        out.contains("rewrote 2 data file(s)") && out.contains("into 1"),
        "got: {out}"
    );
    assert!(
        out.contains("applied 2 position delete(s) from 1 delete file(s)"),
        "got: {out}"
    );
    let inspect = run(&["inspect", t]);
    let out = String::from_utf8_lossy(&inspect.stdout);
    assert!(out.contains("total-records: 3"), "got: {out}");
    let again = run(&["compact", t]);
    let out = String::from_utf8_lossy(&again.stdout);
    assert!(out.starts_with("nothing to compact"), "got: {out}");

    // Expiry: nothing is five days old; with no age limit, the two merges go and the
    // compaction stays, and the rows are the same.
    let young = run(&["expire", t]);
    let out = String::from_utf8_lossy(&young.stdout);
    assert!(out.starts_with("nothing to expire"), "got: {out}");
    let dry = run(&["expire", t, "--older-than", "0s", "--dry-run"]);
    let out = String::from_utf8_lossy(&dry.stdout);
    assert!(
        out.contains("would expire 2 snapshot(s)") && out.contains("dry run"),
        "got: {out}"
    );
    let expire = run(&["expire", t, "--older-than", "0s"]);
    let out = String::from_utf8_lossy(&expire.stdout);
    assert!(expire.status.success(), "expire failed: {out}");
    assert!(out.contains("expired 2 snapshot(s)"), "got: {out}");
    assert!(
        out.contains("deleted 2 data file(s), 1 delete file(s)"),
        "got: {out}"
    );
    let inspect = run(&["inspect", t]);
    let out = String::from_utf8_lossy(&inspect.stdout);
    assert!(out.contains("total-records: 3"), "got: {out}");
}

#[test]
fn compact_refuses_what_is_not_a_table_and_sizes_it_cannot_read() {
    let d = workdir("compact-refused");
    let missing = run(&["compact", d.join("nope").to_str().unwrap()]);
    assert!(!missing.status.success());
    let err = String::from_utf8_lossy(&missing.stderr);
    assert!(err.contains("no Iceberg table"), "got: {err}");

    let bad = run(&["compact", "t", "--target-file-size", "12XB"]);
    assert!(!bad.status.success());
    let err = String::from_utf8_lossy(&bad.stderr);
    assert!(err.contains("unknown size unit"), "got: {err}");

    let missing = run(&["expire", d.join("nope").to_str().unwrap()]);
    let err = String::from_utf8_lossy(&missing.stderr);
    assert!(err.contains("no Iceberg table"), "got: {err}");
    let unitless = run(&["expire", "t", "--older-than", "5"]);
    let err = String::from_utf8_lossy(&unitless.stderr);
    assert!(err.contains("give a unit"), "got: {err}");
    let none = run(&["expire", "t", "--retain-last", "0"]);
    assert!(!none.status.success());
}

#[test]
fn merge_on_is_refused_where_it_cannot_apply() {
    let d = workdir("merge-refused");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let i = input.to_str().unwrap();
    let parquet = d.join("out.parquet");

    let out = run(&[
        "shape",
        "-i",
        i,
        "-o",
        parquet.to_str().unwrap(),
        "--merge-on",
        "id",
    ]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("applies only to Iceberg"), "got: {err}");

    // The lean binary has no object-store support at all, merge or not.
    #[cfg(not(feature = "object_store"))]
    {
        let out = run(&[
            "shape",
            "-i",
            i,
            "-o",
            "s3://bucket/t",
            "--to",
            "iceberg",
            "--merge-on",
            "id",
        ]);
        assert!(!out.status.success());
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("--features object_store"), "got: {err}");
    }

    let table = d.join("t");
    let out = run(&[
        "shape",
        "-i",
        i,
        "-o",
        table.to_str().unwrap(),
        "--to",
        "iceberg",
        "--merge-on",
        "missing",
    ]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("merge_on column `missing`"), "got: {err}");
}

/// The same upsert through the object-store sink: a `file://` URL exercises the code an
/// `s3://` table runs (ranged reads of the existing table, staged delete files).
#[cfg(feature = "object_store")]
#[test]
fn merge_on_upserts_into_an_object_store_table() {
    let d = workdir("merge-url");
    let day1 = d.join("day1.jsonl");
    let day2 = d.join("day2.jsonl");
    fs::write(
        &day1,
        "{\"id\":1,\"status\":\"new\"}\n{\"id\":2,\"status\":\"new\"}\n",
    )
    .unwrap();
    fs::write(
        &day2,
        "{\"id\":2,\"status\":\"paid\"}\n{\"id\":3,\"status\":\"new\"}\n{\"id\":3,\"status\":\"paid\"}\n",
    )
    .unwrap();
    let url = format!("file://{}", d.join("orders").display());

    for input in [&day1, &day2] {
        let out = run(&[
            "shape",
            "-i",
            input.to_str().unwrap(),
            "-o",
            &url,
            "--to",
            "iceberg",
            "--merge-on",
            "id",
        ]);
        assert!(
            out.status.success(),
            "merge failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        if input == &day2 {
            assert!(
                stdout.contains("2 key(s) — 1 updated, 1 inserted; 1 prior row(s) replaced"),
                "got: {stdout}"
            );
        }
    }
    let inspect = run(&["inspect", &url]);
    let out = String::from_utf8_lossy(&inspect.stdout);
    assert!(
        out.contains("live-records: 3 (5 written, 2 removed by position deletes)"),
        "got: {out}"
    );

    let compact = run(&["compact", &url]);
    let out = String::from_utf8_lossy(&compact.stdout);
    assert!(compact.status.success(), "compact failed: {out}");
    assert!(
        out.contains("applied 2 position delete(s) from 1 delete file(s)"),
        "got: {out}"
    );
    let inspect = run(&["inspect", &url]);
    let out = String::from_utf8_lossy(&inspect.stdout);
    assert!(out.contains("total-records: 3"), "got: {out}");

    let expire = run(&["expire", &url, "--older-than", "0s"]);
    let out = String::from_utf8_lossy(&expire.stdout);
    assert!(expire.status.success(), "expire failed: {out}");
    assert!(out.contains("expired 2 snapshot(s)"), "got: {out}");
    let inspect = run(&["inspect", &url]);
    let out = String::from_utf8_lossy(&inspect.stdout);
    assert!(out.contains("total-records: 3"), "got: {out}");

    // The JSON summary names the version and metadata the store now holds.
    let (out, doc) = run_json(&[
        "shape",
        "-i",
        day1.to_str().unwrap(),
        "-o",
        &url,
        "--to",
        "iceberg",
        "--merge-on",
        "id",
    ]);
    assert!(out.status.success(), "{doc}");
    assert_eq!(doc["table"]["version"], 5);
    assert_eq!(
        doc["table"]["metadata"],
        format!("{url}/metadata/v5.metadata.json").as_str()
    );
    assert_eq!(doc["merge"]["keys_updated"], 2);
    let (_, doc) = run_json(&["compact", &url]);
    assert_eq!(
        (doc["committed"].as_bool(), doc["version"].as_u64()),
        (Some(true), Some(6))
    );
    let (_, doc) = run_json(&["expire", &url, "--older-than", "0s"]);
    assert_eq!(
        (doc["committed"].as_bool(), doc["version"].as_u64()),
        (Some(true), Some(7))
    );
    assert_eq!(doc["snapshots"]["retained"], 1);
}

/// Run with `--summary-json` and parse stdout, which must be exactly one JSON document.
fn run_json(args: &[&str]) -> (std::process::Output, serde_json::Value) {
    let mut args = args.to_vec();
    args.push("--summary-json");
    let out = run(&args);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let doc: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout is not one JSON document ({e}): {stdout}"));
    assert_eq!(doc["summary_version"], 1, "{doc}");
    (out, doc)
}

#[test]
fn summary_json_reports_every_run_as_one_document() {
    let d = workdir("summary-json");
    let day1 = d.join("day1.jsonl");
    let day2 = d.join("day2.jsonl");
    fs::write(
        &day1,
        "{\"id\":1,\"status\":\"new\"}\n{\"id\":2,\"status\":\"new\"}\nnot json\n",
    )
    .unwrap();
    fs::write(
        &day2,
        "{\"id\":2,\"status\":\"paid\"}\n{\"id\":3,\"status\":\"new\"}\n",
    )
    .unwrap();
    let table = d.join("orders");
    let t = table.to_str().unwrap();
    let shape = |input: &PathBuf| {
        run_json(&[
            "shape",
            "-i",
            input.to_str().unwrap(),
            "-o",
            t,
            "--to",
            "iceberg",
            "--merge-on",
            "id",
        ])
    };

    // A fresh table: version 1, one parse error sidecarred, nothing replaced.
    let (out, doc) = shape(&day1);
    assert!(out.status.success(), "{doc}");
    assert_eq!(doc["command"], "shape");
    assert_eq!(doc["ok"], true);
    assert_eq!(doc["output"]["format"], "iceberg");
    assert_eq!(doc["rows"]["in"], 2);
    assert_eq!(doc["rows"]["out"], 2);
    assert_eq!(doc["rows"]["parse_errors"], 1);
    assert_eq!(doc["rejects"]["rows"], 1);
    assert!(doc["rejects"]["path"]
        .as_str()
        .unwrap()
        .ends_with("orders.rejects.jsonl"));
    assert_eq!(doc["table"]["version"], 1);
    let first_snapshot = doc["table"]["snapshot_id"].as_str().unwrap().to_string();
    first_snapshot.parse::<i64>().unwrap();
    assert!(doc["table"]["metadata"]
        .as_str()
        .unwrap()
        .ends_with("metadata/v1.metadata.json"));
    assert_eq!(doc["merge"]["keys_inserted"], 2);
    // Drift was checked (the default `warn`) and found nothing.
    assert_eq!(doc["drift"]["policy"], "warn");
    assert_eq!(doc["drift"]["rows_with_drift"], 0);
    assert!(doc["drift"]["report"].is_null());

    // The merge: version 2, one key updated.
    let (out, doc) = shape(&day2);
    assert!(out.status.success(), "{doc}");
    assert_eq!(doc["table"]["version"], 2);
    assert!(doc["rejects"].is_null());
    let m = &doc["merge"];
    assert_eq!(m["key_columns"], serde_json::json!(["id"]));
    assert_eq!(
        (m["keys_updated"].as_u64(), m["keys_inserted"].as_u64()),
        (Some(1), Some(1))
    );
    assert_eq!(m["prior_rows_replaced"], 1);
    assert_eq!(m["delete_files"], 1);

    // Compaction: version 3, the delete applied.
    let (out, doc) = run_json(&["compact", t]);
    assert!(out.status.success(), "{doc}");
    assert_eq!(doc["command"], "compact");
    assert_eq!(doc["committed"], true);
    assert_eq!(doc["version"], 3);
    assert_eq!(doc["data_files"]["rewritten"], 2);
    assert_eq!(doc["data_files"]["written"], 1);
    assert_eq!(doc["delete_files_removed"], 1);
    assert_eq!(doc["position_deletes_applied"], 1);
    assert_eq!(doc["rows_rewritten"], 3);
    let (_, again) = run_json(&["compact", t]);
    assert_eq!(again["committed"], false);
    assert!(again["version"].is_null() && again["snapshot_id"].is_null());

    // Expiry: the dry run names what a real run then does.
    let (out, dry) = run_json(&["expire", t, "--older-than", "0s", "--dry-run"]);
    assert!(out.status.success(), "{dry}");
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["committed"], false);
    assert!(dry["version"].is_null());
    let (out, doc) = run_json(&["expire", t, "--older-than", "0s"]);
    assert!(out.status.success(), "{doc}");
    assert_eq!(doc["committed"], true);
    assert_eq!(doc["version"], 4);
    assert_eq!(doc["snapshots"], dry["snapshots"]);
    assert_eq!(doc["deleted"], dry["deleted"]);
    assert_eq!(doc["snapshots"]["expired"][0], first_snapshot.as_str());
    assert_eq!(doc["snapshots"]["retained"], 1);
    assert_eq!(doc["deleted"]["data_files"], 2);
    assert_eq!(doc["deleted"]["delete_files"], 1);

    // Parquet output has no table.
    let (out, doc) = run_json(&[
        "shape",
        "-i",
        day1.to_str().unwrap(),
        "-o",
        d.join("p.parquet").to_str().unwrap(),
    ]);
    assert!(out.status.success(), "{doc}");
    assert_eq!(doc["output"]["format"], "parquet");
    assert!(doc["table"].is_null() && doc["merge"].is_null());
}

#[test]
fn summary_json_names_the_drift_and_its_report() {
    let (d, spec) = drift_fixture("summary-json-drift");
    let input = d.join("in.jsonl");
    let out = d.join("out.parquet");
    let args = [
        "shape",
        "--spec",
        spec.to_str().unwrap(),
        "--input",
        input.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ];
    let (run, doc) = run_json(&args);
    assert!(run.status.success(), "{doc}");
    let drift = &doc["drift"];
    assert_eq!(drift["policy"], "warn");
    assert_eq!(drift["rows_scanned"], 4);
    assert_eq!(drift["rows_with_drift"], 2);
    assert_eq!(drift["new_fields"], serde_json::json!(["currency"]));
    assert_eq!(drift["type_mismatches"], serde_json::json!(["amount"]));
    assert_eq!(doc["rows"]["out"], 4);
    let report = PathBuf::from(drift["report"].as_str().unwrap());
    assert_eq!(report, d.join("out.parquet.drift.json"));
    assert!(report.exists());

    // Quarantined rows are counted as rejects, with their sidecar.
    let mut quarantine = args.to_vec();
    quarantine.extend(["--on-drift", "quarantine"]);
    let (run, doc) = run_json(&quarantine);
    assert!(run.status.success(), "{doc}");
    assert_eq!(doc["drift"]["rows_quarantined"], 2);
    assert_eq!(doc["rows"]["out"], 2);
    assert_eq!(doc["rows"]["rejected"], 2);
    assert_eq!(doc["rejects"]["rows"], 2);

    // A run that fails on drift says so, in either writer mode.
    for pipeline in ["on", "off"] {
        let mut error = args.to_vec();
        error.extend(["--on-drift", "error", "--pipeline", pipeline]);
        let (run, doc) = run_json(&error);
        assert_eq!(run.status.code(), Some(1), "{doc}");
        assert_eq!(doc["error"]["kind"], "drift", "{doc}");
    }

    // With drift checks off there is nothing to report.
    let mut ignore = args.to_vec();
    ignore.extend(["--on-drift", "ignore"]);
    let (run, doc) = run_json(&ignore);
    assert!(run.status.success(), "{doc}");
    assert!(doc["drift"].is_null(), "{doc}");

    fs::remove_dir_all(&d).ok();
}

#[test]
fn summary_json_reports_a_failure_as_one_document() {
    let d = workdir("summary-json-err");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();

    // No table: a failure the engine did not name.
    let (out, doc) = run_json(&["compact", d.join("nope").to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(doc["command"], "compact");
    assert_eq!(doc["ok"], false);
    assert_eq!(doc["error"]["kind"], "other");
    assert!(doc["error"]["message"]
        .as_str()
        .unwrap()
        .contains("no Iceberg table"));
    // The text error still goes to stderr.
    assert!(String::from_utf8_lossy(&out.stderr).contains("no Iceberg table"));

    // A bad spec.
    let spec = d.join("bad.yaml");
    fs::write(
        &spec,
        "dataset: x\nsource: {format: jsonl}\noutput: {format: parquet, path: x}\nbogus: 1\n",
    )
    .unwrap();
    let (out, doc) = run_json(&[
        "shape",
        "-s",
        spec.to_str().unwrap(),
        "-i",
        input.to_str().unwrap(),
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(doc["error"]["kind"], "spec");

    // A leftover later version from an older shapeshift: refused, and not as a conflict
    // (running it again would not help).
    let table = d.join("t");
    let t = table.to_str().unwrap();
    let args = [
        "shape",
        "-i",
        input.to_str().unwrap(),
        "-o",
        t,
        "--to",
        "iceberg",
    ];
    assert!(run(&args).status.success());
    fs::write(table.join("metadata/v2.metadata.json"), "{}").unwrap();
    let mut append = args.to_vec();
    append.push("--append");
    let (out, doc) = run_json(&append);
    assert_eq!(out.status.code(), Some(1), "{doc}");
    assert_eq!(doc["error"]["kind"], "sink");
    assert!(doc["error"]["message"]
        .as_str()
        .unwrap()
        .contains("left over"));
}
