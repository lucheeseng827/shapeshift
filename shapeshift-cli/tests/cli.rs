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
