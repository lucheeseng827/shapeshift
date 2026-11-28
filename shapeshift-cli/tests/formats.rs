//! CSV, TSV and XML inputs through the binary: `convert`, `infer` and `shape`.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn workdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-fmt-{}-{}", std::process::id(), name));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_shapeshift"))
        .args(args)
        .output()
        .unwrap()
}

fn ok(o: &std::process::Output) -> String {
    assert!(
        o.status.success(),
        "failed: {}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn lines(text: &str) -> Vec<serde_json::Value> {
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn convert_writes_json_lines_and_stops_at_a_bad_row_unless_told() {
    let d = workdir("convert");
    let input = d.join("in.csv");
    fs::write(&input, "id,name\n1,Ada\n2\n3,\"Lin, Jr\"\n").unwrap();
    let out = d.join("out.jsonl");
    let (i, o) = (input.to_str().unwrap(), out.to_str().unwrap());

    // The ragged row stops the run, and no half-written output is left behind.
    let r = run(&["convert", "-i", i, "--format", "csv", "-o", o]);
    assert!(!r.status.success());
    let err = String::from_utf8_lossy(&r.stderr);
    assert!(
        err.contains("line 3") && err.contains("--skip-bad"),
        "{err}"
    );
    assert!(!out.exists());
    assert_eq!(
        fs::read_dir(&d).unwrap().count(),
        1,
        "no temporary file is left"
    );

    let r = run(&["convert", "-i", i, "--format", "csv", "-o", o, "--skip-bad"]);
    ok(&r);
    let err = String::from_utf8_lossy(&r.stderr);
    assert!(
        err.contains("converted 2 records") && err.contains("left out 1"),
        "{err}"
    );
    assert_eq!(
        lines(&fs::read_to_string(&out).unwrap()),
        vec![
            serde_json::json!({"id":1,"name":"Ada"}),
            serde_json::json!({"id":3,"name":"Lin, Jr"})
        ]
    );

    // To standard output, with a limit.
    let s = ok(&run(&[
        "convert",
        "-i",
        i,
        "--format",
        "csv",
        "--skip-bad",
        "--limit",
        "1",
    ]));
    assert_eq!(lines(&s), vec![serde_json::json!({"id":1,"name":"Ada"})]);
    fs::remove_dir_all(&d).ok();
}

#[test]
fn infer_records_the_csv_options_and_shape_reads_them_back() {
    let d = workdir("csv-spec");
    let input = d.join("orders.csv");
    fs::write(&input, "1;Ada;12.50\n2;Grace;3\n").unwrap();
    let spec = d.join("orders.yaml");
    let i = input.to_str().unwrap();
    ok(&run(&[
        "infer",
        "-i",
        i,
        "--format",
        "csv",
        "--delimiter",
        ";",
        "--no-header",
        "--columns",
        "id,name,amount",
        "-o",
        spec.to_str().unwrap(),
    ]));
    let text = fs::read_to_string(&spec).unwrap();
    assert!(
        text.contains("format: csv") && text.contains("delimiter: ';'"),
        "{text}"
    );
    assert!(
        text.contains("name: amount") && text.contains("type: float64"),
        "{text}"
    );

    let out = d.join("orders.parquet");
    let s = ok(&run(&[
        "shape",
        "--spec",
        spec.to_str().unwrap(),
        "-o",
        out.to_str().unwrap(),
    ]));
    assert!(
        s.contains("rows_in=2") && s.contains("parse_errors=0"),
        "{s}"
    );
    let s = ok(&run(&["inspect", out.to_str().unwrap()]));
    assert!(s.contains("rows: 2"), "{s}");
    fs::remove_dir_all(&d).ok();
}

#[test]
fn a_broken_xml_document_fails_even_with_skip_bad() {
    let d = workdir("xml-broken");
    let input = d.join("in.xml");
    fs::write(&input, "<r><a>1</a><a>2</b><a>3</a></r>").unwrap();
    let r = run(&[
        "convert",
        "-i",
        input.to_str().unwrap(),
        "--format",
        "xml",
        "--xml-record",
        "a",
        "--skip-bad",
    ]);
    assert!(
        !r.status.success(),
        "a truncated read must not look like a skipped record"
    );
    assert!(String::from_utf8_lossy(&r.stderr).contains("past record 2"));
    fs::remove_dir_all(&d).ok();
}

#[test]
fn source_flags_with_a_spec_are_refused_not_ignored() {
    let r = run(&[
        "shape",
        "--spec",
        "s.yaml",
        "--delimiter",
        ";",
        "-o",
        "o.parquet",
    ]);
    assert!(!r.status.success());
    assert!(String::from_utf8_lossy(&r.stderr).contains("with --spec, set them"));
}

#[test]
fn xml_shapes_by_record_path_and_needs_one() {
    let d = workdir("xml");
    let input = d.join("orders.xml");
    fs::write(
        &input,
        r#"<?xml version="1.0"?>
<Orders><Order id="1"><Customer><Name>Ada</Name></Customer><Total>9.5</Total></Order>
<Order id="2"><Customer><Name>Grace</Name></Customer><Total>4</Total></Order></Orders>"#,
    )
    .unwrap();
    let (i, out) = (input.to_str().unwrap(), d.join("orders.parquet"));

    let r = run(&[
        "shape",
        "-i",
        i,
        "--format",
        "xml",
        "-o",
        out.to_str().unwrap(),
    ]);
    assert!(!r.status.success());
    let err = String::from_utf8_lossy(&r.stderr);
    assert!(err.contains("--xml-record"), "{err}");

    let s = ok(&run(&[
        "shape",
        "-i",
        i,
        "--format",
        "xml",
        "--xml-record",
        "/Orders/Order",
        "-o",
        out.to_str().unwrap(),
    ]));
    assert!(s.contains("rows_in=2"), "{s}");
    let s = ok(&run(&["inspect", out.to_str().unwrap()]));
    assert!(
        s.contains("rows: 2") && s.contains("Customer.Name") && s.contains("@id"),
        "{s}"
    );
    fs::remove_dir_all(&d).ok();
}

#[test]
fn a_tsv_converts_and_a_doctype_is_refused() {
    let d = workdir("tsv-xml");
    let tsv = d.join("in.tsv");
    fs::write(&tsv, "a\tb\nx y\t2\n").unwrap();
    let s = ok(&run(&[
        "convert",
        "-i",
        tsv.to_str().unwrap(),
        "--format",
        "tsv",
    ]));
    assert_eq!(lines(&s), vec![serde_json::json!({"a":"x y","b":2})]);

    let xml = d.join("evil.xml");
    fs::write(
        &xml,
        r#"<!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]><r><a>&x;</a></r>"#,
    )
    .unwrap();
    let r = run(&[
        "convert",
        "-i",
        xml.to_str().unwrap(),
        "--format",
        "xml",
        "--xml-record",
        "a",
        "--skip-bad",
    ]);
    assert!(
        !r.status.success(),
        "a refused document fails even with --skip-bad"
    );
    assert!(String::from_utf8_lossy(&r.stderr).contains("document type"));
    assert!(r.stdout.is_empty());
    fs::remove_dir_all(&d).ok();
}
