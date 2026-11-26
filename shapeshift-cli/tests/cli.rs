//! End-to-end CLI test: run the real `shapeshift` binary over a temp JSONL and
//! assert the shape + inspect + cost flows.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::json;

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
fn a_catalog_takes_the_place_of_the_output_path() {
    let d = workdir("catalog");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let run = |extra: &[&str]| {
        let mut args = vec!["shape", "--input", input.to_str().unwrap()];
        args.extend_from_slice(extra);
        let out = Command::new(bin()).args(&args).output().unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    // The catalog names the table's location, so --output conflicts with it, and the two
    // catalog flags come together.
    let (code, err) = run(&[
        "--catalog",
        "http://127.0.0.1:1",
        "--table",
        "a.t",
        "-o",
        "x",
    ]);
    assert_eq!(code, Some(2), "{err}");
    let (code, err) = run(&["--table", "a.t", "-o", "x"]);
    assert_eq!(code, Some(2), "{err}");
    // Without --output it is accepted, as an Iceberg table. The default build has no
    // catalog client and says so; a build with one fails to reach this catalog.
    let (code, err) = run(&["--catalog", "http://127.0.0.1:1", "--table", "a.t"]);
    assert_eq!(code, Some(1), "{err}");
    let expected = if cfg!(feature = "object_store") {
        "could not reach the catalog"
    } else {
        "needs a build with `--features object_store`"
    };
    assert!(err.contains(expected), "{err}");
    // An explicit --to parquet is refused rather than silently changed.
    let (code, err) = run(&[
        "--catalog",
        "http://127.0.0.1:1",
        "--table",
        "a.t",
        "--to",
        "parquet",
    ]);
    assert_eq!(code, Some(1), "{err}");
    assert!(
        err.contains("`output.catalog` applies only to Iceberg tables"),
        "{err}"
    );
    // A merge goes through the catalog too (it was refused before the catalog was reached).
    let (code, err) = run(&[
        "--catalog",
        "http://127.0.0.1:1",
        "--table",
        "a.t",
        "--merge-on",
        "id",
    ]);
    assert_eq!(code, Some(1), "{err}");
    assert!(err.contains(expected), "{err}");
    // `compact` and `expire` take --catalog, with the table named `<namespace>.<name>`; a
    // summary still comes out as JSON when the catalog cannot be reached.
    for cmd in ["compact", "expire"] {
        let out = Command::new(bin())
            .args([
                cmd,
                "--catalog",
                "http://127.0.0.1:1",
                "a.t",
                "--summary-json",
            ])
            .output()
            .unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(1), "{cmd}: {err}");
        assert!(err.contains(expected), "{cmd}: {err}");
        let doc: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!((&doc["command"], &doc["ok"]), (&json!(cmd), &json!(false)));
    }
    fs::remove_dir_all(&d).ok();
}

/// A table moved after it was written says so in `inspect`, and a write into it warns, since
/// other engines' default readers look for its files where it was. shapeshift itself reads
/// and writes it where it is.
#[test]
fn a_moved_table_says_so_and_a_write_into_it_warns() {
    let d = workdir("moved");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let (a, b) = (d.join("a"), d.join("b"));
    let run = |args: &[&str]| {
        let out = Command::new(bin()).args(args).output().unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let (i, at) = (input.to_str().unwrap(), a.to_str().unwrap());
    run(&["shape", "-i", i, "-o", at, "--to", "iceberg"]);
    let (out, _) = run(&["inspect", at]);
    assert!(!out.contains("moved:"), "{out}");
    let (_, err) = run(&["shape", "-i", i, "-o", at, "--to", "iceberg", "--append"]);
    assert!(!err.contains("not where its metadata says"), "{err}");

    fs::rename(&a, &b).unwrap();
    let to = b.to_str().unwrap();
    let (out, _) = run(&["inspect", to]);
    assert!(
        out.contains("moved: its metadata says it is at") && out.contains(at),
        "{out}"
    );
    assert!(out.contains("total-records: 4"), "{out}");
    let (_, err) = run(&["shape", "-i", i, "-o", to, "--to", "iceberg", "--append"]);
    assert!(
        err.contains("the table is not where its metadata says"),
        "{err}"
    );
    let (out, _) = run(&["inspect", to]);
    assert!(out.contains("total-records: 6"), "{out}");
    fs::remove_dir_all(&d).ok();
}

/// `relocate`: a copy whose original still answers is refused until named one (exit 1, the
/// way out in the message); `--dry-run` changes nothing; `--copy` re-anchors it with a new
/// identity, reported in `--summary-json`; `inspect` stops saying it moved; a second run
/// has nothing to do.
#[test]
fn relocate_reanchors_a_copied_table() {
    let d = workdir("relocate");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1,\"v\":1}\n{\"id\":2,\"v\":2}\n").unwrap();
    let again = d.join("again.jsonl");
    fs::write(&again, "{\"id\":2,\"v\":20}\n").unwrap();
    let (a, b) = (d.join("a"), d.join("b"));
    let run = |args: &[&str]| {
        let out = Command::new(bin()).args(args).output().unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let at = a.to_str().unwrap();
    for i in [&input, &again] {
        let (code, _, err) = run(&[
            "shape",
            "-i",
            i.to_str().unwrap(),
            "-o",
            at,
            "--to",
            "iceberg",
            "--merge-on",
            "id",
        ]);
        assert_eq!(code, Some(0), "{err}");
    }
    let cp = Command::new("cp")
        .args(["-r", at, b.to_str().unwrap()])
        .status();
    assert!(cp.unwrap().success());
    let to = b.to_str().unwrap();

    let (code, _, err) = run(&["relocate", to]);
    assert_eq!(code, Some(1), "{err}");
    assert!(
        err.contains("still answers at") && err.contains("--copy"),
        "{err}"
    );
    let (code, out, err) = run(&["relocate", to, "--dry-run"]);
    assert_eq!(code, Some(0), "{err}");
    assert!(
        out.contains("would relocate") && out.contains("dry run"),
        "{out}"
    );
    assert!(out.contains("a real run needs --moved"), "{out}");
    let (_, out, _) = run(&["inspect", to]);
    assert!(out.contains("moved:"), "a dry run changes nothing: {out}");
    // Registering takes a catalog and a name together; a dry run registers nothing, so it
    // reaches no catalog (none listens on port 1) and says what it would register.
    let (code, _, err) = run(&["relocate", to, "--table", "lake.t"]);
    assert_eq!(code, Some(2), "{err}");
    let (code, _, err) = run(&["relocate", to, "--catalog", "http://127.0.0.1:1"]);
    assert_eq!(code, Some(2), "{err}");
    let (code, out, err) = run(&[
        "relocate",
        to,
        "--dry-run",
        "--copy",
        "--catalog",
        "http://127.0.0.1:1",
        "--table",
        "lake.t",
    ]);
    assert_eq!(code, Some(0), "{err}");
    assert!(
        out.contains("would register lake.t with the catalog at http://127.0.0.1:1"),
        "{out}"
    );
    // Credentials in the catalog URI: refused before anything runs, a dry run included, and
    // never printed.
    let (code, out, err) = run(&[
        "relocate",
        to,
        "--dry-run",
        "--copy",
        "--catalog",
        "http://reader:s3cr3t@127.0.0.1:1",
        "--table",
        "lake.t",
    ]);
    assert_eq!(code, Some(1), "{out}{err}");
    assert!(err.contains("carries credentials"), "{err}");
    assert!(
        !out.contains("s3cr3t") && !err.contains("s3cr3t"),
        "{out}{err}"
    );

    let (code, out, err) = run(&["relocate", to, "--copy", "--summary-json"]);
    assert_eq!(code, Some(0), "{err}");
    let doc: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(doc["command"], "relocate");
    assert_eq!(doc["committed"], true);
    assert_eq!(doc["new_uuid"], true);
    assert_eq!(doc["to"], b.canonicalize().unwrap().to_str().unwrap());
    assert_eq!(doc["snapshots"], 2);
    assert!(
        doc["rewritten"]["delete_files"].as_u64().unwrap() > 0,
        "{doc}"
    );
    assert_eq!(doc["superseded"]["not_deleted"], 0);
    let (_, out, _) = run(&["inspect", to]);
    assert!(!out.contains("moved:"), "{out}");
    assert!(out.contains("live-records: 2"), "{out}");

    let (code, out, _) = run(&["relocate", to]);
    assert_eq!(code, Some(0));
    assert!(out.contains("already anchored where it is"), "{out}");
    let (code, _, err) = run(&["relocate", to, "--moved", "--copy"]);
    assert_eq!(code, Some(2), "{err}");
    fs::remove_dir_all(&d).ok();
}

#[test]
fn catalog_credentials_come_from_the_environment_never_the_spec() {
    let d = workdir("catalog-auth");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let spec = d.join("s.yaml");
    let run = |args: &[&str], env: &[(&str, &str)]| {
        let mut cmd = Command::new(bin());
        cmd.args(args)
            .env_remove("SHAPESHIFT_CATALOG_TOKEN")
            .env_remove("SHAPESHIFT_CATALOG_CREDENTIAL")
            .env_remove("SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI")
            .env_remove("SHAPESHIFT_CATALOG_SCOPE");
        for (k, v) in env {
            cmd.env(k, v);
        }
        let out = cmd.output().unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };

    // A token written into a spec is refused, and the error says where it goes instead.
    fs::write(
        &spec,
        "dataset: s\noutput:\n  format: iceberg\n  catalog:\n    uri: http://127.0.0.1:1\n    \
         table: a.t\n    token: do-not-commit-me\n",
    )
    .unwrap();
    let (code, err) = run(
        &[
            "shape",
            "-s",
            spec.to_str().unwrap(),
            "-i",
            input.to_str().unwrap(),
        ],
        &[],
    );
    assert_eq!(code, Some(1), "{err}");
    assert!(
        err.contains("unknown field `token`")
            && err.contains("never go in a spec")
            && err.contains("SHAPESHIFT_CATALOG_TOKEN"),
        "{err}"
    );

    // --warehouse belongs to a catalog.
    let out = d.join("o.parquet");
    let (code, err) = run(
        &[
            "shape",
            "-i",
            input.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
            "--warehouse",
            "w",
        ],
        &[],
    );
    assert_eq!(code, Some(1), "{err}");
    assert!(
        err.contains("--warehouse names a warehouse of a REST catalog"),
        "{err}"
    );
    let (code, err) = run(&["compact", "a.t", "--warehouse", "w"], &[]);
    assert_eq!(code, Some(2), "{err}");
    // So does --insecure-catalog.
    let (code, err) = run(
        &[
            "shape",
            "-i",
            input.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
            "--insecure-catalog",
        ],
        &[],
    );
    assert_eq!(code, Some(1), "{err}");
    assert!(err.contains("writes to no catalog"), "{err}");
    let (code, err) = run(&["expire", "a.t", "--insecure-catalog"], &[]);
    assert_eq!(code, Some(2), "{err}");

    let catalog = [
        "--catalog",
        "http://127.0.0.1:1",
        "--table",
        "a.t",
        "--warehouse",
        "w",
    ];
    let mut shape = vec!["shape", "-i", input.to_str().unwrap()];
    shape.extend_from_slice(&catalog);
    if cfg!(feature = "object_store") {
        // Two kinds of credentials at once is a mistake, refused before any request.
        let (code, err) = run(
            &shape,
            &[
                ("SHAPESHIFT_CATALOG_TOKEN", "tok-value"),
                ("SHAPESHIFT_CATALOG_CREDENTIAL", "id:secret-value"),
            ],
        );
        assert_eq!(code, Some(1), "{err}");
        assert!(err.contains("set one"), "{err}");
        // A token is used, and never shown.
        let (code, err) = run(&shape, &[("SHAPESHIFT_CATALOG_TOKEN", "tok-value")]);
        assert_eq!(code, Some(1), "{err}");
        assert!(err.contains("could not reach the catalog"), "{err}");
        assert!(!err.contains("tok-value"), "{err}");
        // Not over plain http to another machine, unless the run asks for the insecure
        // mode in so many words (and then it is warned about). Likewise for compact and
        // expire.
        let token = [("SHAPESHIFT_CATALOG_TOKEN", "tok-value")];
        let away = "http://catalog.invalid:8181";
        let mut shape_away = vec!["shape", "-i", input.to_str().unwrap()];
        shape_away.extend_from_slice(&["--catalog", away, "--table", "a.t"]);
        for args in [
            shape_away,
            vec!["compact", "a.t", "--catalog", away],
            vec!["expire", "a.t", "--catalog", away],
        ] {
            let (code, err) = run(&args, &token);
            assert_eq!(code, Some(1), "{args:?}: {err}");
            assert!(
                err.contains("refusing to send catalog credentials over plain http")
                    && err.contains("--insecure-catalog"),
                "{args:?}: {err}"
            );
            assert!(!err.contains("tok-value"), "{err}");
            let insecure = [args.as_slice(), &["--insecure-catalog"]].concat();
            let (code, err) = run(&insecure, &token);
            assert_eq!(code, Some(1), "{args:?}: {err}");
            assert!(!err.contains("refusing"), "{args:?}: {err}");
            assert!(
                err.contains("sending catalog credentials over plain http"),
                "a warning: {args:?}: {err}"
            );
            assert!(!err.contains("tok-value"), "{err}");
        }
    } else {
        let (code, err) = run(&shape, &[("SHAPESHIFT_CATALOG_TOKEN", "tok-value")]);
        assert_eq!(code, Some(1), "{err}");
        assert!(
            err.contains("needs a build with `--features object_store`"),
            "{err}"
        );
    }
    fs::remove_dir_all(&d).ok();
}

/// On a system with no CA certificates, the HTTP clients cannot start. A catalog run and an
/// object-store run both say so, and what to do, rather than "builder error". (The system's
/// roots are hidden with SSL_CERT_FILE and SSL_CERT_DIR, which the TLS setup reads on Linux;
/// macOS and Windows verify against the system's own store and ignore them.) The remedy the
/// message gives, SSL_CERT_FILE pointed at a bundle, lets the run reach the catalog.
#[cfg(all(feature = "object_store", target_os = "linux"))]
#[test]
fn a_system_without_ca_certificates_is_named_as_the_cause() {
    let d = workdir("no-ca-certs");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let empty = d.join("no-certs");
    fs::create_dir_all(&empty).unwrap();
    let input = input.to_str().unwrap();
    for args in [
        vec![
            "shape",
            "-i",
            input,
            "--catalog",
            "http://127.0.0.1:1",
            "--table",
            "lake.t",
        ],
        vec![
            "shape",
            "-i",
            input,
            "-o",
            "s3://bucket/t",
            "--to",
            "iceberg",
        ],
    ] {
        // Only these variables: none of the caller's (an AWS_PROFILE, an AWS_ENDPOINT) can
        // steer the run.
        let out = Command::new(bin())
            .args(&args)
            .env_clear()
            .env("SSL_CERT_FILE", "/dev/null")
            .env("SSL_CERT_DIR", &empty)
            .env("RUST_LOG", "off")
            .env("AWS_ACCESS_KEY_ID", "x")
            .env("AWS_SECRET_ACCESS_KEY", "y")
            .env("AWS_REGION", "us-east-1")
            .output()
            .unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{args:?}");
        assert!(
            err.contains("no CA certificates were found") && err.contains("ca-certificates"),
            "{args:?}: {err}"
        );
        assert!(
            !err.contains("sink error: builder error"),
            "{args:?}: {err}"
        );
    }

    // The remedy: SSL_CERT_FILE at a bundle (here one throwaway root, its key discarded).
    // The client starts and the run reaches the catalog, which answers 401.
    let bundle = d.join("bundle.pem");
    fs::write(&bundle, TEST_ROOT_PEM).unwrap();
    let (addr, seen) = listener();
    let uri = format!("http://{addr}");
    let out = Command::new(bin())
        .args(["shape", "-i", input, "--catalog", &uri, "--table", "lake.t"])
        .env_clear()
        .env("SSL_CERT_FILE", &bundle)
        .env("SSL_CERT_DIR", &empty)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!err.contains("no CA certificates"), "{err}");
    assert!(
        !seen.lock().unwrap().is_empty(),
        "the catalog was not reached: {err}"
    );
}

/// A self-signed root for the test above; its private key was discarded when it was made.
#[cfg(all(feature = "object_store", target_os = "linux"))]
const TEST_ROOT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBlDCCATugAwIBAgIUd8ikWnR+wSdWcAB7PVkqZ89V2+owCgYIKoZIzj0EAwIw\n\
HzEdMBsGA1UEAwwUc2hhcGVzaGlmdCB0ZXN0IHJvb3QwIBcNMjYxMTI2MTExODAy\n\
WhgPMjEyNjExMDIxMTE4MDJaMB8xHTAbBgNVBAMMFHNoYXBlc2hpZnQgdGVzdCBy\n\
b290MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEUpB9y/IgK1Q3ib4ZmL2p8sgB\n\
ao554Be8tlGJL8M22jSiO4iutQ9WCMcw982jz+cSvp6J1tHLU57t1rHR8G3dWKNT\n\
MFEwHQYDVR0OBBYEFFifigRTMumd+oRnyZCA6VGYK301MB8GA1UdIwQYMBaAFFif\n\
igRTMumd+oRnyZCA6VGYK301MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwID\n\
RwAwRAIgGfYLVroRMnw2gogoyYGoGfuaxcJL8UDlqj5OOJeMARwCIAgR/KX3uw69\n\
92NhacpfQ9g+eDxNJJGMBP79kv32S2ZW\n\
-----END CERTIFICATE-----\n";

/// A one-shot HTTP listener on this machine: answers whatever comes with a 401 and keeps
/// the request head, so a test can tell which address a client actually reached.
#[cfg(feature = "object_store")]
fn listener() -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use std::io::{BufRead, BufReader, Write};
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("127.0.0.1:{}", l.local_addr().unwrap().port());
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let keep = seen.clone();
    std::thread::spawn(move || {
        for conn in l.incoming().flatten() {
            let mut r = BufReader::new(conn.try_clone().unwrap());
            let mut head = String::new();
            let mut line = String::new();
            while r.read_line(&mut line).is_ok_and(|n| n > 2) {
                head.push_str(&line);
                line.clear();
            }
            keep.lock().unwrap().push(head);
            let body = r#"{"error":{"message":"no","type":"NotAuthorizedException","code":401}}"#;
            let mut out = conn;
            let _ = write!(
                out,
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (addr, seen)
}

#[cfg(feature = "object_store")]
#[test]
fn credentials_to_this_machine_never_go_through_a_proxy() {
    // A proxy set for every scheme and nothing exempt from it: a client that honoured it for
    // a loopback catalog would hand the proxy the token, or the secret, in the clear.
    let d = workdir("catalog-proxy");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    for credential in [false, true] {
        let (proxy, trapped) = listener();
        let (catalog, reached) = listener();
        let (idp, asked) = listener();
        let mut cmd = Command::new(bin());
        cmd.args(["shape", "-i", input.to_str().unwrap()]).args([
            "--catalog",
            &format!("http://{catalog}"),
            "--table",
            "a.t",
        ]);
        for k in [
            "NO_PROXY",
            "no_proxy",
            "SHAPESHIFT_CATALOG_TOKEN",
            "SHAPESHIFT_CATALOG_CREDENTIAL",
            "SHAPESHIFT_CATALOG_SCOPE",
        ] {
            cmd.env_remove(k);
        }
        for k in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            cmd.env(k, format!("http://{proxy}"));
        }
        if credential {
            cmd.env("SHAPESHIFT_CATALOG_CREDENTIAL", "id:secret-value")
                .env(
                    "SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI",
                    format!("http://{idp}/token"),
                );
        } else {
            cmd.env("SHAPESHIFT_CATALOG_TOKEN", "tok-value")
                .env_remove("SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI");
        }
        let out = cmd.output().unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(1), "{err}");
        assert!(
            trapped.lock().unwrap().is_empty(),
            "the proxy was handed a request: {:?}",
            trapped.lock().unwrap()
        );
        if credential {
            // The token endpoint was asked directly (and refused), so the catalog never was.
            let asked = asked.lock().unwrap();
            assert_eq!(asked.len(), 1, "{err}");
            assert!(asked[0].starts_with("POST /token"), "{asked:?}");
            assert!(err.contains("refused the client credentials"), "{err}");
        } else {
            let reached = reached.lock().unwrap();
            assert_eq!(reached.len(), 1, "{err}");
            assert!(reached[0].starts_with("GET /v1/config"), "{reached:?}");
            assert!(reached[0].contains("Bearer tok-value"), "{reached:?}");
        }
    }
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

/// A catalog URI that carries credentials is refused, with or without object-store support,
/// and the secret is never printed: the URI is repeated in errors and summaries, so
/// credentials belong in the environment.
#[test]
fn a_catalog_uri_with_credentials_never_reaches_the_output() {
    let d = workdir("catalog-userinfo");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();
    let uri = "http://reader:s3cr3t@127.0.0.1:1";
    let input = input.to_str().unwrap();
    for args in [
        vec!["shape", "-i", input, "--catalog", uri, "--table", "lake.t"],
        vec!["compact", "lake.t", "--catalog", uri],
        vec!["expire", "lake.t", "--catalog", uri, "--older-than", "1d"],
    ] {
        let out = run(&args);
        let (stdout, stderr) = (
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        assert!(!out.status.success(), "{args:?}: {stdout}");
        assert!(
            !stdout.contains("s3cr3t") && !stderr.contains("s3cr3t"),
            "{args:?}\n{stdout}\n{stderr}"
        );
    }
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

#[test]
fn a_run_already_in_the_table_is_not_written_again() {
    let d = workdir("run-id");
    let input = d.join("in.jsonl");
    fs::write(&input, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let table = d.join("t");
    let shape = |id: &str| {
        let (out, doc) = run_json(&[
            "shape",
            "-i",
            input.to_str().unwrap(),
            "-o",
            table.to_str().unwrap(),
            "--to",
            "iceberg",
            "--append",
            "--run-id",
            id,
        ]);
        assert!(out.status.success(), "{doc}");
        doc
    };

    // The first attempt commits, and names its run.
    let first = shape("r-1");
    assert_eq!(first["run_id"], "r-1");
    assert_eq!(first["table"]["earlier_attempt"], false);
    assert_eq!(first["table"]["version"], 1);

    // A second attempt of the same run finds it, reads nothing and commits nothing.
    let again = shape("r-1");
    assert_eq!(again["ok"], true);
    assert_eq!(again["table"]["earlier_attempt"], true);
    assert_eq!(again["table"]["snapshot_id"], first["table"]["snapshot_id"]);
    assert_eq!(again["table"]["version"], serde_json::Value::Null);
    assert_eq!(again["rows"]["in"], 0);
    let versions = || {
        fs::read_dir(table.join("metadata"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
            .count()
    };
    assert_eq!(versions(), 1, "no new version");

    // Another run appends as usual.
    let next = shape("r-2");
    assert_eq!(next["table"]["version"], 2);
    assert_eq!(versions(), 2);

    // A run id is an Iceberg notion, and has a shape.
    let out = run(&[
        "shape",
        "-i",
        input.to_str().unwrap(),
        "-o",
        d.join("p.parquet").to_str().unwrap(),
        "--run-id",
        "r-3",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--run-id applies only to Iceberg"));
    let out = run(&[
        "shape",
        "-i",
        input.to_str().unwrap(),
        "--run-id",
        "no spaces",
    ]);
    assert_eq!(out.status.code(), Some(2));
    fs::remove_dir_all(&d).ok();
}

/// In a build without the console (the release binary, the image), `serve` says how to get
/// it, with exit status 2, rather than clap's "unrecognized subcommand … similar: 'expire'".
#[cfg(not(feature = "serve"))]
#[test]
fn serve_without_the_console_says_how_to_get_it() {
    for args in [
        &["serve"][..],
        &["serve", "--addr", "0.0.0.0:9000", "--data-dir", "x"][..],
    ] {
        let out = Command::new(bin()).args(args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("built without the web console"), "{err}");
        assert!(err.contains("--features serve"), "{err}");
        assert!(!err.contains("expire"), "{err}");
    }
    // Still hidden from --help: this build has no console to offer.
    let help = Command::new(bin()).arg("--help").output().unwrap();
    assert!(!String::from_utf8_lossy(&help.stdout).contains("serve"));
}

/// One stream, three kinds of record, a non-JSON line: a spec about one kind takes its
/// rows with `where`, and the others are counted as filtered, never rejected.
#[test]
fn where_takes_one_kind_of_record_out_of_a_mixed_stream() {
    let d = workdir("where");
    let input = d.join("mixed.jsonl");
    fs::write(
        &input,
        concat!(
            r#"{"record_type":"gate","repo":"a/b","head_sha":"abc","duration_ms":812}"#,
            "\n",
            r#"{"record_type":"queue","repo":"a/b","pr":17,"position":2}"#,
            "\n",
            "starting worker 3 (not json)\n",
            r#"{"record_type":"gate","repo":"a/c","head_sha":"def","duration_ms":95}"#,
            "\n",
            r#"{"record_type":"turnover","repo":"a/b","files":12}"#,
            "\n",
        ),
    )
    .unwrap();
    let out = d.join("gate.parquet");
    let spec = d.join("gate.yaml");
    fs::write(
        &spec,
        format!(
            "dataset: gate\nwhere: record_type = 'gate'\nschema: strict\n\
             output: {{ format: parquet, path: {} }}\ncolumns:\n\
             \x20 - {{ name: repo, type: string, required: true }}\n\
             \x20 - {{ name: head_sha, type: string, required: true }}\n\
             \x20 - {{ name: duration_ms, type: int64 }}\n",
            out.display()
        ),
    )
    .unwrap();
    let run = |extra: &[&str]| {
        let mut args = vec![
            "shape",
            "-s",
            spec.to_str().unwrap(),
            "-i",
            input.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        Command::new(bin()).args(&args).output().unwrap()
    };

    // The strict spec lands the two gate rows; nothing of the others reaches the sidecar,
    // and the field the filter reads is not reported as drift.
    let o = run(&[]);
    let stdout = String::from_utf8_lossy(&o.stdout);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(
        stdout.contains("rows_in=4 rows_out=2 filtered=2 rejected=0 parse_errors=1"),
        "{stdout}"
    );
    assert!(!stdout.contains("drift"), "{stdout}");
    let rejects = fs::read_to_string(d.join("gate.parquet.rejects.jsonl")).unwrap();
    assert_eq!(
        rejects.lines().count(),
        1,
        "only the non-JSON line: {rejects}"
    );

    // --where overrides the spec's; the summary carries the counts and the expression.
    let o = run(&[
        "--where",
        "record_type = 'gate' and duration_ms > 100",
        "--summary-json",
    ]);
    let doc: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(doc["rows"]["out"], 1);
    assert_eq!(doc["rows"]["filtered"], 3);
    assert_eq!(
        doc["where"]["expr"],
        "record_type = 'gate' and duration_ms > 100"
    );
    assert_eq!(doc["where"]["type_mismatches"], 0);

    // A comparison of different kinds is reported, not silent.
    let o = run(&["--where", "duration_ms = '812'"]);
    let stdout = String::from_utf8_lossy(&o.stdout);
    assert!(stdout.contains("kept none of the 4 record(s)"), "{stdout}");
    assert!(
        stdout.contains("compares a number with a string"),
        "{stdout}"
    );

    // A mistake is refused before anything is read, with what to write instead.
    let o = run(&["--where", "record_type == 'gate'"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("use `=`, not `==`"));

    // `infer --where` samples only the kept records and writes the filter into the spec.
    let o = Command::new(bin())
        .args([
            "infer",
            "-i",
            input.to_str().unwrap(),
            "--where",
            "record_type = 'gate'",
        ])
        .output()
        .unwrap();
    let inferred = String::from_utf8_lossy(&o.stdout);
    assert!(
        inferred.contains("where: record_type = 'gate'"),
        "{inferred}"
    );
    assert!(inferred.contains("name: head_sha"), "{inferred}");
    for other in ["position", "files", "pr"] {
        assert!(
            !inferred.contains(&format!("name: {other}\n")),
            "{other}: {inferred}"
        );
    }
    fs::remove_dir_all(&d).ok();
}

/// A stream is sampled from its head, and only so much of it is held back: when the filter
/// drops every record of that head, inference stops and says why, naming the filter rather
/// than calling the records unparseable. Kept records past a dropped head are sampled.
#[test]
fn a_stream_whose_head_the_filter_drops_says_so() {
    use std::io::Write;
    use std::process::Stdio;
    let d = workdir("where-stream");
    let shape = |input: String| {
        let mut child = Command::new(bin())
            .args([
                "shape",
                "-i",
                "-",
                "-o",
                d.join("s.parquet").to_str().unwrap(),
            ])
            .args(["--where", "kind = 'gate'"])
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
        child.wait_with_output().unwrap()
    };
    let queue = |n: usize| "{\"kind\":\"queue\",\"pos\":1}\n".repeat(n);
    let gate = "{\"kind\":\"gate\",\"sha\":\"abc\"}\n";

    let o = shape(queue(10_000) + gate);
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("kept none of the first 10000 records") && err.contains("10000 filtered"),
        "{err}"
    );
    assert!(!err.contains("unparseable, so"), "{err}");

    let o = shape(queue(500) + gate);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("rows_in=501 rows_out=1 filtered=500"), "{out}");
    let inspect = Command::new(bin())
        .args(["inspect", d.join("s.parquet").to_str().unwrap()])
        .output()
        .unwrap();
    let cols = String::from_utf8_lossy(&inspect.stdout);
    assert!(cols.contains("sha") && !cols.contains("pos"), "{cols}");
    // Kept one, then the bound: the run goes ahead on a smaller sample, and says so.
    let o = shape(gate.to_string() + &queue(10_000) + gate);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("inferred the schema from 1 record(s), not the 1000")
            && err.contains("(10000 filtered, 0 unparseable)"),
        "{err}"
    );
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(
        out.contains("rows_in=10002 rows_out=2 filtered=10000"),
        "{out}"
    );
    // A sample the bound did not cut short says nothing of the kind.
    let o = shape(queue(500) + gate);
    assert!(!String::from_utf8_lossy(&o.stderr).contains("inferred the schema from"));
    fs::remove_dir_all(&d).ok();
}

#[test]
fn where_on_the_command_line_stands_in_for_a_bad_one_in_the_spec() {
    let d = workdir("where-override");
    let input = d.join("in.jsonl");
    fs::write(
        &input,
        "{\"kind\":\"gate\",\"a\":1}\n{\"kind\":\"q\",\"b\":2}\n",
    )
    .unwrap();
    let spec = d.join("s.yaml");
    fs::write(
        &spec,
        format!(
            "dataset: g\nwhere: kind == 'gate'\noutput: {{ format: parquet, path: {} }}\n",
            d.join("o.parquet").display()
        ),
    )
    .unwrap();
    let run = |extra: &[&str]| {
        Command::new(bin())
            .args([
                "shape",
                "-s",
                spec.to_str().unwrap(),
                "-i",
                input.to_str().unwrap(),
            ])
            .args(extra)
            .output()
            .unwrap()
    };
    let o = run(&[]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("use `=`, not `==`"));
    let o = run(&["--where", "kind = 'gate'"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("rows_in=2 rows_out=1 filtered=1"), "{out}");
    fs::remove_dir_all(&d).ok();
}

#[test]
fn a_record_nested_past_the_limit_is_a_parse_error_not_a_crash() {
    // 50,000 levels used to overflow the stack and abort the whole run. Now the line is
    // a parse error like any other, sidecarred for repair, and the rest of the file
    // shapes, whether `infer` samples it first or a strict spec reads it straight.
    let d = workdir("deep");
    let input = d.join("in.jsonl");
    let deep = format!("{}1{}", "{\"a\":".repeat(50_000), "}".repeat(50_000));
    fs::write(&input, format!("{{\"id\":1}}\n{deep}\n{{\"id\":2}}\n")).unwrap();
    let spec = d.join("s.yaml");
    fs::write(
        &spec,
        format!(
            "dataset: s\nschema: strict\noutput: {{ format: parquet, path: {} }}\n\
             columns:\n  - {{ name: id, type: int64 }}\n",
            d.join("strict.parquet").display()
        ),
    )
    .unwrap();
    for (name, args) in [
        (
            "inferred.parquet",
            vec![
                "-o".to_string(),
                d.join("inferred.parquet").display().to_string(),
            ],
        ),
        (
            "strict.parquet",
            vec!["-s".to_string(), spec.display().to_string()],
        ),
    ] {
        let o = Command::new(bin())
            .args(["shape", "-i", input.to_str().unwrap()])
            .args(&args)
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        let out = String::from_utf8_lossy(&o.stdout);
        assert!(
            out.contains("rows_in=2 rows_out=2 rejected=0 parse_errors=1"),
            "{name}: {out}"
        );
        let rejects = fs::read_to_string(d.join(format!("{name}.rejects.jsonl"))).unwrap();
        let reject: serde_json::Value = serde_json::from_str(rejects.trim()).unwrap();
        assert_eq!(reject["line"], 2, "{name}");
        assert!(
            reject["error"]
                .as_str()
                .unwrap()
                .contains("nests deeper than 512 levels"),
            "{name}: {reject}"
        );
        assert_eq!(reject["raw"].as_str().unwrap().len(), deep.len(), "{name}");
    }
    fs::remove_dir_all(&d).ok();
}
