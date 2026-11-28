//! The handoff between a Debezium snapshot and its stream: a seed
//! row (`op: r`) is older than every change, so the table ends as the source's rows in any
//! order of arrival and any split into writes.
//!
//! The feeds are captured from real databases (`examples/changes/debezium/handoff/`), each
//! with a transaction held open across Debezium's snapshot. On Postgres it commits after the
//! snapshot with LSNs below the snapshot's (the loss SY1 fixes); on MySQL, whose binlog is in
//! commit order, above it. Each feed's `-expected.csv` is its database's final rows. The named
//! cases of the rule are checked for both presets with feeds written here.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};

const PG_FEED: &str = include_str!("../../examples/changes/debezium/handoff/postgres-feed.jsonl");
const PG_EXPECTED: &str =
    include_str!("../../examples/changes/debezium/handoff/postgres-expected.csv");
const MY_FEED: &str = include_str!("../../examples/changes/debezium/handoff/mysql-feed.jsonl");
const MY_EXPECTED: &str =
    include_str!("../../examples/changes/debezium/handoff/mysql-expected.csv");
const POSTGRES: &str = include_str!("../../examples/changes/debezium/postgres.yaml");
const MYSQL: &str = include_str!("../../examples/changes/debezium/mysql.yaml");

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_shapeshift")
}

fn workdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-handoff-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

type Row = (i64, String, i64);

/// A table applied from one example spec, a write at a time.
struct Table {
    dir: PathBuf,
    spec: PathBuf,
    path: PathBuf,
    writes: usize,
}

impl Table {
    fn new(dir: &Path, example: &str) -> Table {
        let path = dir.join("orders");
        let spec = dir.join("spec.yaml");
        std::fs::write(
            &spec,
            example.replace("path: PATH", &format!("path: {}", path.display())),
        )
        .unwrap();
        Table {
            dir: dir.to_path_buf(),
            spec,
            path,
            writes: 0,
        }
    }

    /// One `shapeshift shape` of these records.
    fn write(&mut self, records: &[&str]) {
        self.writes += 1;
        let input = self.dir.join(format!("write-{}.jsonl", self.writes));
        std::fs::write(&input, records.join("\n") + "\n").unwrap();
        let out = Command::new(bin())
            .args(["shape", "--spec"])
            .arg(&self.spec)
            .arg("-i")
            .arg(&input)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// The live rows, as a reader sees them: each data file less the positions its delete
    /// files remove.
    fn rows(&self) -> BTreeSet<Row> {
        let mut rows = BTreeSet::new();
        for f in shapeshift_iceberg::current_scan_files(&self.path).unwrap() {
            let file = std::fs::File::open(&f.path).unwrap();
            let all = shapeshift_parquet::head_file(file, f.record_count as usize).unwrap();
            for (pos, r) in all.iter().enumerate() {
                if f.deleted_positions.binary_search(&(pos as u64)).is_err() {
                    let row = (
                        r["id"].as_i64().unwrap(),
                        r["status"].as_str().unwrap_or("").to_string(),
                        r["amount"].as_i64().unwrap_or(0),
                    );
                    assert!(rows.insert(row.clone()), "{row:?} is live twice");
                }
            }
        }
        rows
    }
}

fn expected(csv: &str) -> BTreeSet<Row> {
    csv.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let f: Vec<&str> = l.split(',').collect();
            (
                f[0].parse().unwrap(),
                f[1].to_string(),
                f[2].parse().unwrap(),
            )
        })
        .collect()
}

/// A small deterministic generator, so a failing order can be named and rerun.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

#[test]
fn the_captured_postgres_feed_ends_as_the_database_did_in_any_order_and_any_writes() {
    captured_feed("postgres", POSTGRES, PG_FEED, PG_EXPECTED);
}

#[test]
fn the_captured_mysql_feed_ends_as_the_database_did_in_any_order_and_any_writes() {
    captured_feed("mysql", MYSQL, MY_FEED, MY_EXPECTED);
}

fn captured_feed(name: &str, example: &str, feed: &str, csv: &str) {
    let feed: Vec<&str> = feed.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(feed.len(), 9, "{name}: the captured feed");
    let want = expected(csv);
    // Seed 0 is the feed as it arrived, in one write; the others shuffle it into 1 to 5 writes.
    for seed in 0..12u64 {
        let base = workdir(&format!("{name}-{seed}"));
        let mut t = Table::new(&base, example);
        let mut order = feed.clone();
        let mut g = Lcg(seed);
        let writes = if seed == 0 {
            1
        } else {
            for i in (1..order.len()).rev() {
                order.swap(i, (g.next() % (i as u64 + 1)) as usize);
            }
            1 + (g.next() % 5) as usize
        };
        let per = order.len().div_ceil(writes);
        for chunk in order.chunks(per) {
            t.write(chunk);
        }
        assert_eq!(
            t.rows(),
            want,
            "{name}, seed {seed}: the feed in {writes} write(s), in the order {:?}",
            order
                .iter()
                .map(|l| serde_json::from_str::<Value>(l).unwrap()["op"].clone())
                .collect::<Vec<_>>()
        );
        // A replay of the whole feed changes nothing.
        t.write(&feed);
        assert_eq!(t.rows(), want, "{name}, seed {seed}: after a replay");
        std::fs::remove_dir_all(&base).ok();
    }
}

/// A Debezium envelope from either database, at a version `v` (an LSN, or a binlog position
/// in file `binlog.000001`).
fn change(mysql: bool, op: &str, id: i64, status: &str, v: i64) -> String {
    let row = json!({ "id": id, "status": status, "amount": v });
    let (before, after) = match op {
        "d" => (row, Value::Null),
        _ => (Value::Null, row),
    };
    let source = if mysql {
        json!({ "file": "binlog.000001", "pos": v, "row": 0, "ts_ms": 1 })
    } else {
        json!({ "lsn": v, "ts_ms": 1 })
    };
    json!({ "before": before, "after": after, "op": op, "source": source }).to_string()
}

#[test]
fn the_named_cases_of_the_handoff_hold_for_both_presets() {
    for (mysql, example) in [(false, POSTGRES), (true, MYSQL)] {
        let c = |op: &str, id: i64, status: &str, v: i64| change(mysql, op, id, status, v);
        let name = if mysql { "mysql" } else { "postgres" };
        let base = workdir(&format!("cases-{name}"));
        let mut t = Table::new(&base, example);
        // 1. A change by a transaction open across the snapshot: lower version than the seed
        //    row, applied after it. The change wins.
        t.write(&[&c("r", 1, "seed", 500)]);
        t.write(&[&c("u", 1, "changed", 400)]);
        // 2. A key deleted after its seed row was read.
        t.write(&[&c("r", 2, "seed", 500)]);
        t.write(&[&c("d", 2, "seed", 600)]);
        // 3. A seed row that arrives after its key's delete: the delete's tombstone refuses it,
        //    however late the seed row was read.
        t.write(&[&c("d", 3, "seed", 600)]);
        t.write(&[&c("r", 3, "seed", 900)]);
        // 4. A key inserted and deleted between the start and its chunk: the chunk never sees
        //    it, and the stream's two changes leave nothing.
        t.write(&[&c("c", 4, "new", 550), &c("d", 4, "new", 560)]);
        // 5. A seed row that fills a key no change touched.
        t.write(&[&c("r", 5, "seed", 500)]);
        // 6. The seed row arriving after its key's change, in the same write.
        t.write(&[&c("u", 6, "changed", 400), &c("r", 6, "seed", 500)]);
        assert_eq!(
            t.rows(),
            BTreeSet::from([
                (1, "changed".to_string(), 400),
                (5, "seed".to_string(), 500),
                (6, "changed".to_string(), 400),
            ]),
            "{name}"
        );
        std::fs::remove_dir_all(&base).ok();
    }
}
