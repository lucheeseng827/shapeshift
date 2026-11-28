//! The MySQL source against a real server. Skipped unless `SS_MYSQL_URL` names one
//! (`mysql://root@127.0.0.1:3306/ssdb`, its password in `SHAPESHIFT_MYSQL_PASSWORD`): the
//! position rules' progress gate (RELEASE-0.6 M5) as a loop, both read modes, a late commit
//! inside the lookback, the type mapping, and a bounded run that stops the server's read.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use serde_json::{json, Value};
use shapeshift_core::{Bounds, MysqlSourceSpec, Position, Source};
use shapeshift_source_mysql::{MysqlOptions, MysqlSource, PASSWORD_ENV};

fn url() -> Option<String> {
    let url = std::env::var("SS_MYSQL_URL").ok();
    // Where the server is meant to be there (the workflow that starts it sets this), a
    // missing one must not pass as a skip.
    if url.is_none() && std::env::var_os("SS_LIVE_REQUIRED").is_some() {
        panic!("SS_MYSQL_URL is not set, and SS_LIVE_REQUIRED says the server is there");
    }
    url
}

/// A connection for setting up the data, as the same user.
fn admin() -> mysql::Conn {
    let opts = mysql::OptsBuilder::from_opts(mysql::Opts::from_url(&url().unwrap()).unwrap())
        .pass(std::env::var(PASSWORD_ENV).ok())
        .prefer_socket(false);
    mysql::Conn::new(opts).unwrap()
}

fn table_spec(table: &str, cursor: &str) -> MysqlSourceSpec {
    MysqlSourceSpec {
        url: url().unwrap(),
        table: Some(table.into()),
        cursor: Some(cursor.into()),
        ..MysqlSourceSpec::default()
    }
}

/// Runs, as an upsert by `id` would land them.
struct Table {
    rows: BTreeMap<i64, Value>,
    position: Option<Position>,
    runs: Vec<u64>,
}

impl Table {
    fn new() -> Self {
        Table {
            rows: BTreeMap::new(),
            position: None,
            runs: Vec::new(),
        }
    }

    fn run(&mut self, s: &MysqlSourceSpec, bounds: &Bounds) -> shapeshift_core::Result<u64> {
        let mut src = MysqlSource::new(s, MysqlOptions::default())?;
        let recs: Vec<Value> = src
            .open(self.position.clone(), bounds.clone())?
            .collect::<Result<_, _>>()?;
        for r in recs {
            self.rows.insert(r["id"].as_i64().unwrap(), r);
        }
        let (position, progress) = src.outcome().expect("drained");
        self.position = position;
        self.runs.push(progress.new);
        Ok(progress.new)
    }

    fn drain(&mut self, s: &MysqlSourceSpec, bounds: &Bounds) -> shapeshift_core::Result<()> {
        for _ in 0..200 {
            if self.run(s, bounds)? == 0 {
                return Ok(());
            }
        }
        panic!("the runs never ran dry: {:?}", self.runs);
    }
}

fn limit(n: u64) -> Bounds {
    Bounds {
        max_records: Some(n),
        ..Bounds::default()
    }
}

/// Insert `n` rows `(id, updated_at = at + i / per minute, amount)` in batches.
fn fill(c: &mut mysql::Conn, table: &str, ids: std::ops::Range<i64>, minute: impl Fn(i64) -> i64) {
    let ids: Vec<i64> = ids.collect();
    for chunk in ids.chunks(1000) {
        let values: Vec<String> = chunk
            .iter()
            .map(|i| {
                format!(
                    "({i}, TIMESTAMP('2026-11-01') + INTERVAL {} MINUTE, {})",
                    minute(*i),
                    i % 97
                )
            })
            .collect();
        c.query_drop(format!(
            "INSERT INTO {table} (id, updated_at, amount) VALUES {}",
            values.join(",")
        ))
        .unwrap();
    }
}

fn fresh(c: &mut mysql::Conn, table: &str) {
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, updated_at DATETIME(6) NOT NULL, amount INT, \
         KEY (updated_at))"
    ))
    .unwrap();
}

#[test]
fn ten_thousand_rows_at_one_value_are_pulled_a_thousand_at_a_time() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "gate_one_value");
    fill(&mut c, "gate_one_value", 0..10_000, |_| 0);
    fill(&mut c, "gate_one_value", 10_000..10_050, |i| i - 9_999);

    // A table: its primary key is the tiebreak, so each run stops inside the group.
    let s = table_spec("gate_one_value", "updated_at");
    let mut t = Table::new();
    t.drain(&s, &limit(1000)).unwrap();
    assert_eq!(t.rows.len(), 10_050);
    assert!(t.runs.iter().all(|n| *n <= 1000), "{:?}", t.runs);
    assert_eq!(
        t.runs.iter().sum::<u64>(),
        10_050,
        "each row new once: {:?}",
        t.runs
    );

    // A query with no tiebreak must read the group whole: past max_tie the run fails, naming
    // the fix; under it, it finishes.
    let q = |max_tie| MysqlSourceSpec {
        url: url().unwrap(),
        query: Some(
            "SELECT id, updated_at, amount FROM gate_one_value WHERE updated_at >= :cursor \
             ORDER BY updated_at"
                .into(),
        ),
        cursor: Some("updated_at".into()),
        start: Some("2026-01-01".into()),
        max_tie: Some(max_tie),
        ..MysqlSourceSpec::default()
    };
    let e = Table::new()
        .run(&q(5_000), &limit(1000))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("more than 5000") && e.contains("tiebreak"),
        "{e}"
    );
    let mut t = Table::new();
    t.drain(&q(20_000), &limit(1000)).unwrap();
    assert_eq!(t.rows.len(), 10_050);
}

#[test]
fn incremental_runs_pick_up_updates_and_new_rows_and_a_query_needs_a_start() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "orders_inc");
    fill(&mut c, "orders_inc", 0..500, |i| i / 5);
    let s = table_spec("orders_inc", "updated_at");
    let mut t = Table::new();
    t.drain(&s, &limit(120)).unwrap();
    assert_eq!(t.rows.len(), 500);
    // Updates move rows to a later minute; new rows arrive.
    c.query_drop("UPDATE orders_inc SET amount = amount + 1000, updated_at = TIMESTAMP('2026-11-01') + INTERVAL 200 MINUTE WHERE id % 10 = 0").unwrap();
    fill(&mut c, "orders_inc", 500..600, |i| 200 + i % 7);
    t.drain(&s, &limit(120)).unwrap();
    assert_eq!(t.rows.len(), 600);
    assert_eq!(t.rows[&10]["amount"], json!(1010), "the update landed");
    assert_eq!(t.rows[&10]["updated_at"], json!("2026-11-01T03:20:00Z"));

    // A query's first run needs a start.
    let mut q = MysqlSourceSpec {
        url: url().unwrap(),
        query: Some(
            "SELECT * FROM orders_inc WHERE updated_at >= :cursor ORDER BY updated_at, id".into(),
        ),
        cursor: Some("updated_at".into()),
        tiebreak: Some("id".into()),
        ..MysqlSourceSpec::default()
    };
    let e = Table::new()
        .run(&q, &Bounds::default())
        .unwrap_err()
        .to_string();
    assert!(e.contains("start"), "{e}");
    q.start = Some("2026-11-01 00:00:00".into());
    let mut t = Table::new();
    t.drain(&q, &limit(77)).unwrap();
    assert_eq!(t.rows.len(), 600);
    // A query that does not order by its cursor is caught, not trusted.
    q.query = Some("SELECT * FROM orders_inc WHERE updated_at >= :cursor ORDER BY id".into());
    let e = Table::new()
        .run(&q, &Bounds::default())
        .unwrap_err()
        .to_string();
    assert!(e.contains("not in the ascending order"), "{e}");
}

#[test]
fn a_row_committed_late_inside_the_lookback_is_landed() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "late_commit");
    fill(&mut c, "late_commit", 0..50, |i| i);
    // A transaction writes a row stamped minute 45, and commits only after a run has read
    // past it.
    let mut slow = admin();
    slow.query_drop("START TRANSACTION").unwrap();
    slow.query_drop(
        "INSERT INTO late_commit VALUES (900, TIMESTAMP('2026-11-01') + INTERVAL 45 MINUTE, 1)",
    )
    .unwrap();
    let mut s = table_spec("late_commit", "updated_at");
    s.lookback = Some("10m".into());
    let mut t = Table::new();
    t.drain(&s, &Bounds::default()).unwrap();
    assert!(!t.rows.contains_key(&900), "not committed yet");
    slow.query_drop("COMMIT").unwrap();
    // 4 minutes behind the position (49): inside the 10-minute lookback.
    t.drain(&s, &Bounds::default()).unwrap();
    assert!(t.rows.contains_key(&900));
    // A lookback is a duration: a numeric cursor fails the first run.
    let mut n = table_spec("late_commit", "id");
    n.lookback = Some("10m".into());
    assert!(Table::new().run(&n, &Bounds::default()).is_err());
}

#[test]
fn types_map_and_binary_columns_are_refused() {
    let Some(_) = url() else { return };
    let mut c = admin();
    c.query_drop("DROP TABLE IF EXISTS typed").unwrap();
    c.query_drop(
        "CREATE TABLE typed (id INT PRIMARY KEY, flag TINYINT(1), big BIGINT UNSIGNED, price DECIMAL(10,2), \
         d DATE, ts TIMESTAMP(3), t TIME, j JSON, e ENUM('a','b'), s VARCHAR(20), y YEAR, b BIT(1), \
         f DOUBLE, blobby BLOB)",
    )
    .unwrap();
    c.query_drop(
        "INSERT INTO typed VALUES (1, 1, 18446744073709551615, 12.50, '2026-02-03', \
         '2026-11-01 06:39:00.120', '-26:03:04', '{\"a\": [1]}', 'b', 'héllo', 2026, b'1', 1.5, 'x')",
    )
    .unwrap();
    let q = |sql: &str| MysqlSourceSpec {
        url: url().unwrap(),
        query: Some(sql.into()),
        ..MysqlSourceSpec::default()
    };
    let mut src = MysqlSource::new(
        &q("SELECT id, flag, big, price, d, ts, t, j, e, s, y, b, f FROM typed"),
        MysqlOptions::default(),
    )
    .unwrap();
    let rows: Vec<Value> = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows[0],
        json!({"id": 1, "flag": true, "big": 18446744073709551615u64, "price": "12.50", "d": "2026-02-03",
               "ts": "2026-11-01T06:39:00.120000Z", "t": "-26:03:04", "j": {"a": [1]}, "e": "b",
               "s": "héllo", "y": 2026, "b": true, "f": 1.5})
    );
    let mut src =
        MysqlSource::new(&q("SELECT id, blobby FROM typed"), MysqlOptions::default()).unwrap();
    let e = src.open(None, Bounds::default()).err().unwrap().to_string();
    assert!(e.contains("blobby") && e.contains("HEX"), "{e}");
    let mut src = MysqlSource::new(
        &q("SELECT id, HEX(blobby) AS blobby FROM typed"),
        MysqlOptions::default(),
    )
    .unwrap();
    let rows: Vec<Value> = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows[0]["blobby"], json!("78"));
}

#[test]
fn a_bounded_run_stops_the_servers_read_instead_of_draining_it() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "big_read");
    fill(&mut c, "big_read", 0..200_000, |i| i);
    let s = table_spec("big_read", "updated_at");
    let started = Instant::now();
    let mut t = Table::new();
    t.run(&s, &limit(10)).unwrap();
    assert_eq!(t.runs, vec![10]);
    assert_eq!(
        t.position,
        Some(Position::Inside(json!("2026-11-01T00:09:00Z"), json!(9)))
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "took {:?}",
        started.elapsed()
    );
    // The killed read is gone from the server (KILL QUERY returns before its target notices,
    // so this waits a little).
    let mut open: i64 = 1;
    for _ in 0..40 {
        open = c.query_first("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE 'SELECT * FROM `big_read`%'").unwrap().unwrap();
        if open == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(open, 0);
}

#[test]
fn a_text_key_is_no_tiebreak_and_text_or_decimal_cursors_are_refused() {
    let Some(_) = url() else { return };
    let mut c = admin();
    c.query_drop("DROP TABLE IF EXISTS text_key").unwrap();
    c.query_drop(
        "CREATE TABLE text_key (id VARCHAR(10) PRIMARY KEY, updated_at DATETIME NOT NULL, price DECIMAL(8,2)) \
         COLLATE utf8mb4_0900_ai_ci",
    )
    .unwrap();
    // Under a case-insensitive collation MySQL orders `a` before `B`; bytes would not.
    let rows: Vec<String> = (0..120)
        .map(|i| {
            let id = if i % 2 == 0 {
                format!("a{i:03}")
            } else {
                format!("B{i:03}")
            };
            format!(
                "('{id}', TIMESTAMP('2026-11-01') + INTERVAL {} MINUTE, {i}.50)",
                i / 40
            )
        })
        .collect();
    c.query_drop(format!("INSERT INTO text_key VALUES {}", rows.join(",")))
        .unwrap();
    // Read without a tiebreak: every bounded run finishes its group, and every row lands.
    let s = table_spec("text_key", "updated_at");
    let mut src = MysqlSource::new(&s, MysqlOptions::default()).unwrap();
    let got: Vec<Value> = src
        .open(None, limit(10))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got.len(), 40, "the first group, read whole");
    assert!(matches!(src.outcome().unwrap().0, Some(Position::At(_))));
    let q = |sql: &str, cursor: &str, tiebreak: Option<&str>| MysqlSourceSpec {
        url: url().unwrap(),
        query: Some(sql.into()),
        cursor: Some(cursor.into()),
        tiebreak: tiebreak.map(String::from),
        start: Some("0".into()),
        ..MysqlSourceSpec::default()
    };
    let refuse = |s: MysqlSourceSpec| {
        let mut src = MysqlSource::new(&s, MysqlOptions::default()).unwrap();
        src.open(None, Bounds::default()).err().unwrap().to_string()
    };
    let e = refuse(q(
        "SELECT * FROM text_key WHERE updated_at >= :cursor ORDER BY updated_at, id",
        "updated_at",
        Some("id"),
    ));
    assert!(e.contains("`id`") && e.contains("collation"), "{e}");
    let e = refuse(q(
        "SELECT * FROM text_key WHERE price >= :cursor ORDER BY price",
        "price",
        None,
    ));
    assert!(e.contains("DECIMAL"), "{e}");
}

#[test]
fn a_resume_inside_a_group_replays_the_lookback_below_it() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "inside_lookback");
    fill(&mut c, "inside_lookback", 0..100, |_| 10);
    let mut s = table_spec("inside_lookback", "updated_at");
    s.lookback = Some("5m".into());
    let mut t = Table::new();
    t.run(&s, &limit(30)).unwrap();
    assert!(
        matches!(t.position, Some(Position::Inside(_, _))),
        "{:?}",
        t.position
    );
    c.query_drop(
        "INSERT INTO inside_lookback VALUES (900, TIMESTAMP('2026-11-01') + INTERVAL 7 MINUTE, 1)",
    )
    .unwrap();
    t.drain(&s, &limit(30)).unwrap();
    assert!(
        t.rows.contains_key(&900),
        "the late row is read when resuming inside the group"
    );
    assert_eq!(t.rows.len(), 101);
}
