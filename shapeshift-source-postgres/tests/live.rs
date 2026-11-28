//! The Postgres source against a real server. Skipped unless `SS_POSTGRES_URL` names one
//! (`postgres://postgres@127.0.0.1:5432/ssdb`, its password in `SHAPESHIFT_POSTGRES_PASSWORD`):
//! the progress gate as a loop, both read modes, a late commit inside the lookback, the types,
//! and a bounded run that stops fetching.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use shapeshift_core::{Bounds, Position, PostgresSourceSpec, Source};
use shapeshift_source_postgres::{PostgresOptions, PostgresSource, PASSWORD_ENV};

fn url() -> Option<String> {
    let url = std::env::var("SS_POSTGRES_URL").ok();
    // Where the server is meant to be there (the workflow that starts it sets this), a
    // missing one must not pass as a skip.
    if url.is_none() && std::env::var_os("SS_LIVE_REQUIRED").is_some() {
        panic!("SS_POSTGRES_URL is not set, and SS_LIVE_REQUIRED says the server is there");
    }
    url
}

fn admin() -> postgres::Client {
    let mut c: postgres::Config = url().unwrap().parse().unwrap();
    c.password(std::env::var(PASSWORD_ENV).unwrap());
    c.connect(postgres::NoTls).unwrap()
}

fn table_spec(table: &str, cursor: &str) -> PostgresSourceSpec {
    PostgresSourceSpec {
        url: url().unwrap(),
        table: Some(table.into()),
        cursor: Some(cursor.into()),
        ..PostgresSourceSpec::default()
    }
}

struct Table {
    rows: BTreeMap<String, Value>,
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

    fn run(&mut self, s: &PostgresSourceSpec, bounds: &Bounds) -> shapeshift_core::Result<u64> {
        let mut src = PostgresSource::new(s, PostgresOptions::default())?;
        let recs: Vec<Value> = src
            .open(self.position.clone(), bounds.clone())?
            .collect::<Result<_, _>>()?;
        for r in recs {
            self.rows.insert(r["id"].to_string(), r);
        }
        let (position, progress) = src.outcome().expect("drained");
        self.position = position;
        self.runs.push(progress.new);
        Ok(progress.new)
    }

    fn drain(&mut self, s: &PostgresSourceSpec, bounds: &Bounds) -> shapeshift_core::Result<()> {
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

fn fresh(c: &mut postgres::Client, table: &str, rows: i64, minute: &str) {
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table}; \
         CREATE TABLE {table} (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL, amount int); \
         CREATE INDEX ON {table} (updated_at); \
         INSERT INTO {table} SELECT i, timestamptz '2026-11-01 00:00:00+00' + ({minute}) * interval '1 minute', \
         i % 97 FROM generate_series(0, {} ) AS i;",
        rows - 1
    ))
    .unwrap();
}

#[test]
fn ten_thousand_rows_at_one_value_are_pulled_a_thousand_at_a_time() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "gate_one_value", 10_000, "0");
    c.batch_execute(
        "INSERT INTO gate_one_value SELECT i, timestamptz '2026-11-01 00:00:00+00' + (i - 9999) * interval '1 minute', 1 \
         FROM generate_series(10000, 10049) AS i",
    )
    .unwrap();
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

    let q = |max_tie| PostgresSourceSpec {
        url: url().unwrap(),
        query: Some(
            "SELECT * FROM gate_one_value WHERE updated_at >= :cursor ORDER BY updated_at".into(),
        ),
        cursor: Some("updated_at".into()),
        start: Some("2026-01-01".into()),
        max_tie: Some(max_tie),
        ..PostgresSourceSpec::default()
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
    fresh(&mut c, "orders_inc", 500, "i / 5");
    let s = table_spec("orders_inc", "updated_at");
    let mut t = Table::new();
    t.drain(&s, &limit(120)).unwrap();
    assert_eq!(t.rows.len(), 500);
    c.batch_execute(
        "UPDATE orders_inc SET amount = amount + 1000, updated_at = timestamptz '2026-11-01 03:20:00+00' WHERE id % 10 = 0; \
         INSERT INTO orders_inc SELECT i, timestamptz '2026-11-01 03:20:00+00' + (i % 7) * interval '1 minute', 1 \
         FROM generate_series(500, 599) AS i;",
    )
    .unwrap();
    t.drain(&s, &limit(120)).unwrap();
    assert_eq!(t.rows.len(), 600);
    assert_eq!(t.rows["10"]["amount"], json!(1010));
    assert_eq!(t.rows["10"]["updated_at"], json!("2026-11-01T03:20:00Z"));

    let mut q = PostgresSourceSpec {
        url: url().unwrap(),
        query: Some(
            "SELECT * FROM orders_inc WHERE updated_at >= :cursor ORDER BY updated_at, id".into(),
        ),
        cursor: Some("updated_at".into()),
        tiebreak: Some("id".into()),
        ..PostgresSourceSpec::default()
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
    fresh(&mut c, "late_commit", 50, "i");
    let mut slow = admin();
    let mut tx = slow.transaction().unwrap();
    tx.batch_execute(
        "INSERT INTO late_commit VALUES (900, timestamptz '2026-11-01 00:45:00+00', 1)",
    )
    .unwrap();
    let mut s = table_spec("late_commit", "updated_at");
    s.lookback = Some("10m".into());
    let mut t = Table::new();
    t.drain(&s, &Bounds::default()).unwrap();
    assert!(!t.rows.contains_key("900"), "not committed yet");
    tx.commit().unwrap();
    t.drain(&s, &Bounds::default()).unwrap();
    assert!(t.rows.contains_key("900"));
}

#[test]
fn types_map_and_unreadable_or_unordered_columns_are_refused() {
    let Some(_) = url() else { return };
    let mut c = admin();
    c.batch_execute(
        "DROP TABLE IF EXISTS typed; DROP TYPE IF EXISTS mood; CREATE TYPE mood AS ENUM ('ok', 'sad'); \
         CREATE TABLE typed (id int PRIMARY KEY, flag bool, big bigint, price numeric(10,2), d date, \
         ts timestamptz, t time, j jsonb, m mood, s text, u uuid, f real, raw bytea); \
         INSERT INTO typed VALUES (1, true, 9223372036854775807, 12.50, '2026-02-03', \
         '2026-11-01 06:39:00.12+00', '10:30:00', '{\"a\": [1]}', 'sad', 'héllo', \
         'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1.1, '\\x78');",
    )
    .unwrap();
    let q = |sql: &str| PostgresSourceSpec {
        url: url().unwrap(),
        query: Some(sql.into()),
        ..PostgresSourceSpec::default()
    };
    let mut src = PostgresSource::new(
        &q("SELECT id, flag, big, price, d, ts, t, j, m, s, u, f FROM typed"),
        PostgresOptions::default(),
    )
    .unwrap();
    let rows: Vec<Value> = src
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows[0],
        json!({"id": 1, "flag": true, "big": 9223372036854775807i64, "price": "12.50", "d": "2026-02-03",
               "ts": "2026-11-01T06:39:00.120000Z", "t": "10:30:00", "j": {"a": [1]}, "m": "sad",
               "s": "héllo", "u": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "f": 1.1})
    );
    let mut src =
        PostgresSource::new(&q("SELECT id, raw FROM typed"), PostgresOptions::default()).unwrap();
    let e = src.open(None, Bounds::default()).err().unwrap().to_string();
    assert!(e.contains("raw") && e.contains("encode"), "{e}");

    // A text key is no tiebreak: the table is read whole groups at a time.
    c.batch_execute(
        "DROP TABLE IF EXISTS text_key; CREATE TABLE text_key (id text PRIMARY KEY, updated_at timestamptz NOT NULL, price numeric); \
         INSERT INTO text_key SELECT CASE WHEN i % 2 = 0 THEN 'a' ELSE 'B' END || lpad(i::text, 3, '0'), \
         timestamptz '2026-11-01 00:00:00+00' + (i / 40) * interval '1 minute', i FROM generate_series(0, 119) AS i;",
    )
    .unwrap();
    let mut src = PostgresSource::new(
        &table_spec("text_key", "updated_at"),
        PostgresOptions::default(),
    )
    .unwrap();
    let got: Vec<Value> = src
        .open(None, limit(10))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got.len(), 40, "the first group, read whole");
    let refuse = |s: PostgresSourceSpec| {
        let mut src = PostgresSource::new(&s, PostgresOptions::default()).unwrap();
        src.open(None, Bounds::default()).err().unwrap().to_string()
    };
    let mut tq = q("SELECT * FROM text_key WHERE updated_at >= :cursor ORDER BY updated_at, id");
    tq.cursor = Some("updated_at".into());
    tq.tiebreak = Some("id".into());
    tq.start = Some("2026-01-01".into());
    let e = refuse(tq);
    assert!(e.contains("`id`") && e.contains("collation"), "{e}");
    let mut nq = q("SELECT * FROM text_key WHERE price >= :cursor ORDER BY price");
    nq.cursor = Some("price".into());
    nq.start = Some("0".into());
    let e = refuse(nq);
    assert!(e.contains("numeric"), "{e}");
}

#[test]
fn a_bounded_run_stops_fetching() {
    let Some(_) = url() else { return };
    let mut c = admin();
    fresh(&mut c, "big_read", 200_000, "i");
    // Its own session name, so the check below sees only this read.
    let mut s = table_spec("big_read", "updated_at");
    let base = url().unwrap();
    let sep = if base.contains('?') { '&' } else { '?' };
    s.url = format!("{base}{sep}application_name=ss_big_read");
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
    // The read's connection goes once it stops fetching.
    let mut open = 1i64;
    for _ in 0..40 {
        open = c
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'ss_big_read'",
                &[],
            )
            .unwrap()
            .get(0);
        if open == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(open, 0);
}

#[test]
fn a_key_with_include_columns_is_a_tiebreak_and_a_resume_inside_a_group_replays_the_lookback() {
    let Some(_) = url() else { return };
    let mut c = admin();
    c.batch_execute(
        "DROP TABLE IF EXISTS inc_key; \
         CREATE TABLE inc_key (id bigint, updated_at timestamptz NOT NULL, amount int, PRIMARY KEY (id) INCLUDE (amount)); \
         INSERT INTO inc_key SELECT i, timestamptz '2026-11-01 00:10:00+00', 1 FROM generate_series(0, 99) AS i;",
    )
    .unwrap();
    let mut s = table_spec("inc_key", "updated_at");
    s.lookback = Some("5m".into());
    // A bound inside the group of 100 at one value: only a tiebreak lets the run stop there.
    let mut t = Table::new();
    t.run(&s, &limit(30)).unwrap();
    assert_eq!(t.runs, vec![30]);
    assert!(
        matches!(t.position, Some(Position::Inside(_, _))),
        "{:?}",
        t.position
    );
    // A row commits late, 3 minutes behind the position: inside its lookback.
    c.batch_execute("INSERT INTO inc_key VALUES (900, timestamptz '2026-11-01 00:07:00+00', 1)")
        .unwrap();
    t.drain(&s, &limit(30)).unwrap();
    assert!(
        t.rows.contains_key("900"),
        "the late row is read when resuming inside the group"
    );
    assert_eq!(t.rows.len(), 101);
}

#[test]
fn an_unqualified_table_is_named_as_it_resolves_so_a_changed_search_path_is_caught() {
    let Some(_) = url() else { return };
    let mut c = admin();
    c.batch_execute(
        "DROP SCHEMA IF EXISTS sp_a CASCADE; DROP SCHEMA IF EXISTS sp_b CASCADE; \
         CREATE SCHEMA sp_a; CREATE SCHEMA sp_b; \
         CREATE TABLE sp_a.moving (id int PRIMARY KEY, updated_at timestamptz NOT NULL); \
         CREATE TABLE sp_b.moving (id int PRIMARY KEY, updated_at timestamptz NOT NULL); \
         ALTER ROLE CURRENT_USER SET search_path TO sp_a, public;",
    )
    .unwrap();
    let s = table_spec("moving", "updated_at");
    let q = PostgresSourceSpec {
        url: url().unwrap(),
        query: Some("SELECT * FROM moving WHERE updated_at >= :cursor ORDER BY updated_at".into()),
        cursor: Some("updated_at".into()),
        start: Some("2026-01-01".into()),
        ..PostgresSourceSpec::default()
    };
    c.batch_execute(
        "INSERT INTO sp_a.moving VALUES (1, '2026-11-01T00:00:00Z'); \
         INSERT INTO sp_b.moving VALUES (2, '2026-11-01T00:00:00Z');",
    )
    .unwrap();
    let mut src_a = PostgresSource::new(&s, PostgresOptions::default()).unwrap();
    let a = src_a.identity();
    let qa = PostgresSource::new(&q, PostgresOptions::default())
        .unwrap()
        .identity();
    c.batch_execute("ALTER ROLE CURRENT_USER SET search_path TO sp_b, public")
        .unwrap();
    let src_b = PostgresSource::new(&s, PostgresOptions::default()).unwrap();
    let b = src_b.identity();
    let qb = PostgresSource::new(&q, PostgresOptions::default())
        .unwrap()
        .identity();
    // The source resolved under sp_a reads sp_a's table though the role's path has moved on:
    // the read is bound to the relation its identity names.
    let read: Vec<Value> = src_a
        .open(None, Bounds::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    c.batch_execute("ALTER ROLE CURRENT_USER RESET search_path")
        .unwrap();
    assert!(
        a.contains("table sp_a.moving") && b.contains("table sp_b.moving"),
        "{a} / {b}"
    );
    assert_eq!(read.len(), 1, "{read:?}");
    assert_eq!(read[0]["id"], 1, "{read:?}");
    // The position one recorded is refused for the other, table and query alike.
    let recorded = json!({"at": "2026-11-01T00:00:00Z", "of": a});
    assert!(shapeshift_core::resume_from(Some(recorded), &src_b).is_err());
    assert_ne!(
        qa, qb,
        "a query's identity names the search path it resolves through"
    );
    // With the default path neither schema is on it: the run fails, naming the table.
    let mut gone = PostgresSource::new(&s, PostgresOptions::default()).unwrap();
    let e = gone
        .open(None, Bounds::default())
        .err()
        .expect("refused")
        .to_string();
    assert!(e.contains("no table `moving`"), "{e}");
    c.batch_execute("DROP SCHEMA sp_a CASCADE; DROP SCHEMA sp_b CASCADE")
        .unwrap();
}

#[test]
fn a_run_dropped_part_way_cancels_the_fetch_the_server_is_slow_to_answer() {
    let Some(base) = url() else { return };
    let sep = if base.contains('?') { '&' } else { '?' };
    // Row 601 takes 30 s: the second fetch (rows 501-1000) stalls there.
    let s = PostgresSourceSpec {
        url: format!("{base}{sep}application_name=ss_cancel"),
        query: Some(
            "SELECT g AS id, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 second' \
             AS updated_at FROM generate_series(1, 2000) g, \
             LATERAL (SELECT 1 FROM pg_sleep(CASE WHEN g = 601 THEN 30 ELSE 0 END)) s \
             WHERE timestamptz '2026-01-01 00:00:00+00' + g * interval '1 second' >= :cursor"
                .into(),
        ),
        cursor: Some("updated_at".into()),
        start: Some("2026-01-01".into()),
        ..PostgresSourceSpec::default()
    };
    let mut src = PostgresSource::new(&s, PostgresOptions::default()).unwrap();
    // A run that stops reading part-way (the caller gave up on it) drops its iterator while
    // the reader waits on that fetch.
    let mut it = src.open(None, Bounds::default()).unwrap();
    for _ in 0..50 {
        it.next().unwrap().unwrap();
    }
    std::thread::sleep(Duration::from_millis(500));
    drop(it);
    // The session goes in seconds, not when the stalled fetch would have returned.
    let mut admin = admin();
    let t = Instant::now();
    loop {
        let n: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'ss_cancel'",
                &[],
            )
            .unwrap()
            .get(0);
        if n == 0 {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "the stalled fetch kept its session open"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_float_cursor_reads_and_resumes_past_values_that_are_not_finite() {
    let Some(_) = url() else { return };
    let mut c = admin();
    c.batch_execute(
        "DROP TABLE IF EXISTS floaty; \
         CREATE TABLE floaty (id int PRIMARY KEY, v float8 NOT NULL); \
         INSERT INTO floaty VALUES (1, '-Infinity'), (2, 1.0), (3, 'Infinity'), (4, 'NaN');",
    )
    .unwrap();
    let s = table_spec("floaty", "v");
    let mut t = Table::new();
    assert_eq!(t.run(&s, &Bounds::default()).unwrap(), 4);
    // The last value is NaN, which Postgres orders above Infinity.
    let at = match &t.position {
        Some(Position::At(v)) | Some(Position::Inside(v, _)) => v.clone(),
        None => Value::Null,
    };
    assert_eq!(at, json!("NaN"), "{:?}", t.position);
    // The run resumes from there: a new NaN row lands, and the rows before it are not re-read.
    c.batch_execute("INSERT INTO floaty VALUES (5, 'NaN')")
        .unwrap();
    t.run(&s, &Bounds::default()).unwrap();
    assert!(t.rows.contains_key("5"), "{:?}", t.rows.keys());
    assert_eq!(t.rows.len(), 5);
}
