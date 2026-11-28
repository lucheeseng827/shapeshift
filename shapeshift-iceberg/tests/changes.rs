//! Change feeds (`changes:`) merged into a table: deletes, `order_by`, and what a reader sees
//! after each write. Live rows are read as an Iceberg reader reads them (data files minus the
//! positions their delete files remove), and checked against a plain model of the rules in
//! `docs/SPEC.md` §5b, over random feeds split into random writes.

use std::collections::BTreeMap;

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{
    ApplyMode, ChangesSpec, ColumnSpec, ColumnType, Compression, MergeSummary, PushOutcome, Shaper,
    Sink,
};
use shapeshift_iceberg::{current_scan_files, IcebergSink};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ss-changes-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn col(name: &str, ty: ColumnType) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        from: None,
        ty,
        transform: None,
        required: false,
    }
}

fn feed(order_by: &[&str]) -> ChangesSpec {
    ChangesSpec {
        preset: None,
        op: "op".into(),
        insert: vec![json!("c")],
        update: vec![json!("u")],
        delete: vec![json!("d")],
        skip: vec![],
        row: None,
        key_from: None,
        order_by: order_by.iter().map(|s| s.to_string()).collect(),
        tombstone_retention: None,
    }
}

/// Apply one write of `changes` to the table at `dir`, merging on `id`.
fn apply(
    dir: &std::path::Path,
    changes: &[Value],
    order_by: &[&str],
) -> Result<MergeSummary, String> {
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false)
        .unwrap()
        .with_changes(&feed(order_by), ApplyMode::Upsert, &["id".to_string()])
        .unwrap();
    for c in changes {
        assert_eq!(sh.push(c).unwrap(), PushOutcome::Appended, "{c}");
    }
    let mut sink = IcebergSink::create_merge(
        dir,
        sh.table_schema(),
        Compression::Snappy,
        &[],
        &["id".to_string()],
    )
    .map_err(|e| e.to_string())?;
    if !order_by.is_empty() {
        let o: Vec<String> = order_by.iter().map(|s| s.to_string()).collect();
        sink.order_changes_by(&o, 7 * 86_400_000)
            .map_err(|e| e.to_string())?;
    }
    if let Some(b) = sh.flush().unwrap() {
        sink.write_batch(&b).map_err(|e| e.to_string())?;
    }
    let s = sink.finish().map_err(|e| e.to_string())?;
    Ok(s.merge.expect("a merge reports"))
}

fn c(op: &str, id: i64, v: &str, ver: i64) -> Value {
    json!({ "op": op, "id": id, "v": v, "ver": ver })
}

/// The table's live rows, `id → (v, ver)`.
fn live(dir: &std::path::Path) -> BTreeMap<i64, (String, Option<i64>)> {
    let mut out = BTreeMap::new();
    for f in current_scan_files(dir).unwrap() {
        let file = std::fs::File::open(&f.path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut pos = 0u64;
        for batch in reader {
            let batch = batch.unwrap();
            let get = |name: &str| batch.column(batch.schema().index_of(name).unwrap()).clone();
            let id = get("id");
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            let v = get("v");
            let v = v.as_any().downcast_ref::<StringArray>().unwrap();
            let ver = get("ver");
            let ver = ver.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..batch.num_rows() {
                if f.deleted_positions.binary_search(&pos).is_err() {
                    let prev = out.insert(
                        id.value(i),
                        (
                            v.value(i).to_string(),
                            (!ver.is_null(i)).then(|| ver.value(i)),
                        ),
                    );
                    assert!(prev.is_none(), "key {} is live twice", id.value(i));
                }
                pos += 1;
            }
        }
    }
    out
}

#[test]
fn deletes_remove_the_keys_row_and_are_never_written() {
    let dir = tmp_dir("deletes");
    let s = apply(
        &dir,
        &[c("c", 1, "a", 1), c("c", 2, "b", 1), c("c", 3, "c", 1)],
        &[],
    )
    .unwrap();
    assert_eq!((s.keys_inserted, s.keys_deleted), (3, 0));
    // An update, a delete of a live key, and a delete of a key the table never had.
    let s = apply(
        &dir,
        &[c("u", 1, "A", 2), c("d", 2, "", 2), c("d", 9, "", 2)],
        &[],
    )
    .unwrap();
    assert_eq!((s.keys_updated, s.keys_deleted, s.rows_deleted), (1, 2, 1));
    assert_eq!(s.prior_rows_replaced, 1);
    let want: BTreeMap<_, _> = [
        (1, ("A".to_string(), Some(2))),
        (3, ("c".to_string(), Some(1))),
    ]
    .into_iter()
    .collect();
    assert_eq!(live(&dir), want);
    // A delete row never reached a data file: the snapshot added one row, for the update.
    let rows: usize = current_scan_files(&dir)
        .unwrap()
        .iter()
        .map(|f| f.record_count as usize)
        .sum();
    assert_eq!(rows, 4);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn within_one_write_the_newest_change_wins_whatever_the_arrival() {
    let dir = tmp_dir("within");
    // Without order_by: the last arrival. A delete after an insert in one write leaves nothing.
    apply(
        &dir,
        &[
            c("c", 1, "a", 1),
            c("d", 1, "", 2),
            c("c", 2, "x", 1),
            c("u", 2, "y", 2),
        ],
        &[],
    )
    .unwrap();
    assert_eq!(live(&dir).keys().copied().collect::<Vec<_>>(), vec![2]);
    assert_eq!(live(&dir)[&2].0, "y");
    // With order_by: the greatest version, though it arrived first.
    let dir2 = tmp_dir("within-ordered");
    let s = apply(
        &dir2,
        &[
            c("u", 1, "new", 9),
            c("u", 1, "old", 3),
            c("d", 2, "", 5),
            c("c", 2, "back", 4),
        ],
        &["ver"],
    )
    .unwrap();
    assert_eq!(live(&dir2).get(&1).map(|r| r.0.as_str()), Some("new"));
    assert!(
        !live(&dir2).contains_key(&2),
        "the delete (5) is newer than the insert (4)"
    );
    assert_eq!(
        s.duplicates_in_input, 2,
        "the two losers were written, then deleted"
    );
    // A tie: the later arrival wins.
    let dir3 = tmp_dir("within-tie");
    apply(
        &dir3,
        &[c("u", 1, "first", 7), c("u", 1, "second", 7)],
        &["ver"],
    )
    .unwrap();
    assert_eq!(live(&dir3)[&1].0, "second");
    for d in [dir, dir2, dir3] {
        std::fs::remove_dir_all(&d).ok();
    }
}

#[test]
fn a_change_not_newer_than_the_live_row_is_stale() {
    let dir = tmp_dir("stale");
    apply(
        &dir,
        &[c("c", 1, "v10", 10), c("c", 2, "v10", 10)],
        &["ver"],
    )
    .unwrap();
    // Older for 1, equal for 2 (a replay), newer for 3 (an insert): only 3 applies.
    let s = apply(
        &dir,
        &[
            c("u", 1, "v5", 5),
            c("u", 2, "v10b", 10),
            c("c", 3, "v1", 1),
        ],
        &["ver"],
    )
    .unwrap();
    assert_eq!(
        (s.changes_stale, s.keys_inserted, s.keys_updated),
        (2, 1, 0)
    );
    assert_eq!(s.order_by, vec!["ver".to_string()]);
    let l = live(&dir);
    assert_eq!(
        (l[&1].0.as_str(), l[&2].0.as_str(), l[&3].0.as_str()),
        ("v10", "v10", "v1")
    );
    // A stale delete leaves the row too.
    let s = apply(&dir, &[c("d", 1, "", 9)], &["ver"]).unwrap();
    assert_eq!((s.changes_stale, s.rows_deleted), (1, 0));
    assert!(live(&dir).contains_key(&1));
    // Replaying the whole first write changes nothing.
    let before = live(&dir);
    let s = apply(
        &dir,
        &[c("c", 1, "v10", 10), c("c", 2, "v10", 10)],
        &["ver"],
    )
    .unwrap();
    assert_eq!(s.changes_stale, 2);
    assert_eq!(live(&dir), before);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_table_ordered_one_way_refuses_a_merge_ordered_another() {
    let dir = tmp_dir("order-guard");
    apply(&dir, &[c("c", 1, "a", 1)], &["ver"]).unwrap();
    let e = apply(&dir, &[c("u", 1, "b", 2)], &[]).unwrap_err();
    assert!(
        e.contains("ordered by `ver`") && e.contains("no `changes.order_by`"),
        "{e}"
    );
    assert_eq!(live(&dir)[&1].0, "a", "the refused write left nothing");
    // A plain merge (no change feed) is refused the same way.
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 1, "v": "plain", "ver": 3})).unwrap();
    let mut sink = IcebergSink::create_merge(
        &dir,
        sh.schema(),
        Compression::Snappy,
        &[],
        &["id".to_string()],
    )
    .unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    let e = sink.finish().unwrap_err().to_string();
    assert!(e.contains("is a plain merge"), "{e}");
    assert_eq!(live(&dir)[&1].0, "a");
    std::fs::remove_dir_all(&dir).ok();
}

/// A small deterministic generator, so a failure names its seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The rules, as plainly as they can be written: per write, each key's winner is its
/// greatest version (the later arrival on a tie); with `order_by` it applies only if it is
/// newer than the key's live row and than the delete the key's tombstone remembers. An
/// applied delete is remembered; an applied insert or update forgets it. (The retention is
/// a week, so nothing is trimmed in a test.)
fn model(writes: &[Vec<Value>], ordered: bool) -> BTreeMap<i64, (String, Option<i64>)> {
    let mut state: BTreeMap<i64, (String, Option<i64>)> = BTreeMap::new();
    let mut tombs: BTreeMap<i64, i64> = BTreeMap::new();
    for w in writes {
        let mut winners: BTreeMap<i64, &Value> = BTreeMap::new();
        for ch in w {
            let id = ch["id"].as_i64().unwrap();
            let newer = match winners.get(&id) {
                None => true,
                Some(prev) => !ordered || ch["ver"].as_i64() >= prev["ver"].as_i64(),
            };
            if newer {
                winners.insert(id, ch);
            }
        }
        for (id, ch) in winners {
            let ver = ch["ver"].as_i64();
            if ordered {
                let live = state.get(&id).and_then(|r| r.1);
                let tomb = tombs.get(&id).copied();
                if live.is_some_and(|l| ver <= Some(l)) || tomb.is_some_and(|t| ver <= Some(t)) {
                    continue;
                }
            }
            if ch["op"] == "d" {
                state.remove(&id);
                if ordered {
                    tombs.insert(id, ver.unwrap());
                }
            } else {
                state.insert(id, (ch["v"].as_str().unwrap().to_string(), ver));
                tombs.remove(&id);
            }
        }
    }
    state
}

#[test]
fn random_feeds_in_random_writes_match_the_model() {
    for ordered in [false, true] {
        for seed in 1..=12u64 {
            let mut rng =
                Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1 | ((ordered as u64) << 1));
            let dir = tmp_dir(&format!("model-{ordered}-{seed}"));
            let keys = 1 + rng.below(12) as i64;
            let n = 20 + rng.below(80) as usize;
            let mut all = Vec::with_capacity(n);
            for i in 0..n {
                let op = ["c", "u", "d"][rng.below(3) as usize];
                // Versions repeat and arrive out of order on purpose.
                let ver = rng.below(n as u64 / 2 + 1) as i64;
                all.push(c(
                    op,
                    rng.below(keys as u64) as i64,
                    &format!("s{seed}-{i}"),
                    ver,
                ));
            }
            let mut writes: Vec<Vec<Value>> = Vec::new();
            let mut rest = all.as_slice();
            while !rest.is_empty() {
                let take = 1 + rng.below(rest.len() as u64 / 2 + 1) as usize;
                writes.push(rest[..take.min(rest.len())].to_vec());
                rest = &rest[take.min(rest.len())..];
            }
            let order: &[&str] = if ordered { &["ver"] } else { &[] };
            for w in &writes {
                apply(&dir, w, order).unwrap();
            }
            assert_eq!(
                live(&dir),
                model(&writes, ordered),
                "ordered={ordered} seed={seed}"
            );
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}

#[test]
fn the_order_survives_appends_and_the_expiry_of_the_snapshot_that_set_it() {
    use shapeshift_iceberg::{expire_table, ExpireOptions};
    let dir = tmp_dir("order-kept");
    apply(&dir, &[c("c", 1, "a", 1)], &["ver"]).unwrap();
    // Two plain appends after it, then every snapshot but the newest expired.
    for v in ["x", "y"] {
        let cols = vec![
            col("id", ColumnType::Int64),
            col("v", ColumnType::String),
            col("ver", ColumnType::Int64),
        ];
        let mut sh = Shaper::new(cols, false).unwrap();
        sh.push(&json!({"id": 50, "v": v, "ver": 1})).unwrap();
        let mut sink =
            IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]).unwrap();
        sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
        sink.finish().unwrap();
    }
    let s = expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert!(s.expired_snapshots.len() >= 2, "{s:?}");
    let e = apply(&dir, &[c("u", 1, "b", 2)], &[]).unwrap_err();
    assert!(e.contains("ordered by `ver`"), "{e}");
    // A plain merge (no change feed) is refused the same way.
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 1, "v": "plain", "ver": 9})).unwrap();
    let mut sink = IcebergSink::create_merge(
        &dir,
        sh.schema(),
        Compression::Snappy,
        &[],
        &["id".to_string()],
    )
    .unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    let e = sink.finish().unwrap_err().to_string();
    assert!(e.contains("is a plain merge"), "{e}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_write_of_deletes_only_commits_onto_a_partitioned_table() {
    let dir = tmp_dir("partitioned-deletes");
    let cols = || {
        vec![
            col("id", ColumnType::Int64),
            col("v", ColumnType::String),
            col("ver", ColumnType::Int64),
        ]
    };
    let write = |rows: &[Value]| {
        let mut sh = Shaper::new(cols(), false)
            .unwrap()
            .with_changes(&feed(&[]), ApplyMode::Upsert, &["id".to_string()])
            .unwrap();
        for r in rows {
            sh.push(r).unwrap();
        }
        let mut sink = IcebergSink::create_merge(
            &dir,
            sh.table_schema(),
            Compression::Snappy,
            &["v".to_string()],
            &["id".to_string()],
        )
        .unwrap();
        sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
        sink.finish().unwrap().merge.unwrap()
    };
    write(&[c("c", 1, "eu", 1), c("c", 2, "us", 1)]);
    let s = write(&[c("d", 1, "", 2)]);
    assert_eq!(s.rows_deleted, 1);
    assert_eq!(live(&dir).keys().copied().collect::<Vec<_>>(), vec![2]);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_recorded_order_is_a_list_a_comma_cannot_split() {
    let meta = |order: &str| {
        json!({
            "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1, "summary": {"shapeshift.changes.order-by": order}}]
        })
    };
    use shapeshift_iceberg::table_change_order;
    assert_eq!(
        table_change_order(&meta(r#"["a,b"]"#)),
        Some(Ok(vec!["a,b".to_string()]))
    );
    // A value that is not a list cannot be matched by any merge.
    assert_eq!(
        table_change_order(&meta("a,b")),
        Some(Err("a,b".to_string()))
    );
    assert_eq!(
        table_change_order(&meta(r#"["a","b"]"#)),
        Some(Ok(vec!["a".to_string(), "b".to_string()]))
    );
    assert_eq!(table_change_order(&meta("[]")), Some(Ok(vec![])));
}

#[test]
fn an_order_this_build_cannot_read_refuses_every_merge() {
    let dir = tmp_dir("order-unreadable");
    apply(&dir, &[c("c", 1, "a", 1)], &["ver"]).unwrap();
    // Rewrite the recorded order as something that is not a list of names.
    let meta_dir = dir.join("metadata");
    let newest = std::fs::read_dir(&meta_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .max()
        .unwrap();
    let text = std::fs::read_to_string(&newest).unwrap();
    let text = text.replace(r#"[\"ver\"]"#, "ver");
    assert!(
        text.contains(r#""shapeshift.changes.order-by": "ver""#),
        "{text}"
    );
    std::fs::write(&newest, text).unwrap();
    for order in [&["ver"][..], &[][..]] {
        let e = apply(&dir, &[c("u", 1, "b", 2)], order).unwrap_err();
        assert!(e.contains("cannot read"), "{e}");
    }
    assert_eq!(live(&dir)[&1].0, "a");
    std::fs::remove_dir_all(&dir).ok();
}

/// [`apply`], with a tombstone retention other than a week.
fn apply_retaining(
    dir: &std::path::Path,
    changes: &[Value],
    retention_ms: i64,
) -> Result<MergeSummary, String> {
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false)
        .unwrap()
        .with_changes(&feed(&["ver"]), ApplyMode::Upsert, &["id".to_string()])
        .unwrap();
    for c in changes {
        sh.push(c).unwrap();
    }
    let mut sink = IcebergSink::create_merge(
        dir,
        sh.table_schema(),
        Compression::Snappy,
        &[],
        &["id".to_string()],
    )
    .map_err(|e| e.to_string())?;
    sink.order_changes_by(&["ver".to_string()], retention_ms)
        .map_err(|e| e.to_string())?;
    sink.write_batch(&sh.flush().unwrap().unwrap())
        .map_err(|e| e.to_string())?;
    Ok(sink.finish().map_err(|e| e.to_string())?.merge.unwrap())
}

/// The newest `metadata.json` of a local table, parsed, and its path.
fn newest_metadata(dir: &std::path::Path) -> (std::path::PathBuf, Value) {
    let path = std::fs::read_dir(dir.join("metadata"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .max_by_key(|p| {
            let n = p.file_name().unwrap().to_string_lossy().to_string();
            n.trim_start_matches('v')
                .trim_end_matches(".metadata.json")
                .parse::<u64>()
                .unwrap_or(0)
        })
        .unwrap();
    let meta = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    (path, meta)
}

#[test]
fn a_late_change_older_than_the_delete_does_not_bring_the_row_back() {
    let dir = tmp_dir("tomb-late");
    apply(&dir, &[c("c", 1, "a", 1), c("c", 2, "b", 1)], &["ver"]).unwrap();
    let s = apply(&dir, &[c("d", 1, "", 10)], &["ver"]).unwrap();
    assert_eq!(
        (s.rows_deleted, s.tombstones_written, s.tombstones_live),
        (1, 1, 1)
    );
    // An update and an insert for the deleted key, both older than the delete: stale.
    let s = apply(&dir, &[c("u", 1, "late", 5)], &["ver"]).unwrap();
    assert_eq!(s.changes_stale, 1);
    let s = apply(&dir, &[c("c", 1, "replayed", 10)], &["ver"]).unwrap();
    assert_eq!(
        s.changes_stale, 1,
        "a replay of the delete's own version is stale too"
    );
    assert!(!live(&dir).contains_key(&1));
    // Newer than the delete: the key comes back, and its tombstone is retired.
    let s = apply(&dir, &[c("c", 1, "back", 11)], &["ver"]).unwrap();
    assert_eq!((s.tombstones_retired, s.tombstones_live), (1, 0));
    assert_eq!(live(&dir)[&1].0, "back");
    // A delete of a key the table never had is remembered too.
    let s = apply(&dir, &[c("d", 9, "", 20)], &["ver"]).unwrap();
    assert_eq!((s.rows_deleted, s.tombstones_written), (0, 1));
    let s = apply(&dir, &[c("c", 9, "early", 19)], &["ver"]).unwrap();
    assert_eq!(s.changes_stale, 1);
    assert!(!live(&dir).contains_key(&9));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_tombstone_past_the_retention_is_trimmed() {
    let dir = tmp_dir("tomb-retention");
    apply_retaining(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], 1).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    // The retention is 1 ms: by now the tombstone is past it, so the late insert applies.
    let s = apply_retaining(&dir, &[c("c", 2, "late", 3)], 1).unwrap();
    assert_eq!((s.tombstones_trimmed, s.changes_stale), (1, 0));
    assert_eq!(live(&dir)[&2].0, "late");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn tombstones_survive_appends_compaction_expiry_and_a_move() {
    use shapeshift_iceberg::{
        compact_table, expire_table, relocate_table, table_tombstones, CompactOptions,
        ExpireOptions, RelocateOptions,
    };
    let dir = tmp_dir("tomb-carried");
    apply(&dir, &[c("c", 1, "a", 1), c("c", 2, "b", 1)], &["ver"]).unwrap();
    apply(&dir, &[c("d", 1, "", 10)], &["ver"]).unwrap();
    let (_, meta) = newest_metadata(&dir);
    let tomb = table_tombstones(&meta).unwrap().unwrap();
    // A plain append, a compaction, and an expiry down to the newest snapshot.
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 50, "v": "x", "ver": 1})).unwrap();
    let mut sink = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]).unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    sink.finish().unwrap();
    compact_table(&dir, &CompactOptions::default()).unwrap();
    let s = expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert!(s.expired_snapshots.len() >= 3, "{s:?}");
    let (_, meta) = newest_metadata(&dir);
    assert_eq!(table_tombstones(&meta).unwrap(), Some(tomb.clone()));
    assert!(
        dir.join(&tomb.key).exists(),
        "the newest snapshot names it, so expiry kept it"
    );
    let stats = meta["statistics"].as_array().unwrap();
    assert_eq!(stats.len(), 1, "only the retained snapshot's entry is left");
    // Every snapshot that names the file says which write computed it, as the blob does.
    assert_eq!(
        stats[0]["blob-metadata"][0]["snapshot-id"],
        json!(tomb.snapshot_id)
    );
    assert_eq!(
        stats[0]["blob-metadata"][0]["sequence-number"],
        json!(tomb.sequence_number)
    );
    // Moved, and relocated: the tombstones go with the table.
    let moved = tmp_dir("tomb-carried-moved");
    std::fs::rename(&dir, &moved).unwrap();
    relocate_table(&moved, &RelocateOptions::default()).unwrap();
    let s = apply(&moved, &[c("u", 1, "late", 5)], &["ver"]).unwrap();
    assert_eq!(s.changes_stale, 1);
    assert!(!live(&moved).contains_key(&1));
    std::fs::remove_dir_all(&moved).ok();
}

#[test]
fn an_expiry_deletes_a_tombstone_file_no_retained_snapshot_names() {
    use shapeshift_iceberg::{expire_table, table_tombstones, ExpireOptions};
    let dir = tmp_dir("tomb-expired");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    let (_, first) = newest_metadata(&dir);
    let old = table_tombstones(&first).unwrap().unwrap();
    // A second delete writes a new tombstone file; the first is named only by older snapshots.
    apply(&dir, &[c("d", 3, "", 6)], &["ver"]).unwrap();
    let (_, meta) = newest_metadata(&dir);
    let new = table_tombstones(&meta).unwrap().unwrap();
    assert_ne!(old.key, new.key);
    assert_eq!(new.count, 2);
    let s = expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert_eq!(s.statistics_files_deleted, 1, "{s:?}");
    assert!(!dir.join(&old.key).exists());
    assert!(dir.join(&new.key).exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_table_whose_tombstones_are_unreadable_or_newer_is_not_written() {
    let dir = tmp_dir("tomb-guard");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    let (path, meta) = newest_metadata(&dir);
    let tomb = shapeshift_iceberg::table_tombstones(&meta)
        .unwrap()
        .unwrap();
    // The file gone: the merge stops rather than go on without it.
    let saved = std::fs::read(dir.join(&tomb.key)).unwrap();
    std::fs::remove_file(dir.join(&tomb.key)).unwrap();
    assert!(apply(&dir, &[c("c", 2, "late", 3)], &["ver"]).is_err());
    std::fs::write(dir.join(&tomb.key), saved).unwrap();
    // A format newer than this build's: no write at all, a plain append included.
    let text = std::fs::read_to_string(&path).unwrap();
    let newer = text.replace(
        r#""shapeshift.changes.format": "1""#,
        r#""shapeshift.changes.format": "2""#,
    );
    assert_ne!(text, newer);
    std::fs::write(&path, newer).unwrap();
    let e = apply(&dir, &[c("c", 3, "x", 9)], &["ver"]).unwrap_err();
    assert!(e.contains("format 2") || e.contains("\"2\""), "{e}");
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let sh = Shaper::new(cols, false).unwrap();
    let e = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[])
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("newer shapeshift"), "{e}");
    std::fs::remove_dir_all(&dir).ok();
}

/// Rewrite the newest `metadata.json` of a local table with `f`.
fn rewrite_newest(dir: &std::path::Path, f: impl FnOnce(&mut Value)) {
    let (path, mut meta) = newest_metadata(dir);
    f(&mut meta);
    std::fs::write(&path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
}

#[test]
fn another_engines_statistics_or_snapshots_do_not_lose_the_tombstones() {
    use shapeshift_iceberg::{expire_table, table_tombstones, ExpireOptions};
    let expire_all = |dir: &std::path::Path| {
        expire_table(
            dir,
            &ExpireOptions {
                older_than_ms: i64::MAX,
                retain_last: 1,
                dry_run: false,
            },
        )
        .unwrap()
    };
    // (A) An `ANALYZE` replaced the current snapshot's statistics entry with its own file.
    let dir = tmp_dir("tomb-foreign-stats");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    apply(&dir, &[c("c", 3, "x", 6)], &["ver"]).unwrap(); // names the same tombstone file
    let (_, meta) = newest_metadata(&dir);
    let tomb = table_tombstones(&meta).unwrap().unwrap();
    rewrite_newest(&dir, |m| {
        let head = m["current-snapshot-id"].clone();
        for e in m["statistics"].as_array_mut().unwrap() {
            if e["snapshot-id"] == head {
                e["statistics-path"] = json!("/elsewhere/analyze.puffin");
            }
        }
    });
    expire_all(&dir);
    assert!(
        dir.join(&tomb.key).exists(),
        "the current snapshot's summary still names it"
    );
    assert_eq!(
        apply(&dir, &[c("c", 2, "late", 4)], &["ver"])
            .unwrap()
            .changes_stale,
        1
    );
    std::fs::remove_dir_all(&dir).ok();

    // (B) Another engine's append on top, which records nothing of the change-feed state.
    let dir = tmp_dir("tomb-foreign-snapshot");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 50, "v": "x", "ver": 1})).unwrap();
    let mut sink = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]).unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    sink.finish().unwrap();
    rewrite_newest(&dir, |m| {
        let head = m["current-snapshot-id"].clone();
        for s in m["snapshots"].as_array_mut().unwrap() {
            if s["snapshot-id"] == head {
                let summary = s["summary"].as_object_mut().unwrap();
                summary.retain(|k, _| !k.starts_with("shapeshift."));
            }
        }
        m["statistics"]
            .as_array_mut()
            .unwrap()
            .retain(|e| e["snapshot-id"] != head);
    });
    let s = expire_all(&dir);
    assert!(
        s.expired_snapshots.is_empty(),
        "the snapshot that records the state is kept: {s:?}"
    );
    assert_eq!(
        apply(&dir, &[c("c", 2, "late", 4)], &["ver"])
            .unwrap()
            .changes_stale,
        1
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_change_with_no_version_cannot_retire_a_tombstone() {
    let dir = tmp_dir("tomb-null-version");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    // A batch the shaper would refuse (a null version), written straight to the sink.
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        arrow_schema::Field::new("v", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new("ver", arrow_schema::DataType::Int64, true),
        arrow_schema::Field::new(
            shapeshift_core::CHANGE_COLUMN,
            arrow_schema::DataType::UInt8,
            false,
        ),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![2])),
            std::sync::Arc::new(StringArray::from(vec!["unversioned"])),
            std::sync::Arc::new(Int64Array::from(vec![None::<i64>])),
            std::sync::Arc::new(arrow_array::UInt8Array::from(vec![0u8])),
        ],
    )
    .unwrap();
    let table: Vec<_> = schema
        .fields()
        .iter()
        .take(3)
        .map(|f| f.as_ref().clone())
        .collect();
    let mut sink = IcebergSink::create_merge(
        &dir,
        std::sync::Arc::new(arrow_schema::Schema::new(table)),
        Compression::Snappy,
        &[],
        &["id".to_string()],
    )
    .unwrap();
    sink.order_changes_by(&["ver".to_string()], 7 * 86_400_000)
        .unwrap();
    sink.write_batch(&batch).unwrap();
    let s = sink.finish().unwrap().merge.unwrap();
    assert_eq!((s.changes_stale, s.tombstones_retired), (1, 0));
    assert!(!live(&dir).contains_key(&2));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_tag_on_a_foreign_snapshot_keeps_the_state_it_rolls_back_to() {
    use shapeshift_iceberg::{expire_table, ExpireOptions};
    let dir = tmp_dir("tomb-tag");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    // Another engine's append, which records nothing, tagged.
    let cols = vec![
        col("id", ColumnType::Int64),
        col("v", ColumnType::String),
        col("ver", ColumnType::Int64),
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 50, "v": "x", "ver": 1})).unwrap();
    let mut sink = IcebergSink::create(&dir, sh.schema(), Compression::Snappy, true, &[]).unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    sink.finish().unwrap();
    let mut tagged = 0i64;
    rewrite_newest(&dir, |m| {
        let head = m["current-snapshot-id"].clone();
        tagged = head.as_i64().unwrap();
        for s in m["snapshots"].as_array_mut().unwrap() {
            if s["snapshot-id"] == head {
                s["summary"]
                    .as_object_mut()
                    .unwrap()
                    .retain(|k, _| !k.starts_with("shapeshift."));
            }
        }
        m["statistics"]
            .as_array_mut()
            .unwrap()
            .retain(|e| e["snapshot-id"] != head);
    });
    // A change-feed write on top, the tag set on the foreign snapshot, then an expiry down
    // to the newest.
    apply(&dir, &[c("c", 3, "y", 6)], &["ver"]).unwrap();
    rewrite_newest(&dir, |m| {
        m["refs"]["t"] = json!({ "snapshot-id": tagged, "type": "tag" });
    });
    expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    // Roll `main` back to the tag: its tombstones are still found.
    rewrite_newest(&dir, |m| {
        m["current-snapshot-id"] = json!(tagged);
        m["refs"]["main"] = json!({ "snapshot-id": tagged, "type": "branch" });
    });
    assert_eq!(
        apply(&dir, &[c("c", 2, "late", 4)], &["ver"])
            .unwrap()
            .changes_stale,
        1
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn an_expired_snapshots_summary_only_tombstone_file_is_collected() {
    use shapeshift_iceberg::{expire_table, table_tombstones, ExpireOptions};
    let dir = tmp_dir("tomb-summary-only");
    apply(&dir, &[c("c", 1, "a", 1), c("d", 2, "", 5)], &["ver"]).unwrap();
    let (_, first) = newest_metadata(&dir);
    let old = table_tombstones(&first).unwrap().unwrap();
    // Another engine replaced this snapshot's statistics entry.
    rewrite_newest(&dir, |m| {
        let head = m["current-snapshot-id"].clone();
        for e in m["statistics"].as_array_mut().unwrap() {
            if e["snapshot-id"] == head {
                e["statistics-path"] = json!("/elsewhere/analyze.puffin");
            }
        }
    });
    // A newer delete writes a new tombstone file; the old one is named only by that summary.
    apply(&dir, &[c("d", 3, "", 6)], &["ver"]).unwrap();
    expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert!(!dir.join(&old.key).exists(), "nothing retained names it");
    std::fs::remove_dir_all(&dir).ok();
}
