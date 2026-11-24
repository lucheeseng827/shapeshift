//! Snapshot expiry (`expire_table`): which snapshots stay, which files go, and that every
//! file a retained snapshot reads is still there afterwards. DuckDB time-travels the
//! retained snapshots in `examples/merge/verify_duckdb.py`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, Shaper, Sink};
use shapeshift_iceberg::{
    compact_table, current_scan_files, expire_table, inspect, read_manifest_entries,
    read_manifest_list, CompactOptions, ExpireOptions, ExpireSummary, IcebergSink,
};

fn tmp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-expire-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn merge(dir: &Path, rows: &[Value]) {
    let col = |name: &str, ty| ColumnSpec {
        name: name.into(),
        from: None,
        ty,
        transform: None,
        required: false,
    };
    let mut sh = Shaper::new(
        vec![col("id", ColumnType::Int64), col("v", ColumnType::Int64)],
        false,
    )
    .unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    let key = ["id".to_string()];
    let mut sink =
        IcebergSink::create_merge(dir, sh.schema(), Compression::Snappy, &[], &key).unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    sink.finish().unwrap();
}

fn row(id: i64, v: i64) -> Value {
    json!({ "id": id, "v": v })
}

/// Four merges: ids 1-3, then updates of 2, then of 3, then a new id 4.
fn history(name: &str) -> PathBuf {
    let dir = tmp_dir(name);
    merge(&dir, &[row(1, 1), row(2, 2), row(3, 3)]);
    merge(&dir, &[row(2, 20)]);
    merge(&dir, &[row(3, 30)]);
    merge(&dir, &[row(4, 4)]);
    dir
}

fn expire(dir: &Path, retain_last: usize, dry_run: bool) -> ExpireSummary {
    expire_table(
        dir,
        &ExpireOptions {
            older_than_ms: i64::MAX, // everything is old enough
            retain_last,
            dry_run,
        },
    )
    .unwrap()
}

fn metadata(dir: &Path) -> Value {
    let hint = std::fs::read_to_string(dir.join("metadata/version-hint.text")).unwrap();
    let path = dir.join(format!("metadata/v{}.metadata.json", hint.trim()));
    serde_json::from_reader(std::fs::File::open(path).unwrap()).unwrap()
}

/// Every file under the table directory, relative to it.
fn on_disk(dir: &Path) -> BTreeSet<String> {
    fn walk(root: &Path, at: &Path, out: &mut BTreeSet<String>) {
        for e in std::fs::read_dir(at).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(root, &p, out);
            } else {
                out.insert(p.strip_prefix(root).unwrap().to_string_lossy().into_owned());
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(dir, dir, &mut out);
    out
}

/// Every file each listed snapshot reads: its manifest list, manifests, and live files.
fn read_by_snapshots(meta: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for s in meta["snapshots"].as_array().unwrap() {
        let list = s["manifest-list"].as_str().unwrap();
        out.insert(list.to_string());
        for m in read_manifest_list(&std::fs::read(list).unwrap()).unwrap() {
            for e in read_manifest_entries(&std::fs::read(&m.path).unwrap()).unwrap() {
                if e.is_live() {
                    out.insert(e.file_path);
                }
            }
            out.insert(m.path);
        }
    }
    out
}

fn live_ids(dir: &Path) -> usize {
    current_scan_files(dir)
        .unwrap()
        .iter()
        .map(|f| f.record_count as usize - f.deleted_positions.len())
        .sum()
}

#[test]
fn after_a_compaction_expiry_deletes_everything_only_history_read() {
    let dir = history("after-compact");
    compact_table(&dir, &CompactOptions::default()).unwrap();
    let before = on_disk(&dir);

    let s = expire(&dir, 1, false);
    assert!(s.committed);
    assert_eq!(s.expired_snapshots.len(), 4, "the four merges");
    assert_eq!(s.retained_snapshots, 1, "the compaction");
    assert_eq!(s.data_files_deleted, 4, "the merges' data files");
    assert_eq!(s.delete_files_deleted, 2, "merges 2 and 3 replaced rows");
    assert_eq!(s.manifest_lists_deleted, 4);
    assert_eq!(
        s.manifests_deleted, 6,
        "four data manifests, two delete manifests"
    );
    assert!(s.bytes_deleted > 0);
    assert_eq!((s.files_outside_table, s.files_not_deleted), (0, 0));

    let meta = metadata(&dir);
    assert_eq!(meta["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(meta["snapshot-log"].as_array().unwrap().len(), 1);
    assert_eq!(
        meta["refs"]["main"]["snapshot-id"],
        meta["current-snapshot-id"]
    );
    assert_eq!(live_ids(&dir), 4);
    assert_eq!(inspect(&dir).unwrap().total_records, 4);

    // What is left under data/ is exactly what the one remaining snapshot reads.
    let read: BTreeSet<String> = read_by_snapshots(&meta)
        .iter()
        .map(|p| {
            Path::new(p)
                .strip_prefix(dir.canonicalize().unwrap())
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    let left = on_disk(&dir);
    let data_left: BTreeSet<&String> = left.iter().filter(|p| p.starts_with("data/")).collect();
    let data_read: BTreeSet<&String> = read.iter().filter(|p| p.starts_with("data/")).collect();
    assert_eq!(data_left, data_read);
    assert!(
        read.is_subset(&left),
        "every file the snapshot reads is still there"
    );
    assert_eq!(
        before.len() - left.len(),
        (s.data_files_deleted
            + s.delete_files_deleted
            + s.manifests_deleted
            + s.manifest_lists_deleted) as usize
            - 1, // the new metadata file
        "exactly the files counted were deleted"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_merge_only_history_still_needs_its_data_files() {
    // Merges append files and carry manifests forward, so the latest snapshot still reads
    // every data and delete file the earlier ones wrote: expiring them frees only their
    // manifest lists.
    let dir = history("merges-only");
    let s = expire(&dir, 2, false);
    assert_eq!(s.expired_snapshots.len(), 2);
    assert_eq!(s.retained_snapshots, 2);
    assert_eq!(s.manifest_lists_deleted, 2);
    assert_eq!(
        (
            s.data_files_deleted,
            s.delete_files_deleted,
            s.manifests_deleted
        ),
        (0, 0, 0)
    );
    let meta = metadata(&dir);
    assert!(read_by_snapshots(&meta)
        .iter()
        .all(|p| Path::new(p).exists()));
    assert_eq!(live_ids(&dir), 4);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_cutoff_and_retain_last_keep_snapshots() {
    let dir = history("cutoff");
    // Nothing is older than the epoch.
    let none = expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: 0,
            retain_last: 1,
            dry_run: false,
        },
    )
    .unwrap();
    assert!(none.expired_snapshots.is_empty() && !none.committed);
    assert_eq!(none.retained_snapshots, 4);

    // Retaining more than there are keeps them all.
    let all = expire(&dir, 10, false);
    assert!(all.expired_snapshots.is_empty());

    let zero = expire_table(
        &dir,
        &ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 0,
            dry_run: false,
        },
    )
    .unwrap_err()
    .to_string();
    assert!(zero.contains("at least 1"), "{zero}");
    assert_eq!(metadata(&dir)["snapshots"].as_array().unwrap().len(), 4);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_dry_run_reports_what_it_would_delete_and_changes_nothing() {
    let dir = history("dry");
    compact_table(&dir, &CompactOptions::default()).unwrap();
    let before = on_disk(&dir);
    let version = std::fs::read_to_string(dir.join("metadata/version-hint.text")).unwrap();

    let dry = expire(&dir, 1, true);
    assert!(!dry.committed);
    assert_eq!(dry.expired_snapshots.len(), 4);
    assert_eq!(dry.data_files_deleted, 4);
    assert_eq!(on_disk(&dir), before);
    assert_eq!(
        std::fs::read_to_string(dir.join("metadata/version-hint.text")).unwrap(),
        version
    );

    let real = expire(&dir, 1, false);
    assert_eq!(
        (
            real.data_files_deleted,
            real.delete_files_deleted,
            real.manifests_deleted
        ),
        (
            dry.data_files_deleted,
            dry.delete_files_deleted,
            dry.manifests_deleted
        ),
        "the dry run counted what the real run deleted"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_file_outside_the_table_is_never_deleted() {
    // A copy whose metadata names another location: its old snapshots' paths point into
    // the original table, which an expiry of the copy must not touch.
    let original = history("outside-original");
    compact_table(&original, &CompactOptions::default()).unwrap();
    let copy = tmp_dir("outside-copy");
    copy_dir(&original, &copy);
    let hint = std::fs::read_to_string(copy.join("metadata/version-hint.text")).unwrap();
    let meta_path = copy.join(format!("metadata/v{}.metadata.json", hint.trim()));
    let mut meta: Value =
        serde_json::from_reader(std::fs::File::open(&meta_path).unwrap()).unwrap();
    meta["location"] = json!("/somewhere/else");
    std::fs::write(&meta_path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();

    let before = on_disk(&original);
    let s = expire(&copy, 1, false);
    assert!(s.committed);
    assert!(s.files_outside_table > 0, "{s:?}");
    assert_eq!(
        s.data_files_deleted
            + s.delete_files_deleted
            + s.manifests_deleted
            + s.manifest_lists_deleted,
        0,
        "every path named the original table"
    );
    assert_eq!(
        on_disk(&original),
        before,
        "the original table is untouched"
    );
    assert_eq!(live_ids(&original), 4);
    std::fs::remove_dir_all(&original).ok();
    std::fs::remove_dir_all(&copy).ok();
}

#[test]
fn merges_after_an_expiry_work_and_expire_again() {
    let dir = history("again");
    compact_table(&dir, &CompactOptions::default()).unwrap();
    expire(&dir, 1, false);
    merge(&dir, &[row(1, 100), row(5, 5)]);
    assert_eq!(live_ids(&dir), 5);
    compact_table(&dir, &CompactOptions::default()).unwrap();
    let s = expire(&dir, 1, false);
    assert_eq!(s.expired_snapshots.len(), 2);
    assert_eq!(live_ids(&dir), 5);
    let meta = metadata(&dir);
    assert!(read_by_snapshots(&meta)
        .iter()
        .all(|p| Path::new(p).exists()));
    std::fs::remove_dir_all(&dir).ok();
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let p = e.path();
        let dest = to.join(e.file_name());
        if p.is_dir() {
            copy_dir(&p, &dest);
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}
