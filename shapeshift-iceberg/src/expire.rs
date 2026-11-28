//! Snapshot expiry: `shapeshift expire`.
//!
//! Every write keeps the table's whole history: each snapshot's manifest list, its
//! manifests, and every file any of them names stay on disk, so time travel can read
//! them. A compaction makes that visible — it leaves its inputs in place for the
//! snapshots before it. Expiry is how history is let go:
//!
//! - **Which snapshots stay.** The current one; any a ref (branch or tag) points at; the
//!   newest `retain_last` of the current snapshot's ancestry, however old; and every
//!   snapshot committed at or after the cutoff. Everything else is expired.
//! - **Which files go.** Every file an expired snapshot references — its manifest list,
//!   its manifests, the data and delete files they name — **minus** every file a
//!   retained snapshot still needs: its manifest list, its manifests, and the files they
//!   list as live. A file a retained snapshot only records as removed (a DELETED entry) is
//!   not read by it, so it goes too.
//! - **Never outside the table.** A file is deleted only if its recorded path lies under
//!   the table's `location` (or where the table is now). Any other path is left in place
//!   and counted.
//! - **Commit first, delete after.** The new metadata (the same table, minus the expired
//!   snapshots) is published before a single file is deleted. A run that stops halfway
//!   leaves unreferenced files behind, never a table that names a missing file.
//!
//! The planner reads every snapshot's manifest list, then every manifest they name, once
//! each (a manifest many snapshots carry is read once), [`TableFiles::readers`] at a time.
//! It touches no data file.
//!
//! **Concurrent writers.** If another writer commits after the plan is made, the commit
//! guard refuses the expiry before a single file is deleted (that writer's snapshot may read
//! files the plan would remove). A reader still on an expired snapshot loses its files.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use shapeshift_core::{Result, ShapeError};

use crate::anchor::Anchors;
use crate::avro::{read_manifest_entries, read_manifest_list, ManifestEntry, ManifestFileEntry};
use crate::files::{read_each, TableFiles};
use crate::metadata::{self, RemoveInput};
use crate::PriorTable;

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// Iceberg's default maximum snapshot age (`history.expire.max-snapshot-age-ms`): 5 days.
pub const DEFAULT_MAX_SNAPSHOT_AGE_MS: i64 = 5 * 24 * 60 * 60 * 1000;

/// Which snapshots to expire.
#[derive(Debug, Clone)]
pub struct ExpireOptions {
    /// Snapshots committed before this time (epoch ms) may be expired.
    pub older_than_ms: i64,
    /// The newest this-many snapshots of the current snapshot's history are kept, however
    /// old. At least 1: the current snapshot is never expired.
    pub retain_last: usize,
    /// Plan and report, but commit nothing and delete nothing.
    pub dry_run: bool,
}

/// What an expiry did (or, in a dry run, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpireSummary {
    /// The snapshots expired, oldest first.
    pub expired_snapshots: Vec<i64>,
    pub retained_snapshots: u64,
    pub data_files_deleted: u64,
    pub delete_files_deleted: u64,
    pub manifests_deleted: u64,
    pub manifest_lists_deleted: u64,
    /// Statistics files (tombstone files) only expired snapshots named.
    pub statistics_files_deleted: u64,
    /// Bytes of the files deleted, from the sizes the metadata records (a manifest list's
    /// own size is not recorded anywhere, so it is not counted).
    pub bytes_deleted: u64,
    /// Files only expired snapshots referenced, left in place because their path is not
    /// under the table's location.
    pub files_outside_table: u64,
    /// Files that should have been deleted but could not be (the commit still stands;
    /// they are left unreferenced).
    pub files_not_deleted: u64,
    /// Whether new metadata was published (false for a dry run, or nothing to expire).
    pub committed: bool,
    /// The table version published (`v{N}.metadata.json`), when `committed`.
    pub version: Option<u64>,
}

/// What kind of file a deletion removes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Data,
    Deletes,
    Manifest,
    ManifestList,
    /// A statistics file a snapshot names (a change feed's tombstones).
    Statistics,
}

/// A file to delete once the commit is published.
#[derive(Debug, Clone)]
pub struct DeleteTarget {
    /// The path exactly as the table's metadata records it.
    pub path: String,
    pub kind: FileKind,
    /// Its size, when the metadata records one.
    pub size: Option<u64>,
}

/// The metadata an expiry publishes: the table minus the expired snapshots.
pub struct ExpireCommit {
    /// The table version this commit creates.
    pub version: u64,
    pub metadata_key: String,
    pub metadata_bytes: Vec<u8>,
    pub version_hint_key: String,
    pub version_hint_bytes: Vec<u8>,
}

/// The plan: what to publish, then what to delete. `commit` is `None` when no snapshot
/// expires; `delete` is then empty.
pub struct ExpirePlan {
    pub commit: Option<ExpireCommit>,
    pub delete: Vec<DeleteTarget>,
    pub summary: ExpireSummary,
}

impl ExpireSummary {
    /// Count a file deleted (or, in a dry run, to be deleted).
    pub fn count_deleted(&mut self, t: &DeleteTarget) {
        match t.kind {
            FileKind::Data => self.data_files_deleted += 1,
            FileKind::Deletes => self.delete_files_deleted += 1,
            FileKind::Manifest => self.manifests_deleted += 1,
            FileKind::ManifestList => self.manifest_lists_deleted += 1,
            FileKind::Statistics => self.statistics_files_deleted += 1,
        }
        self.bytes_deleted += t.size.unwrap_or(0);
    }
}

/// Whether `path` names a file under `root`: past the root's own boundary, and with no `.`
/// or `..` segment that could lead back out of it (`<root>/data/../../other/x` is not under
/// `<root>`). An empty root holds nothing.
fn under_root(path: &str, root: &str) -> bool {
    let root = root.trim_end_matches('/');
    !root.is_empty()
        && path.strip_prefix(root).is_some_and(|rest| {
            rest.starts_with('/') && !rest.split('/').any(|seg| seg == "." || seg == "..")
        })
}

/// `paths` once each, in the order first seen.
fn unique<'a>(paths: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut seen = HashSet::new();
    paths.into_iter().filter(|p| seen.insert(*p)).collect()
}

/// Plan the expiry of `table` (its current metadata, read through `files`). `anchors` are
/// the directories its recorded paths were written against (see [`Anchors`]): a file
/// under one of them, or under `base_location` (where the table is now), is the table's
/// own and is deleted once nothing retained reads it; any other is counted as outside the
/// table and left alone. `now_ms` is recorded as the metadata's `last-updated-ms`. In a dry
/// run the summary counts what would be deleted.
pub fn plan_expiry<F: TableFiles>(
    table: &PriorTable,
    anchors: &Anchors,
    base_location: &str,
    files: &F,
    options: &ExpireOptions,
    now_ms: i64,
) -> Result<ExpirePlan> {
    if options.retain_last == 0 {
        return Err(err(
            "retain_last must be at least 1: the current snapshot is never expired",
        ));
    }
    let meta = &table.metadata;
    let snapshots = meta["snapshots"]
        .as_array()
        .ok_or_else(|| err("table metadata has no snapshots"))?;
    let id = |s: &Value| s["snapshot-id"].as_i64();
    let by_id: HashMap<i64, &Value> = snapshots.iter().filter_map(|s| Some((id(s)?, s))).collect();

    let mut keep: HashSet<i64> = HashSet::new();
    let mut at = meta["current-snapshot-id"].as_i64().filter(|c| *c >= 0);
    let mut kept_history = 0;
    while let Some(sid) = at {
        if kept_history == options.retain_last {
            break;
        }
        let Some(s) = by_id.get(&sid) else { break };
        keep.insert(sid);
        kept_history += 1;
        at = s["parent-snapshot-id"].as_i64();
    }
    if let Some(refs) = meta["refs"].as_object() {
        keep.extend(refs.values().filter_map(|r| r["snapshot-id"].as_i64()));
    }
    // A change feed's state (its order and tombstones) is found by walking back from the
    // current snapshot to the nearest that records it. Writes by other engines record
    // nothing, so every snapshot on that walk is kept: expiring one would cut the walk off,
    // and the next merge would find no tombstones at all.
    // The walk starts from every ref too: a tagged snapshot is one `main` can be rolled
    // back to, and its state must still be found then.
    let starts: Vec<i64> = meta["current-snapshot-id"]
        .as_i64()
        .filter(|c| *c >= 0)
        .into_iter()
        .chain(
            meta["refs"]
                .as_object()
                .into_iter()
                .flat_map(|r| r.values().filter_map(|r| r["snapshot-id"].as_i64())),
        )
        .collect();
    for start in starts {
        let mut at = Some(start);
        let mut walk = Vec::new();
        while let Some(sid) = at {
            let Some(s) = by_id.get(&sid) else { break };
            walk.push(sid);
            if !s["summary"][metadata::CHANGES_FORMAT_KEY].is_null() {
                keep.extend(walk.iter().copied());
                break;
            }
            at = s["parent-snapshot-id"].as_i64();
            if walk.len() > snapshots.len() {
                break;
            }
        }
    }
    for s in snapshots {
        if s["timestamp-ms"].as_i64().unwrap_or(i64::MAX) >= options.older_than_ms {
            keep.extend(id(s));
        }
    }
    // A snapshot without an id cannot be named in the new metadata's terms, so it is kept
    // (with everything it reads) rather than guessed at.
    let (retained, expired): (Vec<&Value>, Vec<&Value>) = snapshots
        .iter()
        .partition(|s| !matches!(id(s), Some(i) if !keep.contains(&i)));

    let mut summary = ExpireSummary {
        retained_snapshots: retained.len() as u64,
        ..ExpireSummary::default()
    };
    if expired.is_empty() {
        return Ok(ExpirePlan {
            commit: None,
            delete: Vec::new(),
            summary,
        });
    }
    let mut expired_sorted: Vec<&Value> = expired.clone();
    expired_sorted.sort_by_key(|s| s["sequence-number"].as_i64().unwrap_or(0));
    summary.expired_snapshots = expired_sorted.iter().filter_map(|s| id(s)).collect();

    let list_of = |s: &Value| -> Result<String> {
        s["manifest-list"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| err(format!("snapshot {:?} has no manifest-list", id(s))))
    };
    let retained_lists: Vec<String> = retained.iter().map(|s| list_of(s)).collect::<Result<_>>()?;
    let expired_lists: Vec<String> = expired_sorted
        .iter()
        .map(|s| list_of(s))
        .collect::<Result<_>>()?;

    // Every manifest list, then every manifest they name, read once and several at a time:
    // on an object store these reads are what planning costs. The plan is built from them
    // in table order, so it never depends on which read finished first.
    let lists = unique(
        retained_lists
            .iter()
            .chain(&expired_lists)
            .map(String::as_str),
    );
    let read = read_each(files, &lists, |l| read_manifest_list(&files.read(l)?))?;
    let manifest_lists: HashMap<&str, Vec<ManifestFileEntry>> =
        lists.iter().copied().zip(read).collect();
    let paths = unique(
        lists
            .iter()
            .flat_map(|l| manifest_lists[l].iter().map(|m| m.path.as_str())),
    );
    let read = read_each(files, &paths, |m| read_manifest_entries(&files.read(m)?))?;
    let manifests: HashMap<&str, Vec<ManifestEntry>> = paths.iter().copied().zip(read).collect();

    // What the retained snapshots need. A statistics file is named per snapshot, in the
    // table's `statistics` list: one a retained snapshot names stays.
    let statistics: Vec<(i64, String, Option<u64>)> = ["statistics", "partition-statistics"]
        .iter()
        .flat_map(|l| meta[*l].as_array().into_iter().flatten())
        .filter_map(|e| {
            Some((
                e["snapshot-id"].as_i64()?,
                e["statistics-path"].as_str()?.to_string(),
                e["file-size-in-bytes"].as_u64(),
            ))
        })
        .collect();
    let mut needed: HashSet<String> = statistics
        .iter()
        .filter(|(id, _, _)| keep.contains(id))
        .map(|(_, p, _)| p.clone())
        .collect();
    // A retained snapshot's summary names its tombstone file too, and is what a merge reads:
    // that keeps the file even when another engine replaced the snapshot's statistics entry
    // (an `ANALYZE` writes its own).
    let location = meta["location"].as_str().unwrap_or(base_location);
    for s in &retained {
        if let Some(key) = s["summary"][metadata::TOMBSTONES_KEY].as_str() {
            needed.insert(metadata::join_location(location, key));
        }
    }
    for list in &retained_lists {
        for m in &manifest_lists[list.as_str()] {
            let live = manifests[m.path.as_str()].iter().filter(|e| e.is_live());
            needed.extend(live.map(|e| e.file_path.clone()));
            needed.insert(m.path.clone());
        }
        needed.insert(list.clone());
    }

    // What the expired ones reference, minus that.
    let inside = |path: &str| anchors.holds(path) || under_root(path, base_location);
    let mut seen: HashSet<String> = HashSet::new();
    let mut delete: Vec<DeleteTarget> = Vec::new();
    let mut consider = |t: DeleteTarget, summary: &mut ExpireSummary| {
        if needed.contains(&t.path) || !seen.insert(t.path.clone()) {
            return;
        }
        if inside(&t.path) {
            delete.push(t);
        } else {
            summary.files_outside_table += 1;
        }
    };
    for list in &expired_lists {
        for m in &manifest_lists[list.as_str()] {
            for e in &manifests[m.path.as_str()] {
                let kind = if e.content == 0 {
                    FileKind::Data
                } else {
                    FileKind::Deletes
                };
                consider(
                    DeleteTarget {
                        path: e.file_path.clone(),
                        kind,
                        size: u64::try_from(e.file_size).ok(),
                    },
                    &mut summary,
                );
            }
            consider(
                DeleteTarget {
                    path: m.path.clone(),
                    kind: FileKind::Manifest,
                    size: u64::try_from(m.length).ok(),
                },
                &mut summary,
            );
        }
        consider(
            DeleteTarget {
                path: list.clone(),
                kind: FileKind::ManifestList,
                size: None,
            },
            &mut summary,
        );
    }
    // A tombstone file an expired snapshot names only in its summary (another engine replaced
    // its statistics entry) goes too, unless something retained still names it.
    for s in &expired_sorted {
        if let Some(key) = s["summary"][metadata::TOMBSTONES_KEY].as_str() {
            consider(
                DeleteTarget {
                    path: metadata::join_location(location, key),
                    kind: FileKind::Statistics,
                    size: s["summary"][metadata::TOMBSTONES_BYTES_KEY]
                        .as_str()
                        .and_then(|v| v.parse().ok()),
                },
                &mut summary,
            );
        }
    }
    for (id, path, size) in &statistics {
        if !keep.contains(id) {
            consider(
                DeleteTarget {
                    path: path.clone(),
                    kind: FileKind::Statistics,
                    size: *size,
                },
                &mut summary,
            );
        }
    }

    let version = table.version + 1;
    let metadata_value = metadata::remove_snapshots(&RemoveInput {
        prior: meta,
        base_location,
        prior_version: table.version,
        keep: &keep,
        timestamp_ms: now_ms,
    })?;
    let metadata_bytes = serde_json::to_vec_pretty(&metadata_value).map_err(err)?;
    if options.dry_run {
        for t in &delete {
            summary.count_deleted(t);
        }
    }
    Ok(ExpirePlan {
        commit: Some(ExpireCommit {
            version,
            metadata_key: format!("metadata/v{version}.metadata.json"),
            metadata_bytes,
            version_hint_key: "metadata/version-hint.text".to_string(),
            version_hint_bytes: version.to_string().into_bytes(),
        }),
        delete,
        summary,
    })
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use shapeshift_core::{Compression, Sink};

    use super::*;

    /// A local table read `readers` files at a time, counting each file's reads and the most
    /// in flight at once. A read takes a moment, as an object store's does, so reads that
    /// can overlap do.
    struct Counted<'a> {
        inner: &'a crate::LocalTableFiles,
        readers: usize,
        reads: Mutex<HashMap<String, usize>>,
        now: AtomicUsize,
        max: AtomicUsize,
    }

    impl TableFiles for Counted<'_> {
        fn read(&self, path: &str) -> Result<Vec<u8>> {
            *self.reads.lock().unwrap().entry(path.into()).or_default() += 1;
            let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(n, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(2));
            let got = self.inner.read(path);
            self.now.fetch_sub(1, Ordering::SeqCst);
            got
        }
        fn size(&self, _: &str) -> Result<u64> {
            unreachable!("planning reads whole files")
        }
        fn read_ranges(&self, _: &str, _: &[Range<u64>]) -> Result<Vec<bytes::Bytes>> {
            unreachable!("planning reads whole files")
        }
        fn readers(&self) -> usize {
            self.readers
        }
    }

    #[test]
    fn planning_reads_each_file_once_and_plans_the_same_however_many_at_once() {
        let dir = std::env::temp_dir().join(format!("ss-expire-readers-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let merge = |round: i64| {
            let ids: Vec<i64> = (0..200).map(|i| (i * 7 + round * 61) % 600).collect();
            let vs = vec![round; ids.len()];
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap();
            let mut sink = crate::IcebergSink::create_merge(
                &dir,
                schema.clone(),
                Compression::Snappy,
                &[],
                &["id".to_string()],
            )
            .unwrap();
            sink.write_batch(&batch).unwrap();
            sink.finish().unwrap();
        };
        // Merges of overlapping keys, a compaction, then more merges: snapshots that share
        // manifests, and files only the expired ones read.
        (0..6).for_each(merge);
        crate::compact_table(&dir, &crate::CompactOptions::default()).unwrap();
        (6..8).for_each(merge);

        let table = crate::load_current_local(&dir).unwrap();
        let location = table.metadata["location"].as_str().unwrap().to_string();
        let anchors = Anchors::of(&table.metadata);
        let local = crate::LocalTableFiles::new(&dir, anchors.clone());
        let options = ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 3,
            dry_run: true,
        };
        let run = |readers: usize| {
            let files = Counted {
                inner: &local,
                readers,
                reads: Mutex::default(),
                now: AtomicUsize::new(0),
                max: AtomicUsize::new(0),
            };
            let plan = plan_expiry(&table, &anchors, &location, &files, &options, 1).unwrap();
            let reads = files.reads.into_inner().unwrap();
            let again: Vec<_> = reads.iter().filter(|(_, &n)| n > 1).collect();
            assert!(again.is_empty(), "read more than once: {again:?}");
            let max = files.max.into_inner();
            assert!(max <= readers, "{max} reads in flight, {readers} readers");
            let delete: Vec<(String, FileKind, Option<u64>)> = plan
                .delete
                .iter()
                .map(|t| (t.path.clone(), t.kind, t.size))
                .collect();
            let commit = plan.commit.map(|c| (c.version, c.metadata_bytes));
            ((delete, plan.summary, commit, reads.len()), max)
        };
        let (one, _) = run(1);
        let (delete, summary, _, files_read) = &one;
        assert_eq!(summary.expired_snapshots.len(), 6);
        assert!(summary.data_files_deleted > 0 && summary.delete_files_deleted > 0);
        // Every manifest list and manifest in the table (no snapshot has expired yet, so
        // that is every Avro file under metadata/), and nothing else.
        let avro = std::fs::read_dir(dir.join("metadata"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "avro")
            })
            .count();
        assert_eq!(*files_read, avro);
        let s = summary;
        let counted = s.data_files_deleted
            + s.delete_files_deleted
            + s.manifests_deleted
            + s.manifest_lists_deleted;
        assert_eq!(delete.len() as u64, counted);
        for readers in [2, 3, 8, 64] {
            let (got, max) = run(readers);
            assert_eq!(got, one, "{readers} readers");
            assert!(max > 1, "{readers} readers, but the reads never overlapped");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_file_under_no_anchor_is_never_deleted() {
        // Planned with anchors that hold none of the table's recorded paths (as a table whose
        // files live under another engine's `write.data.path` is), every file the expired
        // snapshots read is outside the table: counted, and kept.
        let dir = std::env::temp_dir().join(format!("ss-expire-outside-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        for round in 0..3_i64 {
            let ids = Int64Array::from(vec![round, round + 1]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
            let mut sink = crate::IcebergSink::create_merge(
                &dir,
                schema.clone(),
                Compression::Snappy,
                &[],
                &["id".to_string()],
            )
            .unwrap();
            sink.write_batch(&batch).unwrap();
            sink.finish().unwrap();
        }
        let table = crate::load_current_local(&dir).unwrap();
        let own = Anchors::of(&table.metadata);
        let local = crate::LocalTableFiles::new(&dir, own.clone());
        let options = ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
            dry_run: true,
        };
        let elsewhere = Anchors::at("/somewhere/else");
        let plan = plan_expiry(&table, &elsewhere, "/elsewhere/too", &local, &options, 1).unwrap();
        assert!(plan.delete.is_empty(), "{:?}", plan.delete);
        assert!(plan.summary.files_outside_table > 0, "{:?}", plan.summary);
        // The same plan with the table's own anchors deletes them.
        let plan = plan_expiry(&table, &own, "/elsewhere/too", &local, &options, 1).unwrap();
        assert!(!plan.delete.is_empty());
        assert_eq!(plan.summary.files_outside_table, 0, "{:?}", plan.summary);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn only_paths_that_stay_under_the_root_are_inside() {
        let root = "s3://lake/t";
        assert!(under_root("s3://lake/t/data/a.parquet", root));
        assert!(under_root("s3://lake/t/data/a.parquet", "s3://lake/t/"));
        assert!(under_root("/w/t/metadata/snap-1.avro", "/w/t"));
        // Another table that shares a prefix, and the root itself, are not inside.
        assert!(!under_root("s3://lake/t2/data/a.parquet", root));
        assert!(!under_root("s3://lake/t", root));
        // A path that climbs back out of the root is not inside, however it starts.
        assert!(!under_root("s3://lake/t/data/../../other/x.parquet", root));
        assert!(!under_root("/w/t/../u/data/x.parquet", "/w/t"));
        assert!(!under_root("s3://lake/t/./data/a.parquet", root));
        assert!(!under_root("s3://lake/t/data/..", root));
        assert!(!under_root("anything", ""));
    }
}
