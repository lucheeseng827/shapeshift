//! Relocating a table (RELOCATE-PLAN §2–§5): rewrite what it records so that every path names
//! where the table is now, and any Iceberg reader opens it with no flag.
//!
//! - **Copying stays the user's job.** The table's files are already where it is (any tool
//!   copied or moved them); relocation never copies a data file.
//! - **The whole history moves.** Every snapshot keeps its id, sequence number, timestamp and
//!   summary, and no snapshot is added.
//! - **What is rewritten** is what Iceberg's own `RewriteTablePath` rewrites: each position
//!   delete file whose rows name a moved data file (its paths replaced, re-sorted), each
//!   manifest and manifest list that names a moved file (transcoded: decoded against its own
//!   schema and written back with every other field as the engine wrote it), and the metadata
//!   (`location`, every snapshot's manifest list, the `metadata-log`, statistics files, the
//!   table's own path properties). Data files are neither read nor written.
//! - **Nothing is overwritten.** Rewritten files get new names beside the old ones; one new
//!   metadata version names them, through the same commit guard as every write. The files
//!   they supersede are removed after the commit.
//! - **It checks first.** Every file a snapshot reads must be where the table is now, by one
//!   listing; an incomplete copy fails before anything is written, naming what is missing.
//! - **Running it again does nothing**: a table already anchored where it is commits nothing.
//!
//! Paths are found through the table's [`Anchors`] (every place it has been written at), so
//! a table moved several times, written to in between, relocates in one run.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};
use uuid::Uuid;

use shapeshift_core::{Compression, Result, ShapeError};

use crate::anchor::{is_elsewhere, Anchors};
use crate::avro::{read_manifest_entries, read_manifest_list, Container, Datum, ManifestEntry};
use crate::files::{batch_err, read_each, RangedParquet, TableFiles};
use crate::metadata::join_location;
use crate::{metadata_key, PriorTable};

/// The reserved field-ids of a position delete file's columns.
const DELETE_FILE_PATH_ID: i32 = 2_147_483_546;
const DELETE_POS_ID: i32 = 2_147_483_545;

/// Table properties that name a place in the table, rewritten with its paths.
const PATH_PROPERTIES: [&str; 4] = [
    "write.data.path",
    "write.metadata.path",
    "write.folder-storage.path",
    "write.object-storage.path",
];

/// How many missing files an incomplete copy's error names.
const MISSING_SHOWN: usize = 5;

fn err(msg: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(msg.to_string())
}

/// What a relocated table is to the one it was copied or moved from (RELOCATE-PLAN §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity {
    /// The original is being retired: the table keeps its `table-uuid`.
    Moved,
    /// Both tables live on: the relocated one gets a new `table-uuid`, so a catalog's
    /// `assert-table-uuid` and anyone reading the two histories can tell them apart.
    Copy,
}

/// What is found at a place the table was written at, other than where it is now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OldPlace {
    /// No table there: the table was moved away from it.
    Gone,
    /// A table's metadata is still there.
    Answers,
    /// It could not be checked (another store, no credentials for it, an I/O error), why.
    Unknown(String),
}

/// The identity a relocation gives the table: the one asked for, or, when none was, a move
/// if no table answers at any place it was written at. Anything else is not guessed (a
/// wrong guess gives two tables one identity): the error says why, and what to pass.
pub fn decide_identity(
    asked: Option<Identity>,
    places: &[(String, OldPlace)],
) -> std::result::Result<Identity, String> {
    if let Some(i) = asked {
        return Ok(i);
    }
    for (root, place) in places {
        match place {
            OldPlace::Gone => {}
            OldPlace::Answers => {
                return Err(format!(
                    "a table still answers at {root}, where this one was written"
                ))
            }
            OldPlace::Unknown(why) => {
                return Err(format!(
                    "whether a table still answers at {root}, where this one was written, \
                     could not be checked ({why})"
                ))
            }
        }
    }
    Ok(Identity::Moved)
}

/// The error for an identity [`decide_identity`] would not guess.
pub fn undecided_error(why: &str) -> ShapeError {
    err(format!(
        "{why}. Pass --moved if the original is being retired (this table keeps its \
         table-uuid), or --copy if both live on (this one gets a new table-uuid); guessing \
         could give two tables one identity"
    ))
}

/// What `relocate` is asked to do.
#[derive(Debug, Clone, Copy, Default)]
pub struct RelocateOptions {
    /// `None`: decide by looking at the places the table was written at (see
    /// [`decide_identity`]).
    pub identity: Option<Identity>,
    /// Plan and report, but write and delete nothing.
    pub dry_run: bool,
}

/// What a relocation did (or, in a dry run, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelocateSummary {
    /// The `location` the metadata recorded.
    pub from: String,
    /// Where the table is now, which every path names afterwards.
    pub to: String,
    /// The `table-uuid` afterwards (in a dry run, the one it has now).
    pub table_uuid: String,
    /// The table was given a new `table-uuid` (a copy), or in a dry run would be.
    pub new_uuid: bool,
    /// In a dry run whose identity could not be decided: why a real run needs `--moved` or
    /// `--copy`. (A real run fails instead.)
    pub undecided: Option<String>,
    /// Snapshots kept, every one of them.
    pub snapshots: u64,
    pub manifest_lists_rewritten: u64,
    pub manifests_rewritten: u64,
    pub delete_files_rewritten: u64,
    /// Data files the snapshots read: never rewritten.
    pub data_files: u64,
    /// Files the table records under no place it was written at (another engine's
    /// `write.data.path`): left where they are, and counted.
    pub files_outside_table: u64,
    /// Rewritten files' old copies where the table is now, which nothing names afterwards.
    pub superseded: u64,
    pub superseded_deleted: u64,
    pub superseded_not_deleted: u64,
    /// The metadata file the table names afterwards, where it is: the one a catalog
    /// registers (the current one when nothing was committed).
    pub metadata: String,
    /// Every path already names where the table is: nothing to do.
    pub already_anchored: bool,
    pub committed: bool,
    pub version: Option<u64>,
}

/// What a relocation plan is made from.
pub struct RelocateContext<'a> {
    /// The table's current metadata and version (its `manifests` are not used).
    pub table: &'a PriorTable,
    /// The places its recorded paths were written at ([`Anchors::of`] the metadata).
    pub anchors: &'a Anchors,
    /// Where the table is now, as its paths will record it.
    pub here: &'a str,
    /// The keys (relative to the table's root, `/`-separated) of every file under it: one
    /// listing, against which every file a snapshot reads is checked.
    pub listing: &'a HashSet<String>,
    /// Where rewritten files are written, at their keys: the table's own directory for a
    /// local table, a staging directory for an object store.
    pub staging_dir: &'a Path,
    pub new_uuid: bool,
    pub dry_run: bool,
    /// Recorded as the metadata's `last-updated-ms`.
    pub now_ms: i64,
}

/// A relocation, planned: what to commit, if anything.
pub struct RelocatePlan {
    pub summary: RelocateSummary,
    /// `None` in a dry run, or when the table is already anchored where it is.
    pub commit: Option<RelocateCommit>,
}

#[cfg_attr(test, derive(Clone))]
pub struct RelocateCommit {
    pub version: u64,
    pub metadata_bytes: Vec<u8>,
    /// The keys written under the staging directory: uploaded before the commit (an object
    /// store), and removed if the commit is refused.
    pub written: Vec<String>,
    /// The keys of the superseded files, removed after the commit.
    pub superseded: Vec<String>,
}

/// A position delete file, rewritten: what its manifest entries record afterwards.
#[derive(Debug, Clone)]
struct Rewritten {
    path: String,
    size: i64,
    /// The rewritten `file_path` column's bounds.
    lower: Vec<u8>,
    upper: Vec<u8>,
    /// Compressed bytes per leaf field-id, as `column_sizes` records them.
    column_sizes: Vec<(i32, i64)>,
    /// Where each row group starts, as `split_offsets` records them.
    split_offsets: Vec<i64>,
}

/// The check of every file a snapshot reads against the listing.
struct Check<'a> {
    anchors: &'a Anchors,
    listing: &'a HashSet<String>,
    here: &'a str,
    outside: BTreeSet<String>,
    missing: Vec<String>,
}

impl Check<'_> {
    fn need(&mut self, path: &str) -> Result<()> {
        match self.anchors.key(path)? {
            Some(key) if !self.listing.contains(key) => self.missing.push(path.to_string()),
            Some(_) => {}
            None => {
                self.outside.insert(path.to_string());
            }
        }
        Ok(())
    }

    /// Fail, before anything is written, if a file is missing.
    fn complete(&mut self) -> Result<()> {
        if self.missing.is_empty() {
            return Ok(());
        }
        self.missing.sort();
        self.missing.dedup();
        let n = self.missing.len();
        let shown: Vec<&str> = self
            .missing
            .iter()
            .take(MISSING_SHOWN)
            .map(String::as_str)
            .collect();
        let more = if n > MISSING_SHOWN {
            format!(" and {} more", n - MISSING_SHOWN)
        } else {
            String::new()
        };
        Err(err(format!(
            "the copy at {} is incomplete: {n} file(s) the table's snapshots read are not \
             there ({}{more}). Copy them, then relocate again; nothing was written",
            self.here,
            shown.join(", ")
        )))
    }
}

/// Plan the relocation of `cx.table` to `cx.here`, reading its files through `files`, and
/// writing the rewritten ones under `cx.staging_dir` (nothing in a dry run). If planning
/// fails, whatever it wrote is removed.
pub fn plan_relocation<F: TableFiles>(cx: &RelocateContext, files: &F) -> Result<RelocatePlan> {
    let mut written = Vec::new();
    let planned = plan(cx, files, &mut written);
    if planned.is_err() {
        for key in &written {
            let _ = std::fs::remove_file(cx.staging_dir.join(key));
        }
    }
    planned
}

fn plan<F: TableFiles>(
    cx: &RelocateContext,
    files: &F,
    written: &mut Vec<String>,
) -> Result<RelocatePlan> {
    let meta = &cx.table.metadata;
    let version = meta["format-version"].as_i64().unwrap_or(0);
    if version != 2 {
        return Err(err(format!(
            "relocate rewrites format version 2 tables; this one is version {version}"
        )));
    }
    let here = cx.here.trim_end_matches('/');
    let anchors = cx.anchors;
    let from = meta["location"].as_str().unwrap_or("").to_string();
    let mut summary = RelocateSummary {
        from: from.clone(),
        to: here.to_string(),
        new_uuid: cx.new_uuid,
        ..RelocateSummary::default()
    };
    // A path as it will be recorded: at its key where the table is now, or as it was for a
    // file outside the table.
    let moved = |p: &str| -> Result<String> {
        Ok(match anchors.key(p)? {
            Some(key) => join_location(here, key),
            None => p.to_string(),
        })
    };
    let mut check = Check {
        anchors,
        listing: cx.listing,
        here,
        outside: BTreeSet::new(),
        missing: Vec::new(),
    };

    // The manifest lists, then the manifests, then every file a snapshot reads: each level
    // checked against the listing before it is read, so an incomplete copy fails on what is
    // missing rather than on a read.
    let snapshots: Vec<&Value> = meta["snapshots"]
        .as_array()
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    summary.snapshots = snapshots.len() as u64;
    let mut lists: Vec<&str> = Vec::new();
    let mut list_snapshot: HashMap<&str, i64> = HashMap::new();
    for s in &snapshots {
        let list = s["manifest-list"]
            .as_str()
            .ok_or_else(|| err("a snapshot has no manifest-list"))?;
        if !list_snapshot.contains_key(list) {
            lists.push(list);
            list_snapshot.insert(list, s["snapshot-id"].as_i64().unwrap_or(0));
        }
    }
    for l in &lists {
        check.need(l)?;
    }
    check.complete()?;
    let list_bytes = read_each(files, &lists, |l| files.read(l))?;
    let mut manifests: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for bytes in &list_bytes {
        for m in read_manifest_list(bytes)? {
            if seen.insert(m.path.clone()) {
                manifests.push(m.path);
            }
        }
    }
    for m in &manifests {
        check.need(m)?;
    }
    check.complete()?;
    let manifest_refs: Vec<&str> = manifests.iter().map(String::as_str).collect();
    let manifest_bytes = read_each(files, &manifest_refs, |m| files.read(m))?;
    let entries: Vec<Vec<ManifestEntry>> = manifest_bytes
        .iter()
        .map(|b| read_manifest_entries(b))
        .collect::<Result<_>>()?;
    let mut data_files: BTreeSet<&str> = BTreeSet::new();
    let mut live_deletes: BTreeMap<&str, i64> = BTreeMap::new();
    for e in entries.iter().flatten().filter(|e| e.is_live()) {
        check.need(&e.file_path)?;
        match e.content {
            0 => {
                data_files.insert(&e.file_path);
            }
            1 => {
                live_deletes.insert(&e.file_path, e.file_size);
            }
            _ => {}
        }
    }
    // The statistics files the metadata names go with the table too.
    for list in ["statistics", "partition-statistics"] {
        for e in meta[list].as_array().into_iter().flatten() {
            if let Some(path) = e["statistics-path"].as_str() {
                check.need(path)?;
            }
        }
    }
    check.complete()?;
    summary.data_files = data_files.len() as u64;
    summary.files_outside_table = check.outside.len() as u64;

    let run = Uuid::new_v4().simple().to_string();
    let mut superseded: Vec<String> = Vec::new();
    let supersede = |old: &str, superseded: &mut Vec<String>| -> Result<()> {
        if let Some(key) = anchors.key(old)? {
            if cx.listing.contains(key) {
                superseded.push(key.to_string());
            }
        }
        Ok(())
    };
    let write = |key: String, bytes: &[u8], written: &mut Vec<String>| -> Result<String> {
        if !cx.dry_run {
            let path = cx.staging_dir.join(&key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(err)?;
            }
            written.push(key.clone());
            std::fs::write(&path, bytes).map_err(err)?;
        }
        Ok(join_location(here, &key))
    };

    // Position delete files whose rows name a moved data file.
    let mut deletes: HashMap<&str, Rewritten> = HashMap::new();
    for (n, (&old, &size)) in live_deletes.iter().enumerate() {
        let dir = match anchors.key(old)? {
            Some(key) => key.rsplit_once('/').map_or("", |(dir, _)| dir),
            None => "data",
        };
        let key = match dir {
            "" => format!("{run}-{n}-deletes.parquet"),
            dir => format!("{dir}/{run}-{n}-deletes.parquet"),
        };
        if let Some(rw) = rewrite_deletes(cx, files, old, size, &moved, key, here, written)? {
            supersede(old, &mut superseded)?;
            deletes.insert(old, rw);
        }
    }
    summary.delete_files_rewritten = deletes.len() as u64;

    // Manifests naming a moved file, transcoded. Every manifest maps to where it is recorded
    // afterwards, and to its new length if it was rewritten.
    let mut new_manifests: HashMap<&str, (String, Option<i64>)> = HashMap::new();
    for (i, (m, bytes)) in manifest_refs.iter().zip(&manifest_bytes).enumerate() {
        let mut c = Container::read(bytes)?;
        if edit_manifest(&mut c, &moved, &deletes)? {
            summary.manifests_rewritten += 1;
            supersede(m, &mut superseded)?;
            let out = c.write()?;
            let path = write(format!("metadata/{run}-m{i}.avro"), &out, written)?;
            new_manifests.insert(m, (path, Some(out.len() as i64)));
        } else {
            new_manifests.insert(m, (moved(m)?, None));
        }
    }

    // Manifest lists naming a moved or rewritten manifest, transcoded the same way.
    let mut new_lists: HashMap<&str, String> = HashMap::new();
    for (i, (l, bytes)) in lists.iter().zip(&list_bytes).enumerate() {
        let mut c = Container::read(bytes)?;
        let mut changed = false;
        for rec in &mut c.records {
            let old = str_field(rec, "manifest_path")?;
            let (new, length) = new_manifests
                .get(old.as_str())
                .ok_or_else(|| err(format!("internal error: manifest {old} was not planned")))?;
            if *new != old {
                set(rec, "manifest_path", Datum::Str(new.clone()));
                changed = true;
            }
            if let Some(length) = length {
                set(rec, "manifest_length", Datum::Long(*length));
                changed = true;
            }
        }
        if changed {
            summary.manifest_lists_rewritten += 1;
            supersede(l, &mut superseded)?;
            let out = c.write()?;
            let key = format!("metadata/snap-{}-{run}-{i}.avro", list_snapshot[l]);
            new_lists.insert(l, write(key, &out, written)?);
        } else {
            new_lists.insert(l, moved(l)?);
        }
    }

    // The metadata: one new version naming all of it.
    let mut m = meta.clone();
    // Only a path that moves makes a relocation: a copy's new identity comes with one, never
    // on its own (`--copy` run twice must not mint two).
    let mut changed = is_elsewhere(&from, here);
    m["location"] = json!(here);
    if let Some(snaps) = m.get_mut("snapshots").and_then(Value::as_array_mut) {
        for s in snaps {
            let old = s["manifest-list"].as_str().unwrap_or("");
            let new = &new_lists[old];
            if new != old {
                s["manifest-list"] = json!(new);
                changed = true;
            }
        }
    }
    let mut repoint = |v: &mut Value, key: &str| -> Result<()> {
        if let Some(old) = v[key].as_str() {
            let new = moved(old)?;
            if new != old {
                v[key] = json!(new);
                changed = true;
            }
        }
        Ok(())
    };
    // `get_mut`, not indexing: indexing a missing key would add it as `null`, which readers
    // refuse where they expect a list.
    let list_mut = |m: &mut Value, key: &str| {
        m.get_mut(key)
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
    };
    if let Some(mut log) = list_mut(&mut m, "metadata-log") {
        for e in &mut log {
            repoint(e, "metadata-file")?;
        }
        m["metadata-log"] = Value::Array(log);
    }
    for list in ["statistics", "partition-statistics"] {
        if let Some(mut entries) = list_mut(&mut m, list) {
            for e in &mut entries {
                repoint(e, "statistics-path")?;
            }
            m[list] = Value::Array(entries);
        }
    }
    if let Some(props) = m.get_mut("properties").and_then(Value::as_object_mut) {
        for key in PATH_PROPERTIES {
            if let Some(Value::String(old)) = props.get(key).cloned() {
                let trimmed = old.trim_end_matches('/');
                let new = if anchors.roots().iter().any(|r| r == trimmed) {
                    here.to_string()
                } else {
                    moved(trimmed)?
                };
                if new != trimmed {
                    props.insert(key.to_string(), json!(new));
                    changed = true;
                }
            }
        }
    }
    summary.superseded = superseded.len() as u64;
    summary.metadata = join_location(here, &metadata_key(cx.table.version));
    summary.table_uuid = meta["table-uuid"].as_str().unwrap_or("").to_string();
    if !changed {
        // Nothing moves, so no identity is minted either.
        summary.already_anchored = true;
        summary.new_uuid = false;
        return Ok(RelocatePlan {
            summary,
            commit: None,
        });
    }
    // A dry run says a copy would get a new `table-uuid`, and reports the one it has now.
    if cx.dry_run {
        return Ok(RelocatePlan {
            summary,
            commit: None,
        });
    }
    if cx.new_uuid {
        m["table-uuid"] = json!(Uuid::new_v4().to_string());
        summary.table_uuid = m["table-uuid"].as_str().unwrap_or("").to_string();
    }
    let prior_ts = meta["last-updated-ms"].as_i64().unwrap_or(cx.now_ms);
    m["last-updated-ms"] = json!(cx.now_ms);
    let prior_file = join_location(here, &metadata_key(cx.table.version));
    let entry = json!({ "timestamp-ms": prior_ts, "metadata-file": prior_file });
    match m["metadata-log"].as_array_mut() {
        Some(log) => log.push(entry),
        None => m["metadata-log"] = json!([entry]),
    }
    summary.metadata = join_location(here, &metadata_key(cx.table.version + 1));
    Ok(RelocatePlan {
        summary,
        commit: Some(RelocateCommit {
            version: cx.table.version + 1,
            metadata_bytes: serde_json::to_vec_pretty(&m).map_err(err)?,
            written: written.clone(),
            superseded,
        }),
    })
}

/// A record's string field.
fn str_field(rec: &Datum, name: &str) -> Result<String> {
    match rec.field(name).map(Datum::inner) {
        Some(Datum::Str(s)) => Ok(s.clone()),
        _ => Err(err(format!("a manifest record has no string `{name}`"))),
    }
}

/// Set a record's field, inside its union branch if it has one (only a present value is
/// replaced: a null stays null).
fn set(rec: &mut Datum, name: &str, value: Datum) {
    if let Some(v) = rec.field_mut(name).map(Datum::inner_mut) {
        if *v != Datum::Null {
            *v = value;
        }
    }
}

/// The entries of an Iceberg `map<int, X>` field (an array of `key`/`value` records).
fn int_map_mut<'a>(
    rec: &'a mut Datum,
    name: &str,
) -> impl Iterator<Item = (i32, &'a mut Datum)> + 'a {
    let items = match rec.field_mut(name).map(Datum::inner_mut) {
        Some(Datum::Array(items)) => items.as_mut_slice(),
        _ => &mut [],
    };
    items.iter_mut().filter_map(|item| {
        let key = match item.field("key").map(Datum::inner) {
            Some(Datum::Int(k)) => *k,
            _ => return None,
        };
        item.field_mut("value").map(|v| (key, v.inner_mut()))
    })
}

/// Point a manifest's entries at where their files are recorded afterwards: every path
/// moved, and a rewritten delete file's path, size, `file_path` bounds, column sizes and
/// split offsets those of its new file. Whether anything changed.
fn edit_manifest(
    c: &mut Container,
    moved: &impl Fn(&str) -> Result<String>,
    deletes: &HashMap<&str, Rewritten>,
) -> Result<bool> {
    let mut changed = false;
    for rec in &mut c.records {
        let df = rec
            .field_mut("data_file")
            .ok_or_else(|| err("a manifest entry has no data_file"))?;
        let old = str_field(df, "file_path")?;
        if let Some(rw) = deletes.get(old.as_str()) {
            set(df, "file_path", Datum::Str(rw.path.clone()));
            set(df, "file_size_in_bytes", Datum::Long(rw.size));
            for (name, bound) in [("lower_bounds", &rw.lower), ("upper_bounds", &rw.upper)] {
                for (key, v) in int_map_mut(df, name) {
                    if key == DELETE_FILE_PATH_ID {
                        *v = Datum::Bytes(bound.clone());
                    }
                }
            }
            for (key, v) in int_map_mut(df, "column_sizes") {
                if let Some((_, size)) = rw.column_sizes.iter().find(|(id, _)| *id == key) {
                    *v = Datum::Long(*size);
                }
            }
            if let Some(Datum::Array(offsets)) = df.field_mut("split_offsets").map(Datum::inner_mut)
            {
                *offsets = rw.split_offsets.iter().map(|o| Datum::Long(*o)).collect();
            }
            changed = true;
        } else {
            let new = moved(&old)?;
            if new != old {
                set(df, "file_path", Datum::Str(new));
                changed = true;
            }
        }
        // A delete file that applies to one data file names it (Iceberg 1.8+).
        if let Some(Datum::Str(r)) = df.field_mut("referenced_data_file").map(Datum::inner_mut) {
            let new = moved(r)?;
            if new != *r {
                *r = new;
                changed = true;
            }
        }
    }
    Ok(changed)
}

/// Rewrite the position delete file at `old` if any of its rows names a moved data file:
/// every row's `file_path` replaced, the rows sorted by path and position again (the spec's
/// order, which replacing a prefix can change), any other column (Spark's optional `row`)
/// carried with its row. `None` if no row's path moves.
#[allow(clippy::too_many_arguments)]
fn rewrite_deletes<F: TableFiles>(
    cx: &RelocateContext,
    files: &F,
    old: &str,
    size: i64,
    moved: &impl Fn(&str) -> Result<String>,
    key: String,
    here: &str,
    written: &mut Vec<String>,
) -> Result<Option<Rewritten>> {
    let pq = RangedParquet::open(files, old, u64::try_from(size).ok())?;
    let schema = pq.schema().clone();
    let column = |id: i32, name: &str| {
        let fields = schema.fields();
        fields
            .iter()
            .position(|f| {
                f.metadata().get("PARQUET:field_id").map(String::as_str)
                    == Some(id.to_string().as_str())
            })
            .or_else(|| fields.iter().position(|f| f.name() == name))
            .ok_or_else(|| err(format!("{old}: not a position-delete file")))
    };
    let (path_col, pos_col) = (
        column(DELETE_FILE_PATH_ID, "file_path")?,
        column(DELETE_POS_ID, "pos")?,
    );
    let all = ProjectionMask::all();
    let mut batches = Vec::new();
    for rg in 0..pq.num_row_groups() {
        for b in pq.row_group(rg, &all)? {
            batches.push(b.map_err(|e| batch_err(old, e))?);
        }
    }
    // Each row: its new path, its position, and where it is among all the rows.
    let mut rows: Vec<(String, i64, u32)> = Vec::new();
    let mut changed = false;
    for b in &batches {
        let paths = b
            .column(path_col)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| err(format!("{old}: file_path is not a string column")))?;
        let positions = b
            .column(pos_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err(format!("{old}: pos is not a long column")))?;
        for i in 0..b.num_rows() {
            let path = paths.value(i);
            let new = moved(path)?;
            changed |= new != path;
            let at = u32::try_from(rows.len()).map_err(|_| err(format!("{old}: too many rows")))?;
            rows.push((new, positions.value(i), at));
        }
    }
    if !changed {
        return Ok(None);
    }
    rows.sort_by(|a, b| (a.0.as_str(), a.1).cmp(&(b.0.as_str(), b.1)));
    let path = join_location(here, &key);
    let (lower, upper) = (
        rows[0].0.as_bytes().to_vec(),
        rows[rows.len() - 1].0.as_bytes().to_vec(),
    );
    if cx.dry_run {
        return Ok(Some(Rewritten {
            path,
            size: 0,
            lower,
            upper,
            column_sizes: Vec::new(),
            split_offsets: Vec::new(),
        }));
    }
    let all_rows = arrow_select::concat::concat_batches(&schema, &batches).map_err(err)?;
    let order = UInt32Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        columns.push(if i == path_col {
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            ))
        } else if i == pos_col {
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            ))
        } else {
            arrow_select::take::take(all_rows.column(i), &order, None).map_err(err)?
        });
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(err)?;

    // The file's own key-value metadata (an engine's `iceberg.schema`, say) goes with it;
    // the writer records the Arrow schema itself.
    let kv: Vec<KeyValue> = pq
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|kv| kv.key != "ARROW:schema")
        .cloned()
        .collect();
    let props = WriterProperties::builder()
        .set_compression(crate::pq_compression(Compression::Snappy)?)
        .set_key_value_metadata((!kv.is_empty()).then_some(kv))
        .build();
    let local = cx.staging_dir.join(&key);
    if let Some(parent) = local.parent() {
        std::fs::create_dir_all(parent).map_err(err)?;
    }
    written.push(key);
    let file = std::fs::File::create(&local).map_err(err)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(err)?;
    writer.write(&batch).map_err(err)?;
    writer.close().map_err(err)?;

    // What the manifest records of the new file: its size, and, read back from its footer,
    // each leaf column's compressed size and where each row group starts.
    let size = std::fs::metadata(&local).map_err(err)?.len() as i64;
    let reader =
        parquet::file::reader::SerializedFileReader::new(std::fs::File::open(&local).map_err(err)?)
            .map_err(err)?;
    let footer = parquet::file::reader::FileReader::metadata(&reader);
    let mut column_sizes: BTreeMap<i32, i64> = BTreeMap::new();
    let mut split_offsets = Vec::new();
    for rg in footer.row_groups() {
        for c in rg.columns() {
            let info = c.column_descr().self_type().get_basic_info();
            if info.has_id() {
                *column_sizes.entry(info.id()).or_default() += c.compressed_size();
            }
        }
        let first = rg.column(0);
        split_offsets.push(rg.file_offset().unwrap_or_else(|| {
            first
                .dictionary_page_offset()
                .unwrap_or(first.data_page_offset())
        }));
    }
    Ok(Some(Rewritten {
        path,
        size,
        lower,
        upper,
        column_sizes: column_sizes.into_iter().collect(),
        split_offsets,
    }))
}

/// The keys of every file under a local table, `/`-separated. A symlinked directory (a
/// table's `data/` on another disk) is followed, each directory once, so a link loop ends.
fn list_local(table_dir: &Path) -> Result<HashSet<String>> {
    fn walk(
        root: &Path,
        at: &Path,
        seen: &mut HashSet<std::path::PathBuf>,
        out: &mut HashSet<String>,
    ) -> std::io::Result<()> {
        if !seen.insert(at.canonicalize()?) {
            return Ok(());
        }
        for e in std::fs::read_dir(at)? {
            let p = e?.path();
            if p.is_dir() {
                walk(root, &p, seen, out)?;
            } else if let Ok(rel) = p.strip_prefix(root) {
                let key: Vec<String> = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect();
                out.insert(key.join("/"));
            }
        }
        Ok(())
    }
    let mut out = HashSet::new();
    walk(table_dir, table_dir, &mut HashSet::new(), &mut out).map_err(err)?;
    Ok(out)
}

/// Whether a table answers at `root` on this machine: its `metadata/` holds a version hint
/// or a metadata file.
pub fn local_old_place(root: &str) -> OldPlace {
    let path = root
        .strip_prefix("file://")
        .or_else(|| root.strip_prefix("file:"))
        .unwrap_or(root);
    if !path.starts_with('/') {
        return OldPlace::Unknown("it is not a path on this machine".into());
    }
    match std::fs::read_dir(Path::new(path).join("metadata")) {
        Ok(entries) => {
            let answers = entries.flatten().any(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name == "version-hint.text" || name.ends_with(".metadata.json")
            });
            if answers {
                OldPlace::Answers
            } else {
                OldPlace::Gone
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => OldPlace::Gone,
        Err(e) => OldPlace::Unknown(e.to_string()),
    }
}

/// Relocate the local table at `table_dir` to where it is (see the module docs): the
/// rewritten files are written under it at new names, then the new metadata version through
/// the commit guard, the version hint last; then the files they supersede are removed. A
/// commit another writer beats is a conflict, and leaves nothing behind.
pub fn relocate_table(
    table_dir: impl AsRef<Path>,
    options: &RelocateOptions,
) -> Result<RelocateSummary> {
    let table_dir = table_dir.as_ref();
    let version = crate::local_version(table_dir)?.ok_or_else(|| {
        err(format!(
            "no Iceberg table at {} (no version-hint.text or metadata)",
            table_dir.display()
        ))
    })?;
    let text = std::fs::read_to_string(table_dir.join(metadata_key(version))).map_err(err)?;
    let metadata: Value = serde_json::from_str(&text).map_err(err)?;
    // A table whose paths are `file:` URIs keeps them so.
    let at = crate::abspath(table_dir);
    let here = match metadata["location"].as_str() {
        Some(l) if l.starts_with("file:") => format!("file://{at}"),
        _ => at,
    };
    let anchors = Anchors::of(&metadata);
    // Looked at only when no identity was given: `--moved` or `--copy` probes nothing.
    let places: Vec<(String, OldPlace)> = anchors
        .roots()
        .iter()
        .filter(|root| options.identity.is_none() && is_elsewhere(root, &here))
        .map(|root| (root.clone(), local_old_place(root)))
        .collect();
    let (identity, undecided) = match decide_identity(options.identity, &places) {
        Ok(i) => (i, None),
        Err(why) if options.dry_run => (Identity::Moved, Some(why)),
        Err(why) => return Err(undecided_error(&why)),
    };
    let listing = list_local(table_dir)?;
    let files = crate::LocalTableFiles::new(table_dir, anchors.clone());
    let table = PriorTable {
        metadata,
        version,
        manifests: Vec::new(),
    };
    let plan = plan_relocation(
        &RelocateContext {
            table: &table,
            anchors: &anchors,
            here: &here,
            listing: &listing,
            staging_dir: table_dir,
            new_uuid: identity == Identity::Copy,
            dry_run: options.dry_run,
            now_ms: crate::snapshot_timestamp_ms(),
        },
        &files,
    )?;
    let mut summary = plan.summary;
    summary.undecided = undecided;
    if let Some(commit) = plan.commit {
        commit_relocation(table_dir, version, commit, &mut summary)?;
    }
    Ok(summary)
}

/// Publish a planned relocation of the local table at `table_dir`, planned against version
/// `base`: the metadata through the commit guard, the version hint, then the superseded
/// files removed. A commit another writer beat removes what the plan wrote, and is a
/// conflict.
fn commit_relocation(
    table_dir: &Path,
    base: u64,
    commit: RelocateCommit,
    summary: &mut RelocateSummary,
) -> Result<()> {
    if let Err(e) = crate::publish_metadata_local(table_dir, commit.version, &commit.metadata_bytes)
    {
        for key in &commit.written {
            let _ = std::fs::remove_file(table_dir.join(key));
        }
        return Err(match crate::local_version(table_dir) {
            Ok(now) => crate::moved_on(e, base, now),
            Err(_) => e,
        });
    }
    if let Err(e) = crate::publish_hint_local(table_dir, commit.version) {
        tracing::warn!(version = commit.version, error = %e,
            "relocated, but version-hint.text was not updated; readers find the commit past it");
    }
    summary.committed = true;
    summary.version = Some(commit.version);
    for key in &commit.superseded {
        match std::fs::remove_file(table_dir.join(key)) {
            Ok(()) => summary.superseded_deleted += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => summary.superseded_deleted += 1,
            Err(_) => summary.superseded_not_deleted += 1,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use shapeshift_core::Sink;

    fn merge(dir: &Path, rows: &[(i64, i64)]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut sink = crate::IcebergSink::create_merge(
            dir,
            schema,
            Compression::Snappy,
            &[],
            &["id".to_string()],
        )
        .unwrap();
        sink.write_batch(&batch).unwrap();
        sink.finish().unwrap();
    }

    #[test]
    fn an_identity_is_guessed_only_when_no_table_answers() {
        let gone = ("/w/a".to_string(), OldPlace::Gone);
        let alive = ("/w/b".to_string(), OldPlace::Answers);
        let unknown = (
            "s3://x/t".to_string(),
            OldPlace::Unknown("no credentials".into()),
        );
        assert_eq!(decide_identity(None, &[]), Ok(Identity::Moved));
        assert_eq!(
            decide_identity(None, std::slice::from_ref(&gone)),
            Ok(Identity::Moved)
        );
        let why = decide_identity(None, &[gone.clone(), alive.clone()]).unwrap_err();
        assert!(why.contains("still answers at /w/b"), "{why}");
        let why = decide_identity(None, std::slice::from_ref(&unknown)).unwrap_err();
        assert!(
            why.contains("could not be checked (no credentials)"),
            "{why}"
        );
        for asked in [Identity::Moved, Identity::Copy] {
            assert_eq!(
                decide_identity(Some(asked), &[alive.clone(), unknown.clone()]),
                Ok(asked)
            );
        }
        let e = undecided_error("x").to_string();
        assert!(e.contains("--moved") && e.contains("--copy"), "{e}");
    }

    #[test]
    fn a_relocation_another_writer_beats_leaves_nothing_behind() {
        let root = std::env::temp_dir().join(format!("ss-relocate-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let (a, b) = (root.join("a"), root.join("b"));
        merge(&a, &[(1, 1), (2, 2)]);
        merge(&a, &[(2, 20)]);
        std::fs::rename(&a, &b).unwrap();
        let version = crate::local_version(&b).unwrap().unwrap();
        let text = std::fs::read_to_string(b.join(metadata_key(version))).unwrap();
        let table = PriorTable {
            metadata: serde_json::from_str(&text).unwrap(),
            version,
            manifests: Vec::new(),
        };
        let anchors = Anchors::of(&table.metadata);
        let files = crate::LocalTableFiles::new(&b, anchors.clone());
        let listing = list_local(&b).unwrap();
        let here = crate::abspath(&b);
        let plan = plan_relocation(
            &RelocateContext {
                table: &table,
                anchors: &anchors,
                here: &here,
                listing: &listing,
                staging_dir: &b,
                new_uuid: false,
                dry_run: false,
                now_ms: 1,
            },
            &files,
        )
        .unwrap();
        let commit = plan.commit.unwrap();
        assert!(!commit.written.is_empty());
        assert!(commit.written.iter().all(|k| b.join(k).exists()));
        // Another writer commits the next version first.
        merge(&b, &[(3, 3)]);
        let mut summary = plan.summary;
        let e = commit_relocation(&b, version, commit.clone(), &mut summary).unwrap_err();
        assert!(matches!(e, ShapeError::Conflict(_)), "{e}");
        assert!(!summary.committed);
        assert!(
            commit.written.iter().all(|k| !b.join(k).exists()),
            "the relocation's files are removed"
        );
        assert!(
            commit.superseded.iter().all(|k| b.join(k).exists()),
            "nothing it would have superseded is removed"
        );
        // The table is the other writer's: ids 1, 2 and 3 live.
        let live: usize = crate::current_scan_files(&b)
            .unwrap()
            .iter()
            .map(|f| f.record_count as usize - f.deleted_positions.len())
            .sum();
        assert_eq!(live, 3);
        std::fs::remove_dir_all(&root).ok();
    }
}
