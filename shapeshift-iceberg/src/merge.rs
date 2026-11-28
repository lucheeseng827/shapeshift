//! Row-level upsert: `merge_on`.
//!
//! A merge writes the incoming rows exactly as an append does. It then removes every row
//! they supersede with Iceberg v2 **position deletes** (merge-on-read). Each partition
//! that loses rows gets one small Parquet file of `(file_path, pos)` pairs, and those
//! files are listed in a delete manifest. No existing data file is rewritten, so a
//! merge's cost tracks the size of the delta rather than the table.
//!
//! A row is superseded in two ways:
//!
//! - **By a later write.** A row already in the table loses when this write carries its
//!   key. Finding those rows means reading the **key columns only** of the existing data
//!   files that could hold one. A file whose manifest bounds rule out every incoming key
//!   is never opened.
//! - **Within the same write.** When one write carries a key more than once, the last
//!   occurrence wins and the earlier ones are deleted from the files this write produced.
//!
//! A row with a null in any key column matches nothing, as in SQL `MERGE`. It is
//! appended and counted, never deduplicated.
//!
//! **Memory:** one entry per *distinct key in the write* (the key's bytes plus a few dozen
//! bytes of bookkeeping), plus the positions being deleted. The entry count tracks the
//! delta, not the table, but unlike an append it is not constant: a merge holds its keys.
//!
//! **Wherever the table lives.** Every read of the existing table goes through
//! [`TableFiles`], and a data file's key columns are fetched one row group at a time
//! (see the `files` module). A local table and an object-store table therefore run the
//! same merge; only the byte source differs. Manifests, delete files and the data files
//! to scan are each read several at a time, and every result is used in table order, so
//! the delete files a merge writes do not depend on which read finished first.
//!
//! **Concurrent writers.** A merge builds on the version it loaded; if another writer
//! commits first, the commit guard refuses this one (see the crate root), and it is run
//! again. Serialising writers instead is a catalog's job.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use shapeshift_core::{
    Compression, MergeSummary, Result, ShapeError, CHANGE_COLUMN, CHANGE_DELETE,
};

use crate::avro::{read_manifest_entries, ManifestEntry};
use crate::files::{batch_err, read_each, RangedParquet, TableFiles};
use crate::metadata::{table_tombstones, TombstoneRef};
use crate::partition::{
    partition_dir, PartVal, PartitionDataFile, PartitionField, PartitionedWriter, Placement,
};
use crate::stats::StatsCollector;
use crate::tombstones::{encode_version, Tombstone, TombstoneSet, VersionValue};
use crate::{join_location, pq_compression, IceField, PriorTable};

/// Iceberg's reserved field-ids for a position-delete file's two columns.
const DELETE_FILE_PATH_ID: i32 = 2_147_483_546;
const DELETE_POS_ID: i32 = 2_147_483_545;

/// Rows per batch when writing a position-delete file.
const DELETE_BATCH_ROWS: usize = 65_536;

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// How a key column's values are compared. A timestamp is a `long` of microseconds and
/// compares as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Bool,
    Long,
    Date,
    Str,
}

/// One column of the merge key, resolved against the table's fields.
#[derive(Debug, Clone)]
pub struct KeyColumn {
    pub name: String,
    pub field_id: i32,
    /// The column's index in the batches this write produces.
    index: usize,
    kind: KeyKind,
}

/// Resolve `merge_on` against the table's (already evolved) fields. Every key column
/// must exist and be a type whose values compare exactly. `double` is refused, because a
/// float cannot be trusted to equal itself across writers (Iceberg forbids float
/// identifier fields for the same reason).
pub fn resolve_key_columns(ice_fields: &[IceField], merge_on: &[String]) -> Result<Vec<KeyColumn>> {
    if merge_on.is_empty() {
        return Err(err("merge_on names no key column"));
    }
    let mut out: Vec<KeyColumn> = Vec::with_capacity(merge_on.len());
    for name in merge_on {
        if out.iter().any(|k| &k.name == name) {
            return Err(err(format!("merge_on names `{name}` twice")));
        }
        let (index, f) = ice_fields
            .iter()
            .enumerate()
            .find(|(_, f)| &f.name == name)
            .ok_or_else(|| err(format!("merge_on column `{name}` is not in the schema")))?;
        let kind = match f.iceberg_type.as_str() {
            "boolean" => KeyKind::Bool,
            "long" | "timestamp" => KeyKind::Long,
            "date" => KeyKind::Date,
            "string" => KeyKind::Str,
            "double" => {
                return Err(err(format!(
                    "cannot merge on `{name}` (double): a floating-point value is not a \
                     reliable key — use an integer, string, date, or timestamp column"
                )))
            }
            other => {
                return Err(err(format!(
                    "cannot merge on `{name}`: type `{other}` is not a supported key type"
                )))
            }
        };
        out.push(KeyColumn {
            name: name.clone(),
            field_id: f.id,
            index,
            kind,
        });
    }
    Ok(out)
}

/// A key column's array for one batch, downcast once so the per-row work is a slice
/// index.
enum KeyArr<'a> {
    Bool(&'a BooleanArray),
    I64(&'a dyn Array, &'a [i64]),
    I32(&'a dyn Array, &'a [i32]),
    Str(&'a StringArray),
}

impl<'a> KeyArr<'a> {
    fn new(arr: &'a dyn Array, kind: KeyKind) -> Result<Self> {
        let bad = || {
            err(format!(
                "merge key column has Arrow type {:?}, which does not match its {kind:?} key",
                arr.data_type()
            ))
        };
        Ok(match (kind, arr.data_type()) {
            (KeyKind::Bool, DataType::Boolean) => {
                KeyArr::Bool(arr.as_any().downcast_ref().ok_or_else(bad)?)
            }
            (KeyKind::Long, DataType::Int64) => {
                let a: &Int64Array = arr.as_any().downcast_ref().ok_or_else(bad)?;
                KeyArr::I64(arr, a.values())
            }
            (KeyKind::Long, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                let a: &TimestampMicrosecondArray = arr.as_any().downcast_ref().ok_or_else(bad)?;
                KeyArr::I64(arr, a.values())
            }
            (KeyKind::Date, DataType::Date32) => {
                let a: &Date32Array = arr.as_any().downcast_ref().ok_or_else(bad)?;
                KeyArr::I32(arr, a.values())
            }
            (KeyKind::Str, DataType::Utf8) => {
                KeyArr::Str(arr.as_any().downcast_ref().ok_or_else(bad)?)
            }
            _ => return Err(bad()),
        })
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            KeyArr::Bool(a) => a.is_null(i),
            KeyArr::I64(a, _) | KeyArr::I32(a, _) => a.is_null(i),
            KeyArr::Str(a) => a.is_null(i),
        }
    }

    /// Append row `i`'s value to a key buffer. Strings are length-prefixed, so a
    /// composite key's encoding is unambiguous.
    fn encode(&self, i: usize, out: &mut Vec<u8>) {
        match self {
            KeyArr::Bool(a) => out.push(a.value(i) as u8),
            KeyArr::I64(_, v) => out.extend_from_slice(&v[i].to_le_bytes()),
            KeyArr::I32(_, v) => out.extend_from_slice(&v[i].to_le_bytes()),
            KeyArr::Str(a) => {
                let s = a.value(i).as_bytes();
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
    }
}

/// Encode row `row`'s key into `out`; `false` if any key column is null (no key).
fn encode_row(arrs: &[KeyArr], row: usize, out: &mut Vec<u8>) -> bool {
    out.clear();
    for a in arrs {
        if a.is_null(row) {
            return false;
        }
        a.encode(row, out);
    }
    true
}

/// The smallest and largest value one key column takes across the write. A data file
/// whose manifest bounds fall outside it cannot hold a match.
#[derive(Debug, Clone)]
enum KeyRange {
    Empty,
    Bool(bool, bool),
    Long(i64, i64),
    Int(i32, i32),
    Str(Vec<u8>, Vec<u8>),
}

impl KeyRange {
    fn fold(&mut self, a: &KeyArr, i: usize) {
        match (&mut *self, a) {
            (KeyRange::Empty, KeyArr::Bool(a)) => *self = KeyRange::Bool(a.value(i), a.value(i)),
            (KeyRange::Empty, KeyArr::I64(_, v)) => *self = KeyRange::Long(v[i], v[i]),
            (KeyRange::Empty, KeyArr::I32(_, v)) => *self = KeyRange::Int(v[i], v[i]),
            (KeyRange::Empty, KeyArr::Str(a)) => {
                let s = a.value(i).as_bytes().to_vec();
                *self = KeyRange::Str(s.clone(), s);
            }
            (KeyRange::Bool(lo, hi), KeyArr::Bool(a)) => {
                *lo = *lo && a.value(i);
                *hi = *hi || a.value(i);
            }
            (KeyRange::Long(lo, hi), KeyArr::I64(_, v)) => {
                *lo = (*lo).min(v[i]);
                *hi = (*hi).max(v[i]);
            }
            (KeyRange::Int(lo, hi), KeyArr::I32(_, v)) => {
                *lo = (*lo).min(v[i]);
                *hi = (*hi).max(v[i]);
            }
            (KeyRange::Str(lo, hi), KeyArr::Str(a)) => {
                let s = a.value(i).as_bytes();
                if s < lo.as_slice() {
                    *lo = s.to_vec();
                }
                if s > hi.as_slice() {
                    *hi = s.to_vec();
                }
            }
            _ => {} // resolve_key_columns fixed each column's kind; unreachable in practice
        }
    }

    /// Could a file whose bounds for this column are `[lo, hi]` hold a value in this
    /// range? Bounds are Iceberg single-value binary. Anything unexpected answers "yes":
    /// a wrong "no" would leave a stale row live, so pruning only ever errs toward reading.
    fn may_overlap(&self, lo: &[u8], hi: &[u8]) -> bool {
        fn le8(b: &[u8]) -> Option<i64> {
            Some(i64::from_le_bytes(b.try_into().ok()?))
        }
        fn le4(b: &[u8]) -> Option<i32> {
            Some(i32::from_le_bytes(b.try_into().ok()?))
        }
        match self {
            KeyRange::Empty => false,
            KeyRange::Bool(a, b) => match (lo, hi) {
                ([flo], [fhi]) => (*flo != 0) <= *b && (*fhi != 0) >= *a,
                _ => true,
            },
            KeyRange::Long(a, b) => match (le8(lo), le8(hi)) {
                (Some(flo), Some(fhi)) => flo <= *b && fhi >= *a,
                _ => true,
            },
            KeyRange::Int(a, b) => match (le4(lo), le4(hi)) {
                (Some(flo), Some(fhi)) => flo <= *b && fhi >= *a,
                _ => true,
            },
            // Iceberg orders strings by their UTF-8 bytes. A truncated lower bound is a
            // prefix (still ≤ the true minimum) and a truncated upper bound is rounded up,
            // so a bound written by any engine is safe to compare this way.
            KeyRange::Str(a, b) => lo <= b.as_slice() && hi >= a.as_slice(),
        }
    }
}

/// One part of a change's version (`changes.order_by`), compared as the column's type does.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum VersionPart {
    Int(i64),
    Bytes(Box<[u8]>),
}

/// A change's version: its `order_by` values, compared left to right.
type Version = Box<[VersionPart]>;

/// Row `row`'s version, or `None` if any part is null (a row written before the table was
/// ordered, or by a writer that left the column empty: older than any change).
fn version_of(arrs: &[KeyArr], row: usize) -> Option<Version> {
    let mut out = Vec::with_capacity(arrs.len());
    for a in arrs {
        if a.is_null(row) {
            return None;
        }
        out.push(match a {
            KeyArr::I64(_, v) => VersionPart::Int(v[row]),
            KeyArr::I32(_, v) => VersionPart::Int(v[row] as i64),
            KeyArr::Str(s) => VersionPart::Bytes(s.value(row).as_bytes().into()),
            KeyArr::Bool(b) => VersionPart::Int(b.value(row) as i64),
        });
    }
    Some(out.into_boxed_slice())
}

/// A version in the tombstones' byte encoding, which compares as the version does.
fn version_bytes(v: &Version) -> Vec<u8> {
    let parts: Vec<VersionValue<'_>> = v
        .iter()
        .map(|p| match p {
            VersionPart::Int(i) => VersionValue::Int(*i),
            VersionPart::Bytes(b) => VersionValue::Bytes(b),
        })
        .collect();
    encode_version(&parts)
}

/// How long a delete is remembered when the spec does not say (7 days).
pub const DEFAULT_TOMBSTONE_RETENTION_MS: i64 = 7 * 86_400_000;

/// The latest change to one key in this write: where its row landed (an upsert), or that it
/// is a delete, and its version. Whether the key had a live row in the table is set by
/// whichever scan finds that row.
struct Latest {
    slot: u32,
    pos: u64,
    delete: bool,
    version: Option<Version>,
    matched: AtomicBool,
}

/// The write half of a merge, shared by the local and object-store sinks. Rows go to the
/// partitioned writer exactly as an append's do, and their keys and placements go to the
/// tracker; [`MergeWriter::finish`] then finds and deletes what they supersede.
///
/// A batch that carries [`CHANGE_COLUMN`] is a change feed's (`changes:`): its delete rows
/// are not written at all, only remembered, and at finish they remove the key's live rows.
/// With [`MergeWriter::order_by`], the latest change to a key is the one with the greatest
/// version (the later arrival on a tie), and it applies only if it is newer than the key's
/// live row; otherwise it is stale, and the table keeps what it has.
pub struct MergeWriter {
    tracker: MergeTracker,
    /// Reused per batch: where each row of the batch landed.
    placed: Vec<Placement>,
}

impl MergeWriter {
    /// Resolve `merge_on` against the table's (already evolved) fields. Fails before any
    /// data is written if a key column is missing, repeated, or of a type that cannot key.
    pub fn new(ice_fields: &[IceField], merge_on: &[String]) -> Result<Self> {
        Ok(MergeWriter {
            tracker: MergeTracker::new(resolve_key_columns(ice_fields, merge_on)?),
            placed: Vec::new(),
        })
    }

    /// Version changes by these columns (`changes.order_by`): resolved now, against the
    /// table's fields, so a missing column or one that cannot order fails before any write.
    /// A delete is then remembered as a tombstone for `retention_ms`.
    pub fn order_by(
        &mut self,
        ice_fields: &[IceField],
        order_by: &[String],
        retention_ms: i64,
    ) -> Result<()> {
        if retention_ms < 0 {
            return Err(err("the tombstone retention must not be negative"));
        }
        self.tracker.retention_ms = retention_ms;
        let mut out = Vec::with_capacity(order_by.len());
        for name in order_by {
            let (index, f) = ice_fields
                .iter()
                .enumerate()
                .find(|(_, f)| &f.name == name)
                .ok_or_else(|| err(format!("order_by column `{name}` is not in the schema")))?;
            let kind = match f.iceberg_type.as_str() {
                "long" | "timestamp" => KeyKind::Long,
                "date" => KeyKind::Date,
                "string" => KeyKind::Str,
                other => {
                    return Err(err(format!(
                        "cannot order changes by `{name}`: type `{other}` is not a version (use \
                         a long, timestamp, date or string column)"
                    )))
                }
            };
            out.push(KeyColumn {
                name: name.clone(),
                field_id: f.id,
                index,
                kind,
            });
        }
        self.tracker.order = out;
        Ok(())
    }

    /// Write one batch through `writer`, recording its rows' keys and placements. A change
    /// feed's batch has its [`CHANGE_COLUMN`] taken off, and its deletes kept out of the
    /// data files.
    pub fn write_batch(
        &mut self,
        writer: &mut PartitionedWriter,
        batch: &RecordBatch,
    ) -> Result<()> {
        let Ok(ci) = batch.schema().index_of(CHANGE_COLUMN) else {
            writer.write_batch_placed(batch, Some(&mut self.placed))?;
            return self.tracker.observe(batch, None, &self.placed);
        };
        self.tracker.changes = true;
        let flags = batch
            .column(ci)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| {
                err(format!(
                    "internal error: `{CHANGE_COLUMN}` is not a flag column"
                ))
            })?
            .clone();
        let keep: Vec<usize> = (0..batch.num_columns()).filter(|&i| i != ci).collect();
        let data = batch.project(&keep).map_err(err)?;
        let is_upsert: BooleanArray = flags
            .iter()
            .map(|f| Some(f != Some(CHANGE_DELETE)))
            .collect();
        let upserts = arrow_select::filter::filter_record_batch(&data, &is_upsert).map_err(err)?;
        writer.write_batch_placed(&upserts, Some(&mut self.placed))?;
        self.tracker.observe(&data, Some(&flags), &self.placed)
    }

    /// Find every row the write supersedes, reading the existing table through `files`,
    /// and write the position-delete files that remove them under `cx.staging_dir`.
    pub fn finish<F: TableFiles>(self, cx: &MergeContext, files: &F) -> Result<MergeOutcome> {
        self.tracker.finish(cx, files)
    }
}

/// The snapshot summary key a change feed's merge records its `order_by` in (the columns,
/// as a JSON array; empty for arrival order). A later merge reads the nearest one in the
/// table's history and refuses to apply changes ordered another way.
pub const CHANGES_ORDER_KEY: &str = "shapeshift.changes.order-by";

/// The `order_by` the table's changes were last applied with: the value of
/// [`CHANGES_ORDER_KEY`] on the nearest snapshot in the current snapshot's ancestry that
/// has one (a write by another engine records none, and changes nothing). `Err` holds a
/// recorded value that is not a list of names: an order this build cannot read, which no
/// merge may claim to match.
pub fn table_change_order(
    meta: &serde_json::Value,
) -> Option<std::result::Result<Vec<String>, String>> {
    let snapshots = meta["snapshots"].as_array()?;
    let by_id = |id: i64| {
        snapshots
            .iter()
            .find(|s| s["snapshot-id"].as_i64() == Some(id))
    };
    let mut cur = meta["current-snapshot-id"].as_i64().filter(|&id| id != -1);
    let mut hops = 0usize;
    while let Some(id) = cur {
        let s = by_id(id)?;
        if let Some(v) = s["summary"][CHANGES_ORDER_KEY].as_str() {
            return Some(serde_json::from_str(v).map_err(|_| v.to_string()));
        }
        cur = s["parent-snapshot-id"].as_i64();
        hops += 1;
        if hops > snapshots.len() {
            return None; // a cycle: malformed metadata, and nothing to compare with
        }
    }
    None
}

/// Follows the keys of one merge write as its batches are written.
struct MergeTracker {
    keys: Vec<KeyColumn>,
    /// `changes.order_by`, resolved; empty: arrival order.
    order: Vec<KeyColumn>,
    /// A batch of this write carried [`CHANGE_COLUMN`].
    changes: bool,
    /// How long a delete is remembered (with `order`).
    retention_ms: i64,
    index: HashMap<Box<[u8]>, u32>,
    latest: Vec<Latest>,
    /// Rows this write wrote that a later change to their key superseded — deleted at finish.
    superseded: Vec<Placement>,
    ranges: Vec<KeyRange>,
    null_key_rows: u64,
    scratch: Vec<u8>,
}

impl MergeTracker {
    fn new(keys: Vec<KeyColumn>) -> Self {
        let ranges = vec![KeyRange::Empty; keys.len()];
        MergeTracker {
            keys,
            order: Vec::new(),
            changes: false,
            retention_ms: DEFAULT_TOMBSTONE_RETENTION_MS,
            index: HashMap::new(),
            latest: Vec::new(),
            superseded: Vec::new(),
            ranges,
            null_key_rows: 0,
            scratch: Vec::new(),
        }
    }

    /// Record one batch. `flags`, for a change feed, says which rows are deletes; the rows
    /// that are not were written, and `placed[i]` is where the `i`th of them landed.
    fn observe(
        &mut self,
        batch: &RecordBatch,
        flags: Option<&UInt8Array>,
        placed: &[Placement],
    ) -> Result<()> {
        let arrs = self
            .keys
            .iter()
            .map(|k| KeyArr::new(batch.column(k.index).as_ref(), k.kind))
            .collect::<Result<Vec<_>>>()?;
        let order = self
            .order
            .iter()
            .map(|k| KeyArr::new(batch.column(k.index).as_ref(), k.kind))
            .collect::<Result<Vec<_>>>()?;
        let mut next_placed = placed.iter();
        for row in 0..batch.num_rows() {
            let delete = flags.is_some_and(|f| f.value(row) == CHANGE_DELETE);
            let (slot, pos) = if delete {
                (u32::MAX, 0)
            } else {
                *next_placed.next().ok_or_else(|| {
                    err("internal error: a merge row was written without a placement")
                })?
            };
            if !encode_row(&arrs, row, &mut self.scratch) {
                // A delete always has a key (the shaper refuses one without).
                self.null_key_rows += 1;
                continue;
            }
            let version = if order.is_empty() {
                None
            } else {
                version_of(&order, row)
            };
            for (r, a) in self.ranges.iter_mut().zip(&arrs) {
                r.fold(a, row);
            }
            match self.index.get(self.scratch.as_slice()) {
                Some(&i) => {
                    let l = &mut self.latest[i as usize];
                    // The greater version wins; on a tie (or with no order), the later.
                    if version >= l.version {
                        if !l.delete {
                            self.superseded.push((l.slot, l.pos));
                        }
                        l.slot = slot;
                        l.pos = pos;
                        l.delete = delete;
                        l.version = version;
                    } else if !delete {
                        self.superseded.push((slot, pos));
                    }
                }
                None => {
                    let i = u32::try_from(self.latest.len())
                        .map_err(|_| err("a merge write carries more than 2^32 distinct keys"))?;
                    self.index
                        .insert(self.scratch.clone().into_boxed_slice(), i);
                    self.latest.push(Latest {
                        slot,
                        pos,
                        delete,
                        version,
                        matched: AtomicBool::new(false),
                    });
                }
            }
        }
        Ok(())
    }

    /// Whether a data file's manifest bounds leave room for one of this write's keys.
    /// A key column without bounds (all-null, or never recorded) cannot prune.
    fn may_hold(&self, entry: &ManifestEntry) -> bool {
        self.keys.iter().zip(&self.ranges).all(|(k, range)| {
            let lo = entry.lower_bounds.iter().find(|(id, _)| *id == k.field_id);
            let hi = entry.upper_bounds.iter().find(|(id, _)| *id == k.field_id);
            match (lo, hi) {
                (Some((_, lo)), Some((_, hi))) => range.may_overlap(lo, hi),
                _ => true,
            }
        })
    }

    /// Read one existing data file's key columns (and, with `order_by`, its version
    /// columns) and return its live rows whose key this write carries: each row's position,
    /// the key's entry, and the row's version. `deleted` is the file's already-deleted
    /// positions (sorted): a row a previous merge removed is not live, so it is neither
    /// matched nor deleted twice. Only the footer and, one row group at a time, those
    /// columns' chunks are fetched. Several files are scanned at once, so this only reads
    /// the tracker (marking a key matched is atomic).
    fn scan_file<F: TableFiles>(
        &self,
        files: &F,
        entry: &ManifestEntry,
        deleted: &[u64],
    ) -> Result<FileHits> {
        let path = entry.file_path.as_str();
        let pq = RangedParquet::open(files, path, u64::try_from(entry.file_size).ok())?;
        let schema = pq.schema().clone();
        // Locate each column by its Iceberg field-id (the name is the fallback for a file
        // written without ids). A file without a key column predates it, so every row has a
        // null key there and nothing in the file can match; a file without a version column
        // predates the ordering, so its rows have no version (older than any change).
        let locate = |k: &KeyColumn| {
            let by_id = schema.fields().iter().position(|f| {
                f.metadata()
                    .get("PARQUET:field_id")
                    .and_then(|v| v.parse::<i32>().ok())
                    == Some(k.field_id)
            });
            by_id.or_else(|| schema.fields().iter().position(|f| f.name() == &k.name))
        };
        let mut roots = Vec::with_capacity(self.keys.len());
        for k in &self.keys {
            match locate(k) {
                Some(i) => roots.push(i),
                None => return Ok(FileHits::default()),
            }
        }
        let order_roots: Vec<Option<usize>> = self.order.iter().map(locate).collect();
        let versioned = !self.order.is_empty() && order_roots.iter().all(Option::is_some);
        // A projection keeps the file's column order, so map each column to its rank.
        let mut sorted = roots.clone();
        if versioned {
            sorted.extend(order_roots.iter().flatten().copied());
        }
        sorted.sort_unstable();
        sorted.dedup();
        let rank = |r: usize| sorted.binary_search(&r).unwrap();
        let cols: Vec<usize> = roots.iter().map(|&r| rank(r)).collect();
        let order_cols: Vec<usize> = if versioned {
            order_roots.iter().flatten().map(|&r| rank(r)).collect()
        } else {
            Vec::new()
        };
        let mask = ProjectionMask::roots(pq.parquet_schema(), sorted.iter().copied());

        let mut hits = FileHits::default();
        let mut scratch = Vec::new();
        let mut pos: u64 = 0;
        let mut next_deleted = deleted.iter().peekable();
        for rg in 0..pq.num_row_groups() {
            for batch in pq.row_group(rg, &mask)? {
                let batch = batch.map_err(|e| batch_err(path, e))?;
                let arrs = self
                    .keys
                    .iter()
                    .zip(&cols)
                    .map(|(k, &c)| KeyArr::new(batch.column(c).as_ref(), k.kind))
                    .collect::<Result<Vec<_>>>()?;
                let order = self
                    .order
                    .iter()
                    .zip(&order_cols)
                    .map(|(k, &c)| KeyArr::new(batch.column(c).as_ref(), k.kind))
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..batch.num_rows() {
                    let here = pos;
                    pos += 1;
                    while next_deleted.next_if(|&&d| d < here).is_some() {}
                    if next_deleted.next_if(|&&d| d == here).is_some() {
                        continue;
                    }
                    if !encode_row(&arrs, row, &mut scratch) {
                        continue;
                    }
                    if let Some(&i) = self.index.get(scratch.as_slice()) {
                        self.latest[i as usize]
                            .matched
                            .store(true, Ordering::Relaxed);
                        hits.hits.push((here, i));
                        // Only the newest version of each key in the file is kept.
                        if let Some(v) = (!order.is_empty())
                            .then(|| version_of(&order, row))
                            .flatten()
                        {
                            match hits.newest.get_mut(&i) {
                                Some(n) if *n >= v => {}
                                Some(n) => *n = v,
                                None => {
                                    hits.newest.insert(i, v);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(hits)
    }

    /// Find every row this write supersedes, write the position-delete files that remove
    /// them (under `staging_dir`, one per partition), and report what the merge did.
    fn finish<F: TableFiles>(mut self, cx: &MergeContext, files: &F) -> Result<MergeOutcome> {
        // A table whose changes were applied in one order takes no merge in another: its
        // rows' versions would be compared with something they never were.
        let ours = self.changes.then(|| {
            self.order
                .iter()
                .map(|k| k.name.clone())
                .collect::<Vec<_>>()
        });
        let recorded = cx.prior.and_then(|p| table_change_order(&p.metadata));
        if let Some(Err(raw)) = &recorded {
            return Err(err(format!(
                "the table records its change order as {raw:?} ({CHANGES_ORDER_KEY}), which this \
                 build cannot read: it merges nothing into the table rather than guess the order"
            )));
        }
        if let Some(Ok(theirs)) = recorded {
            if !theirs.is_empty() && ours.as_ref() != Some(&theirs) {
                let theirs = theirs.join(", ");
                return Err(err(format!(
                    "the table's changes are ordered by `{theirs}` ({CHANGES_ORDER_KEY}); this \
                     write {}, which would apply changes out of order: use `changes.order_by: \
                     [{theirs}]`",
                    match ours.as_deref() {
                        None => "is a plain merge".to_string(),
                        Some([]) => "has no `changes.order_by`".to_string(),
                        Some(o) => format!("orders them by `{}`", o.join(", ")),
                    }
                )));
            }
        }

        // The keys the table deleted, and the versions that deleted them. A file that cannot
        // be read, or that was written for another key or order, stops the merge: going on
        // without it could bring deleted rows back.
        let ordered = self.changes && !self.order.is_empty();
        let order_names: Vec<String> = self.order.iter().map(|k| k.name.clone()).collect();
        let key_names: Vec<String> = self.keys.iter().map(|k| k.name.clone()).collect();
        let mut tombstones: Option<TombstoneSet> = None;
        // A tombstone past the retention is let go before any change is judged by it.
        let now = crate::snapshot_timestamp_ms();
        let cutoff = now.saturating_sub(self.retention_ms);
        let mut t_trimmed = 0u64;
        if let Some(r) = cx
            .prior
            .map(|p| table_tombstones(&p.metadata))
            .transpose()?
            .flatten()
        {
            let path = join_location(cx.base_location, &r.key);
            if ordered {
                let set = TombstoneSet::from_puffin(&path, &files.read(&path)?)?;
                if set.key_columns != key_names || set.order_by != order_names {
                    return Err(err(format!(
                        "the table's tombstones ({path}) are keyed on [{}] and ordered by [{}]; \
                         this merge is keyed on [{}] and ordered by [{}], so it cannot apply them",
                        set.key_columns.join(", "),
                        set.order_by.join(", "),
                        key_names.join(", "),
                        order_names.join(", ")
                    )));
                }
                let mut set = set;
                set.by_key.retain(|_, t| {
                    let keep = t.deleted_at_ms >= cutoff;
                    t_trimmed += u64::from(!keep);
                    keep
                });
                tombstones = Some(set);
            } else if r.count > 0 {
                return Err(err(format!(
                    "the table remembers {} deleted key(s) ({path}); a merge that does not order \
                     its changes would ignore them and could bring those rows back",
                    r.count
                )));
            }
        }

        let mut scanned = 0u64;
        let mut pruned = 0u64;
        let mut table_hits: Vec<(&ManifestEntry, FileHits)> = Vec::new();

        // With no non-null key in the write there is nothing to match: skip the scan
        // rather than report every file as "pruned".
        let live = match cx.prior.filter(|_| !self.latest.is_empty()) {
            Some(prior) => Some(live_files(files, prior)?),
            None => None,
        };
        if let Some(live) = &live {
            let (candidates, skipped): (Vec<&ManifestEntry>, Vec<&ManifestEntry>) =
                live.data.iter().partition(|e| self.may_hold(e));
            pruned = skipped.len() as u64;
            scanned = candidates.len() as u64;
            let tracker = &self;
            let found = read_each(files, &candidates, |entry| {
                let deleted = live
                    .deletes
                    .get(&entry.file_path)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                tracker.scan_file(files, entry, deleted)
            })?;
            table_hits = candidates.into_iter().zip(found).collect();
        }

        // With `order_by`, a key's change applies only if it is newer than every live row of
        // the key: equal is stale too, so a replayed feed changes nothing.
        let mut stale = vec![false; self.latest.len()];
        if !self.order.is_empty() {
            let mut newest: Vec<Option<&Version>> = vec![None; self.latest.len()];
            for (_, hits) in &table_hits {
                for (&i, v) in &hits.newest {
                    let n = &mut newest[i as usize];
                    if Some(v) > *n {
                        *n = Some(v);
                    }
                }
            }
            for (i, l) in self.latest.iter().enumerate() {
                if let Some(table) = newest[i] {
                    stale[i] = l.version.as_ref() <= Some(table);
                }
            }
        }
        // And not newer than the delete the key's tombstone remembers.
        if let Some(set) = &tombstones {
            for (key, &i) in &self.index {
                let l = &self.latest[i as usize];
                if let Some(t) = set.by_key.get(key.as_ref()) {
                    // A null version is older than any change: it cannot retire the tombstone.
                    if l.version
                        .as_ref()
                        .map_or(true, |v| version_bytes(v) <= t.version)
                    {
                        stale[i as usize] = true;
                    }
                }
            }
        }

        let mut targets: Vec<Target> = Vec::new();
        let mut prior_rows_replaced = 0u64;
        let mut rows_deleted = 0u64;
        for (entry, hits) in table_hits {
            let mut positions = Vec::new();
            for (pos, i) in hits.hits {
                if stale[i as usize] {
                    continue;
                }
                if self.latest[i as usize].delete {
                    rows_deleted += 1;
                } else {
                    prior_rows_replaced += 1;
                }
                positions.push(pos);
            }
            if !positions.is_empty() {
                targets.push(Target {
                    path: entry.file_path.clone(),
                    partition: entry.partition.clone(),
                    positions,
                });
            }
        }

        // Earlier occurrences of a key within this write, and a stale upsert's own row, in
        // the files it just wrote.
        let duplicates_in_input = self.superseded.len() as u64;
        let mut changes_stale = 0u64;
        for (i, l) in self.latest.iter().enumerate() {
            if stale[i] {
                changes_stale += 1;
                if !l.delete {
                    self.superseded.push((l.slot, l.pos));
                }
            }
        }
        let written_removed = self.superseded.len() as u64;
        let mut by_slot: HashMap<u32, Vec<u64>> = HashMap::new();
        for (slot, pos) in self.superseded.drain(..) {
            by_slot.entry(slot).or_default().push(pos);
        }
        for (slot, positions) in by_slot {
            let f = cx.new_files.get(slot as usize).ok_or_else(|| {
                err("internal error: a merge placement names a data file that was not written")
            })?;
            targets.push(Target {
                path: join_location(cx.base_location, &f.data_key),
                partition: f.partition.clone(),
                positions,
            });
        }

        let applied = |i: usize| !stale[i];

        // The tombstones after this write: its applied deletes remembered, the keys it
        // re-created retired, and those past the retention trimmed. Written only when
        // something changed; otherwise the snapshot names the file the table has.
        let (mut t_written, mut t_retired) = (0u64, 0u64);
        let mut new_tombstones = None;
        let mut t_live = tombstones.as_ref().map_or(0, |s| s.by_key.len() as u64);
        // The clock of a delete this write remembers starts once the scan is done, as close
        // to the commit as the merge can know it.
        let written_at = crate::snapshot_timestamp_ms();
        if ordered {
            let mut set = tombstones.unwrap_or_else(|| TombstoneSet {
                key_columns: key_names.clone(),
                order_by: order_names.clone(),
                ..TombstoneSet::default()
            });
            for (key, &i) in &self.index {
                let l = &self.latest[i as usize];
                if !applied(i as usize) {
                    continue;
                }
                match (&l.version, l.delete) {
                    (Some(v), true) => {
                        set.by_key.insert(
                            key.to_vec(),
                            Tombstone {
                                version: version_bytes(v),
                                deleted_at_ms: written_at,
                            },
                        );
                        t_written += 1;
                    }
                    (_, false) => {
                        if set.by_key.remove(key.as_ref()).is_some() {
                            t_retired += 1;
                        }
                    }
                    (None, true) => {}
                }
            }
            t_live = set.by_key.len() as u64;
            if t_written + t_retired + t_trimmed > 0 {
                let file = set.to_puffin(cx.snapshot_id, cx.sequence_number)?;
                let key = format!("metadata/tombstones-{}.puffin", Uuid::new_v4());
                let local = cx.staging_dir.join(&key);
                if let Some(dir) = local.parent() {
                    std::fs::create_dir_all(dir).map_err(err)?;
                }
                if let Err(e) = std::fs::write(&local, &file.bytes) {
                    let _ = std::fs::remove_file(&local);
                    return Err(err(e));
                }
                new_tombstones = Some(TombstoneRef {
                    key,
                    count: file.count,
                    bytes: file.bytes.len() as u64,
                    footer_bytes: file.footer_size,
                    snapshot_id: cx.snapshot_id,
                    sequence_number: cx.sequence_number,
                });
            }
        }
        // The delete files. If they cannot be written, the tombstone file goes too: nothing
        // of a failed merge stays behind.
        let delete_files = match write_delete_files(cx, targets) {
            Ok(files) => files,
            Err(e) => {
                if let Some(t) = &new_tombstones {
                    let _ = std::fs::remove_file(cx.staging_dir.join(&t.key));
                }
                return Err(e);
            }
        };

        let keys_updated = self
            .latest
            .iter()
            .enumerate()
            .filter(|(i, l)| applied(*i) && !l.delete && l.matched.load(Ordering::Relaxed))
            .count() as u64;
        let keys_inserted = self
            .latest
            .iter()
            .enumerate()
            .filter(|(i, l)| applied(*i) && !l.delete && !l.matched.load(Ordering::Relaxed))
            .count() as u64;
        let keys_deleted = self
            .latest
            .iter()
            .enumerate()
            .filter(|(i, l)| applied(*i) && l.delete)
            .count() as u64;
        let distinct_keys = self.latest.len() as u64;
        let summary = MergeSummary {
            key_columns: self.keys.iter().map(|k| k.name.clone()).collect(),
            distinct_keys,
            keys_updated,
            keys_inserted,
            prior_rows_replaced,
            duplicates_in_input,
            null_key_rows: self.null_key_rows,
            data_files_scanned: scanned,
            data_files_pruned: pruned,
            delete_files: delete_files.len() as u64,
            position_deletes: prior_rows_replaced + rows_deleted + written_removed,
            keys_deleted,
            rows_deleted,
            changes_stale,
            order_by: self.order.iter().map(|k| k.name.clone()).collect(),
            tombstones_written: t_written,
            tombstones_retired: t_retired,
            tombstones_trimmed: t_trimmed,
            tombstones_live: t_live,
        };
        Ok(MergeOutcome {
            delete_files,
            summary,
            changes_order: ours,
            tombstones: new_tombstones,
        })
    }
}

/// The live rows of one data file whose key this write carries.
#[derive(Default)]
struct FileHits {
    /// Each such row's position, and its key's entry in the tracker.
    hits: Vec<(u64, u32)>,
    /// With `changes.order_by`: the newest version each key's live rows in the file carry.
    newest: HashMap<u32, Version>,
}

/// What a merge needs to know about the write it is finishing.
pub struct MergeContext<'a> {
    /// Where this write's files are staged locally: the table directory for a local
    /// table, a temp directory an object-store sink uploads from. Delete files are written
    /// under it at their table-relative keys.
    pub staging_dir: &'a Path,
    /// The location this write's paths are anchored at (the metadata `location`).
    pub base_location: &'a str,
    /// The table as it was before this write (`None` = this write creates it).
    pub prior: Option<&'a PriorTable>,
    /// The data files this write produced, in slot order.
    pub new_files: &'a [PartitionDataFile],
    pub partition_fields: &'a [PartitionField],
    pub compression: Compression,
    /// The snapshot this write will commit, and its sequence number: a tombstone file it
    /// writes records them as the snapshot that computed it.
    pub snapshot_id: i64,
    pub sequence_number: i64,
}

/// The delete files a merge wrote, and its report.
pub struct MergeOutcome {
    pub delete_files: Vec<PartitionDataFile>,
    pub summary: MergeSummary,
    /// For a change feed's merge: the `order_by` to record in the snapshot
    /// ([`CHANGES_ORDER_KEY`]); `None` for a plain merge.
    pub changes_order: Option<Vec<String>>,
    /// The tombstone file this merge wrote under `staging_dir` (at its key), when it
    /// changed the table's tombstones.
    pub tombstones: Option<TombstoneRef>,
}

/// A data file a merge deletes rows from, as the delete file must name it.
struct Target {
    /// The data file's path exactly as its manifest records it — readers match on it.
    path: String,
    partition: Vec<PartVal>,
    positions: Vec<u64>,
}

/// The live files of a table's current snapshot.
pub(crate) struct LiveFiles {
    /// Live data-file entries.
    pub data: Vec<ManifestEntry>,
    /// Live position-delete-file entries.
    pub delete_files: Vec<ManifestEntry>,
    /// Deleted positions per data-file path (sorted, deduplicated), from the live
    /// position-delete files.
    pub deletes: HashMap<String, Vec<u64>>,
}

/// Read a table's live data files and the positions its delete files remove. Refuses a
/// table carrying equality deletes: shapeshift neither writes nor applies them, so it
/// could not tell which rows are live.
///
/// Every returned entry has its snapshot id and sequence numbers filled in: an entry
/// that left them null inherits its manifest's, as the spec says. A snapshot that
/// carries the entry into a manifest of its own must then write them out explicitly.
pub(crate) fn live_files<F: TableFiles>(files: &F, prior: &PriorTable) -> Result<LiveFiles> {
    let mut data = Vec::new();
    let mut delete_files = Vec::new();
    let manifests = read_each(files, &prior.manifests, |m| {
        read_manifest_entries(&files.read(&m.path)?)
    })?;
    for (m, entries) in prior.manifests.iter().zip(manifests) {
        for mut entry in entries {
            if !entry.is_live() {
                continue;
            }
            entry.snapshot_id.get_or_insert(m.added_snapshot_id);
            entry.sequence_number.get_or_insert(m.sequence_number);
            entry.file_sequence_number.get_or_insert(m.sequence_number);
            match entry.content {
                0 => data.push(entry),
                1 => delete_files.push(entry),
                _ => {
                    return Err(err(
                        "the table has equality-delete files (written by another engine); \
                         shapeshift reads only position deletes, so it cannot tell which \
                         rows are live",
                    ))
                }
            }
        }
    }
    let mut deletes: HashMap<String, Vec<u64>> = HashMap::new();
    let read = read_each(files, &delete_files, |d| read_position_deletes(files, d))?;
    for per_file in read {
        for (path, mut positions) in per_file {
            deletes.entry(path).or_default().append(&mut positions);
        }
    }
    for positions in deletes.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    Ok(LiveFiles {
        data,
        delete_files,
        deletes,
    })
}

/// Read a position-delete file's `(file_path, pos)` rows, grouped by data file.
fn read_position_deletes<F: TableFiles>(
    files: &F,
    entry: &ManifestEntry,
) -> Result<HashMap<String, Vec<u64>>> {
    let path = entry.file_path.as_str();
    let mut out: HashMap<String, Vec<u64>> = HashMap::new();
    let pq = RangedParquet::open(files, path, u64::try_from(entry.file_size).ok())?;
    let all = ProjectionMask::all();
    let mut batches = Vec::new();
    for rg in 0..pq.num_row_groups() {
        batches.extend(pq.row_group(rg, &all)?);
    }
    for batch in batches {
        let batch = batch.map_err(|e| batch_err(path, e))?;
        let col = |id: i32, name: &str| -> Result<&ArrayRef> {
            let schema = batch.schema_ref();
            let i = schema
                .fields()
                .iter()
                .position(|f| {
                    f.metadata().get("PARQUET:field_id").map(String::as_str)
                        == Some(id.to_string().as_str())
                })
                .or_else(|| schema.fields().iter().position(|f| f.name() == name))
                .ok_or_else(|| err(format!("{path}: not a position-delete file")))?;
            Ok(batch.column(i))
        };
        let paths = col(DELETE_FILE_PATH_ID, "file_path")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| err("position-delete file_path is not a string column"))?;
        let positions = col(DELETE_POS_ID, "pos")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("position-delete pos is not a long column"))?;
        for i in 0..batch.num_rows() {
            out.entry(paths.value(i).to_string())
                .or_default()
                .push(positions.value(i) as u64);
        }
    }
    Ok(out)
}

/// The Arrow schema of a position-delete file, carrying Iceberg's reserved field-ids.
fn delete_schema() -> SchemaRef {
    let field = |name: &str, dt: DataType, id: i32| {
        Field::new(name, dt, false).with_metadata(
            [("PARQUET:field_id".to_string(), id.to_string())]
                .into_iter()
                .collect(),
        )
    };
    Arc::new(Schema::new(vec![
        field("file_path", DataType::Utf8, DELETE_FILE_PATH_ID),
        field("pos", DataType::Int64, DELETE_POS_ID),
    ]))
}

/// Write one position-delete file per partition. The spec requires the rows sorted by
/// `file_path`, then `pos`; a delete applies only to data files in its own partition.
fn write_delete_files(cx: &MergeContext, targets: Vec<Target>) -> Result<Vec<PartitionDataFile>> {
    let mut created = Vec::new();
    let out = write_delete_files_into(cx, targets, &mut created);
    if out.is_err() {
        for p in &created {
            let _ = std::fs::remove_file(p);
        }
    }
    out
}

/// [`write_delete_files`], recording each file it creates in `created`.
fn write_delete_files_into(
    cx: &MergeContext,
    targets: Vec<Target>,
    created: &mut Vec<std::path::PathBuf>,
) -> Result<Vec<PartitionDataFile>> {
    let mut groups: HashMap<Vec<PartVal>, Vec<Target>> = HashMap::new();
    for t in targets {
        groups.entry(t.partition.clone()).or_default().push(t);
    }
    let mut groups: Vec<(Vec<PartVal>, Vec<Target>)> = groups.into_iter().collect();
    for (_, ts) in &mut groups {
        ts.sort_by(|a, b| a.path.cmp(&b.path));
        for t in ts.iter_mut() {
            t.positions.sort_unstable();
            t.positions.dedup();
        }
    }
    // A stable file order, so a rerun of the same merge lists its delete files the same way.
    groups.sort_by(|a, b| a.1[0].path.cmp(&b.1[0].path));

    let schema = delete_schema();
    let mut out = Vec::with_capacity(groups.len());
    for (partition, ts) in groups {
        let key = format!(
            "{}/delete-{}.parquet",
            partition_dir(cx.partition_fields, &partition),
            Uuid::new_v4()
        );
        let path = cx.staging_dir.join(&key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(err)?;
        }
        let props = WriterProperties::builder()
            .set_compression(pq_compression(cx.compression)?)
            .build();
        created.push(path.clone());
        let file = File::create(&path).map_err(err)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(err)?;
        let mut stats = StatsCollector::new(&[DELETE_FILE_PATH_ID, DELETE_POS_ID]);
        let mut rows = 0u64;
        let mut paths: Vec<&str> = Vec::with_capacity(DELETE_BATCH_ROWS);
        let mut positions: Vec<i64> = Vec::with_capacity(DELETE_BATCH_ROWS);
        let mut flush = |paths: &mut Vec<&str>, positions: &mut Vec<i64>| -> Result<()> {
            if paths.is_empty() {
                return Ok(());
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(std::mem::take(paths))) as ArrayRef,
                    Arc::new(Int64Array::from(std::mem::take(positions))) as ArrayRef,
                ],
            )?;
            writer.write(&batch).map_err(err)?;
            stats.update(&batch);
            rows += batch.num_rows() as u64;
            Ok(())
        };
        for t in &ts {
            for &p in &t.positions {
                paths.push(&t.path);
                positions.push(p as i64);
                if paths.len() == DELETE_BATCH_ROWS {
                    flush(&mut paths, &mut positions)?;
                }
            }
        }
        flush(&mut paths, &mut positions)?;
        writer.close().map_err(err)?;
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) as i64;
        out.push(PartitionDataFile {
            data_key: key,
            partition,
            rows,
            size,
            stats: stats.finish(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ice(id: i32, name: &str, ty: &str) -> IceField {
        IceField {
            id,
            name: name.into(),
            iceberg_type: ty.into(),
            required: false,
        }
    }

    #[test]
    fn key_columns_resolve_or_explain() {
        let fields = vec![
            ice(1, "id", "long"),
            ice(2, "amt", "double"),
            ice(3, "region", "string"),
        ];
        let keys = resolve_key_columns(&fields, &["region".into(), "id".into()]).unwrap();
        assert_eq!(keys[0].field_id, 3);
        assert_eq!(keys[0].index, 2);
        assert_eq!(keys[1].field_id, 1);

        let e = |on: &[&str]| {
            let on: Vec<String> = on.iter().map(|s| s.to_string()).collect();
            resolve_key_columns(&fields, &on).unwrap_err().to_string()
        };
        assert!(e(&["amt"]).contains("floating-point"));
        assert!(e(&["nope"]).contains("not in the schema"));
        assert!(e(&["id", "id"]).contains("twice"));
        assert!(e(&[]).contains("no key column"));
    }

    #[test]
    fn composite_keys_encode_unambiguously() {
        // ("ab", "c") and ("a", "bc") concatenate to the same bytes; the length prefix
        // keeps them apart.
        let a = StringArray::from(vec!["ab", "a"]);
        let b = StringArray::from(vec!["c", "bc"]);
        let arrs = vec![KeyArr::Str(&a), KeyArr::Str(&b)];
        let (mut k0, mut k1) = (Vec::new(), Vec::new());
        assert!(encode_row(&arrs, 0, &mut k0));
        assert!(encode_row(&arrs, 1, &mut k1));
        assert_ne!(k0, k1);
    }

    #[test]
    fn ranges_prune_only_what_cannot_match() {
        let mut r = KeyRange::Empty;
        let v = Int64Array::from(vec![10, 20]);
        let a = KeyArr::I64(&v, v.values());
        r.fold(&a, 0);
        r.fold(&a, 1);
        let b = |n: i64| n.to_le_bytes().to_vec();
        assert!(r.may_overlap(&b(0), &b(10)), "touches the low end");
        assert!(r.may_overlap(&b(15), &b(15)));
        assert!(r.may_overlap(&b(20), &b(99)), "touches the high end");
        assert!(!r.may_overlap(&b(0), &b(9)));
        assert!(!r.may_overlap(&b(21), &b(99)));
        assert!(
            r.may_overlap(&[1, 2], &b(0)),
            "malformed bounds never prune"
        );

        let mut s = KeyRange::Empty;
        let v = StringArray::from(vec!["m", "p"]);
        s.fold(&KeyArr::Str(&v), 0);
        s.fold(&KeyArr::Str(&v), 1);
        assert!(s.may_overlap(b"a", b"n"));
        assert!(!s.may_overlap(b"a", b"l"));
        assert!(!s.may_overlap(b"q", b"z"));
    }

    /// A local table read `readers` files at a time, whatever the machine, counting
    /// size lookups.
    struct Readers<'a>(
        &'a crate::LocalTableFiles,
        usize,
        std::sync::atomic::AtomicU64,
    );

    impl TableFiles for Readers<'_> {
        fn read(&self, path: &str) -> Result<Vec<u8>> {
            self.0.read(path)
        }
        fn size(&self, path: &str) -> Result<u64> {
            self.2.fetch_add(1, Ordering::Relaxed);
            self.0.size(path)
        }
        fn read_ranges(
            &self,
            path: &str,
            ranges: &[std::ops::Range<u64>],
        ) -> Result<Vec<bytes::Bytes>> {
            self.0.read_ranges(path, ranges)
        }
        fn readers(&self) -> usize {
            self.1
        }
    }

    #[test]
    fn a_merge_finds_the_same_rows_however_many_files_it_reads_at_once() {
        use shapeshift_core::Sink;
        let dir = std::env::temp_dir().join(format!("ss-merge-readers-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = |ids: Vec<i64>, v: i64| {
            let vs = vec![v; ids.len()];
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap()
        };
        // Eight merges of overlapping keys: eight data files, seven delete files.
        for round in 0..8i64 {
            let ids: Vec<i64> = (0..400).map(|i| (i * 7 + round * 131) % 1500).collect();
            let mut sink = crate::IcebergSink::create_merge(
                &dir,
                schema.clone(),
                Compression::Snappy,
                &[],
                &["id".to_string()],
            )
            .unwrap();
            sink.write_batch(&batch(ids, round)).unwrap();
            sink.finish().unwrap();
        }

        let prior = crate::load_current_local(&dir).unwrap();
        let location = prior.metadata["location"].as_str().unwrap().to_string();
        let local = crate::LocalTableFiles::new(&dir, crate::Anchors::of(&prior.metadata));
        let fields = crate::table_fields(&prior.metadata).unwrap();
        let delta = batch((0..1500).step_by(3).collect(), 99);
        let run = |readers: usize| {
            let keys = resolve_key_columns(&fields, &["id".into()]).unwrap();
            let mut tracker = MergeTracker::new(keys);
            let placed: Vec<Placement> = (0..delta.num_rows() as u64).map(|i| (0, i)).collect();
            tracker.observe(&delta, None, &placed).unwrap();
            let staging = dir.join(format!("staging-{readers}"));
            let cx = MergeContext {
                staging_dir: &staging,
                base_location: &location,
                prior: Some(&prior),
                new_files: &[],
                partition_fields: &[],
                compression: Compression::Snappy,
                snapshot_id: 1,
                sequence_number: 1,
            };
            let files = Readers(&local, readers, Default::default());
            let out = tracker.finish(&cx, &files).unwrap();
            // Every file opened at the size its manifest records: no lookups.
            assert_eq!(files.2.load(Ordering::Relaxed), 0);
            let mut rows: Vec<(String, i64)> = Vec::new();
            for f in &out.delete_files {
                let file = File::open(staging.join(&f.data_key)).unwrap();
                for b in
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                        .unwrap()
                        .build()
                        .unwrap()
                {
                    let b = b.unwrap();
                    let paths = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let pos = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    rows.extend(
                        (0..b.num_rows()).map(|i| (paths.value(i).to_string(), pos.value(i))),
                    );
                }
            }
            (out.summary, rows)
        };
        let one = run(1);
        assert_eq!(one.0.data_files_scanned, 8);
        assert!(one.0.keys_updated > 0 && one.0.prior_rows_replaced == one.0.keys_updated);
        assert_eq!(one.1.len() as u64, one.0.prior_rows_replaced);
        for readers in [2, 3, 8, 64] {
            assert_eq!(run(readers), one, "{readers} readers");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_last_occurrence_of_a_key_wins_within_a_write() {
        let fields = vec![ice(1, "id", "long")];
        let keys = resolve_key_columns(&fields, &["id".into()]).unwrap();
        let mut t = MergeTracker::new(keys);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(1),
                None,
                Some(1),
            ]))],
        )
        .unwrap();
        let placed: Vec<Placement> = (0..5).map(|i| (0, i)).collect();
        t.observe(&batch, None, &placed).unwrap();
        assert_eq!(t.latest.len(), 2);
        assert_eq!(t.null_key_rows, 1);
        // Rows 0 and 2 (both id=1) lose to row 4.
        assert_eq!(t.superseded, vec![(0, 0), (0, 2)]);
        assert_eq!(t.latest[t.index[&1i64.to_le_bytes()[..]] as usize].pos, 4);
    }
}
