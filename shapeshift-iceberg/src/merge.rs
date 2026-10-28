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
//! same merge; only the byte source differs.
//!
//! **Single writer.** Like `--append`, a merge assumes nothing else commits to the table
//! while it runs; serialising concurrent writers is a catalog's job.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use shapeshift_core::{Compression, MergeSummary, Result, ShapeError};

use crate::avro::{read_manifest_entries, ManifestEntry};
use crate::files::{RangedParquet, TableFiles};
use crate::partition::{
    partition_dir, PartVal, PartitionDataFile, PartitionField, PartitionedWriter, Placement,
};
use crate::stats::StatsCollector;
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

/// Where the latest occurrence of a key in this write landed, and whether the key
/// matched a row already in the table.
struct Latest {
    slot: u32,
    matched: bool,
    pos: u64,
}

/// The write half of a merge, shared by the local and object-store sinks. Rows go to the
/// partitioned writer exactly as an append's do, and their keys and placements go to the
/// tracker; [`MergeWriter::finish`] then finds and deletes what they supersede.
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

    /// Write one batch through `writer`, recording its rows' keys and placements.
    pub fn write_batch(
        &mut self,
        writer: &mut PartitionedWriter,
        batch: &RecordBatch,
    ) -> Result<()> {
        writer.write_batch_placed(batch, Some(&mut self.placed))?;
        self.tracker.observe(batch, &self.placed)
    }

    /// Find every row the write supersedes, reading the existing table through `files`,
    /// and write the position-delete files that remove them under `cx.staging_dir`.
    pub fn finish<F: TableFiles>(self, cx: &MergeContext, files: &F) -> Result<MergeOutcome> {
        self.tracker.finish(cx, files)
    }
}

/// Follows the keys of one merge write as its batches are written.
struct MergeTracker {
    keys: Vec<KeyColumn>,
    latest: HashMap<Box<[u8]>, Latest>,
    /// Earlier occurrences of a key this write carried again — deleted at finish.
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
            latest: HashMap::new(),
            superseded: Vec::new(),
            ranges,
            null_key_rows: 0,
            scratch: Vec::new(),
        }
    }

    /// Record one written batch. `placed[i]` is where row `i` landed.
    fn observe(&mut self, batch: &RecordBatch, placed: &[Placement]) -> Result<()> {
        let arrs = self
            .keys
            .iter()
            .map(|k| KeyArr::new(batch.column(k.index).as_ref(), k.kind))
            .collect::<Result<Vec<_>>>()?;
        for (row, &(slot, pos)) in placed.iter().enumerate().take(batch.num_rows()) {
            if !encode_row(&arrs, row, &mut self.scratch) {
                self.null_key_rows += 1;
                continue;
            }
            for (r, a) in self.ranges.iter_mut().zip(&arrs) {
                r.fold(a, row);
            }
            match self.latest.get_mut(self.scratch.as_slice()) {
                Some(l) => {
                    self.superseded.push((l.slot, l.pos));
                    l.slot = slot;
                    l.pos = pos;
                }
                None => {
                    let key = self.scratch.clone().into_boxed_slice();
                    self.latest.insert(
                        key,
                        Latest {
                            slot,
                            matched: false,
                            pos,
                        },
                    );
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

    /// Read one existing data file's key columns and return the positions of its live
    /// rows whose key this write carries, marking those keys as updates. `deleted` is the
    /// file's already-deleted positions (sorted): a row a previous merge removed is not
    /// live, so it is neither matched nor deleted twice. Only the footer and, one row
    /// group at a time, the key columns' chunks are fetched.
    fn scan_file<F: TableFiles>(
        &mut self,
        files: &F,
        path: &str,
        deleted: &[u64],
    ) -> Result<Vec<u64>> {
        let pq = RangedParquet::open(files, path)?;
        let schema = pq.schema().clone();
        // Locate each key column by its Iceberg field-id (the name is the fallback for a
        // file written without ids). A file without a key column predates it, so every
        // row has a null key there and nothing in the file can match.
        let mut roots = Vec::with_capacity(self.keys.len());
        for k in &self.keys {
            let by_id = schema.fields().iter().position(|f| {
                f.metadata()
                    .get("PARQUET:field_id")
                    .and_then(|v| v.parse::<i32>().ok())
                    == Some(k.field_id)
            });
            match by_id.or_else(|| schema.fields().iter().position(|f| f.name() == &k.name)) {
                Some(i) => roots.push(i),
                None => return Ok(Vec::new()),
            }
        }
        // A projection keeps the file's column order, so map each key to its rank.
        let mut sorted = roots.clone();
        sorted.sort_unstable();
        let cols: Vec<usize> = roots
            .iter()
            .map(|r| sorted.binary_search(r).unwrap())
            .collect();
        let mask = ProjectionMask::roots(pq.parquet_schema(), sorted.iter().copied());

        let mut hits = Vec::new();
        let mut pos: u64 = 0;
        let mut next_deleted = deleted.iter().peekable();
        for rg in 0..pq.num_row_groups() {
            for batch in pq.row_group(rg, &mask)? {
                let batch = batch.map_err(err)?;
                let arrs = self
                    .keys
                    .iter()
                    .zip(&cols)
                    .map(|(k, &c)| KeyArr::new(batch.column(c).as_ref(), k.kind))
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..batch.num_rows() {
                    let here = pos;
                    pos += 1;
                    while next_deleted.next_if(|&&d| d < here).is_some() {}
                    if next_deleted.next_if(|&&d| d == here).is_some() {
                        continue;
                    }
                    if !encode_row(&arrs, row, &mut self.scratch) {
                        continue;
                    }
                    if let Some(l) = self.latest.get_mut(self.scratch.as_slice()) {
                        l.matched = true;
                        hits.push(here);
                    }
                }
            }
        }
        Ok(hits)
    }

    /// Find every row this write supersedes, write the position-delete files that remove
    /// them (under `staging_dir`, one per partition), and report what the merge did.
    fn finish<F: TableFiles>(mut self, cx: &MergeContext, files: &F) -> Result<MergeOutcome> {
        let mut targets: Vec<Target> = Vec::new();
        let mut scanned = 0u64;
        let mut pruned = 0u64;
        let mut prior_rows_replaced = 0u64;

        // With no non-null key in the write there is nothing to match: skip the scan
        // rather than report every file as "pruned".
        if let Some(prior) = cx.prior.filter(|_| !self.latest.is_empty()) {
            let live = live_files(files, prior)?;
            for entry in &live.data {
                if !self.may_hold(entry) {
                    pruned += 1;
                    continue;
                }
                scanned += 1;
                let deleted = live
                    .deletes
                    .get(&entry.file_path)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                let positions = self.scan_file(files, &entry.file_path, deleted)?;
                if !positions.is_empty() {
                    prior_rows_replaced += positions.len() as u64;
                    targets.push(Target {
                        path: entry.file_path.clone(),
                        partition: entry.partition.clone(),
                        positions,
                    });
                }
            }
        }

        // Earlier occurrences of a key within this write, in the files it just wrote.
        let duplicates_in_input = self.superseded.len() as u64;
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

        let delete_files = write_delete_files(cx, targets)?;
        let keys_updated = self.latest.values().filter(|l| l.matched).count() as u64;
        let distinct_keys = self.latest.len() as u64;
        let summary = MergeSummary {
            key_columns: self.keys.iter().map(|k| k.name.clone()).collect(),
            distinct_keys,
            keys_updated,
            keys_inserted: distinct_keys - keys_updated,
            prior_rows_replaced,
            duplicates_in_input,
            null_key_rows: self.null_key_rows,
            data_files_scanned: scanned,
            data_files_pruned: pruned,
            delete_files: delete_files.len() as u64,
            position_deletes: prior_rows_replaced + duplicates_in_input,
        };
        Ok(MergeOutcome {
            delete_files,
            summary,
        })
    }
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
}

/// The delete files a merge wrote, and its report.
pub struct MergeOutcome {
    pub delete_files: Vec<PartitionDataFile>,
    pub summary: MergeSummary,
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
    /// Deleted positions per data-file path (sorted, deduplicated), from the live
    /// position-delete files.
    pub deletes: HashMap<String, Vec<u64>>,
}

/// Read a table's live data files and the positions its delete files remove. Refuses a
/// table carrying equality deletes: shapeshift neither writes nor applies them, so it
/// could not tell which rows are live.
pub(crate) fn live_files<F: TableFiles>(files: &F, prior: &PriorTable) -> Result<LiveFiles> {
    let mut data = Vec::new();
    let mut delete_files = Vec::new();
    for m in &prior.manifests {
        for entry in read_manifest_entries(&files.read(&m.path)?)? {
            if !entry.is_live() {
                continue;
            }
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
    for d in &delete_files {
        read_position_deletes(files, &d.file_path, &mut deletes)?;
    }
    for positions in deletes.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    Ok(LiveFiles { data, deletes })
}

/// Read a position-delete file's `(file_path, pos)` rows into `out`.
fn read_position_deletes<F: TableFiles>(
    files: &F,
    path: &str,
    out: &mut HashMap<String, Vec<u64>>,
) -> Result<()> {
    let pq = RangedParquet::open(files, path)?;
    let all = ProjectionMask::all();
    let mut batches = Vec::new();
    for rg in 0..pq.num_row_groups() {
        batches.extend(pq.row_group(rg, &all)?);
    }
    for batch in batches {
        let batch = batch.map_err(err)?;
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
    Ok(())
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
        t.observe(&batch, &placed).unwrap();
        assert_eq!(t.latest.len(), 2);
        assert_eq!(t.null_key_rows, 1);
        // Rows 0 and 2 (both id=1) lose to row 4.
        assert_eq!(t.superseded, vec![(0, 0), (0, 2)]);
        assert_eq!(t.latest[&1i64.to_le_bytes()[..]].pos, 4);
    }
}
