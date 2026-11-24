//! Compaction: `shapeshift compact`.
//!
//! Every merge leaves a table a little more expensive to read. It adds data files and a
//! position-delete file, and a reader must load every delete file and skip the rows they
//! name before it can return a single live row. Appends add files too, one per partition
//! per run. Compaction pays that debt down in one snapshot:
//!
//! - **Every data file that carries a delete is rewritten** with the deleted rows dropped,
//!   so afterwards the table has **no delete files** at all.
//! - **Small files are packed together**, per partition, into files of up to the target
//!   size (`target_file_size`; `0` packs nothing and only applies deletes).
//! - The snapshot is an Iceberg **`replace`**: the table's rows are exactly what they
//!   were, only the files holding them change. Its manifest list names **one** data
//!   manifest, holding the files it wrote (ADDED), the files it kept (EXISTING, with their
//!   original sequence numbers), and the files it removed (DELETED). A delete manifest
//!   records the delete files it removed. However many manifests the table had built up,
//!   a reader of the new snapshot opens one.
//!
//! Earlier snapshots are untouched: their manifests still list the old files, which stay
//! on disk, so time travel reads exactly what it read before. Removing files no snapshot
//! needs any more is snapshot expiry's job (the `expire` module), not this one's.
//!
//! **Reads ahead, writes in order.** One writer copies the files' rows in table order, one
//! output row group at a time. Up to [`TableFiles::readers`] readers fetch the files ahead
//! of it — each takes the next file, its footer and then its row groups — with about
//! 16 MiB of fetched bytes waiting (`read_ahead`), so against an object store the requests
//! overlap while the files written stay exactly what one reader would write. The positions
//! to drop come from the delete files, as a merge reads them.
//!
//! **Concurrent writers.** A compaction runs longest of any commit, so it is the likeliest
//! to lose a race: if another writer commits while it runs, the commit guard refuses it and
//! the files it wrote are removed.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use arrow_array::{new_null_array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use parquet::arrow::ProjectionMask;
use uuid::Uuid;

use shapeshift_core::{Compression, Result, ShapeError};

use crate::avro::{self, DataFileMeta, EntryOut, EntryStatus, ManifestContent, ManifestEntry};
use crate::files::{read_ahead, AheadFile, TableFiles, READ_AHEAD_BYTES};
use crate::merge::live_files;
use crate::metadata::{self, AppendInput, SnapshotCounts};
use crate::partition::{partition_dir, PartVal, PartitionDataFile, PartitionField};
use crate::{
    arrow_schema_for, join_location, last_sequence_number, snapshot_id_from_uuid, table_fields,
    table_partition_fields, IceField, IcebergDataWriter, ManifestFileEntry, MetadataArtifacts,
    PriorTable,
};

/// Iceberg's default target data-file size (`write.target-file-size-bytes`).
pub const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;

/// Rows per row group in a rewritten file — the shaper's own default.
pub const DEFAULT_ROW_GROUP_ROWS: usize = 50_000;

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// How to compact.
#[derive(Debug, Clone)]
pub struct CompactOptions {
    /// Files smaller than this are packed together, per partition, into files of up to
    /// about this size (measured by the live share of the inputs). `0` packs nothing: only
    /// files carrying deletes are rewritten, each on its own.
    pub target_file_size: u64,
    /// The codec of the rewritten files.
    pub compression: Compression,
    /// Rows per row group in the rewritten files.
    pub row_group_rows: usize,
}

impl Default for CompactOptions {
    fn default() -> Self {
        CompactOptions {
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            compression: Compression::Snappy,
            row_group_rows: DEFAULT_ROW_GROUP_ROWS,
        }
    }
}

/// What a compaction did. Every count is exact.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactSummary {
    /// The `replace` snapshot committed; `None` when there was nothing to compact.
    pub snapshot_id: Option<i64>,
    /// The table version it committed (`v{N}.metadata.json`); `None` likewise.
    pub version: Option<u64>,
    /// Data files rewritten (removed from the table).
    pub data_files_rewritten: u64,
    /// Data files written in their place.
    pub data_files_written: u64,
    /// Data files left as they were.
    pub data_files_kept: u64,
    /// Delete files removed. After a compaction the table has none.
    pub delete_files_removed: u64,
    /// Rows the removed delete files had deleted, now gone from the data files too.
    pub position_deletes_applied: u64,
    /// Rows copied into the new files.
    pub rows_rewritten: u64,
    /// Bytes of the data files rewritten, and of the files written.
    pub bytes_rewritten: u64,
    pub bytes_written: u64,
    /// Partitions that had at least one file rewritten.
    pub partitions_compacted: u64,
}

/// What a compaction needs to know about the table and where to stage its files.
pub struct CompactContext<'a> {
    /// Where the new data files are written, at their table-relative keys: the table
    /// directory for a local table, a temp directory an object-store table uploads from.
    pub staging_dir: &'a Path,
    /// The location this snapshot's paths are anchored at.
    pub base_location: &'a str,
    /// The table's current snapshot.
    pub table: &'a PriorTable,
    pub options: &'a CompactOptions,
}

/// The result: files staged and the metadata that commits them. `artifacts` is `None`
/// when there was nothing to compact (no delete files, and no partition with two or more
/// small files); nothing was written then, and nothing should be committed.
pub struct CompactOutcome {
    pub data_files: Vec<PartitionDataFile>,
    pub artifacts: Option<MetadataArtifacts>,
    pub summary: CompactSummary,
}

/// One live data file and the positions deleted out of it.
struct Candidate {
    entry: ManifestEntry,
    deleted: Vec<u64>,
}

impl Candidate {
    fn live_rows(&self) -> u64 {
        (self.entry.record_count.max(0) as u64).saturating_sub(self.deleted.len() as u64)
    }

    /// The bytes this file's live rows will take once rewritten — its size, scaled by the
    /// share of its rows still live.
    fn live_bytes(&self) -> u64 {
        let rows = self.entry.record_count.max(0) as u64;
        if rows == 0 {
            return 0;
        }
        (self.entry.file_size.max(0) as u128 * self.live_rows() as u128 / rows as u128) as u64
    }
}

/// Plan and run a compaction of the table's current snapshot: read it through `files`,
/// write the new data files under `cx.staging_dir`, and build the `replace` snapshot's
/// metadata. The caller persists the files and then the metadata (version hint last).
pub fn compact<F: TableFiles>(cx: &CompactContext, files: &F) -> Result<CompactOutcome> {
    let meta = &cx.table.metadata;
    let ice_fields = table_fields(meta)?;
    let partition_fields = table_partition_fields(meta, &ice_fields)?;
    let live = live_files(files, cx.table)?;
    let target = cx.options.target_file_size;

    // Group the live data files by partition, in manifest order.
    let mut deletes = live.deletes;
    let mut order: Vec<Vec<PartVal>> = Vec::new();
    let mut groups: HashMap<Vec<PartVal>, Vec<Candidate>> = HashMap::new();
    for entry in live.data {
        let deleted = deletes.remove(&entry.file_path).unwrap_or_default();
        let group = groups.entry(entry.partition.clone()).or_insert_with(|| {
            order.push(entry.partition.clone());
            Vec::new()
        });
        group.push(Candidate { entry, deleted });
    }
    // What is left in `deletes` names files that are not live: dropping those delete
    // rows changes nothing a reader sees.

    // Bins of files to rewrite together, and the files kept as they are.
    let mut bins: Vec<Vec<Candidate>> = Vec::new();
    let mut kept: Vec<ManifestEntry> = Vec::new();
    for partition in &order {
        let group = groups.remove(partition).unwrap_or_default();
        // Files this partition will rewrite or could: those carrying deletes, and the
        // small ones.
        let rewritable = group
            .iter()
            .filter(|c| !c.deleted.is_empty() || (c.entry.file_size.max(0) as u64) < target)
            .count();
        let mut bin: Vec<Candidate> = Vec::new();
        let mut bin_bytes = 0u64;
        let mut close = |bin: &mut Vec<Candidate>, kept: &mut Vec<ManifestEntry>| {
            let one_clean = bin.len() == 1 && bin[0].deleted.is_empty();
            match std::mem::take(bin) {
                b if one_clean => kept.extend(b.into_iter().map(|c| c.entry)),
                b if !b.is_empty() => bins.push(b),
                _ => {}
            }
        };
        for c in group {
            let has_deletes = !c.deleted.is_empty();
            // A clean small file is worth rewriting only if there is another file to pack
            // it with. A file carrying deletes is always rewritten.
            let is_small = (c.entry.file_size.max(0) as u64) < target && rewritable >= 2;
            if !has_deletes && !is_small {
                kept.push(c.entry);
                continue;
            }
            let bytes = c.live_bytes();
            if !bin.is_empty() && bin_bytes + bytes > target {
                close(&mut bin, &mut kept);
                bin_bytes = 0;
            }
            bin_bytes += bytes;
            bin.push(c);
        }
        close(&mut bin, &mut kept);
    }

    if bins.is_empty() && live.delete_files.is_empty() {
        return Ok(CompactOutcome {
            data_files: Vec::new(),
            artifacts: None,
            summary: CompactSummary {
                data_files_kept: kept.len() as u64,
                ..CompactSummary::default()
            },
        });
    }

    let schema = arrow_schema_for(&ice_fields)?;
    let layout = Layout {
        ice_fields: &ice_fields,
        partition_fields: &partition_fields,
        schema: &schema,
    };
    let mut written: Vec<PartitionDataFile> = Vec::new();
    let mut removed: Vec<ManifestEntry> = Vec::new();
    let mut summary = CompactSummary::default();
    // Every file this compaction starts writing, finished or not.
    let mut staged: Vec<String> = Vec::new();
    let built = (|| -> Result<MetadataArtifacts> {
        // Every file to rewrite, bin after bin, and where each sits in its bin.
        let inputs: Vec<(&str, Option<u64>)> = bins
            .iter()
            .flatten()
            .map(|c| {
                let size = u64::try_from(c.entry.file_size).ok();
                (c.entry.file_path.as_str(), size)
            })
            .collect();
        let at: Vec<(usize, usize)> = bins
            .iter()
            .enumerate()
            .flat_map(|(b, bin)| (0..bin.len()).map(move |k| (b, k)))
            .collect();
        let mut bin_writer: Option<BinWriter> = None;
        read_ahead(
            files,
            &inputs,
            &ProjectionMask::all(),
            READ_AHEAD_BYTES,
            |i, file| {
                let (b, k) = at[i];
                let bin = &bins[b];
                let w = bin_writer.get_or_insert_with(|| BinWriter::new(cx, &layout, bin));
                w.copy(&bin[k], file, &mut staged)?;
                if k + 1 == bin.len() {
                    let w = bin_writer.take().expect("a bin being written");
                    summary.rows_rewritten += w.finish(&mut written, &mut staged)?;
                }
                Ok(())
            },
        )?;
        let mut partitions: HashSet<Vec<PartVal>> = HashSet::new();
        for bin in bins {
            partitions.insert(bin[0].entry.partition.clone());
            for c in bin {
                summary.bytes_rewritten += c.entry.file_size.max(0) as u64;
                removed.push(c.entry);
            }
        }
        summary.partitions_compacted = partitions.len() as u64;
        build_replace_artifacts(&ReplaceBuild {
            base_location: cx.base_location,
            table: cx.table,
            ice_fields: &ice_fields,
            partition_fields: &partition_fields,
            added: &written,
            kept: &kept,
            removed: &removed,
            removed_deletes: &live.delete_files,
            timestamp_ms: crate::snapshot_timestamp_ms(),
        })
    })();
    let artifacts = match built {
        Ok(a) => a,
        Err(e) => {
            // Leave nothing behind that no snapshot will ever name.
            for key in &staged {
                let _ = std::fs::remove_file(cx.staging_dir.join(key));
            }
            return Err(e);
        }
    };

    summary.data_files_rewritten = removed.len() as u64;
    summary.data_files_written = written.len() as u64;
    summary.data_files_kept = kept.len() as u64;
    summary.bytes_written = written.iter().map(|f| f.size.max(0) as u64).sum();
    summary.delete_files_removed = live.delete_files.len() as u64;
    summary.position_deletes_applied = live
        .delete_files
        .iter()
        .map(|d| d.record_count.max(0) as u64)
        .sum();
    summary.snapshot_id = Some(artifacts.snapshot_id);
    summary.version = Some(artifacts.version);
    Ok(CompactOutcome {
        data_files: written,
        artifacts: Some(artifacts),
        summary,
    })
}

/// The table's current layout: what a rewritten file is written as.
struct Layout<'a> {
    ice_fields: &'a [IceField],
    partition_fields: &'a [PartitionField],
    schema: &'a SchemaRef,
}

/// Copies the live rows of one bin's files, in order, into a new file of the table's
/// current schema. A bin whose rows were all deleted writes no file.
struct BinWriter<'c> {
    cx: &'c CompactContext<'c>,
    layout: &'c Layout<'c>,
    partition: Vec<PartVal>,
    writer: Option<(String, IcebergDataWriter)>,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

impl<'c> BinWriter<'c> {
    fn new(cx: &'c CompactContext<'c>, layout: &'c Layout<'c>, bin: &[Candidate]) -> Self {
        BinWriter {
            cx,
            layout,
            partition: bin[0].entry.partition.clone(),
            writer: None,
            pending: Vec::new(),
            pending_rows: 0,
        }
    }

    /// Copy the live rows of `c`, read from `file`. The key of a file is pushed to `staged`
    /// before the file is created, so a failure at any point can remove it.
    fn copy<F: TableFiles>(
        &mut self,
        c: &Candidate,
        file: &mut AheadFile<'_, '_, F>,
        staged: &mut Vec<String>,
    ) -> Result<()> {
        let path = c.entry.file_path.as_str();
        let schema = self.layout.schema;
        let columns = column_map(file.schema(), self.layout.ice_fields, path)?;
        let mut pos: u64 = 0;
        let mut next_deleted = 0usize;
        while let Some(group) = file.next_group()? {
            for batch in group {
                let batch = batch.map_err(|e| err(format!("{path}: {e}")))?;
                let n = batch.num_rows();
                let batch = drop_deleted(&batch, pos, &c.deleted, &mut next_deleted)?;
                pos += n as u64;
                if batch.num_rows() == 0 {
                    continue;
                }
                let batch = conform(&batch, schema, &columns, path)?;
                self.pending_rows += batch.num_rows();
                self.pending.push(batch);
                if self.pending_rows >= self.cx.options.row_group_rows.max(1) {
                    self.flush(staged)?;
                }
            }
        }
        if pos != c.entry.record_count.max(0) as u64 {
            return Err(err(format!(
                "{path}: the manifest records {} rows but the file holds {pos}; \
                 refusing to rewrite it",
                c.entry.record_count
            )));
        }
        Ok(())
    }

    fn flush(&mut self, staged: &mut Vec<String>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let Layout {
            ice_fields,
            partition_fields,
            schema,
        } = *self.layout;
        let batch = concat_batches(schema, self.pending.iter()).map_err(err)?;
        self.pending.clear();
        self.pending_rows = 0;
        if self.writer.is_none() {
            let key = format!(
                "{}/{}.parquet",
                partition_dir(partition_fields, &self.partition),
                Uuid::new_v4()
            );
            let path = self.cx.staging_dir.join(&key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(err)?;
            }
            staged.push(key.clone());
            let w = IcebergDataWriter::create_with_fields(
                &path,
                schema.clone(),
                self.cx.options.compression,
                ice_fields.to_vec(),
            )?;
            self.writer = Some((key, w));
        }
        self.writer.as_mut().unwrap().1.write_batch(&batch)
    }

    /// Close the bin's file and record it in `out`. Returns the rows written.
    fn finish(mut self, out: &mut Vec<PartitionDataFile>, staged: &mut Vec<String>) -> Result<u64> {
        self.flush(staged)?;
        let Some((key, mut w)) = self.writer else {
            return Ok(0);
        };
        let rows = w.close()?;
        let size = std::fs::metadata(self.cx.staging_dir.join(&key))
            .map(|m| m.len())
            .unwrap_or(0) as i64;
        out.push(PartitionDataFile {
            data_key: key,
            partition: self.partition,
            rows,
            size,
            stats: w.column_stats(),
        });
        Ok(rows)
    }
}

/// For each table field, the column of a data file that holds it: matched by Iceberg
/// field-id, or by name in a file written without ids. `None` for a column the file
/// predates (added by a later, additive schema change), which reads as null.
fn column_map(
    file_schema: &SchemaRef,
    ice_fields: &[IceField],
    path: &str,
) -> Result<Vec<Option<usize>>> {
    ice_fields
        .iter()
        .map(|f| {
            let by_id = file_schema.fields().iter().position(|c| {
                c.metadata()
                    .get("PARQUET:field_id")
                    .and_then(|v| v.parse::<i32>().ok())
                    == Some(f.id)
            });
            let found = by_id.or_else(|| {
                file_schema
                    .fields()
                    .iter()
                    .position(|c| c.name() == &f.name)
            });
            if found.is_none() && f.required {
                return Err(err(format!(
                    "{path}: has no column for the required field `{}`",
                    f.name
                )));
            }
            Ok(found)
        })
        .collect()
}

/// Drop the rows of `batch` (which starts at file position `start`) that `deleted` names.
/// `deleted` is sorted; `next` is a cursor into it, advanced across a file's batches.
fn drop_deleted(
    batch: &RecordBatch,
    start: u64,
    deleted: &[u64],
    next: &mut usize,
) -> Result<RecordBatch> {
    let end = start + batch.num_rows() as u64;
    while *next < deleted.len() && deleted[*next] < start {
        *next += 1;
    }
    if *next >= deleted.len() || deleted[*next] >= end {
        return Ok(batch.clone());
    }
    let mut keep = vec![true; batch.num_rows()];
    while *next < deleted.len() && deleted[*next] < end {
        keep[(deleted[*next] - start) as usize] = false;
        *next += 1;
    }
    filter_record_batch(batch, &BooleanArray::from(keep)).map_err(err)
}

/// A file's batch, laid out as the table's current schema: columns in schema order, a
/// column the file predates filled with nulls.
fn conform(
    batch: &RecordBatch,
    schema: &SchemaRef,
    columns: &[Option<usize>],
    path: &str,
) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, col)| match col {
            Some(i) => {
                let a = batch.column(*i).clone();
                if a.data_type() != field.data_type() {
                    return Err(err(format!(
                        "{path}: column `{}` is stored as {:?}, but the table says {:?}",
                        field.name(),
                        a.data_type(),
                        field.data_type()
                    )));
                }
                Ok(a)
            }
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(schema.clone(), arrays).map_err(err)
}

/// Inputs for the `replace` snapshot's metadata.
struct ReplaceBuild<'a> {
    base_location: &'a str,
    table: &'a PriorTable,
    ice_fields: &'a [IceField],
    partition_fields: &'a [PartitionField],
    added: &'a [PartitionDataFile],
    kept: &'a [ManifestEntry],
    removed: &'a [ManifestEntry],
    removed_deletes: &'a [ManifestEntry],
    timestamp_ms: i64,
}

/// Build the manifests, manifest list and metadata of a `replace` snapshot. Its manifest
/// list holds one data manifest (and a delete manifest recording the delete files it
/// removes); no earlier manifest is carried.
fn build_replace_artifacts(b: &ReplaceBuild) -> Result<MetadataArtifacts> {
    let meta = &b.table.metadata;
    let snapshot_id = snapshot_id_from_uuid(&Uuid::new_v4());
    let seq = last_sequence_number(meta)? + 1;
    let schema_json = metadata::schema_json(b.ice_fields).to_string();

    let added: Vec<DataFileMeta> = b
        .added
        .iter()
        .map(|f| DataFileMeta {
            path: join_location(b.base_location, &f.data_key),
            partition: f.partition.clone(),
            record_count: f.rows as i64,
            file_size: f.size,
            value_counts: f.stats.value_counts.clone(),
            null_counts: f.stats.null_counts.clone(),
            lower_bounds: f.stats.lower_bounds.clone(),
            upper_bounds: f.stats.upper_bounds.clone(),
        })
        .collect();
    let kept: Vec<DataFileMeta> = b.kept.iter().map(ManifestEntry::file_meta).collect();
    let removed: Vec<DataFileMeta> = b.removed.iter().map(ManifestEntry::file_meta).collect();
    let carried = |status, e: &ManifestEntry, file| EntryOut {
        status,
        snapshot_id: match status {
            EntryStatus::Existing => e.snapshot_id.unwrap_or(snapshot_id),
            _ => snapshot_id,
        },
        sequence_number: e.sequence_number,
        file_sequence_number: e.file_sequence_number,
        file,
    };
    let mut entries: Vec<EntryOut> = Vec::new();
    entries.extend(
        b.kept
            .iter()
            .zip(&kept)
            .map(|(e, f)| carried(EntryStatus::Existing, e, f)),
    );
    entries.extend(added.iter().map(|file| EntryOut {
        status: EntryStatus::Added,
        snapshot_id,
        sequence_number: None,
        file_sequence_number: None,
        file,
    }));
    entries.extend(
        b.removed
            .iter()
            .zip(&removed)
            .map(|(e, f)| carried(EntryStatus::Deleted, e, f)),
    );
    let manifest_bytes = avro::write_manifest_entries(
        &entries,
        b.partition_fields,
        &schema_json,
        ManifestContent::Data,
        *Uuid::new_v4().as_bytes(),
    );
    let manifest_key = format!("metadata/{}-m0.avro", Uuid::new_v4());

    let rows = |es: &[ManifestEntry]| -> i64 { es.iter().map(|e| e.record_count).sum() };
    let added_rows: i64 = b.added.iter().map(|f| f.rows as i64).sum();
    // The smallest data sequence number among the manifest's live files.
    let min_seq = b
        .kept
        .iter()
        .filter_map(|e| e.sequence_number)
        .chain(std::iter::once(seq))
        .min()
        .unwrap_or(seq);
    let mut manifests = vec![ManifestFileEntry {
        path: join_location(b.base_location, &manifest_key),
        length: manifest_bytes.len() as i64,
        partition_spec_id: 0,
        content: ManifestContent::Data.id(),
        sequence_number: seq,
        min_sequence_number: min_seq,
        added_snapshot_id: snapshot_id,
        added_files: b.added.len() as i32,
        existing_files: b.kept.len() as i32,
        deleted_files: b.removed.len() as i32,
        added_rows,
        existing_rows: rows(b.kept),
        deleted_rows: rows(b.removed),
    }];

    let delete_manifest = if b.removed_deletes.is_empty() {
        None
    } else {
        let metas: Vec<DataFileMeta> = b
            .removed_deletes
            .iter()
            .map(ManifestEntry::file_meta)
            .collect();
        let entries: Vec<EntryOut> = b
            .removed_deletes
            .iter()
            .zip(&metas)
            .map(|(e, f)| carried(EntryStatus::Deleted, e, f))
            .collect();
        let bytes = avro::write_manifest_entries(
            &entries,
            b.partition_fields,
            &schema_json,
            ManifestContent::PositionDeletes,
            *Uuid::new_v4().as_bytes(),
        );
        let key = format!("metadata/{}-m1.avro", Uuid::new_v4());
        manifests.push(ManifestFileEntry {
            path: join_location(b.base_location, &key),
            length: bytes.len() as i64,
            partition_spec_id: 0,
            content: ManifestContent::PositionDeletes.id(),
            sequence_number: seq,
            min_sequence_number: seq,
            added_snapshot_id: snapshot_id,
            added_files: 0,
            existing_files: 0,
            deleted_files: b.removed_deletes.len() as i32,
            added_rows: 0,
            existing_rows: 0,
            deleted_rows: rows(b.removed_deletes),
        });
        Some((key, bytes))
    };

    let manifest_list_bytes = avro::write_manifest_list(&manifests, *Uuid::new_v4().as_bytes());
    let manifest_list_key = format!("metadata/snap-{snapshot_id}-1-{}.avro", Uuid::new_v4());
    let manifest_list_uri = join_location(b.base_location, &manifest_list_key);

    let counts = SnapshotCounts {
        added_data_files: b.added.len() as i64,
        added_records: added_rows,
        removed_data_files: b.removed.len() as i64,
        removed_records: rows(b.removed),
        removed_delete_files: b.removed_deletes.len() as i64,
        removed_position_deletes: rows(b.removed_deletes),
        total_data_files: (b.kept.len() + b.added.len()) as i64,
        total_records: rows(b.kept) + added_rows,
        total_delete_files: 0,
        total_position_deletes: 0,
        replace: true,
        ..SnapshotCounts::default()
    };
    let version = b.table.version + 1;
    let metadata_value = metadata::append_snapshot(&AppendInput {
        prior: meta,
        base_location: b.base_location,
        prior_version: b.table.version,
        snapshot_id,
        sequence_number: seq,
        timestamp_ms: b.timestamp_ms,
        manifest_list_path: &manifest_list_uri,
        counts,
        schema_fields: b.ice_fields,
    })?;
    let metadata_bytes = serde_json::to_vec_pretty(&metadata_value).map_err(err)?;

    Ok(MetadataArtifacts {
        manifest_key,
        manifest_bytes,
        manifest_list_key,
        manifest_list_bytes,
        metadata_key: format!("metadata/v{version}.metadata.json"),
        metadata_bytes,
        version_hint_key: "metadata/version-hint.text".to_string(),
        version_hint_bytes: version.to_string().into_bytes(),
        delete_manifest,
        snapshot_id,
        version,
    })
}

/// The staged path of each file a compaction wrote, for the caller to persist.
pub fn staged_paths(staging_dir: &Path, files: &[PartitionDataFile]) -> Vec<(String, PathBuf)> {
    files
        .iter()
        .map(|f| (f.data_key.clone(), staging_dir.join(&f.data_key)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use shapeshift_core::Sink;

    use super::*;

    /// A local table read `readers` files at a time. Each fetch takes a moment, as an object
    /// store's does, so fetches that can overlap do; the most fetches of the `data` files in
    /// flight at once is counted (the delete files are read in parallel before them). Until two
    /// data-file fetches have overlapped once, one waits up to 20 ms for company.
    struct Slow<'a> {
        inner: &'a crate::LocalTableFiles,
        readers: usize,
        data: &'a HashSet<String>,
        now: AtomicUsize,
        max: AtomicUsize,
    }

    impl TableFiles for Slow<'_> {
        fn read(&self, path: &str) -> Result<Vec<u8>> {
            self.inner.read(path)
        }
        fn size(&self, path: &str) -> Result<u64> {
            self.inner.size(path)
        }
        fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<bytes::Bytes>> {
            let data = self.data.contains(path);
            if data {
                let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
                self.max.fetch_max(n, Ordering::SeqCst);
                let until = std::time::Instant::now() + std::time::Duration::from_millis(20);
                while self.max.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < until {
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
            let got = self.inner.read_ranges(path, ranges);
            if data {
                self.now.fetch_sub(1, Ordering::SeqCst);
            }
            got
        }
        fn readers(&self) -> usize {
            self.readers
        }
    }

    #[test]
    fn a_compaction_writes_the_same_files_however_many_readers_read_ahead() {
        let dir = std::env::temp_dir().join(format!("ss-compact-ahead-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        // Twelve merges of overlapping keys: twelve data files, eleven delete files. The
        // first is big enough that its row groups are fetched past the footer's read.
        for round in 0..12i64 {
            let n = if round == 0 { 30_000 } else { 400 };
            let ids: Vec<i64> = (0..n).map(|i| (i * 7 + round * 131) % 30_000).collect();
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
        }

        let table = crate::load_current_local(&dir).unwrap();
        let location = table.metadata["location"].as_str().unwrap().to_string();
        let local = crate::LocalTableFiles::new(&dir, &location);
        let data: HashSet<String> = live_files(&local, &table)
            .unwrap()
            .data
            .into_iter()
            .map(|e| e.file_path)
            .collect();
        assert_eq!(data.len(), 12);
        // Packing everything into one file, and a target small enough for many bins.
        for target in [DEFAULT_TARGET_FILE_SIZE, 8 * 1024] {
            let run = |readers: usize| {
                let staging = dir.join(format!("staging-{target}-{readers}"));
                let options = CompactOptions {
                    target_file_size: target,
                    ..CompactOptions::default()
                };
                let cx = CompactContext {
                    staging_dir: &staging,
                    base_location: &location,
                    table: &table,
                    options: &options,
                };
                let files = Slow {
                    inner: &local,
                    readers,
                    data: &data,
                    now: AtomicUsize::new(0),
                    max: AtomicUsize::new(0),
                };
                let out = compact(&cx, &files).unwrap();
                // Every file written, its partition and its rows in order.
                let written: Vec<(String, Vec<(i64, i64)>)> = out
                    .data_files
                    .iter()
                    .map(|f| {
                        let file = std::fs::File::open(staging.join(&f.data_key)).unwrap();
                        let mut rows = Vec::new();
                        for b in ParquetRecordBatchReaderBuilder::try_new(file)
                            .unwrap()
                            .build()
                            .unwrap()
                        {
                            let b = b.unwrap();
                            let col = |i: usize| {
                                b.column(i)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .clone()
                            };
                            let (ids, vs) = (col(0), col(1));
                            rows.extend((0..b.num_rows()).map(|r| (ids.value(r), vs.value(r))));
                        }
                        (format!("{:?}", f.partition), rows)
                    })
                    .collect();
                let mut summary = out.summary;
                summary.snapshot_id = None; // random per run
                std::fs::remove_dir_all(&staging).ok();
                // How many readers ran (`SHAPESHIFT_READERS` can say otherwise).
                let ran = crate::files::readers_for(&files);
                ((summary, written), files.max.load(Ordering::SeqCst), ran)
            };
            let (one, ..) = run(1);
            assert_eq!(one.0.data_files_rewritten, 12, "target {target}");
            assert_eq!(one.0.delete_files_removed, 11);
            assert_eq!(one.0.rows_rewritten, 30_000);
            if target < DEFAULT_TARGET_FILE_SIZE {
                assert!(one.1.len() > 2, "{} files written", one.1.len());
            } else {
                assert_eq!(one.1.len(), 1);
            }
            for readers in [2, 3, 8, 64] {
                let (got, max, ran) = run(readers);
                assert_eq!(got, one, "target {target}, {readers} readers");
                assert!(max <= ran, "{max} data-file reads in flight, {ran} readers");
                if ran > 1 {
                    assert!(
                        max > 1,
                        "{ran} readers, but the data files' reads never overlapped"
                    );
                }
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
