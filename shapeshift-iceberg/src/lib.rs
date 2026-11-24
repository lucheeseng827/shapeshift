//! # shapeshift-iceberg — the Apache Iceberg v2 sink
//!
//! Writes a self-contained Iceberg v2 table around the Parquet writer — no catalog
//! server required. One shape produces one atomic append snapshot:
//!
//! ```text
//! <table>/
//!   data/<uuid>.parquet                 data file (Parquet, carrying PARQUET:field_id)
//!   metadata/<uuid>-m0.avro             manifest (lists data files)
//!   metadata/snap-<id>-1-<uuid>.avro    manifest list (lists manifests)
//!   metadata/v1.metadata.json           table metadata (v2)
//!   metadata/version-hint.text          → 1
//! ```
//!
//! The data file's Arrow schema is rewritten to carry `PARQUET:field_id` on every
//! column, matching the Iceberg schema's field-ids — the detail that makes the
//! Parquet genuinely Iceberg-readable rather than just Parquet-in-a-directory.
//!
//! ## Backend-agnostic building blocks
//!
//! The table is built from two pieces the caller can drive against any store:
//!
//! - [`IcebergDataWriter`] streams the data Parquet to a local file (one row group per
//!   batch, bounded RAM), stamping the field-ids.
//! - [`build_metadata_artifacts`] turns the field list + a **base location** (a local
//!   path *or* an object-store URI like `s3://bucket/db/table`) into the manifest,
//!   manifest-list, metadata JSON, and version-hint **as bytes with relative keys** —
//!   with every embedded path anchored at that location.
//!
//! [`IcebergSink`] composes them for the local filesystem; `shapeshift-objstore` reuses
//! the same two pieces to land a table in S3/GCS/Azure. Because the embedded paths are
//! `<base_location>/<key>`, the table reads back at its real location — a bucket
//! prefix, exactly like a server-less Hadoop catalog — instead of a `canonicalize`d
//! local path.
//!
//! One snapshot per run, composing with: **multi-snapshot appends** (`--append`,
//! with **additive schema evolution** — new optional columns keep prior columns'
//! field-ids stable), **identity + hidden (transform) partitioning**, **per-column
//! statistics** in the manifest, and **row-level upsert** ([`IcebergSink::create_merge`],
//! `--merge-on`): the incoming rows are appended and the rows they supersede are removed
//! with Iceberg v2 position deletes (see the `merge` module). [`compact_table`] pays the
//! merges back: it applies every delete and packs small files in one `replace` snapshot
//! (see the `compact` module), and [`expire_table`] lets old snapshots go with the files
//! only they read (see the `expire` module).

mod avro;
mod compact;
mod expire;
mod files;
mod merge;
mod metadata;
mod partition;
mod stats;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use shapeshift_core::{Compression, Result, ShapeError, Sink, SinkSummary};

use crate::avro::{DataFileMeta, ManifestContent};
use crate::metadata::{MetadataInput, SnapshotCounts};

pub use crate::avro::{
    read_manifest_data_files, read_manifest_entries, read_manifest_list, ManifestDataFile,
    ManifestEntry, ManifestFileEntry,
};
pub use crate::compact::{
    compact, staged_paths, CompactContext, CompactOptions, CompactOutcome, CompactSummary,
    DEFAULT_ROW_GROUP_ROWS, DEFAULT_TARGET_FILE_SIZE,
};
pub use crate::expire::{
    plan_expiry, DeleteTarget, ExpireCommit, ExpireOptions, ExpirePlan, ExpireSummary, FileKind,
    DEFAULT_MAX_SNAPSHOT_AGE_MS,
};
pub use crate::files::{LocalTableFiles, TableFiles};
pub use crate::merge::{resolve_key_columns, KeyColumn, MergeContext, MergeOutcome, MergeWriter};
pub use crate::metadata::{join_location, IceField};
pub use crate::partition::{
    parse_partition_expr, resolve_partition_fields, PartVal, PartitionDataFile, PartitionField,
    PartitionedWriter, Transform,
};
pub use crate::stats::Stats;

use crate::stats::StatsCollector;

/// Key-value pairs a data file carries in its Parquet footer, made as the file closes from
/// the number of rows it holds. Any Parquet reader sees them in the footer's key-value
/// metadata, so a writer can say where the rows in a file came from.
///
/// Only the data files a write produces carry them: a merge's position-delete files do
/// not, and [`compact`] writes its new files without them — a compacted file holds rows
/// of many writes, which no one write's footer describes.
pub type FileFooter = Arc<dyn Fn(u64) -> Vec<(String, String)> + Send + Sync>;

fn sink_err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// Map an Arrow type (only the six the shaper emits) to its Iceberg primitive name.
fn iceberg_type_of(dt: &DataType) -> Result<&'static str> {
    Ok(match dt {
        DataType::Boolean => "boolean",
        DataType::Int64 => "long",
        DataType::Float64 => "double",
        DataType::Utf8 => "string",
        DataType::Date32 => "date",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp",
        other => {
            return Err(ShapeError::Sink(format!(
                "no Iceberg mapping for Arrow type {other:?}"
            )))
        }
    })
}

/// The Arrow type a column of the given Iceberg primitive type is written as — the
/// inverse of [`iceberg_type_of`].
fn arrow_type_of(iceberg_type: &str) -> Result<DataType> {
    Ok(match iceberg_type {
        "boolean" => DataType::Boolean,
        "long" => DataType::Int64,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        other => {
            return Err(ShapeError::Sink(format!(
                "column type `{other}` is not one shapeshift writes"
            )))
        }
    })
}

/// The Arrow schema of a table's data files, built from its Iceberg fields (an optional
/// field is nullable).
pub(crate) fn arrow_schema_for(fields: &[IceField]) -> Result<SchemaRef> {
    let fields = fields
        .iter()
        .map(|f| {
            Ok(Field::new(
                &f.name,
                arrow_type_of(&f.iceberg_type)?,
                !f.required,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// A table's current schema, as Iceberg fields in schema order.
pub(crate) fn table_fields(meta: &serde_json::Value) -> Result<Vec<IceField>> {
    Ok(prior_schema_fields(meta)?
        .into_iter()
        .map(|(name, id, iceberg_type, required)| IceField {
            id,
            name,
            iceberg_type,
            required,
        })
        .collect())
}

/// A table's partition fields, read back from its partition spec and resolved against
/// its current schema exactly as a write resolves `partition_by`.
pub(crate) fn table_partition_fields(
    meta: &serde_json::Value,
    fields: &[IceField],
) -> Result<Vec<PartitionField>> {
    let empty = Vec::new();
    let spec = meta["partition-specs"][0]["fields"]
        .as_array()
        .unwrap_or(&empty);
    let mut exprs = Vec::with_capacity(spec.len());
    for pf in spec {
        let source_id = pf["source-id"].as_i64().unwrap_or(-1);
        let col = fields
            .iter()
            .find(|f| f.id as i64 == source_id)
            .map(|f| f.name.as_str())
            .ok_or_else(|| {
                ShapeError::Sink(format!(
                    "the partition spec names source column id {source_id}, which the \
                     current schema does not have"
                ))
            })?;
        let transform = pf["transform"].as_str().unwrap_or("");
        let arg = |prefix: &str| {
            transform
                .strip_prefix(prefix)
                .and_then(|s| s.strip_suffix(']'))
        };
        exprs.push(match transform {
            "identity" => col.to_string(),
            "year" | "month" | "day" | "hour" => format!("{transform}({col})"),
            _ => match (arg("bucket["), arg("truncate[")) {
                (Some(n), _) => format!("bucket({n}, {col})"),
                (_, Some(w)) => format!("truncate({w}, {col})"),
                _ => {
                    return Err(ShapeError::Sink(format!(
                        "partition transform `{transform}` is not one shapeshift writes"
                    )))
                }
            },
        });
    }
    let mut out = resolve_partition_fields(fields, &exprs)?;
    ensure_partition_compatible(&out, meta)?;
    // Keep the spec's own partition field-ids: the manifests' partition records use them.
    for (f, pf) in out.iter_mut().zip(spec) {
        if let Some(id) = pf["field-id"].as_i64() {
            f.field_id = id as i32;
        }
    }
    Ok(out)
}

/// Derive the Iceberg field list (ids `1..=N`, required = non-nullable) from the Arrow
/// schema every batch will carry.
pub fn ice_fields_from_schema(schema: &Schema) -> Result<Vec<IceField>> {
    let mut ice_fields = Vec::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        ice_fields.push(IceField {
            id: i as i32 + 1,
            name: f.name().clone(),
            iceberg_type: iceberg_type_of(f.data_type())?.to_string(),
            required: !f.is_nullable(),
        });
    }
    Ok(ice_fields)
}

/// Clone an Arrow schema, stamping `PARQUET:field_id` = 1..=N on each column so the
/// written Parquet carries the same field-ids as the Iceberg schema.
pub fn schema_with_field_ids(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let mut md = f.metadata().clone();
            md.insert("PARQUET:field_id".to_string(), (i as i32 + 1).to_string());
            f.as_ref().clone().with_metadata(md)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Clone an Arrow schema, stamping each column's `PARQUET:field_id` from the given
/// Iceberg fields (positionally aligned — they were derived from this schema). This is
/// the evolution-aware variant: after an append onto an evolved table, existing columns
/// keep their prior ids and new columns carry fresh ones, so the ids need not be 1..=N.
fn schema_with_field_ids_from(schema: &Schema, ice_fields: &[IceField]) -> SchemaRef {
    debug_assert_eq!(
        schema.fields().len(),
        ice_fields.len(),
        "schema and ice_fields must be positionally aligned — a silent zip truncation \
         here would stamp field-ids onto the wrong columns"
    );
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(ice_fields)
        .map(|(f, ice)| {
            let mut md = f.metadata().clone();
            md.insert("PARQUET:field_id".to_string(), ice.id.to_string());
            f.as_ref().clone().with_metadata(md)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn pq_compression(c: Compression) -> Result<PqCompression> {
    match c {
        Compression::Snappy => Ok(PqCompression::SNAPPY),
        Compression::Uncompressed => Ok(PqCompression::UNCOMPRESSED),
        // Keep both arms in sync with shapeshift-parquet's `pq_compression`.
        // The mapping is intentionally duplicated rather than shared, so
        // shapeshift-core stays free of a `parquet` dependency.
        #[cfg(feature = "zstd")]
        Compression::Zstd => Ok(PqCompression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).map_err(sink_err)?,
        )),
        #[cfg(not(feature = "zstd"))]
        Compression::Zstd => Err(ShapeError::Sink(
            "zstd is not compiled into this build (kept out for a musl-static binary); \
             use `snappy` or `uncompressed`, or rebuild with `--features zstd`"
                .into(),
        )),
    }
}

/// The Iceberg snapshot timestamp (epoch ms). This is the **single permitted
/// wall-clock read** in the whole engine — every other value is derived from the data.
/// Exposed so the object-store sink reads the same clock, in the same place.
pub fn snapshot_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Derive a positive i64 snapshot id from a fresh UUID.
fn snapshot_id_from_uuid(u: &Uuid) -> i64 {
    let b = u.as_bytes();
    let raw = i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    (raw & i64::MAX).max(1) // clear sign, avoid 0
}

/// Streams the data Parquet for one Iceberg table to a **local file**, one row group
/// per batch (bounded RAM), stamping the Iceberg field-ids onto the Parquet schema.
///
/// Shared by [`IcebergSink`] (which points it at `<table>/data/<uuid>.parquet`) and by
/// the object-store variant in `shapeshift-objstore` (which points it at a temp file it
/// then uploads). The two differ only in where this file lands and which paths the
/// metadata embeds — never in how the data is written.
pub struct IcebergDataWriter {
    data_schema: SchemaRef,
    ice_fields: Vec<IceField>,
    writer: Option<ArrowWriter<File>>,
    rows: u64,
    stats: StatsCollector,
    footer: Option<FileFooter>,
}

impl IcebergDataWriter {
    /// Open a writer streaming to `data_file` (a local path), deriving the Iceberg
    /// field list (positional ids 1..=N) and the field-id-stamped Parquet schema from
    /// `schema`.
    pub fn create(data_file: &Path, schema: SchemaRef, compression: Compression) -> Result<Self> {
        let ice_fields = ice_fields_from_schema(&schema)?;
        Self::create_with_fields(data_file, schema, compression, ice_fields)
    }

    /// Like [`IcebergDataWriter::create`], but with the Iceberg field list supplied —
    /// the evolution-aware path, where an appended table's existing columns keep their
    /// prior field-ids. The Parquet `PARQUET:field_id` stamps and the per-column stats
    /// keys both come from these ids.
    pub fn create_with_fields(
        data_file: &Path,
        schema: SchemaRef,
        compression: Compression,
        ice_fields: Vec<IceField>,
    ) -> Result<Self> {
        let data_schema = schema_with_field_ids_from(&schema, &ice_fields);
        let field_ids: Vec<i32> = ice_fields.iter().map(|f| f.id).collect();
        // Validate the codec BEFORE `File::create` truncates the target (a rejected
        // compression choice must not clobber an existing file).
        let props = WriterProperties::builder()
            .set_compression(pq_compression(compression)?)
            .build();
        let file = File::create(data_file).map_err(sink_err)?;
        let writer =
            ArrowWriter::try_new(file, data_schema.clone(), Some(props)).map_err(sink_err)?;
        Ok(IcebergDataWriter {
            data_schema,
            ice_fields,
            writer: Some(writer),
            rows: 0,
            stats: StatsCollector::new(&field_ids),
            footer: None,
        })
    }

    /// Have the file carry `footer`'s key-value pairs, made when it closes.
    pub fn set_footer(&mut self, footer: FileFooter) {
        self.footer = Some(footer);
    }

    /// Append a batch, closing its row group immediately (bounded RAM per batch).
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| ShapeError::Sink("write after finish".into()))?;
        // Rewrap with the field-id schema (same Arc the writer was built with) so the
        // Parquet carries Iceberg field-ids; column arrays are shared, not copied.
        let rebatched = RecordBatch::try_new(self.data_schema.clone(), batch.columns().to_vec())?;
        w.write(&rebatched).map_err(sink_err)?;
        w.flush().map_err(sink_err)?;
        // Fold this batch into the per-column min/max + null counts (same column arrays).
        self.stats.update(batch);
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    /// Rows written so far — the 0-based position the next row will take in the file.
    pub fn rows_written(&self) -> u64 {
        self.rows
    }

    /// The Iceberg field list (ids `1..=N`) derived from the schema.
    pub fn ice_fields(&self) -> &[IceField] {
        &self.ice_fields
    }

    /// The accumulated per-column statistics (min/max bounds + null counts) for the
    /// data file, for the manifest's `data_file` record.
    pub fn column_stats(&self) -> Stats {
        self.stats.finish()
    }

    /// Close the Parquet footer and return the total row count. Errors if called twice.
    pub fn close(&mut self) -> Result<u64> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| ShapeError::Sink("finish called twice".into()))?;
        if let Some(footer) = &self.footer {
            for (key, value) in footer(self.rows) {
                writer.append_key_value_metadata(KeyValue::new(key, value));
            }
        }
        writer.close().map_err(sink_err)?;
        Ok(self.rows)
    }
}

/// The relative keys + bytes for an Iceberg v2 table's metadata files — everything
/// except the already-written data Parquet. Backend-agnostic: the caller persists each
/// `*_bytes` blob at `<table>/<*_key>` on whatever store it targets (local FS or an
/// object store). `version-hint.text` must be written **last**, as it is the pointer
/// that makes the snapshot visible.
pub struct MetadataArtifacts {
    pub manifest_key: String,
    pub manifest_bytes: Vec<u8>,
    pub manifest_list_key: String,
    pub manifest_list_bytes: Vec<u8>,
    pub metadata_key: String,
    pub metadata_bytes: Vec<u8>,
    pub version_hint_key: String,
    pub version_hint_bytes: Vec<u8>,
    /// The delete manifest `(key, bytes)`, when the snapshot deletes rows (a merge).
    /// Persisted with the other metadata, before the manifest list that references it.
    pub delete_manifest: Option<(String, Vec<u8>)>,
    pub snapshot_id: i64,
    /// The table version this commit creates (`metadata/v{version}.metadata.json`).
    pub version: u64,
}

/// The current state of an existing table, loaded by a sink so a new snapshot can be
/// appended onto it. Backend-neutral — the sink reads the bytes from local FS or an
/// object store and constructs this.
pub struct PriorTable {
    /// The current `metadata.json`, parsed.
    pub metadata: serde_json::Value,
    /// The version number from `version-hint.text`; the appended table becomes
    /// `version + 1`.
    pub version: u64,
    /// The `manifest_file` records of the current snapshot's manifest list, carried
    /// forward so the appended snapshot keeps the whole table's data live.
    pub manifests: Vec<ManifestFileEntry>,
}

/// Inputs for [`build_metadata_artifacts`]: one snapshot over an already-written data
/// file. With `prior = None` this writes a fresh single-snapshot table; with `prior =
/// Some(..)` it **appends** a new snapshot onto that existing table.
pub struct MetadataBuild<'a> {
    /// The table's base location — a local absolute path or an object-store URI
    /// (`s3://bucket/db/table`). Embedded verbatim as the metadata `location`, and
    /// prefixed onto every file path via [`join_location`].
    pub base_location: &'a str,
    /// Used only when `prior` is `None` (a fresh table); on append the existing
    /// table-uuid is preserved.
    pub table_uuid: &'a str,
    pub ice_fields: &'a [IceField],
    /// The table's identity partition fields (empty = unpartitioned). Recorded in the
    /// partition spec + each data file's partition tuple.
    pub partition_fields: &'a [PartitionField],
    /// The data files written this snapshot — one per partition (one total when
    /// unpartitioned), each with its partition values, row count, size, and stats.
    pub data_files: Vec<PartitionDataFile>,
    /// Position-delete files written this snapshot (a merge; empty for an append). Each
    /// file's `rows` is its count of deleted positions.
    pub delete_files: Vec<PartitionDataFile>,
    pub timestamp_ms: i64,
    /// `Some` → append a new snapshot onto this existing table; `None` → fresh table.
    pub prior: Option<PriorTable>,
    /// With `prior = None`: the version of an existing table at this location that the
    /// fresh table replaces (a write without `append`). The fresh table is committed as the
    /// next version, never over an existing metadata file.
    pub replaces: Option<u64>,
}

/// Build the manifest, manifest-list, `metadata.json`, and `version-hint.text` for one
/// snapshot. Every embedded path is `<base_location>/<key>`, so the table is valid
/// wherever `base_location` points. On append (`prior = Some`), the prior snapshot's
/// manifests are carried forward and the new snapshot is chained onto the existing
/// metadata (`version + 1`).
pub fn build_metadata_artifacts(b: &MetadataBuild) -> Result<MetadataArtifacts> {
    if b.ice_fields.is_empty() {
        return Err(ShapeError::Sink("iceberg table has no columns".into()));
    }
    if b.data_files.is_empty() {
        // The unpartitioned path always writes one (possibly empty) data file, so an empty
        // list here means a partitioned table saw zero rows: there is no partition to write.
        let msg = if b.partition_fields.is_empty() {
            "iceberg snapshot has no data files"
        } else {
            "a partitioned iceberg table needs at least one row \
             (zero-row input with --partition-by writes no data files)"
        };
        return Err(ShapeError::Sink(msg.into()));
    }
    if let Some(prior) = &b.prior {
        // Safety net: the sinks evolved the fields at create time; re-running the
        // (idempotent) evolution must be a no-op, or the ids the data files were
        // stamped with don't match what the metadata is about to record.
        let mut check = b.ice_fields.to_vec();
        evolve_fields(&mut check, &prior.metadata)?;
        if check != b.ice_fields {
            return Err(ShapeError::Sink(
                "internal error: iceberg fields were not evolved against the prior table \
                 before writing (field-ids would be inconsistent)"
                    .into(),
            ));
        }
        ensure_partition_compatible(b.partition_fields, &prior.metadata)?;
    }
    let snapshot_id = snapshot_id_from_uuid(&Uuid::new_v4());
    let seq = match &b.prior {
        Some(p) => last_sequence_number(&p.metadata)? + 1,
        None => 1,
    };

    // 1. Manifest listing every data file written this snapshot (one per partition),
    //    each with its partition tuple + per-column stats.
    let schema_json = metadata::schema_json(b.ice_fields).to_string();
    let df_metas: Vec<DataFileMeta> = b
        .data_files
        .iter()
        .map(|df| DataFileMeta {
            path: join_location(b.base_location, &df.data_key),
            partition: df.partition.clone(),
            record_count: df.rows as i64,
            file_size: df.size,
            value_counts: df.stats.value_counts.clone(),
            null_counts: df.stats.null_counts.clone(),
            lower_bounds: df.stats.lower_bounds.clone(),
            upper_bounds: df.stats.upper_bounds.clone(),
        })
        .collect();
    let manifest_bytes = avro::write_manifest(
        &df_metas,
        b.partition_fields,
        snapshot_id,
        &schema_json,
        ManifestContent::Data,
        *Uuid::new_v4().as_bytes(),
    );
    let manifest_key = format!("metadata/{}-m0.avro", Uuid::new_v4());
    let manifest_uri = join_location(b.base_location, &manifest_key);

    let added_files = b.data_files.len() as i32;
    let added_rows: i64 = b.data_files.iter().map(|df| df.rows as i64).sum();
    let new_manifest = ManifestFileEntry::added(
        manifest_uri,
        manifest_bytes.len() as i64,
        snapshot_id,
        added_files,
        added_rows,
        seq,
    );

    // 2. Manifest list: carry the prior snapshot's manifests forward (append), then the
    //    new one. The whole table's data stays live in the new snapshot. A manifest with
    //    no live file left (a compaction's record of the delete files it removed) has
    //    nothing to add, so it is not carried.
    let mut entries: Vec<ManifestFileEntry> = match &b.prior {
        Some(p) => p
            .manifests
            .iter()
            .filter(|m| m.live_files() > 0)
            .cloned()
            .collect(),
        None => Vec::new(),
    };
    entries.push(new_manifest);

    // 2b. A merge's position deletes ride in their own manifest (a manifest holds one
    //     kind of file). Each delete gets this snapshot's sequence number, so it applies
    //     to every data file up to and including the ones this snapshot added.
    let added_delete_files = b.delete_files.len() as i64;
    let added_position_deletes: i64 = b.delete_files.iter().map(|d| d.rows as i64).sum();
    let delete_manifest = if b.delete_files.is_empty() {
        None
    } else {
        let metas: Vec<DataFileMeta> = b
            .delete_files
            .iter()
            .map(|df| DataFileMeta {
                path: join_location(b.base_location, &df.data_key),
                partition: df.partition.clone(),
                record_count: df.rows as i64,
                file_size: df.size,
                value_counts: df.stats.value_counts.clone(),
                null_counts: df.stats.null_counts.clone(),
                lower_bounds: df.stats.lower_bounds.clone(),
                upper_bounds: df.stats.upper_bounds.clone(),
            })
            .collect();
        let bytes = avro::write_manifest(
            &metas,
            b.partition_fields,
            snapshot_id,
            &schema_json,
            ManifestContent::PositionDeletes,
            *Uuid::new_v4().as_bytes(),
        );
        let key = format!("metadata/{}-m1.avro", Uuid::new_v4());
        entries.push(
            ManifestFileEntry::added(
                join_location(b.base_location, &key),
                bytes.len() as i64,
                snapshot_id,
                added_delete_files as i32,
                added_position_deletes,
                seq,
            )
            .with_content(ManifestContent::PositionDeletes),
        );
        Some((key, bytes))
    };

    let manifest_list_bytes = avro::write_manifest_list(&entries, *Uuid::new_v4().as_bytes());
    let manifest_list_key = format!("metadata/snap-{snapshot_id}-1-{}.avro", Uuid::new_v4());
    let manifest_list_uri = join_location(b.base_location, &manifest_list_key);

    // Cumulative table totals across every live manifest in this snapshot — data and
    // delete manifests counted apart (a delete manifest's "rows" are deleted positions).
    let sum = |content: i32, f: fn(&ManifestFileEntry) -> i64| -> i64 {
        entries.iter().filter(|m| m.content == content).map(f).sum()
    };
    let counts = SnapshotCounts {
        added_data_files: added_files as i64,
        added_records: added_rows,
        added_delete_files,
        added_position_deletes,
        total_data_files: sum(0, ManifestFileEntry::live_files),
        total_records: sum(0, ManifestFileEntry::live_rows),
        total_delete_files: sum(1, ManifestFileEntry::live_files),
        total_position_deletes: sum(1, ManifestFileEntry::live_rows),
        ..SnapshotCounts::default()
    };

    // 3. Table metadata (v2) + version hint — fresh doc, or the prior doc extended.
    let replaced_file = b
        .replaces
        .map(|n| join_location(b.base_location, &format!("metadata/v{n}.metadata.json")));
    let (metadata_value, version) = match &b.prior {
        None => (
            metadata::table_metadata(&MetadataInput {
                table_uuid: b.table_uuid,
                location: b.base_location,
                fields: b.ice_fields,
                partition_fields: b.partition_fields,
                last_column_id: b.ice_fields.len() as i32,
                snapshot_id,
                sequence_number: seq,
                timestamp_ms: b.timestamp_ms,
                manifest_list_path: &manifest_list_uri,
                counts,
                replaces_metadata_file: replaced_file.as_deref(),
            })?,
            b.replaces.map_or(1, |n| n + 1),
        ),
        Some(p) => (
            metadata::append_snapshot(&metadata::AppendInput {
                prior: &p.metadata,
                base_location: b.base_location,
                prior_version: p.version,
                snapshot_id,
                sequence_number: seq,
                timestamp_ms: b.timestamp_ms,
                manifest_list_path: &manifest_list_uri,
                counts,
                schema_fields: b.ice_fields,
            })?,
            p.version + 1,
        ),
    };
    let metadata_bytes = serde_json::to_vec_pretty(&metadata_value).map_err(sink_err)?;

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

/// The `last-sequence-number` of an existing table (the base for the next snapshot's
/// sequence number).
fn last_sequence_number(meta: &serde_json::Value) -> Result<i64> {
    meta["last-sequence-number"]
        .as_i64()
        .ok_or_else(|| ShapeError::Sink("prior metadata has no last-sequence-number".into()))
}

/// Reject an append whose partitioning differs from the existing table's — a table's
/// partition spec is fixed, and changing it (partition evolution) is a separate item.
/// Public so both the local and object-store sinks fail-fast at `create` the same way.
pub fn ensure_partition_compatible(
    fields: &[PartitionField],
    prior_meta: &serde_json::Value,
) -> Result<()> {
    let empty = Vec::new();
    let prior = prior_meta["partition-specs"][0]["fields"]
        .as_array()
        .unwrap_or(&empty);
    if prior.len() != fields.len() {
        return Err(ShapeError::Sink(format!(
            "cannot append: the table is partitioned by {} column(s) but this write has {} \
             (changing partitioning is not supported yet)",
            prior.len(),
            fields.len()
        )));
    }
    for (f, pf) in fields.iter().zip(prior) {
        let pname = pf["name"].as_str().unwrap_or("");
        let psrc = pf["source-id"].as_i64().unwrap_or(-1);
        let ptf = pf["transform"].as_str().unwrap_or("");
        if pname != f.name || psrc != f.source_id as i64 || ptf != f.transform.spec_str() {
            return Err(ShapeError::Sink(format!(
                "cannot append: partition field `{}` ({}) does not match the table's `{}` \
                 ({}) — changing partitioning is not supported yet",
                f.name,
                f.transform.spec_str(),
                pname,
                ptf
            )));
        }
    }
    Ok(())
}

/// The prior table's current schema fields, as `(name → (id, type, required))`.
fn prior_schema_fields(prior_meta: &serde_json::Value) -> Result<Vec<(String, i32, String, bool)>> {
    let current_id = &prior_meta["current-schema-id"];
    let prior_fields = prior_meta["schemas"]
        .as_array()
        .and_then(|schemas| {
            schemas
                .iter()
                .find(|s| &s["schema-id"] == current_id)
                .or_else(|| schemas.first())
        })
        .and_then(|s| s["fields"].as_array())
        .ok_or_else(|| ShapeError::Sink("prior metadata has no schema fields".into()))?;
    Ok(prior_fields
        .iter()
        .map(|pf| {
            (
                pf["name"].as_str().unwrap_or("").to_string(),
                pf["id"].as_i64().unwrap_or(-1) as i32,
                pf["type"].as_str().unwrap_or("").to_string(),
                pf["required"].as_bool().unwrap_or(false),
            )
        })
        .collect())
}

/// Reconcile a new write's schema against an existing table's — **additive schema
/// evolution**, the Iceberg-safe subset:
///
/// - every existing column keeps its prior **field-id** (matched by name; type and
///   required-ness must be unchanged — type promotion is not supported);
/// - genuinely **new columns are allowed** if they are optional (Iceberg gives added
///   columns a null default; a new *required* column would make old files unreadable),
///   and get fresh ids continuing from the table's `last-column-id`;
/// - **dropping or renaming** a column is refused — old data files must stay readable.
///
/// Rewrites `fields`' ids in place. Idempotent: running it again changes nothing.
/// Both sinks call this at `create` time (fail-fast, before any data is written);
/// `build_metadata_artifacts` re-runs it as a safety net.
pub fn evolve_fields(fields: &mut [IceField], prior_meta: &serde_json::Value) -> Result<()> {
    let prior = prior_schema_fields(prior_meta)?;
    let mut last_column_id = prior_meta["last-column-id"].as_i64().unwrap_or_else(|| {
        prior
            .iter()
            .map(|(_, id, _, _)| *id as i64)
            .max()
            .unwrap_or(0)
    }) as i32;

    // Every prior column must still be present, unchanged (match by NAME, not position).
    for (pname, _, ptype, preq) in &prior {
        match fields.iter().find(|f| &f.name == pname) {
            None => {
                return Err(ShapeError::Sink(format!(
                    "cannot append: the table's column `{pname}` is missing from this write \
                     (dropping or renaming columns is not supported — evolution is additive only)"
                )));
            }
            Some(f) if &f.iceberg_type != ptype => {
                return Err(ShapeError::Sink(format!(
                    "cannot append: column `{pname}` changed type from `{ptype}` to `{}` \
                     (type changes are not supported)",
                    f.iceberg_type
                )));
            }
            Some(f) if f.required != *preq => {
                return Err(ShapeError::Sink(format!(
                    "cannot append: column `{pname}` changed required-ness \
                     (the table says required={preq}; this write says required={})",
                    f.required
                )));
            }
            Some(_) => {}
        }
    }

    // Existing columns keep their ids; new columns must be optional and get fresh ids.
    for f in fields.iter_mut() {
        if let Some((_, pid, _, _)) = prior.iter().find(|(pname, ..)| pname == &f.name) {
            f.id = *pid;
        } else {
            if f.required {
                return Err(ShapeError::Sink(format!(
                    "cannot append: new column `{}` must be optional (required: false) — \
                     Iceberg fills it with null for the table's existing rows",
                    f.name
                )));
            }
            last_column_id += 1;
            f.id = last_column_id;
        }
    }
    Ok(())
}

/// The table-relative key (`metadata/snap-….avro`) of the current snapshot's manifest
/// list, derived from the prior metadata by stripping the table base location. Exposed
/// so both the local and object-store sinks resolve the prior manifest list the same way.
pub fn current_manifest_list_key(
    metadata: &serde_json::Value,
    base_location: &str,
) -> Result<String> {
    let current = &metadata["current-snapshot-id"];
    let uri = metadata["snapshots"]
        .as_array()
        .and_then(|snaps| snaps.iter().find(|s| &s["snapshot-id"] == current))
        .and_then(|s| s["manifest-list"].as_str())
        .ok_or_else(|| {
            ShapeError::Sink("prior metadata has no current-snapshot manifest-list".into())
        })?;
    Ok(relative_key(uri, base_location))
}

/// Strip a table's base location off a fully-qualified file URI to get its table-relative
/// key. Falls back to the segment from `metadata/` if the prefix doesn't match (e.g. a
/// table copied to a new location before a catalog rewrite).
fn relative_key(uri: &str, base_location: &str) -> String {
    if let Some(rest) = uri.strip_prefix(base_location) {
        return rest.trim_start_matches('/').to_string();
    }
    if let Some(idx) = uri.rfind("/metadata/") {
        return uri[idx + 1..].to_string();
    }
    uri.trim_start_matches('/').to_string()
}

/// Load the current state of an existing local Iceberg table for append, or `None` if no
/// table is present at `table_dir` yet (a first write).
fn load_prior_local(table_dir: &Path, base_location: &str) -> Result<Option<PriorTable>> {
    let Some(version) = local_version(table_dir)? else {
        return Ok(None);
    };
    let meta_text =
        std::fs::read_to_string(table_dir.join(metadata_key(version))).map_err(sink_err)?;
    let metadata: serde_json::Value = serde_json::from_str(&meta_text).map_err(sink_err)?;
    let mlist_key = current_manifest_list_key(&metadata, base_location)?;
    let mlist_bytes = std::fs::read(table_dir.join(&mlist_key)).map_err(sink_err)?;
    let manifests = avro::read_manifest_list(&mlist_bytes)?;
    Ok(Some(PriorTable {
        metadata,
        version,
        manifests,
    }))
}

/// A write-once Iceberg v2 table sink, landing on the local filesystem.
pub struct IcebergSink {
    table_dir: PathBuf,
    ice_fields: Vec<IceField>,
    partition_fields: Vec<PartitionField>,
    writer: PartitionedWriter,
    table_uuid: Uuid,
    compression: Compression,
    /// The existing table to append onto, loaded and validated at `create` time (`None` =
    /// a fresh table). Consumed by `finish` to chain the new snapshot.
    prior: Option<PriorTable>,
    /// Without `append`: the version of a table already at `table_dir`, which the fresh
    /// table replaces as the next version.
    replaces: Option<u64>,
    /// Set for a merge: follows the written keys so `finish` can delete what they
    /// supersede.
    merge: Option<MergeWriter>,
}

impl IcebergSink {
    /// Create a table sink rooted at `table_dir`, deriving the Iceberg schema from the
    /// Arrow schema every batch will carry. `partition_by` names the partition fields
    /// (identity columns and/or transform expressions; empty = unpartitioned). With
    /// `append = true`, if a table already exists at `table_dir` a new snapshot is
    /// committed onto it — the schema may **add optional columns** (additive evolution;
    /// existing columns keep their field-ids, drops/renames/type-changes are refused)
    /// and the partitioning must match; otherwise a fresh single-snapshot table is
    /// written.
    pub fn create(
        table_dir: impl AsRef<Path>,
        schema: SchemaRef,
        compression: Compression,
        append: bool,
        partition_by: &[String],
    ) -> Result<Self> {
        Self::open(
            table_dir.as_ref(),
            schema,
            compression,
            append,
            partition_by,
            &[],
        )
    }

    /// Create a sink that **merges** (upserts) into the table at `table_dir` on the
    /// `merge_on` key columns, creating the table if there is none. Every written row is
    /// appended; at `finish`, each row it supersedes is removed with a position delete —
    /// an existing row whose key this write carries, and an earlier occurrence of a key
    /// this write carries twice (the last one wins). Schema evolution and partitioning
    /// follow [`IcebergSink::create`] with `append = true`.
    pub fn create_merge(
        table_dir: impl AsRef<Path>,
        schema: SchemaRef,
        compression: Compression,
        partition_by: &[String],
        merge_on: &[String],
    ) -> Result<Self> {
        if merge_on.is_empty() {
            return Err(ShapeError::Sink("merge_on names no key column".into()));
        }
        Self::open(
            table_dir.as_ref(),
            schema,
            compression,
            true,
            partition_by,
            merge_on,
        )
    }

    fn open(
        table_dir: &Path,
        schema: SchemaRef,
        compression: Compression,
        append: bool,
        partition_by: &[String],
        merge_on: &[String],
    ) -> Result<Self> {
        let table_dir = table_dir.to_path_buf();
        std::fs::create_dir_all(table_dir.join("data")).map_err(sink_err)?;
        std::fs::create_dir_all(table_dir.join("metadata")).map_err(sink_err)?;

        let mut ice_fields = ice_fields_from_schema(&schema)?;

        // With `--append`, load the existing table and reconcile schema + partitioning up
        // front, before any data file is written — an incompatible append fails in
        // milliseconds instead of after a full (possibly large) write, and never leaves an
        // orphaned data file behind. Evolution runs BEFORE partition resolution so the
        // partition spec's source-ids reference the stable (prior) field-ids.
        let (prior, replaces) = if append {
            let base_location = abspath(&table_dir);
            let prior = load_prior_local(&table_dir, &base_location)?;
            if let Some(p) = &prior {
                evolve_fields(&mut ice_fields, &p.metadata)?;
            }
            (prior, None)
        } else {
            (None, local_version(&table_dir)?)
        };
        let partition_fields = resolve_partition_fields(&ice_fields, partition_by)?;
        if let Some(p) = &prior {
            ensure_partition_compatible(&partition_fields, &p.metadata)?;
        }
        // Key columns resolve against the evolved fields, so a key matches the table's
        // own field-id. Validated here — before any data is written.
        let merge = if merge_on.is_empty() {
            None
        } else {
            Some(MergeWriter::new(&ice_fields, merge_on)?)
        };

        let writer = PartitionedWriter::new(
            &table_dir,
            schema,
            compression,
            ice_fields.clone(),
            partition_fields.clone(),
        )?;

        Ok(IcebergSink {
            table_dir,
            ice_fields,
            partition_fields,
            writer,
            table_uuid: Uuid::new_v4(),
            compression,
            prior,
            replaces,
            merge,
        })
    }

    /// Have every data file this sink writes carry `footer`'s key-value pairs (see
    /// [`FileFooter`]). Each file gets its own, made from its own row count as it closes.
    pub fn set_footer(&mut self, footer: FileFooter) {
        self.writer.set_footer(footer);
    }
}

impl Sink for IcebergSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        match &mut self.merge {
            None => self.writer.write_batch(batch),
            Some(m) => m.write_batch(&mut self.writer, batch),
        }
    }

    fn finish(&mut self) -> Result<SinkSummary> {
        let data_files = self.writer.finish()?;
        let rows: u64 = data_files.iter().map(|d| d.rows).sum();
        let bytes: u64 = data_files.iter().map(|d| d.size as u64).sum();
        // The data files (already on disk under the table dir) lead the summary.
        let mut files: Vec<PathBuf> = data_files
            .iter()
            .map(|d| self.table_dir.join(&d.data_key))
            .collect();

        let base_location = abspath(&self.table_dir);
        // Loaded and validated at `create` time (fail-fast); consume it to chain the snapshot.
        let prior = self.prior.take();

        // A merge finds the rows this write supersedes — reading only the key columns
        // of existing files that could hold one — and writes their position deletes.
        let files_written = files.clone();
        let (delete_files, merge) = match self.merge.take() {
            None => (Vec::new(), None),
            Some(m) => {
                // Existing files are read where the table's metadata says they are,
                // re-anchored to this directory if the table has moved.
                let location = prior
                    .as_ref()
                    .and_then(|p| p.metadata["location"].as_str())
                    .unwrap_or(&base_location);
                let files = LocalTableFiles::new(&self.table_dir, location);
                let out = m.finish(
                    &MergeContext {
                        staging_dir: &self.table_dir,
                        base_location: &base_location,
                        prior: prior.as_ref(),
                        new_files: &data_files,
                        partition_fields: &self.partition_fields,
                        compression: self.compression,
                    },
                    &files,
                );
                match out {
                    Ok(out) => (out.delete_files, Some(out.summary)),
                    Err(e) => {
                        for f in &files_written {
                            let _ = std::fs::remove_file(f);
                        }
                        let base = prior.as_ref().map_or(0, |p| p.version);
                        return Err(moved_on(e, base, local_version(&self.table_dir)?));
                    }
                }
            }
        };
        files.extend(
            delete_files
                .iter()
                .map(|d| self.table_dir.join(&d.data_key)),
        );

        let table_uuid = self.table_uuid.to_string();
        let art = build_metadata_artifacts(&MetadataBuild {
            base_location: &base_location,
            table_uuid: &table_uuid,
            ice_fields: &self.ice_fields,
            partition_fields: &self.partition_fields,
            data_files,
            delete_files,
            timestamp_ms: snapshot_timestamp_ms(),
            prior,
            replaces: self.replaces,
        })?;

        // Commit: manifests, then the guarded metadata file, then the hint. If another
        // writer committed this version first, nothing of this write stays behind.
        match commit_local(&self.table_dir, &art) {
            Ok(written) => files.extend(written),
            Err(e) => {
                for f in &files {
                    let _ = std::fs::remove_file(f);
                }
                return Err(e);
            }
        }

        Ok(SinkSummary {
            files,
            rows,
            bytes,
            merge,
            table_version: Some(art.version),
            snapshot_id: Some(art.snapshot_id),
        })
    }
}

/// Absolute path string (best effort: canonical parent + file name, falling back to
/// the joined path so it never fails just because the file was already consumed).
fn abspath(p: &Path) -> String {
    if let Ok(c) = p.canonicalize() {
        return c.to_string_lossy().into_owned();
    }
    match (p.parent(), p.file_name()) {
        (Some(parent), Some(name)) => match parent.canonicalize() {
            Ok(cp) => cp.join(name).to_string_lossy().into_owned(),
            Err(_) => p.to_string_lossy().into_owned(),
        },
        _ => p.to_string_lossy().into_owned(),
    }
}

/// What an Iceberg table's current metadata describes (read back for `inspect`).
#[derive(Debug, Clone)]
pub struct IcebergInfo {
    pub format_version: i64,
    pub table_uuid: String,
    pub current_snapshot_id: i64,
    /// Rows in the live data files, including any a position delete removes.
    pub total_records: i64,
    /// Rows removed by position deletes (a merge). Never double-counted: a merge does not
    /// delete a row that is already deleted.
    pub total_position_deletes: i64,
    pub fields: Vec<(String, String)>,
}

impl IcebergInfo {
    /// The rows a reader sees: data rows minus the ones deleted.
    pub fn live_records(&self) -> i64 {
        self.total_records - self.total_position_deletes
    }
}

/// Parse a table's `metadata.json` text into an [`IcebergInfo`] summary. Backend-neutral
/// — the caller supplies the bytes from local FS or an object store.
pub fn parse_metadata_json(text: &str) -> Result<IcebergInfo> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(sink_err)?;

    let format_version = v["format-version"].as_i64().unwrap_or(0);
    let table_uuid = v["table-uuid"].as_str().unwrap_or("").to_string();
    let current_snapshot_id = v["current-snapshot-id"].as_i64().unwrap_or(0);
    let summary_count = |key: &str| -> i64 {
        v["snapshots"]
            .as_array()
            .and_then(|s| s.last())
            .and_then(|s| s["summary"][key].as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    };
    let total_records = summary_count("total-records");
    let total_position_deletes = summary_count("total-position-deletes");
    // Resolve the CURRENT schema by id — after additive evolution `schemas` keeps the
    // old schema objects too, so `.first()` would report a stale column list.
    let current_schema_id = v["current-schema-id"].as_i64().unwrap_or(0);
    let fields = v["schemas"]
        .as_array()
        .and_then(|s| {
            s.iter()
                .find(|s| s["schema-id"].as_i64() == Some(current_schema_id))
                .or_else(|| s.first())
        })
        .and_then(|s| s["fields"].as_array())
        .map(|fs| {
            fs.iter()
                .map(|f| {
                    (
                        f["name"].as_str().unwrap_or("").to_string(),
                        f["type"].as_str().unwrap_or("").to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(IcebergInfo {
        format_version,
        table_uuid,
        current_snapshot_id,
        total_records,
        total_position_deletes,
        fields,
    })
}

// ---- The commit guard ----
//
// A commit is the creation of `metadata/v{N+1}.metadata.json`, and it succeeds only if no
// other writer created that name first: two writers that both built on version N cannot
// both win, and the loser is told so (`ShapeError::Conflict`) instead of silently
// overwriting the other's snapshot. `version-hint.text` is only a hint: it is written
// after the commit, so a crash or a slower writer can leave it behind. Readers and
// writers therefore look past it for later versions — but only for a true successor,
// whose `metadata-log` names the version before it, so a file left over from an earlier
// table at the same path is never mistaken for a newer commit.

/// The key of a table's metadata file for `version`.
pub fn metadata_key(version: u64) -> String {
    format!("metadata/v{version}.metadata.json")
}

/// Whether `next` (a parsed `metadata.json`) continues from version `n`: the last file in
/// its `metadata-log` is `v{n}.metadata.json`. Version 1 is a table's first and continues
/// from nothing.
pub fn continues_from(next: &serde_json::Value, n: u64) -> bool {
    if n == 0 {
        return true;
    }
    let name = format!("v{n}.metadata.json");
    next["metadata-log"]
        .as_array()
        .and_then(|log| log.last())
        .and_then(|e| e["metadata-file"].as_str())
        .is_some_and(|f| f == name || f.ends_with(&format!("/{name}")))
}

/// The newest committed version of a table, starting from what its version hint says
/// (`0` without one) and following every later metadata file that continues from the one
/// before. `next(v)` returns the parsed `v{v}.metadata.json`, or `None` if there is none.
pub fn latest_version(
    hint: u64,
    mut next: impl FnMut(u64) -> Result<Option<serde_json::Value>>,
) -> Result<u64> {
    let mut v = hint;
    while let Some(meta) = next(v + 1)? {
        if !continues_from(&meta, v) {
            break;
        }
        v += 1;
    }
    Ok(v)
}

/// What a writer gets when `version` was already created. A successor of the version it
/// built on means another writer committed first, and a re-run will succeed
/// ([`ShapeError::Conflict`]). Anything else is a file left over from an earlier table at
/// this path, which a re-run cannot get past.
pub fn version_taken(version: u64, existing: Option<&serde_json::Value>) -> ShapeError {
    match existing {
        Some(meta) if !continues_from(meta, version - 1) => ShapeError::Sink(format!(
            "metadata/v{version}.metadata.json already exists but does not continue from \
             v{}: it is left over from an earlier table written at this path (shapeshift \
             0.3 and earlier rewrote v1 in place on a write without --append, keeping later \
             versions). Move metadata/v{version}.metadata.json and any later ones aside, \
             then run this again",
            version - 1
        )),
        _ => ShapeError::Conflict(format!(
            "another writer committed version {version} of the table while this one ran; \
             nothing of this write was published, and its files were removed. Run it again"
        )),
    }
}

/// A write that failed after the table moved on from the version it built on (`base`) to
/// `now` is reported as the conflict it most likely is: a compaction and an expiry that
/// committed meanwhile can remove the very files it was reading. Running it again builds
/// on the new version.
pub fn moved_on(err: ShapeError, base: u64, now: Option<u64>) -> ShapeError {
    match now {
        Some(v) if v > base && !matches!(err, ShapeError::Conflict(_)) => {
            ShapeError::Conflict(format!(
                "the table moved from version {base} to {v} while this write ran, and it \
                 failed ({err}); nothing of it was published. Run it again"
            ))
        }
        _ => err,
    }
}

/// A local table's newest committed version (see [`latest_version`]), or `None` if
/// there is no table at `table_dir`.
pub fn local_version(table_dir: &Path) -> Result<Option<u64>> {
    let hint = match std::fs::read_to_string(table_dir.join("metadata/version-hint.text")) {
        Ok(h) => h
            .trim()
            .parse::<u64>()
            .map_err(|e| ShapeError::Sink(format!("bad version-hint.text {h:?}: {e}")))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(sink_err(e)),
    };
    let v = latest_version(hint, |v| read_local_metadata(table_dir, v))?;
    Ok((v > 0).then_some(v))
}

fn read_local_metadata(table_dir: &Path, version: u64) -> Result<Option<serde_json::Value>> {
    match std::fs::read_to_string(table_dir.join(metadata_key(version))) {
        Ok(text) => serde_json::from_str(&text).map(Some).map_err(sink_err),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(sink_err(e)),
    }
}

/// Create `metadata/v{version}.metadata.json` under `table_dir` only if no writer has: the
/// commit point. The bytes are written to a temporary file and hard-linked into place —
/// linking fails if the name is taken, so the file appears whole or not at all, and once.
/// On a filesystem without hard links the name is created exclusively and then written.
pub(crate) fn publish_metadata_local(table_dir: &Path, version: u64, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let target = table_dir.join(metadata_key(version));
    let tmp = table_dir.join(format!("metadata/.v{version}.{}.tmp", Uuid::new_v4()));
    let write_synced = |path: &Path, create_new: bool| -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .create_new(create_new)
            .truncate(!create_new)
            .open(path)?;
        f.write_all(bytes)?;
        f.sync_all()
    };
    write_synced(&tmp, false).map_err(sink_err)?;
    let linked = std::fs::hard_link(&tmp, &target);
    let _ = std::fs::remove_file(&tmp);
    let taken = || {
        version_taken(
            version,
            read_local_metadata(table_dir, version)
                .ok()
                .flatten()
                .as_ref(),
        )
    };
    match linked {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(taken()),
        Err(_) => match write_synced(&target, true) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(taken()),
            Err(e) => Err(sink_err(e)),
        },
    }
}

/// Point `version-hint.text` at `version` — unless a later version has already been
/// committed, so the hint never moves backwards. Written to a temporary file and renamed
/// into place, so a reader never sees it half-written.
pub(crate) fn publish_hint_local(table_dir: &Path, version: u64) -> Result<()> {
    if let Some(next) = read_local_metadata(table_dir, version + 1)? {
        if continues_from(&next, version) {
            return Ok(());
        }
    }
    let tmp = table_dir.join(format!("metadata/.version-hint.{}.tmp", Uuid::new_v4()));
    std::fs::write(&tmp, version.to_string()).map_err(sink_err)?;
    std::fs::rename(&tmp, table_dir.join("metadata/version-hint.text")).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        sink_err(e)
    })
}

/// Commit a snapshot's metadata to a local table: the manifests and manifest list (fresh
/// names, nothing to collide with), then the guarded metadata file, then the hint.
/// Returns the paths written. If the commit is refused or fails, everything this call
/// wrote is removed. A failure to update the hint after the commit is only logged: the
/// snapshot is committed, and the next reader or writer finds it past the stale hint.
pub(crate) fn commit_local(table_dir: &Path, art: &MetadataArtifacts) -> Result<Vec<PathBuf>> {
    let mut written: Vec<PathBuf> = Vec::new();
    let staged = (|| -> Result<()> {
        let mut put = |key: &str, bytes: &[u8]| -> Result<()> {
            let p = table_dir.join(key);
            std::fs::write(&p, bytes).map_err(sink_err)?;
            written.push(p);
            Ok(())
        };
        put(&art.manifest_key, &art.manifest_bytes)?;
        if let Some((key, bytes)) = &art.delete_manifest {
            put(key, bytes)?;
        }
        put(&art.manifest_list_key, &art.manifest_list_bytes)?;
        publish_metadata_local(table_dir, art.version, &art.metadata_bytes)
    })();
    if let Err(e) = staged {
        for p in &written {
            let _ = std::fs::remove_file(p);
        }
        return Err(e);
    }
    written.push(table_dir.join(&art.metadata_key));
    match publish_hint_local(table_dir, art.version) {
        Ok(()) => written.push(table_dir.join(&art.version_hint_key)),
        Err(e) => tracing::warn!(
            version = art.version,
            error = %e,
            "committed, but version-hint.text was not updated; readers find the commit past it"
        ),
    }
    Ok(written)
}

/// The metadata file version a table's `version-hint.text` points at, and the key of
/// that `metadata.json` under the table root. Backend-neutral so both the local and
/// object-store `inspect` paths resolve the current metadata the same way.
pub fn current_metadata_key(version_hint: &str) -> Result<String> {
    let version: u64 = version_hint.trim().parse().map_err(|e| {
        ShapeError::Sink(format!("invalid version-hint.text {version_hint:?}: {e}"))
    })?;
    Ok(format!("metadata/v{version}.metadata.json"))
}

/// Read a local table's current `metadata.json` (via `version-hint.text`) and
/// summarize it.
pub fn inspect(table_dir: impl AsRef<Path>) -> Result<IcebergInfo> {
    let table_dir = table_dir.as_ref();
    let version = local_version(table_dir)?.ok_or_else(|| {
        ShapeError::Sink(format!(
            "no Iceberg table at {} (no version-hint.text or metadata)",
            table_dir.display()
        ))
    })?;
    let text = std::fs::read_to_string(table_dir.join(metadata_key(version))).map_err(sink_err)?;
    parse_metadata_json(&text)
}

/// Re-anchor a stored absolute path (written against `base`) onto the current
/// `table_dir`, so a moved table still resolves. Paths that don't share `base` (already
/// relative, or externally-anchored) are returned as-is.
fn reanchor(table_dir: &Path, base: &str, path: &str) -> std::path::PathBuf {
    if !base.is_empty() {
        if let Some(rest) = path.strip_prefix(base) {
            return table_dir.join(rest.trim_start_matches('/'));
        }
    }
    std::path::PathBuf::from(path)
}

/// Load a local table's current snapshot — whatever `version-hint.text` points at —
/// as a [`PriorTable`], resolving its manifest list through the stored `location`.
fn load_current_local(table_dir: &Path) -> Result<PriorTable> {
    let version = local_version(table_dir)?.ok_or_else(|| {
        ShapeError::Sink(format!(
            "no Iceberg table at {} (no version-hint.text or metadata)",
            table_dir.display()
        ))
    })?;
    let text = std::fs::read_to_string(table_dir.join(metadata_key(version))).map_err(sink_err)?;
    let metadata: serde_json::Value = serde_json::from_str(&text).map_err(sink_err)?;
    let base = metadata["location"].as_str().unwrap_or("");
    let current = metadata["current-snapshot-id"].as_i64().unwrap_or(-1);
    let snapshots = metadata["snapshots"]
        .as_array()
        .ok_or_else(|| ShapeError::Sink("table metadata has no snapshots".into()))?;
    let snap = snapshots
        .iter()
        .find(|s| s["snapshot-id"].as_i64() == Some(current))
        .or_else(|| snapshots.last())
        .ok_or_else(|| ShapeError::Sink("table has no current snapshot".into()))?;
    let ml_path = snap["manifest-list"]
        .as_str()
        .ok_or_else(|| ShapeError::Sink("snapshot has no manifest-list".into()))?;
    let ml_bytes = std::fs::read(reanchor(table_dir, base, ml_path)).map_err(sink_err)?;
    let manifests = read_manifest_list(&ml_bytes)?;
    Ok(PriorTable {
        metadata,
        version,
        manifests,
    })
}

/// Compact a local table: rewrite every data file that carries deletes, and pack small
/// files together per partition, in one `replace` snapshot (see [`compact`]). The new
/// files are written under the table directory, then the metadata, version hint last.
/// Commits nothing, and writes nothing, when there is nothing to compact.
pub fn compact_table(
    table_dir: impl AsRef<Path>,
    options: &CompactOptions,
) -> Result<CompactSummary> {
    let table_dir = table_dir.as_ref();
    let current = load_current_local(table_dir)?;
    let location = current.metadata["location"].as_str().unwrap_or_default();
    let files = LocalTableFiles::new(table_dir, location);
    let base_location = abspath(table_dir);
    let outcome = compact(
        &CompactContext {
            staging_dir: table_dir,
            base_location: &base_location,
            table: &current,
            options,
        },
        &files,
    )
    .map_err(|e| match local_version(table_dir) {
        Ok(now) => moved_on(e, current.version, now),
        Err(_) => e,
    })?;
    let Some(art) = outcome.artifacts else {
        return Ok(outcome.summary);
    };
    // Guarded: a compaction runs long, and a writer that committed meanwhile wins.
    if let Err(e) = commit_local(table_dir, &art) {
        for f in &outcome.data_files {
            let _ = std::fs::remove_file(table_dir.join(&f.data_key));
        }
        return Err(e);
    }
    Ok(outcome.summary)
}

/// Expire a local table's old snapshots (see [`plan_expiry`]): publish the metadata
/// without them, version hint last, then delete every file only they referenced. A dry
/// run, or a table with nothing to expire, changes nothing.
pub fn expire_table(table_dir: impl AsRef<Path>, options: &ExpireOptions) -> Result<ExpireSummary> {
    let table_dir = table_dir.as_ref();
    let current = load_current_local(table_dir)?;
    let location = current.metadata["location"].as_str().unwrap_or_default();
    let files = LocalTableFiles::new(table_dir, location);
    let base_location = abspath(table_dir);
    let plan = plan_expiry(
        &current,
        &base_location,
        &files,
        options,
        snapshot_timestamp_ms(),
    )
    .map_err(|e| match local_version(table_dir) {
        Ok(now) => moved_on(e, current.version, now),
        Err(_) => e,
    })?;
    let mut summary = plan.summary;
    let Some(commit) = plan.commit.filter(|_| !options.dry_run) else {
        return Ok(summary);
    };
    // Guarded: if another writer committed since the plan was made, nothing is deleted —
    // its snapshot may read files this plan would remove.
    publish_metadata_local(table_dir, commit.version, &commit.metadata_bytes)?;
    if let Err(e) = publish_hint_local(table_dir, commit.version) {
        tracing::warn!(version = commit.version, error = %e,
            "expired, but version-hint.text was not updated; readers find the commit past it");
    }
    summary.committed = true;
    summary.version = Some(commit.version);
    for t in &plan.delete {
        match std::fs::remove_file(files.resolve(&t.path)) {
            Ok(()) => summary.count_deleted(t),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => summary.count_deleted(t),
            Err(_) => summary.files_not_deleted += 1,
        }
    }
    Ok(summary)
}

/// One data file of a table's current snapshot, with the rows deleted out of it.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// The data file, re-anchored to the table's current directory.
    pub path: String,
    /// Rows in the file, including deleted ones.
    pub record_count: i64,
    /// 0-based positions a position delete removes (sorted). A reader skips these rows.
    pub deleted_positions: Vec<u64>,
}

/// The current snapshot's live data files, each with the positions its delete files
/// remove — everything a reader needs to return exactly the live rows of a table that has
/// been merged into. Errors on equality deletes (shapeshift never writes them).
pub fn current_scan_files(table_dir: impl AsRef<Path>) -> Result<Vec<ScanFile>> {
    let table_dir = table_dir.as_ref();
    let current = load_current_local(table_dir)?;
    let location = current.metadata["location"].as_str().unwrap_or_default();
    let files = LocalTableFiles::new(table_dir, location);
    let mut live = merge::live_files(&files, &current)?;
    Ok(live
        .data
        .iter()
        .map(|e| ScanFile {
            path: files.resolve(&e.file_path).to_string_lossy().into_owned(),
            record_count: e.record_count,
            deleted_positions: live.deletes.remove(&e.file_path).unwrap_or_default(),
        })
        .collect())
}

/// The data-file paths referenced by a local table's **current snapshot**, re-anchored
/// to `table_dir`. Reads only *committed* files (from the manifests), so orphan Parquet
/// left by an interrupted write is excluded — unlike a raw directory scan.
///
/// Returns an error (so the caller can fall back) if the manifests can't be read, if the
/// summed data record count doesn't match the snapshot's `total-records`, or if the table
/// has **row-level deletes** (a merge): reading those data files whole would resurrect the
/// rows the deletes remove, so such a table must be read through [`current_scan_files`]
/// or an Iceberg reader instead.
pub fn current_data_files(table_dir: impl AsRef<Path>) -> Result<Vec<String>> {
    let table_dir = table_dir.as_ref();
    let current = load_current_local(table_dir)?;
    let deletes: i64 = current
        .manifests
        .iter()
        .filter(|m| m.content != 0)
        .map(ManifestFileEntry::live_files)
        .sum();
    if deletes > 0 {
        return Err(ShapeError::Sink(format!(
            "the table has {deletes} delete file(s) from a merge; listing its data files \
             alone would resurrect deleted rows — read it through current_scan_files or an \
             Iceberg reader (iceberg_scan)"
        )));
    }
    let base = current.metadata["location"].as_str().unwrap_or("");
    let current_id = current.metadata["current-snapshot-id"].as_i64();
    let total_records: i64 = current.metadata["snapshots"]
        .as_array()
        .and_then(|snaps| {
            snaps
                .iter()
                .find(|s| s["snapshot-id"].as_i64() == current_id)
                .or_else(|| snaps.last())
        })
        .and_then(|s| s["summary"]["total-records"].as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);

    let mut files = Vec::new();
    let mut rec_sum: i64 = 0;
    for m in &current.manifests {
        let mbytes = std::fs::read(reanchor(table_dir, base, &m.path)).map_err(sink_err)?;
        for df in read_manifest_data_files(&mbytes)? {
            // content 0 = data; status 2 = DELETED.
            if df.content == 0 && df.status != 2 {
                rec_sum += df.record_count;
                files.push(
                    reanchor(table_dir, base, &df.file_path)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    if total_records >= 0 && rec_sum != total_records {
        return Err(ShapeError::Sink(format!(
            "manifest data-file record sum {rec_sum} != snapshot total-records {total_records}"
        )));
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_location_anchors_and_normalizes_slashes() {
        assert_eq!(
            join_location("s3://b/db/t", "data/x.parquet"),
            "s3://b/db/t/data/x.parquet"
        );
        // A trailing base slash and a leading key slash collapse to exactly one.
        assert_eq!(
            join_location("s3://b/db/t/", "/data/x.parquet"),
            "s3://b/db/t/data/x.parquet"
        );
        assert_eq!(
            join_location("/warehouse/t", "metadata/v1.metadata.json"),
            "/warehouse/t/metadata/v1.metadata.json"
        );
    }

    #[test]
    fn artifacts_anchor_every_path_at_the_base_location() {
        let fields = vec![
            IceField {
                id: 1,
                name: "id".into(),
                iceberg_type: "long".into(),
                required: true,
            },
            IceField {
                id: 2,
                name: "amt".into(),
                iceberg_type: "double".into(),
                required: false,
            },
        ];
        let base = "s3://bucket/db/table";
        let art = build_metadata_artifacts(&MetadataBuild {
            base_location: base,
            table_uuid: "11111111-1111-1111-1111-111111111111",
            ice_fields: &fields,
            partition_fields: &[],
            data_files: vec![PartitionDataFile {
                data_key: "data/abc.parquet".to_string(),
                partition: vec![],
                rows: 4,
                size: 123,
                stats: Stats::default(),
            }],
            delete_files: Vec::new(),
            timestamp_ms: 1_700_000_000_000,
            prior: None,
            replaces: None,
        })
        .unwrap();

        // metadata.json's `location` is the base, and the manifest-list path is
        // anchored under it — the whole point of a relocatable, object-store table.
        let meta: serde_json::Value = serde_json::from_slice(&art.metadata_bytes).unwrap();
        assert_eq!(meta["location"], base);
        let mlist = meta["snapshots"][0]["manifest-list"].as_str().unwrap();
        assert!(
            mlist.starts_with("s3://bucket/db/table/metadata/snap-"),
            "manifest-list not anchored: {mlist}"
        );
        assert_eq!(art.metadata_key, "metadata/v1.metadata.json");
        assert_eq!(art.version_hint_key, "metadata/version-hint.text");
        assert_eq!(art.version_hint_bytes, b"1");

        // The summary round-trips through the reader path.
        let info = parse_metadata_json(std::str::from_utf8(&art.metadata_bytes).unwrap()).unwrap();
        assert_eq!(info.format_version, 2);
        assert_eq!(info.total_records, 4);
        assert_eq!(info.fields.len(), 2);
    }

    #[test]
    fn empty_field_list_is_rejected() {
        let err = build_metadata_artifacts(&MetadataBuild {
            base_location: "s3://b/t",
            table_uuid: "x",
            ice_fields: &[],
            partition_fields: &[],
            data_files: vec![PartitionDataFile {
                data_key: "data/x.parquet".to_string(),
                partition: vec![],
                rows: 0,
                size: 0,
                stats: Stats::default(),
            }],
            delete_files: Vec::new(),
            timestamp_ms: 0,
            prior: None,
            replaces: None,
        });
        assert!(err.is_err());
    }
}
