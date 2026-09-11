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
//! One append snapshot per run, composing with: **multi-snapshot appends** (`--append`,
//! with **additive schema evolution** — new optional columns keep prior columns'
//! field-ids stable), **identity + hidden (transform) partitioning**, and **per-column
//! statistics** in the manifest.

mod avro;
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
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use shapeshift_core::{Compression, Result, ShapeError, Sink, SinkSummary};

use crate::avro::DataFileMeta;
use crate::metadata::MetadataInput;

pub use crate::avro::{
    read_manifest_data_files, read_manifest_list, ManifestDataFile, ManifestFileEntry,
};
pub use crate::metadata::{join_location, IceField};
pub use crate::partition::{
    parse_partition_expr, resolve_partition_fields, PartVal, PartitionDataFile, PartitionField,
    PartitionedWriter, Transform,
};
pub use crate::stats::Stats;

use crate::stats::StatsCollector;

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
        })
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
        let writer = self
            .writer
            .take()
            .ok_or_else(|| ShapeError::Sink("finish called twice".into()))?;
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
    pub snapshot_id: i64,
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
    pub timestamp_ms: i64,
    /// `Some` → append a new snapshot onto this existing table; `None` → fresh table.
    pub prior: Option<PriorTable>,
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
    //    new one. The whole table's data stays live in the new snapshot.
    let mut entries: Vec<ManifestFileEntry> = match &b.prior {
        Some(p) => p.manifests.clone(),
        None => Vec::new(),
    };
    entries.push(new_manifest);
    let manifest_list_bytes = avro::write_manifest_list(&entries, *Uuid::new_v4().as_bytes());
    let manifest_list_key = format!("metadata/snap-{snapshot_id}-1-{}.avro", Uuid::new_v4());
    let manifest_list_uri = join_location(b.base_location, &manifest_list_key);

    // Cumulative table totals across every live manifest in this snapshot.
    let total_records: i64 = entries.iter().map(|m| m.live_rows()).sum();
    let total_files: i64 = entries.iter().map(|m| m.live_files()).sum();

    // 3. Table metadata (v2) + version hint — fresh doc, or the prior doc extended.
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
                total_records,
                total_files,
            })?,
            1u64,
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
                added_records: added_rows,
                added_files: added_files as i64,
                total_records,
                total_files,
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
        snapshot_id,
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
    let hint_path = table_dir.join("metadata/version-hint.text");
    if !hint_path.exists() {
        return Ok(None);
    }
    let hint = std::fs::read_to_string(&hint_path).map_err(sink_err)?;
    let version: u64 = hint
        .trim()
        .parse()
        .map_err(|e| ShapeError::Sink(format!("bad version-hint.text {hint:?}: {e}")))?;
    let meta_text =
        std::fs::read_to_string(table_dir.join(format!("metadata/v{version}.metadata.json")))
            .map_err(sink_err)?;
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
    /// The existing table to append onto, loaded and validated at `create` time (`None` =
    /// a fresh table). Consumed by `finish` to chain the new snapshot.
    prior: Option<PriorTable>,
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
        let table_dir = table_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(table_dir.join("data")).map_err(sink_err)?;
        std::fs::create_dir_all(table_dir.join("metadata")).map_err(sink_err)?;

        let mut ice_fields = ice_fields_from_schema(&schema)?;

        // With `--append`, load the existing table and reconcile schema + partitioning up
        // front, before any data file is written — an incompatible append fails in
        // milliseconds instead of after a full (possibly large) write, and never leaves an
        // orphaned data file behind. Evolution runs BEFORE partition resolution so the
        // partition spec's source-ids reference the stable (prior) field-ids.
        let prior = if append {
            let base_location = abspath(&table_dir);
            let prior = load_prior_local(&table_dir, &base_location)?;
            if let Some(p) = &prior {
                evolve_fields(&mut ice_fields, &p.metadata)?;
            }
            prior
        } else {
            None
        };
        let partition_fields = resolve_partition_fields(&ice_fields, partition_by)?;
        if let Some(p) = &prior {
            ensure_partition_compatible(&partition_fields, &p.metadata)?;
        }

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
            prior,
        })
    }
}

impl Sink for IcebergSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write_batch(batch)
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
        let table_uuid = self.table_uuid.to_string();
        let art = build_metadata_artifacts(&MetadataBuild {
            base_location: &base_location,
            table_uuid: &table_uuid,
            ice_fields: &self.ice_fields,
            partition_fields: &self.partition_fields,
            data_files,
            timestamp_ms: snapshot_timestamp_ms(),
            prior,
        })?;

        // Persist the metadata blobs under the table dir; version-hint last (the
        // pointer that publishes the snapshot).
        let write = |key: &str, bytes: &[u8]| -> Result<PathBuf> {
            let p = self.table_dir.join(key);
            std::fs::write(&p, bytes).map_err(sink_err)?;
            Ok(p)
        };
        files.push(write(&art.manifest_key, &art.manifest_bytes)?);
        files.push(write(&art.manifest_list_key, &art.manifest_list_bytes)?);
        files.push(write(&art.metadata_key, &art.metadata_bytes)?);
        files.push(write(&art.version_hint_key, &art.version_hint_bytes)?);

        Ok(SinkSummary { files, rows, bytes })
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
    pub total_records: i64,
    pub fields: Vec<(String, String)>,
}

/// Parse a table's `metadata.json` text into an [`IcebergInfo`] summary. Backend-neutral
/// — the caller supplies the bytes from local FS or an object store.
pub fn parse_metadata_json(text: &str) -> Result<IcebergInfo> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(sink_err)?;

    let format_version = v["format-version"].as_i64().unwrap_or(0);
    let table_uuid = v["table-uuid"].as_str().unwrap_or("").to_string();
    let current_snapshot_id = v["current-snapshot-id"].as_i64().unwrap_or(0);
    let total_records = v["snapshots"]
        .as_array()
        .and_then(|s| s.last())
        .and_then(|s| s["summary"]["total-records"].as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
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
        fields,
    })
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
    let hint = std::fs::read_to_string(table_dir.join("metadata/version-hint.text"))
        .map_err(|e| ShapeError::Sink(format!("reading version-hint.text: {e}")))?;
    let meta_key = current_metadata_key(&hint)?;
    let text = std::fs::read_to_string(table_dir.join(&meta_key)).map_err(sink_err)?;
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

/// The data-file paths referenced by a local table's **current snapshot**, re-anchored
/// to `table_dir`. Reads only *committed* files (from the manifests), so orphan Parquet
/// left by an interrupted write is excluded — unlike a raw directory scan.
///
/// Returns an error (so the caller can fall back to a directory scan) if the manifests
/// can't be read, or if the summed data record count doesn't match the snapshot's
/// `total-records` — a guard that trips for partitioned tables, whose partition tuples
/// this unpartitioned-manifest reader doesn't decode, so we never silently drop rows.
pub fn current_data_files(table_dir: impl AsRef<Path>) -> Result<Vec<String>> {
    let table_dir = table_dir.as_ref();
    let hint =
        std::fs::read_to_string(table_dir.join("metadata/version-hint.text")).map_err(sink_err)?;
    let meta_key = current_metadata_key(&hint)?;
    let text = std::fs::read_to_string(table_dir.join(&meta_key)).map_err(sink_err)?;
    let v: serde_json::Value = serde_json::from_str(&text).map_err(sink_err)?;

    let base = v["location"].as_str().unwrap_or("");
    let current = v["current-snapshot-id"].as_i64().unwrap_or(-1);
    let snapshots = v["snapshots"]
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
    let total_records: i64 = snap["summary"]["total-records"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);

    let ml_bytes = std::fs::read(reanchor(table_dir, base, ml_path)).map_err(sink_err)?;
    let manifests = read_manifest_list(&ml_bytes)?;

    let mut files = Vec::new();
    let mut rec_sum: i64 = 0;
    for m in &manifests {
        let mbytes = std::fs::read(reanchor(table_dir, base, &m.path)).map_err(sink_err)?;
        for df in read_manifest_data_files(&mbytes)? {
            // content 0 = data; status 2 = DELETED (shapeshift is append-only, so none).
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
            timestamp_ms: 1_700_000_000_000,
            prior: None,
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
            timestamp_ms: 0,
            prior: None,
        });
        assert!(err.is_err());
    }
}
