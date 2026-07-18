//! # shapeshift-objstore — the object-store Parquet sink
//!
//! Writes shapeshift's output to an object store — **S3 / GCS / Azure**, or `file://` —
//! instead of the local filesystem, for **both** sink formats:
//!
//! - [`ObjectStoreParquetSink`] lands a single Parquet file.
//! - [`ObjectStoreIcebergSink`] lands a whole self-contained Iceberg v2 table (data +
//!   manifests + metadata) under a bucket prefix, with every embedded path anchored at
//!   that prefix — a server-less Hadoop-style catalog layout, readable by DuckDB
//!   `iceberg_scan('s3://…')`.
//!
//! Both implement `shapeshift_core::Sink`, so the shaping engine is unchanged; only the
//! destination moves. This is the first hosting primitive: everything the commercial-edition control
//! plane lands in a bucket flows through here.
//!
//! **Bounded RAM.** The Parquet is written to a local temp file first (one row group
//! at a time, exactly like [`shapeshift_parquet::ParquetSink`]), then streamed to the
//! store with a **multipart upload** in fixed 8 MiB parts on `finish`. Neither the
//! shape nor the upload ever holds the whole object in memory.
//!
//! **Credentials** come from the standard `AWS_*` / `GOOGLE_*` / `AZURE_*` environment
//! variables (the `object_store` builders' `from_env()`), so nothing is logged or
//! stored.
//!
//! **Why a separate crate.** `object_store` pulls the cloud SDKs (reqwest, TLS, the
//! provider clients) — not musl-static-clean and not wanted in the lean default
//! `shapeshift` binary. The CLI depends on this crate behind an **off-by-default
//! `object_store` feature**, so the default build never links any of it.
//!
//! Iceberg tables use **location-anchored** paths (the table's `location` and every
//! file reference are the destination URI), so the table is valid at its bucket
//! prefix; a REST catalog (relative-path relocation, multi-writer commits) stays an
//! a commercial-edition concern.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::path::Path as ObjPath;
// `ObjectStoreExt` carries the convenience methods (`put_multipart`, …); the base
// `ObjectStore` trait only exposes the `_opts` variants.
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt};
use tokio::io::AsyncReadExt;

use shapeshift_core::{Compression, RecordBatch, Result, SchemaRef, ShapeError, Sink, SinkSummary};
use shapeshift_iceberg::{
    build_metadata_artifacts, current_manifest_list_key, current_metadata_key,
    ensure_partition_compatible, evolve_fields, ice_fields_from_schema, join_location,
    parse_metadata_json, read_manifest_list, resolve_partition_fields, snapshot_timestamp_ms,
    IceField, IcebergInfo, MetadataArtifacts, MetadataBuild, PartitionField, PartitionedWriter,
    PriorTable,
};
use shapeshift_parquet::ParquetSink;

/// Multipart chunk size. Above S3's 5 MiB minimum for non-final parts, so any file
/// uploads as ≥1 valid part while RAM stays bounded to one chunk.
const PART_SIZE: usize = 8 * 1024 * 1024;

fn sink_err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// The object-store URL schemes this sink handles. (`memory://` is deliberately
/// absent — each run builds its own store, so an in-memory target would be a
/// silent no-op.)
const SCHEMES: &[&str] = &[
    "s3", "gs", "gcs", "az", "azure", "abfs", "abfss", "adl", "file",
];

/// True if `s` is an object-store URL (`scheme://…` for a scheme we handle) rather
/// than a local path. Used by the CLI to route `--output` to this sink.
pub fn is_object_url(s: &str) -> bool {
    matches!(s.split_once("://"), Some((scheme, _)) if SCHEMES.contains(&scheme))
}

/// Build an `ObjectStore` + object key from a URL, honoring the standard
/// `AWS_*` / `GOOGLE_*` / `AZURE_*` environment variables for credentials.
fn build_store(dest: &str) -> Result<(Arc<dyn ObjectStore>, ObjPath)> {
    let url =
        url::Url::parse(dest).map_err(|e| sink_err(format!("bad output URL {dest:?}: {e}")))?;
    let key = ObjPath::from_url_path(url.path()).map_err(sink_err)?;
    let store: Arc<dyn ObjectStore> = match url.scheme() {
        "s3" => Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_url(dest)
                .build()
                .map_err(sink_err)?,
        ),
        "gs" | "gcs" => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(dest)
                .build()
                .map_err(sink_err)?,
        ),
        "az" | "azure" | "abfs" | "abfss" | "adl" => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_url(dest)
                .build()
                .map_err(sink_err)?,
        ),
        "file" => Arc::new(object_store::local::LocalFileSystem::new()),
        other => {
            return Err(sink_err(format!(
                "unsupported object-store scheme `{other}://` (use s3/gs/az/file)"
            )))
        }
    };
    Ok((store, key))
}

/// A write-once Parquet sink that lands its output in an object store.
pub struct ObjectStoreParquetSink {
    dest_url: String,
    temp_path: PathBuf,
    inner: Option<ParquetSink>,
    store: Arc<dyn ObjectStore>,
    key: ObjPath,
}

impl ObjectStoreParquetSink {
    /// Create a sink that will write Parquet to `dest_url` (an `s3://` / `gs://` /
    /// `az://` / `file://` URL). The URL and scheme are validated up front and the
    /// `object_store` client is built once here and cached for the upload. Credentials
    /// are **not** checked at creation — the `object_store` builders resolve them
    /// lazily, so a missing or invalid credential surfaces on the first request (the
    /// upload at `finish`), not here.
    pub fn create(dest_url: &str, schema: SchemaRef, compression: Compression) -> Result<Self> {
        // Fail fast on a bad URL / unsupported scheme; keep the built store for finish.
        let (store, key) = build_store(dest_url)?;
        let temp_path =
            std::env::temp_dir().join(format!("shapeshift-{}.parquet", uuid::Uuid::new_v4()));
        let inner = ParquetSink::create(&temp_path, schema, compression)?;
        Ok(ObjectStoreParquetSink {
            dest_url: dest_url.to_string(),
            temp_path,
            inner: Some(inner),
            store,
            key,
        })
    }

    /// Stream the finished local temp Parquet to the object store as a multipart
    /// upload. Returns the object's byte length. Bounded to one `PART_SIZE` chunk of
    /// RAM. The async `object_store` API is bridged to the sync `Sink` at finalize
    /// time on a small current-thread runtime.
    fn upload(&self) -> Result<u64> {
        let store = self.store.clone();
        let key = self.key.clone();
        let temp = self.temp_path.clone();
        block_on(async move { upload_multipart(&store, &key, &temp).await })
    }
}

/// Build a small current-thread runtime and drive one finalize future to completion.
/// The async `object_store` API is bridged to shapeshift's sync `Sink` here, at
/// finalize time only.
fn block_on<F: std::future::Future<Output = Result<T>>, T>(fut: F) -> Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(sink_err)?
        .block_on(fut)
}

/// Stream a local file to `key` in `store` as a multipart upload (fixed `PART_SIZE`
/// parts, bounded RAM). Returns bytes uploaded. **Any** error once the session is open —
/// a read failure, a `put_part` failure, or a `complete` failure — aborts the upload, so
/// a failed run never leaves orphaned parts (which some providers bill for) on the remote.
async fn upload_multipart(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
    path: &std::path::Path,
) -> Result<u64> {
    let mut upload = store.put_multipart(key).await.map_err(sink_err)?;
    match stream_parts(&mut upload, path).await {
        Ok(total) => Ok(total),
        Err(e) => {
            // Best-effort: drop the buffered parts. Ignore the abort's own error — the
            // original failure is what the caller needs.
            let _ = upload.abort().await;
            Err(e)
        }
    }
}

/// The body of a multipart upload: read `path` in `PART_SIZE` chunks, `put_part` each,
/// then `complete`. Any error propagates to [`upload_multipart`], which aborts.
async fn stream_parts(
    upload: &mut Box<dyn MultipartUpload>,
    path: &std::path::Path,
) -> Result<u64> {
    let mut file = tokio::fs::File::open(path).await.map_err(sink_err)?;
    let mut buf = vec![0u8; PART_SIZE];
    let mut total = 0u64;
    loop {
        // Fill the buffer to PART_SIZE before emitting a part. A single `File::read` may
        // return short — for a regular file it often yields far less than the buffer — and
        // any non-final part below S3's 5 MiB minimum is rejected with EntityTooSmall. So
        // loop until the buffer is full or EOF; every part but the last is a full
        // PART_SIZE (8 MiB), which is what makes the upload valid on S3/MinIO/GCS/Azure.
        let mut filled = 0usize;
        while filled < PART_SIZE {
            let n = file.read(&mut buf[filled..]).await.map_err(sink_err)?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            break;
        }
        let payload: object_store::PutPayload = buf[..filled].to_vec().into();
        upload.put_part(payload).await.map_err(sink_err)?;
        total += filled as u64;
    }
    upload.complete().await.map_err(sink_err)?;
    Ok(total)
}

/// Put a small in-memory blob (a manifest / metadata file) at `key`.
async fn put_bytes(store: &Arc<dyn ObjectStore>, key: &ObjPath, bytes: Vec<u8>) -> Result<()> {
    let payload: object_store::PutPayload = bytes.into();
    store.put(key, payload).await.map_err(sink_err)?;
    Ok(())
}

/// Fetch a small object as UTF-8 text (`version-hint.text` / `metadata.json`).
async fn get_text(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> Result<String> {
    let res = store.get(key).await.map_err(sink_err)?;
    let bytes = res.bytes().await.map_err(sink_err)?;
    String::from_utf8(bytes.to_vec()).map_err(sink_err)
}

/// Fetch a small object's raw bytes (the manifest-list Avro, for append).
async fn get_bytes(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> Result<Vec<u8>> {
    let res = store.get(key).await.map_err(sink_err)?;
    Ok(res.bytes().await.map_err(sink_err)?.to_vec())
}

/// Fetch an object as text, returning `None` if it does not exist (so append can detect
/// "no table here yet" without treating a missing `version-hint.text` as an error).
async fn get_opt_text(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> Result<Option<String>> {
    match store.get(key).await {
        Ok(res) => {
            let bytes = res.bytes().await.map_err(sink_err)?;
            Ok(Some(String::from_utf8(bytes.to_vec()).map_err(sink_err)?))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(sink_err(e)),
    }
}

/// Join a base object key with a table-relative key, normalizing slashes.
fn child_key(base: &ObjPath, rel: &str) -> ObjPath {
    ObjPath::from(format!("{base}/{rel}"))
}

impl Sink for ObjectStoreParquetSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner
            .as_mut()
            .ok_or_else(|| sink_err("write after finish"))?
            .write_batch(batch)
    }

    fn finish(&mut self) -> Result<SinkSummary> {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| sink_err("finish called twice"))?;
        // Close the Parquet footer in the temp file, then stream it to the store.
        let local = inner.finish()?;
        tracing::debug!(dest = %self.dest_url, rows = local.rows, "uploading parquet to object store");
        let bytes = self.upload()?;
        let _ = std::fs::remove_file(&self.temp_path);
        Ok(SinkSummary {
            files: vec![PathBuf::from(&self.dest_url)],
            rows: local.rows,
            bytes,
        })
    }
}

impl Drop for ObjectStoreParquetSink {
    fn drop(&mut self) {
        // Always attempt cleanup — idempotent and harmless if `finish()` already
        // removed the temp Parquet on the success path. This also catches every
        // *failure* path: `finish()` takes `self.inner` before `inner.finish()` /
        // `upload()` run, so an error there returns early with the temp file still
        // on disk and `self.inner` already `None` — a guarded Drop would miss it.
        let _ = std::fs::remove_file(&self.temp_path);
    }
}

/// A write-once Iceberg v2 table sink that lands the whole table — data Parquet plus
/// manifest, manifest-list, `metadata.json`, and `version-hint.text` — in an object
/// store. Every embedded path is anchored at the destination URI, so the table reads
/// back from its bucket prefix (`iceberg_scan('s3://…')`), not a local path.
///
/// The data Parquet is streamed to a local temp file (bounded RAM, one row group per
/// batch, via [`shapeshift_iceberg::IcebergDataWriter`]); at `finish` the metadata is
/// built for the destination location and all five objects are written — data via a
/// multipart upload, the four metadata blobs via `put`, `version-hint.text` **last** so
/// the snapshot only becomes visible once every file it references is in place.
pub struct ObjectStoreIcebergSink {
    base_url: String,
    temp_dir: PathBuf,
    ice_fields: Vec<IceField>,
    partition_fields: Vec<PartitionField>,
    writer: Option<PartitionedWriter>,
    table_uuid: uuid::Uuid,
    store: Arc<dyn ObjectStore>,
    base_key: ObjPath,
    /// The existing table to append onto, read from the store and validated at `create`
    /// time (`None` = a fresh table). Consumed by `finish` to chain the new snapshot.
    prior: Option<PriorTable>,
}

impl ObjectStoreIcebergSink {
    /// Create a sink writing an Iceberg table under `dest_url` (an `s3://` / `gs://` /
    /// `az://` / `file://` URL used as the table location). The URL and scheme are
    /// validated up front and the `object_store` client is built once here and cached.
    /// Credentials are **not** checked at creation — the `object_store` builders resolve
    /// them lazily, so a missing or invalid credential surfaces on the first request
    /// (the uploads at `finish`), not here.
    ///
    /// `partition_by` names the identity partition columns (empty = unpartitioned). With
    /// `append = true`, if a table already exists at `dest_url` a new snapshot is committed
    /// onto it (schema and partitioning must match); otherwise a fresh table is written.
    pub fn create(
        dest_url: &str,
        schema: SchemaRef,
        compression: Compression,
        append: bool,
        partition_by: &[String],
    ) -> Result<Self> {
        // Fail fast on a bad URL / unsupported scheme; keep the built store for finish.
        let (store, base_key) = build_store(dest_url)?;
        let base_url = dest_url.trim_end_matches('/').to_string();
        let mut ice_fields = ice_fields_from_schema(&schema)?;

        // With `--append`, read the existing table and reconcile schema + partitioning up
        // front — before staging or uploading any data — so an incompatible append fails
        // immediately instead of after streaming (possibly large) files to the store.
        // Evolution runs BEFORE partition resolution so the partition spec's source-ids
        // reference the stable (prior) field-ids.
        let prior = if append {
            let prior = Self::load_prior(&store, &base_key, &base_url)?;
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

        // Partitioned data files are staged under a temp dir, then uploaded at finish.
        let temp_dir =
            std::env::temp_dir().join(format!("shapeshift-ice-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).map_err(sink_err)?;
        let writer = PartitionedWriter::new(
            &temp_dir,
            schema,
            compression,
            ice_fields.clone(),
            partition_fields.clone(),
        )?;
        Ok(ObjectStoreIcebergSink {
            base_url,
            temp_dir,
            ice_fields,
            partition_fields,
            writer: Some(writer),
            table_uuid: uuid::Uuid::new_v4(),
            store,
            base_key,
            prior,
        })
    }

    /// Load the current state of an existing table from the object store for append (the
    /// caller guards on `append`), or `None` if no table (no `version-hint.text`) exists at
    /// the destination yet.
    fn load_prior(
        store: &Arc<dyn ObjectStore>,
        base_key: &ObjPath,
        base_url: &str,
    ) -> Result<Option<PriorTable>> {
        let store = store.clone();
        let base_key = base_key.clone();
        let base_url = base_url.to_string();
        block_on(async move {
            let hint =
                match get_opt_text(&store, &child_key(&base_key, "metadata/version-hint.text"))
                    .await?
                {
                    Some(h) => h,
                    None => return Ok(None), // first write — no table to append to
                };
            let version: u64 = hint
                .trim()
                .parse()
                .map_err(|e| sink_err(format!("bad version-hint.text {hint:?}: {e}")))?;
            let meta_text = get_text(
                &store,
                &child_key(&base_key, &format!("metadata/v{version}.metadata.json")),
            )
            .await?;
            let metadata: serde_json::Value = serde_json::from_str(&meta_text).map_err(sink_err)?;
            let mlist_key = current_manifest_list_key(&metadata, &base_url)?;
            let mlist_bytes = get_bytes(&store, &child_key(&base_key, &mlist_key)).await?;
            let manifests = read_manifest_list(&mlist_bytes)?;
            Ok(Some(PriorTable {
                metadata,
                version,
                manifests,
            }))
        })
    }

    /// Upload every staged data file (multipart), then the built metadata blobs.
    /// `uploads` is `(object key, local staged path)` per data file. Returns the total
    /// data bytes uploaded.
    fn upload_table(&self, uploads: Vec<(String, PathBuf)>, art: MetadataArtifacts) -> Result<u64> {
        let store = self.store.clone();
        let base_key = self.base_key.clone();
        block_on(async move {
            // Data files upload sequentially, one bounded-RAM multipart stream at a time —
            // deliberately, to keep peak memory to a single in-flight file regardless of
            // partition count. Fanning these out with bounded concurrency (e.g.
            // `buffer_unordered`) would cut wall-clock for high-cardinality partitioned
            // tables at the cost of that RAM bound; a roadmap trade, not a v0.1 default.
            let mut data_bytes = 0u64;
            for (data_key, local) in &uploads {
                data_bytes +=
                    upload_multipart(&store, &child_key(&base_key, data_key), local).await?;
            }
            put_bytes(
                &store,
                &child_key(&base_key, &art.manifest_key),
                art.manifest_bytes,
            )
            .await?;
            put_bytes(
                &store,
                &child_key(&base_key, &art.manifest_list_key),
                art.manifest_list_bytes,
            )
            .await?;
            put_bytes(
                &store,
                &child_key(&base_key, &art.metadata_key),
                art.metadata_bytes,
            )
            .await?;
            // version-hint published last — it is the pointer that makes the snapshot
            // visible, so it must land only after every file it references.
            put_bytes(
                &store,
                &child_key(&base_key, &art.version_hint_key),
                art.version_hint_bytes,
            )
            .await?;
            Ok(data_bytes)
        })
    }
}

impl Sink for ObjectStoreIcebergSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .as_mut()
            .ok_or_else(|| sink_err("write after finish"))?
            .write_batch(batch)
    }

    fn finish(&mut self) -> Result<SinkSummary> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| sink_err("finish called twice"))?;
        let data_files = writer.finish()?;
        let rows: u64 = data_files.iter().map(|d| d.rows).sum();
        // Stage the (destination key, local path) for each data file before the move.
        let uploads: Vec<(String, PathBuf)> = data_files
            .iter()
            .map(|d| (d.data_key.clone(), self.temp_dir.join(&d.data_key)))
            .collect();
        // Read + validated at `create` time (fail-fast); consume it to chain the snapshot.
        let prior = self.prior.take();
        let table_uuid = self.table_uuid.to_string();
        let art = build_metadata_artifacts(&MetadataBuild {
            base_location: &self.base_url,
            table_uuid: &table_uuid,
            ice_fields: &self.ice_fields,
            partition_fields: &self.partition_fields,
            data_files,
            timestamp_ms: snapshot_timestamp_ms(),
            prior,
        })?;
        let metadata_uri = join_location(&self.base_url, &art.metadata_key);
        tracing::debug!(dest = %self.base_url, rows, files = uploads.len(), "uploading iceberg table to object store");
        let bytes = self.upload_table(uploads, art)?;
        let _ = std::fs::remove_dir_all(&self.temp_dir);
        Ok(SinkSummary {
            files: vec![PathBuf::from(metadata_uri)],
            rows,
            bytes,
        })
    }
}

impl Drop for ObjectStoreIcebergSink {
    fn drop(&mut self) {
        // Idempotent cleanup on every path, same discipline as the Parquet sink: a failed
        // build/upload leaves the staged data files behind for Drop to remove.
        let _ = std::fs::remove_dir_all(&self.temp_dir);
    }
}

/// Read an Iceberg table's current metadata from an object store (via
/// `version-hint.text`) and summarize it — the object-store analog of
/// [`shapeshift_iceberg::inspect`].
pub fn inspect_iceberg(url: &str) -> Result<IcebergInfo> {
    let (store, base_key) = build_store(url)?;
    block_on(async move {
        let hint = get_text(&store, &child_key(&base_key, "metadata/version-hint.text")).await?;
        let meta_key = current_metadata_key(&hint)?;
        let text = get_text(&store, &child_key(&base_key, &meta_key)).await?;
        parse_metadata_json(&text)
    })
}
