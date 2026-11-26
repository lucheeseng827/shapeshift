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
//! destination moves.
//!
//! **Bounded RAM.** The Parquet is written to a local temp file first (one row group
//! at a time, exactly like [`shapeshift_parquet::ParquetSink`]), then uploaded on
//! `finish`: one `PUT` when it fits in a part, else a **multipart upload** in fixed
//! 8 MiB parts. Neither the shape nor the upload ever holds the whole object in memory.
//!
//! **Overlapped requests.** A table write puts all its objects — data files, delete files,
//! manifests, the manifest list — 8 at a time, since nothing names them until the guarded
//! metadata file does; the file bytes in flight never pass two parts (16 MiB). If any put
//! fails, or the commit is refused, everything the write put is removed again. Deletes go
//! to the store's many-delete call (on S3, one request per thousand keys).
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
//! prefix.
//!
//! **A table a REST catalog names** ([`ObjectStoreIcebergSink::create_in_catalog`],
//! [`ObjectStoreIcebergSink::create_merge_in_catalog`]) is written the same way at the
//! location the catalog gives it, then committed through the catalog instead of by
//! creating the next metadata file (see the `catalog` module). [`compact_in_catalog`] and
//! [`expire_in_catalog`] maintain such a table the same way.
//!
//! **Merge** ([`ObjectStoreIcebergSink::create_merge`]) runs the same upsert as a local
//! table. The existing table is read through [`StoreFiles`]: manifests and delete files
//! whole, and a data file's footer plus its key-column chunks with ranged GETs, one row
//! group at a time. The delete files are staged and uploaded with the data files.
//! [`compact_iceberg`] compacts an object-store table through the same seam, and
//! [`expire_iceberg`] expires its old snapshots.

mod catalog;
pub use catalog::{
    CatalogAuth, CatalogConnection, CREDENTIAL_ENV, DEFAULT_SCOPE, OAUTH2_SERVER_URI_ENV,
    SCOPE_ENV, TOKEN_ENV,
};
#[cfg(test)]
mod catalog_tests;

use std::path::PathBuf;
use std::sync::Arc;

use object_store::path::Path as ObjPath;
// `ObjectStoreExt` carries the convenience methods (`put_multipart`, …); the base
// `ObjectStore` trait only exposes the `_opts` variants.
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt};
use tokio::io::AsyncReadExt;

use shapeshift_core::{Compression, RecordBatch, Result, SchemaRef, ShapeError, Sink, SinkSummary};
use shapeshift_iceberg::{
    build_metadata_artifacts, catalog_commit, compact, continues_from, create_table_request,
    current_manifest_list_key, decide_identity, earlier_attempt_summary,
    ensure_partition_compatible, evolve_fields, expiry_commit, ice_fields_from_schema,
    is_elsewhere, join_location, main_snapshot, metadata_key, moved_on, parse_metadata_json,
    plan_expiry, plan_relocation, read_manifest_list, rebase_blocked, resolve_partition_fields,
    run_in, same_store, snapshot_of_run, snapshot_timestamp_ms, staged_paths, undecided_error,
    validate_run_id, version_taken, warn_if_elsewhere, Anchors, CompactContext, CompactOptions,
    CompactSummary, DeleteTarget, ExpireOptions, ExpireSummary, IceField, IcebergInfo, Identity,
    MergeContext, MergeWriter, MetadataArtifacts, MetadataBuild, OldPlace, PartitionField,
    PartitionedWriter, PriorTable, RelocateContext, RelocateOptions, RelocateSummary, RunSnapshot,
    SnapshotTags, TableFiles, TableIdent,
};

use crate::catalog::{backoff, CatalogOptions, Created, RestCatalog, Settled};
use shapeshift_parquet::ParquetSink;

/// Multipart chunk size. Above S3's 5 MiB minimum for non-final parts, so any file
/// uploads as ≥1 valid part while RAM stays bounded to one chunk.
const PART_SIZE: usize = 8 * 1024 * 1024;

fn sink_err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// An HTTP client (an object store's, or the catalog's) that could not be built. Its own
/// message is only "builder error"; the cause is further down. The common one: a system
/// with no CA certificates (an image on `scratch`, distroless-static or a slim base without
/// `ca-certificates`), where the TLS setup finds no roots and the client cannot start, plain
/// http included. That is said, with what to do; anything else shows its whole chain, each
/// cause once (an `object_store` error already prints its cause in its own message).
pub(crate) fn client_err(e: impl std::error::Error) -> ShapeError {
    let mut chain = vec![e.to_string()];
    let mut cause = e.source();
    while let Some(c) = cause {
        let shown = c.to_string();
        if !chain.iter().any(|m| m.contains(&shown)) {
            chain.push(shown);
        }
        cause = c.source();
    }
    // rustls-platform-verifier's words (0.7, through reqwest 0.13's `rustls` feature) when
    // rustls-native-certs loads no roots. A bump that rewords them falls back to the chain,
    // and the CLI test `a_system_without_ca_certificates_is_named_as_the_cause` fails.
    if chain.iter().any(|m| m.contains("No CA certificates")) {
        return ShapeError::Sink(
            "the HTTP client cannot start: no CA certificates were found on this system (it \
             checks TLS against them, and needs them even for plain http). Install them (the \
             `ca-certificates` package on Debian, Ubuntu and Alpine), or point SSL_CERT_FILE \
             at a PEM bundle"
                .into(),
        );
    }
    ShapeError::Sink(chain.join(": "))
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
    // Spark's `s3a://` and `s3n://` are S3 (a table it wrote records them, and a relocation
    // keeps that spelling): opened as `s3://`, while the paths written keep the table's own.
    let as_s3 = same_store(dest);
    let dest: &str = &as_s3;
    // A table at a local path (a catalog's table registered by its path) is the same place
    // as its `file://` URL. A catalog's `location` reaches here too, and is trusted as the
    // catalog is: a plain path opens the local filesystem, as a `file://` location already
    // did. Every other destination is passed to its store's builder as it was given.
    let url = match dest.starts_with('/') {
        true => url::Url::from_file_path(dest)
            .map_err(|()| sink_err(format!("bad local path {dest:?}")))?,
        false => {
            url::Url::parse(dest).map_err(|e| sink_err(format!("bad output URL {dest:?}: {e}")))?
        }
    };
    let key = ObjPath::from_url_path(url.path()).map_err(sink_err)?;
    let store: Arc<dyn ObjectStore> = match url.scheme() {
        "s3" => Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_url(dest)
                .build()
                .map_err(client_err)?,
        ),
        "gs" | "gcs" => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(dest)
                .build()
                .map_err(client_err)?,
        ),
        "az" | "azure" | "abfs" | "abfss" | "adl" => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_url(dest)
                .build()
                .map_err(client_err)?,
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

    /// Upload the finished local temp Parquet to the object store: one `PUT` if it fits
    /// in a part, else a multipart upload. Returns the object's byte length. Bounded to
    /// one `PART_SIZE` chunk of RAM. The async `object_store` API is bridged to the sync
    /// `Sink` at finalize time on a small current-thread runtime.
    fn upload(&self) -> Result<u64> {
        let store = self.store.clone();
        let key = self.key.clone();
        let temp = self.temp_path.clone();
        let size = std::fs::metadata(&temp).map_err(sink_err)?.len();
        block_on(async move { upload_file(&store, &key, &temp, size).await })
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

/// Upload a local file to `key`: one `PUT` when it fits in a part, else a multipart
/// upload in `PART_SIZE` parts. Returns bytes uploaded. Either way at most one part's worth
/// of the file is in memory. **Any** error once a multipart session is open — a read
/// failure, a `put_part` failure, or a `complete` failure — aborts it, so a failed run
/// never leaves orphaned parts (which some providers bill for) on the remote.
async fn upload_file(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
    path: &std::path::Path,
    size: u64,
) -> Result<u64> {
    if size <= PART_SIZE as u64 {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| sink_err(format!("{}: {e}", path.display())))?;
        let n = bytes.len() as u64;
        store.put(key, bytes.into()).await.map_err(sink_err)?;
        return Ok(n);
    }
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
/// then `complete`. Any error propagates to [`upload_file`], which aborts. Each part's
/// buffer is handed to the request as is (no copy) and dropped when it completes.
async fn stream_parts(
    upload: &mut Box<dyn MultipartUpload>,
    path: &std::path::Path,
) -> Result<u64> {
    let mut file = tokio::fs::File::open(path).await.map_err(sink_err)?;
    let mut total = 0u64;
    loop {
        // Fill the buffer to PART_SIZE before emitting a part. A single `File::read` may
        // return short — for a regular file it often yields far less than the buffer — and
        // any non-final part below S3's 5 MiB minimum is rejected with EntityTooSmall. So
        // loop until the buffer is full or EOF; every part but the last is a full
        // PART_SIZE (8 MiB), which is what makes the upload valid on S3/MinIO/GCS/Azure.
        let mut buf = vec![0u8; PART_SIZE];
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
        buf.truncate(filled);
        upload.put_part(buf.into()).await.map_err(sink_err)?;
        total += filled as u64;
    }
    upload.complete().await.map_err(sink_err)?;
    Ok(total)
}

/// Requests a batch of uploads or deletes keeps in flight. Each waits a round trip on the
/// store whatever else is in flight, so one after another a table's write mostly waits.
const STORE_WRITERS: usize = 8;

/// File bytes a batch of uploads may hold in memory at once: two parts — what a single
/// multipart upload held before uploads overlapped (its part buffer and a copy of it). A
/// file larger than a part costs one part; a smaller one, its size.
const WRITE_BUDGET: u64 = 2 * PART_SIZE as u64;

/// Run `jobs` on the current runtime, at most [`STORE_WRITERS`] at a time and with their
/// `cost`s (bytes each holds in memory) never past [`WRITE_BUDGET`] together; a job that
/// costs more than the budget runs alone. Results come back in job order. After a job
/// fails, no new one starts and the ones in flight finish, so the caller knows exactly
/// what was done: `None` marks a job that never started.
async fn run_bounded<J, T, F, Fut>(
    jobs: Vec<J>,
    cost: impl Fn(&J) -> u64,
    run: F,
) -> Vec<Option<Result<T>>>
where
    F: Fn(J) -> Fut,
    Fut: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let mut results: Vec<Option<Result<T>>> = (0..jobs.len()).map(|_| None).collect();
    let mut running = tokio::task::JoinSet::new();
    let mut held = 0u64;
    let mut failed = false;
    let mut jobs = jobs.into_iter().enumerate().peekable();
    loop {
        while !failed && running.len() < STORE_WRITERS {
            let Some((_, job)) = jobs.peek() else { break };
            let c = cost(job);
            if !running.is_empty() && held + c > WRITE_BUDGET {
                break;
            }
            let (i, job) = jobs.next().expect("peeked");
            held += c;
            let fut = run(job);
            running.spawn(async move { (i, c, fut.await) });
        }
        let Some(done) = running.join_next().await else {
            break;
        };
        let (i, c, r) = done.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()));
        held -= c;
        failed |= r.is_err();
        results[i] = Some(r);
    }
    results
}

/// One object a table write puts: a staged local file, or bytes already in memory.
enum Put {
    /// `counted`: its bytes are reported as the write's data bytes.
    File {
        key: ObjPath,
        path: PathBuf,
        size: u64,
        counted: bool,
    },
    Bytes {
        key: ObjPath,
        bytes: Vec<u8>,
    },
}

impl Put {
    fn file(key: ObjPath, path: PathBuf, counted: bool) -> Result<Put> {
        let size = std::fs::metadata(&path)
            .map_err(|e| sink_err(format!("{}: {e}", path.display())))?
            .len();
        Ok(Put::File {
            key,
            path,
            size,
            counted,
        })
    }

    fn key(&self) -> &ObjPath {
        match self {
            Put::File { key, .. } | Put::Bytes { key, .. } => key,
        }
    }

    /// Bytes it adds to memory while in flight (in-memory bytes are held already).
    fn cost(&self) -> u64 {
        match self {
            Put::File { size, .. } => (*size).min(PART_SIZE as u64),
            Put::Bytes { .. } => 0,
        }
    }

    /// Put it; the data bytes it counts for.
    async fn put(self, store: Arc<dyn ObjectStore>) -> Result<u64> {
        match self {
            Put::File {
                key,
                path,
                size,
                counted,
            } => {
                let n = upload_file(&store, &key, &path, size).await?;
                Ok(if counted { n } else { 0 })
            }
            Put::Bytes { key, bytes } => put_bytes(&store, &key, bytes).await.map(|_| 0),
        }
    }
}

/// Put every object, [`STORE_WRITERS`] at a time; the data bytes counted. If any put
/// fails, every object of the batch is removed again (best effort) and the first failure
/// is returned: a failed write leaves nothing behind.
async fn put_all(store: &Arc<dyn ObjectStore>, puts: Vec<Put>) -> Result<u64> {
    let keys: Vec<ObjPath> = puts.iter().map(|p| p.key().clone()).collect();
    let results = run_bounded(puts, Put::cost, |p| p.put(store.clone())).await;
    let mut bytes = 0u64;
    let mut first_err = None;
    for r in results.into_iter().flatten() {
        match r {
            Ok(n) => bytes += n,
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    if let Some(e) = first_err {
        remove_all(store, keys).await;
        return Err(e);
    }
    Ok(bytes)
}

/// A stream of an iterator's items, every one ready at once.
struct Ready<I>(I);

impl<I: Iterator + Unpin> futures_core::Stream for Ready<I> {
    type Item = I::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<I::Item>> {
        std::task::Poll::Ready(self.0.next())
    }
}

/// Delete every key: whether each one is gone (deleted, or absent already), in key order.
/// Never fails as a whole. The keys go to the store's own many-delete call — one
/// `DeleteObjects` request per thousand keys on S3, batches of 256 on Azure, parallel
/// single deletes elsewhere — and any key it does not confirm is tried once more on its
/// own, [`STORE_WRITERS`] at a time, where a key already gone reads as gone.
async fn remove_all(store: &Arc<dyn ObjectStore>, keys: Vec<ObjPath>) -> Vec<bool> {
    let mut confirmed = std::collections::HashSet::new();
    let mut out = store.delete_stream(Box::pin(Ready(keys.clone().into_iter().map(Ok))));
    while let Some(r) = std::future::poll_fn(|cx| out.as_mut().poll_next(cx)).await {
        if let Ok(key) = r {
            confirmed.insert(key);
        }
    }
    let mut gone: Vec<bool> = keys.iter().map(|k| confirmed.contains(k)).collect();
    let rest: Vec<(usize, ObjPath)> = keys
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !gone[*i])
        .collect();
    let retried = run_bounded(
        rest.iter().map(|(_, k)| k.clone()).collect(),
        |_| 0,
        |key| {
            let store = store.clone();
            async move {
                Ok(matches!(
                    store.delete(&key).await,
                    Ok(()) | Err(object_store::Error::NotFound { .. })
                ))
            }
        },
    )
    .await;
    for ((i, _), r) in rest.iter().zip(retried) {
        gone[*i] = matches!(r, Some(Ok(true)));
    }
    gone
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

/// Fetch and parse `metadata/v{version}.metadata.json`, or `None` if it does not exist.
async fn get_opt_metadata(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
    version: u64,
) -> Result<Option<serde_json::Value>> {
    match get_opt_text(store, &child_key(base_key, &metadata_key(version))).await? {
        Some(text) => serde_json::from_str(&text).map(Some).map_err(sink_err),
        None => Ok(None),
    }
}

/// The newest committed version of the table under `base_key`: its version hint, then
/// every later metadata file that continues from the one before (see
/// `shapeshift_iceberg::latest_version`). `None` if there is no table.
async fn store_version(store: &Arc<dyn ObjectStore>, base_key: &ObjPath) -> Result<Option<u64>> {
    let hint = match get_opt_text(store, &child_key(base_key, "metadata/version-hint.text")).await?
    {
        Some(h) => h
            .trim()
            .parse::<u64>()
            .map_err(|e| sink_err(format!("bad version-hint.text {h:?}: {e}")))?,
        None => 0,
    };
    let mut v = hint;
    while let Some(next) = get_opt_metadata(store, base_key, v + 1).await? {
        if !continues_from(&next, v) {
            break;
        }
        v += 1;
    }
    Ok((v > 0).then_some(v))
}

/// [`moved_on`] for a table in a store: report a failure as a conflict if the table has
/// moved past `base` since the write began.
fn moved_on_in_store(
    err: ShapeError,
    base: u64,
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
) -> ShapeError {
    let (store, base_key) = (store.clone(), base_key.clone());
    match block_on(async move { store_version(&store, &base_key).await }) {
        Ok(now) => moved_on(err, base, now),
        Err(_) => err,
    }
}

/// The snapshot run `run_id` committed to the table under `base_key`, if there is a table
/// and its newest metadata holds one.
fn find_run_at(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
    run_id: &str,
) -> Result<Option<RunSnapshot>> {
    let (store, base_key) = (store.clone(), base_key.clone());
    block_on(async move {
        let Some(version) = store_version(&store, &base_key).await? else {
            return Ok(None);
        };
        let meta = get_opt_metadata(&store, &base_key, version).await?;
        Ok(meta.and_then(|m| snapshot_of_run(&m, run_id)))
    })
}

/// The snapshot run `run_id` committed to the Iceberg table at `url` (a table location in
/// an object store), if the table exists and its history holds one. An orchestrator's
/// retry asks this before doing the work again: a run whose snapshot is there is done.
pub fn find_run(url: &str, run_id: &str) -> Result<Option<RunSnapshot>> {
    let (store, base_key) = build_store(url)?;
    find_run_at(&store, &base_key, run_id)
}

/// The snapshot run `run_id` committed to `table` (`namespace.name`) in the REST catalog
/// `catalog`, if the catalog has the table and its history holds one.
pub fn find_run_in_catalog(
    catalog: &CatalogConnection,
    table: &str,
    run_id: &str,
) -> Result<Option<RunSnapshot>> {
    let ident = TableIdent::parse(table)?;
    let timeout = CatalogOptions::default().timeout;
    block_on(async move {
        let catalog = RestCatalog::connect(catalog, timeout).await?;
        let loaded = catalog.load(&ident).await?;
        Ok(loaded.and_then(|t| snapshot_of_run(&t.metadata, run_id)))
    })
}

/// A commit that did not go through.
struct NotCommitted {
    error: ShapeError,
    /// Another writer's metadata holds the version, so no committed snapshot names this
    /// write's staged objects and they can be removed. When the outcome is unknown, they
    /// stay: an orphan costs storage, a deleted file of a committed snapshot breaks reads.
    refused: bool,
}

/// Create the metadata file for `version` only if no writer has — the commit point — with
/// a conditional put (`If-None-Match: *` on S3). A store that does not honour it cannot
/// guard the commit; one that refuses the mode errors here rather than overwrite.
///
/// An error does not always mean the put failed: a 5xx after the store applied it is
/// retried, and the retry finds the file and fails as `AlreadyExists`; a timeout can come
/// back for a put that landed. So on any error the key is read back, and if it holds these
/// exact bytes — which carry this write's own snapshot id — the commit stands.
async fn publish_metadata(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
    version: u64,
    bytes: Vec<u8>,
) -> std::result::Result<(), NotCommitted> {
    let key = child_key(base_key, &metadata_key(version));
    let opts = object_store::PutOptions {
        mode: object_store::PutMode::Create,
        ..Default::default()
    };
    let payload = bytes::Bytes::from(bytes);
    let err = match store.put_opts(&key, payload.clone().into(), opts).await {
        Ok(_) => return Ok(()),
        Err(e) => e,
    };
    let stored = match store.get(&key).await {
        Ok(r) => r.bytes().await.ok(),
        Err(_) => None,
    };
    // Creates are exclusive, so a key holding other bytes means this put never landed: the
    // only outcome known to be a refusal. A key that cannot be read leaves it unknown, even
    // after `AlreadyExists` — the bytes there may be this write's own — and the error is
    // not one to retry blindly, since the commit may stand.
    Err(match stored {
        Some(ours) if ours[..] == payload[..] => return Ok(()),
        Some(other) => NotCommitted {
            error: version_taken(version, serde_json::from_slice(&other).ok().as_ref()),
            refused: true,
        },
        None => NotCommitted {
            error: match err {
                object_store::Error::AlreadyExists { .. } => sink_err(format!(
                    "committing {key}: it exists, but could not be read back to tell whether \
                     it is this write's; the outcome is unknown"
                )),
                e => sink_err(format!("committing {key}: {e}")),
            },
            refused: false,
        },
    })
}

/// Point `version-hint.text` at `version`, unless a later version is already committed.
/// A failure is only logged: the commit stands, and readers find it past the hint.
async fn publish_hint(store: &Arc<dyn ObjectStore>, base_key: &ObjPath, version: u64) {
    match get_opt_metadata(store, base_key, version + 1).await {
        Ok(Some(next)) if continues_from(&next, version) => return,
        _ => {}
    }
    let key = child_key(base_key, "metadata/version-hint.text");
    if let Err(e) = put_bytes(store, &key, version.to_string().into_bytes()).await {
        tracing::warn!(version, error = %e,
            "committed, but version-hint.text was not updated; readers find the commit past it");
    }
}

/// Join a base object key with a table-relative key, normalizing slashes.
fn child_key(base: &ObjPath, rel: &str) -> ObjPath {
    ObjPath::from(format!("{base}/{rel}"))
}

/// Files an object-store table's reads keep in flight. A store answers each request in
/// tens of milliseconds whatever else is in flight, so reading one file after another
/// leaves a merge waiting; each reader holds at most one row group's key chunks.
const STORE_READERS: usize = 8;

/// An object-store table's existing files, for a merge to read. A path recorded under one
/// of the table's [`Anchors`] (the places it has been written at) resolves under the
/// table's current key, so a moved or copied table reads its own files; any other path is
/// read at the key its URL names in the same store.
///
/// It has **its own client**, used only on its own runtime. A pooled keep-alive connection
/// belongs to the runtime it was opened on: reused from another runtime while that one is
/// not being driven, its request is never sent and hangs until it times out. (Every other
/// call in this crate runs on a runtime of its own that ends with it, which closes its
/// connections; this runtime outlives many reads, so it must not share them.)
pub struct StoreFiles {
    store: Arc<dyn ObjectStore>,
    base_key: ObjPath,
    anchors: Anchors,
    /// One runtime for every read of the merge, rather than one per request. Readers on
    /// several threads share it: the first to block on it drives the I/O for all of them.
    rt: tokio::runtime::Runtime,
}

impl StoreFiles {
    /// The table at `url`; `anchors` are the directories its recorded paths were written
    /// against ([`Anchors::of`] its metadata, or [`Anchors::at`] a catalog table's
    /// location).
    pub fn open(url: &str, anchors: Anchors) -> Result<Self> {
        let (store, base_key) = build_store(url)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(sink_err)?;
        Ok(StoreFiles {
            store,
            base_key,
            anchors,
            rt,
        })
    }

    fn key(&self, path: &str) -> Result<ObjPath> {
        if let Some(key) = self.anchors.key(path)? {
            return Ok(child_key(&self.base_key, key));
        }
        let url = url::Url::parse(path)
            .map_err(|e| sink_err(format!("{path}: not a path in this store ({e})")))?;
        ObjPath::from_url_path(url.path()).map_err(sink_err)
    }
}

impl TableFiles for StoreFiles {
    fn readers(&self) -> usize {
        STORE_READERS
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let key = self.key(path)?;
        self.rt.block_on(get_bytes(&self.store, &key))
    }

    fn size(&self, path: &str) -> Result<u64> {
        let key = self.key(path)?;
        self.rt.block_on(async {
            let meta = self
                .store
                .head(&key)
                .await
                .map_err(|e| sink_err(format!("{path}: {e}")))?;
            Ok(meta.size)
        })
    }

    fn read_ranges(
        &self,
        path: &str,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        let key = self.key(path)?;
        // `get_ranges` coalesces nearby ranges into one request.
        self.rt.block_on(async {
            self.store
                .get_ranges(&key, ranges)
                .await
                .map_err(|e| sink_err(format!("{path}: {e}")))
        })
    }
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
            merge: None,
            table_version: None,
            snapshot_id: None,
            rebases: 0,
            earlier_attempt: false,
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
    compression: Compression,
    /// The existing table to append onto, read from the store and validated at `create`
    /// time (`None` = a fresh table). Consumed by `finish` to chain the new snapshot.
    prior: Option<PriorTable>,
    /// Without `append`: the version of a table already at the destination, which the
    /// fresh table replaces as the next version.
    replaces: Option<u64>,
    /// Set for a merge: follows the written keys so `finish` can delete what they
    /// supersede.
    merge: Option<MergeWriter>,
    /// Set for a table a REST catalog names: `finish` commits through the catalog instead
    /// of creating the next metadata file itself.
    catalog: Option<CatalogTarget>,
    /// The caller's run id, recorded in the snapshot and looked for when a commit is
    /// refused (see [`ObjectStoreIcebergSink::set_run_id`]).
    run_id: Option<String>,
}

/// The catalog a sink commits through, the table, and the table's metadata as the write
/// loaded it (what the commit's requirements assert).
struct CatalogTarget {
    catalog: RestCatalog,
    table: TableIdent,
    loaded: serde_json::Value,
    /// How many times a refused append may be built again onto a newer head: none for a
    /// merge, a compaction or an expiry, each planned against the head it loaded.
    max_rebases: u32,
    /// How many times a commit whose answer was lost may be sent again unchanged.
    max_resends: u32,
    /// The write may add to a table that holds data (`--append`). Without it, a write
    /// another writer beat to the table is not rebuilt onto what they committed.
    append: bool,
}

/// Everything a sink is put together from, once the table is resolved.
struct Assembly {
    store: Arc<dyn ObjectStore>,
    base_key: ObjPath,
    base_url: String,
    schema: SchemaRef,
    compression: Compression,
    ice_fields: Vec<IceField>,
    partition_fields: Vec<PartitionField>,
    prior: Option<PriorTable>,
    replaces: Option<u64>,
    merge: Option<MergeWriter>,
    catalog: Option<CatalogTarget>,
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
        Self::open(dest_url, schema, compression, append, partition_by, &[])
    }

    /// Create a sink that **merges** (upserts) into the table at `dest_url` on the
    /// `merge_on` key columns, creating the table if there is none — the object-store
    /// twin of `IcebergSink::create_merge`, with the same semantics. Finding the rows to
    /// replace reads only the existing data files' footers and key-column chunks (ranged
    /// GETs), skipping files whose key bounds rule out every incoming key.
    pub fn create_merge(
        dest_url: &str,
        schema: SchemaRef,
        compression: Compression,
        partition_by: &[String],
        merge_on: &[String],
    ) -> Result<Self> {
        if merge_on.is_empty() {
            return Err(sink_err("merge_on names no key column"));
        }
        Self::open(dest_url, schema, compression, true, partition_by, merge_on)
    }

    fn open(
        dest_url: &str,
        schema: SchemaRef,
        compression: Compression,
        append: bool,
        partition_by: &[String],
        merge_on: &[String],
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
        let (prior, replaces) = if append {
            let prior = Self::load_prior(&store, &base_key, &base_url)?;
            if let Some(p) = &prior {
                evolve_fields(&mut ice_fields, &p.metadata)?;
            }
            (prior, None)
        } else {
            let (store, base_key) = (store.clone(), base_key.clone());
            (
                None,
                block_on(async move { store_version(&store, &base_key).await })?,
            )
        };
        let partition_fields = resolve_partition_fields(&ice_fields, partition_by)?;
        if let Some(p) = &prior {
            ensure_partition_compatible(&partition_fields, &p.metadata)?;
        }
        // Key columns resolve against the evolved fields, before anything is staged.
        let merge = if merge_on.is_empty() {
            None
        } else {
            Some(MergeWriter::new(&ice_fields, merge_on)?)
        };
        Self::assemble(Assembly {
            store,
            base_key,
            base_url,
            schema,
            compression,
            ice_fields,
            partition_fields,
            prior,
            replaces,
            merge,
            catalog: None,
        })
    }

    /// Create a sink writing to `table` (`namespace.name`) in the Iceberg REST catalog
    /// `catalog`. The catalog names the table's location, where
    /// the files go (any store this crate writes to), and is the commit point: `finish`
    /// asks it to add this write's snapshot only if the table and its `main` branch are
    /// still as loaded here.
    ///
    /// A table the catalog does not have is created empty, with this write's schema and
    /// partitioning (`partition_by`), in a namespace that must exist. A table that already
    /// holds a snapshot is written to only with `append = true`; its schema evolves and
    /// its partitioning must match, as for any append.
    pub fn create_in_catalog(
        catalog: &CatalogConnection,
        table: &str,
        schema: SchemaRef,
        compression: Compression,
        append: bool,
        partition_by: &[String],
    ) -> Result<Self> {
        let options = CatalogOptions::default();
        Self::open_in_catalog(
            catalog,
            table,
            schema,
            compression,
            append,
            partition_by,
            &[],
            options,
        )
    }

    /// Create a sink that **merges** (upserts) into `table` (`namespace.name`) in the REST
    /// catalog `catalog` on the `merge_on` key columns:
    /// [`create_merge`](Self::create_merge) for a table a catalog names, creating the table
    /// if the catalog has none. The commit asserts what every catalog commit does, and is
    /// not rebuilt onto another writer's: the rows it replaces were found in the head it
    /// loaded, and a commit in between may have added rows with the same keys. A refusal is
    /// a conflict (exit 75).
    pub fn create_merge_in_catalog(
        catalog: &CatalogConnection,
        table: &str,
        schema: SchemaRef,
        compression: Compression,
        partition_by: &[String],
        merge_on: &[String],
    ) -> Result<Self> {
        if merge_on.is_empty() {
            return Err(sink_err("merge_on names no key column"));
        }
        Self::open_in_catalog(
            catalog,
            table,
            schema,
            compression,
            true,
            partition_by,
            merge_on,
            CatalogOptions::default(),
        )
    }

    /// [`create_in_catalog`](Self::create_in_catalog) (or, with `merge_on`, a merge), with
    /// how long one catalog call may take and how many times a refused append is rebuilt
    /// onto a newer head.
    #[allow(clippy::too_many_arguments)]
    fn open_in_catalog(
        catalog: &CatalogConnection,
        table: &str,
        schema: SchemaRef,
        compression: Compression,
        append: bool,
        partition_by: &[String],
        merge_on: &[String],
        options: CatalogOptions,
    ) -> Result<Self> {
        let ident = TableIdent::parse(table)?;
        let mut ice_fields = ice_fields_from_schema(&schema)?;
        let (catalog, loaded) = {
            let (conn, ident, fields) = (catalog, &ident, &ice_fields);
            block_on(async move {
                let catalog = RestCatalog::connect(conn, options.timeout).await?;
                let loaded = match catalog.load(ident).await? {
                    Some(t) => t,
                    None => {
                        let parts = resolve_partition_fields(fields, partition_by)?;
                        let request = create_table_request(&ident.name, fields, &parts);
                        match catalog.create(ident, &request).await? {
                            Created::Table(t) => t,
                            // Created by another writer, or perhaps by this request: an
                            // empty table either way, and this write goes to it.
                            Created::LoadIt(why) => {
                                catalog.load(ident).await?.ok_or_else(|| {
                                    sink_err(format!(
                                        "{ident} could not be created ({why}), and the catalog \
                                     does not have it"
                                    ))
                                })?
                            }
                        }
                    }
                };
                Ok((catalog, loaded))
            })?
        };
        let metadata = loaded.metadata;
        let head = main_snapshot(&metadata);
        if let (Some(id), false) = (head, append) {
            return Err(sink_err(format!(
                "{ident} already holds data (snapshot {id}); add --append to commit a new \
                 snapshot onto it"
            )));
        }
        let files = CatalogFiles::of(&metadata, &ident)?;
        // As for any append: evolve against the table's schema (the catalog may have
        // numbered the fields itself), then hold the partitioning to the table's.
        evolve_fields(&mut ice_fields, &metadata)?;
        let partition_fields = resolve_partition_fields(&ice_fields, partition_by)?;
        ensure_partition_compatible(&partition_fields, &metadata)?;
        // Key columns resolve against the evolved fields, before anything is staged.
        let merge = if merge_on.is_empty() {
            None
        } else {
            Some(MergeWriter::new(&ice_fields, merge_on)?)
        };
        let max_rebases = if merge.is_some() {
            0
        } else {
            options.max_rebases
        };
        Self::assemble(Assembly {
            store: files.store,
            base_key: files.base_key,
            base_url: files.location,
            schema,
            compression,
            ice_fields,
            partition_fields,
            prior: Some(files.prior),
            replaces: None,
            merge,
            catalog: Some(CatalogTarget {
                catalog,
                table: ident,
                loaded: metadata,
                max_rebases,
                max_resends: options.max_resends,
                append,
            }),
        })
    }

    /// Stage the writer under a temp dir (the partitioned data files are uploaded at
    /// finish) and put the sink together.
    fn assemble(a: Assembly) -> Result<Self> {
        let temp_dir =
            std::env::temp_dir().join(format!("shapeshift-ice-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).map_err(sink_err)?;
        let writer = PartitionedWriter::new(
            &temp_dir,
            a.schema,
            a.compression,
            a.ice_fields.clone(),
            a.partition_fields.clone(),
        )?;
        Ok(ObjectStoreIcebergSink {
            base_url: a.base_url,
            temp_dir,
            ice_fields: a.ice_fields,
            partition_fields: a.partition_fields,
            writer: Some(writer),
            table_uuid: uuid::Uuid::new_v4(),
            store: a.store,
            base_key: a.base_key,
            compression: a.compression,
            prior: a.prior,
            replaces: a.replaces,
            merge: a.merge,
            catalog: a.catalog,
            run_id: None,
        })
    }

    /// Name the run this write belongs to. The id is recorded in the snapshot
    /// (`shapeshift.run-id`), and a commit that another writer beats is checked against it:
    /// if the table now holds a snapshot with the same id, an earlier attempt of this run
    /// committed first, and this write reports that snapshot instead of failing, rebasing,
    /// or appending the rows a second time.
    pub fn set_run_id(&mut self, run_id: &str) -> Result<()> {
        validate_run_id(run_id)?;
        self.run_id = Some(run_id.to_string());
        Ok(())
    }

    /// After a refused commit, or a merge that found the table moved on (its files already
    /// removed): the summary of an earlier attempt of this run that the table now holds, or
    /// the refusal.
    fn earlier_attempt_or(&self, refusal: ShapeError) -> Result<SinkSummary> {
        if let (Some(id), ShapeError::Conflict(_)) = (&self.run_id, &refusal) {
            let found = match &self.catalog {
                None => find_run_at(&self.store, &self.base_key, id),
                Some(target) => block_on(load_head(target)).map(|now| snapshot_of_run(&now, id)),
            };
            match found {
                Ok(Some(found)) => return Ok(earlier_attempt_summary(&found)),
                Ok(None) => {}
                // The refusal stands (nothing of this write landed, and a retry with the same
                // run id is safe); only the look for an earlier attempt failed.
                Err(e) => tracing::warn!(run = %id, error = %e,
                    "could not look for an earlier attempt of this run after a refusal"),
            }
        }
        Err(refusal)
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
            let Some(version) = store_version(&store, &base_key).await? else {
                return Ok(None); // first write — no table to append to
            };
            let meta_text = get_text(&store, &child_key(&base_key, &metadata_key(version))).await?;
            let metadata: serde_json::Value = serde_json::from_str(&meta_text).map_err(sink_err)?;
            warn_if_elsewhere(&metadata, &base_url);
            let mlist_key = current_manifest_list_key(&metadata)?;
            let mlist_bytes = get_bytes(&store, &child_key(&base_key, &mlist_key)).await?;
            let manifests = read_manifest_list(&mlist_bytes)?;
            Ok(Some(PriorTable {
                metadata,
                version,
                manifests,
            }))
        })
    }
}

/// Upload a table write: every staged data file and delete file, the manifests and the
/// manifest list — together, [`STORE_WRITERS`] at a time, since nothing names them until
/// the metadata file does — then the metadata file (the guarded commit), then the version
/// hint. `uploads` and `deletes` are `(object key, local staged path)` per file. Returns
/// the total **data** bytes uploaded. If an upload fails or the commit is refused,
/// everything this write put is removed again. Shared by the sink's commit and by
/// [`compact_iceberg`].
fn upload_table(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
    uploads: Vec<(String, PathBuf)>,
    deletes: Vec<(String, PathBuf)>,
    mut art: MetadataArtifacts,
) -> Result<u64> {
    let TablePuts { puts, .. } = table_puts(base_key, uploads, deletes, &mut art)?;
    let staged: Vec<ObjPath> = puts.iter().map(|p| p.key().clone()).collect();
    let store = store.clone();
    let base_key = base_key.clone();
    block_on(async move {
        let data_bytes = put_all(&store, puts).await?;
        // The commit: the metadata file, created only if no other writer created it. A
        // refused commit takes back everything it uploaded; one whose outcome is unknown
        // leaves it, since a committed snapshot may name it.
        if let Err(nc) = publish_metadata(&store, &base_key, art.version, art.metadata_bytes).await
        {
            if nc.refused {
                remove_all(&store, staged).await;
            } else {
                tracing::warn!(version = art.version, error = %nc.error,
                    "the commit's outcome is unknown; its uploaded files are left in place");
            }
            return Err(nc.error);
        }
        publish_hint(&store, &base_key, art.version).await;
        Ok(data_bytes)
    })
}

/// The puts of a table write ahead of its commit: every staged data and delete file, then
/// the metadata of this attempt alone (its manifests and manifest list). An attempt rebuilt
/// after a refusal writes new metadata over the same files, so the two are told apart by
/// where the metadata begins, which `table_puts` records rather than its callers infer.
struct TablePuts {
    puts: Vec<Put>,
    /// `puts[metadata..]` are this attempt's manifests and manifest list.
    metadata: usize,
}

/// [`TablePuts`] for staged data and delete files (`(object key, local staged path)`) and
/// the artifacts that name them. Takes the manifest bytes out of `art`.
fn table_puts(
    base_key: &ObjPath,
    uploads: Vec<(String, PathBuf)>,
    deletes: Vec<(String, PathBuf)>,
    art: &mut MetadataArtifacts,
) -> Result<TablePuts> {
    let mut puts = Vec::new();
    for (key, local) in uploads {
        puts.push(Put::file(child_key(base_key, &key), local, true)?);
    }
    for (key, local) in deletes {
        puts.push(Put::file(child_key(base_key, &key), local, false)?);
    }
    let metadata = puts.len();
    puts.push(Put::Bytes {
        key: child_key(base_key, &art.manifest_key),
        bytes: std::mem::take(&mut art.manifest_bytes),
    });
    if let Some((key, bytes)) = art.delete_manifest.take() {
        puts.push(Put::Bytes {
            key: child_key(base_key, &key),
            bytes,
        });
    }
    puts.push(Put::Bytes {
        key: child_key(base_key, &art.manifest_list_key),
        bytes: std::mem::take(&mut art.manifest_list_bytes),
    });
    Ok(TablePuts { puts, metadata })
}

/// What a commit through a catalog came to.
struct CatalogCommitted {
    data_bytes: u64,
    /// The catalog's new metadata file, when its answer says.
    location: Option<String>,
    snapshot_id: i64,
    rebases: u32,
    /// The table already held this run's snapshot, from an earlier attempt: this one
    /// committed nothing, and `snapshot_id` and `rebases` are that attempt's.
    earlier_attempt: bool,
}

/// [`upload_table`] for a table a catalog names: the same puts, then the catalog commit in
/// place of the metadata file (the catalog writes its own).
///
/// A commit whose answer was lost, onto a table that neither moved nor took it, is sent
/// again unchanged, up to `max_resends` times; a refused copy then settles by what the table
/// holds, since an earlier copy may have landed. An append the catalog refuses because
/// another writer moved `main` first is built again on the new head and committed again, up
/// to `max_rebases` times (none for a merge or a compaction, whose `deletes` and rewrites
/// were planned against the head they loaded). Its data files stand as they are and stay
/// where they were put; only the manifest and the manifest list are written again, and the
/// refused attempt's are removed, since nothing can name them now. A table that changed
/// under it in a way its files cannot follow (see [`rebase_blocked`]) is not rebased onto. A
/// refusal it does not rebase past takes back everything it uploaded; an outcome that stays
/// unknown leaves it all in place.
///
/// With a `run_id`, every refusal is first checked against the table: if the snapshot that
/// moved `main` carries the same run id, an earlier attempt of this run landed, and this one
/// takes back its files and reports that snapshot. `rebuild` gets the number of the rebase
/// it builds, which the snapshot records.
#[allow(clippy::too_many_arguments)]
fn commit_to_catalog(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
    uploads: Vec<(String, PathBuf)>,
    deletes: Vec<(String, PathBuf)>,
    first: MetadataArtifacts,
    target: &CatalogTarget,
    run_id: Option<&str>,
    rebuild: &dyn Fn(PriorTable, u32) -> Result<MetadataArtifacts>,
) -> Result<CatalogCommitted> {
    let mut art = first;
    let TablePuts { puts, metadata } = table_puts(base_key, uploads, deletes, &mut art)?;
    let keys = |puts: &[Put]| -> Vec<ObjPath> { puts.iter().map(|p| p.key().clone()).collect() };
    // The data and delete files stay for a rebuild; this attempt's metadata does not.
    let data = keys(&puts[..metadata]);
    let mut attempt = keys(&puts[metadata..]);
    let store = store.clone();
    let (table, catalog) = (&target.table, &target.catalog);
    block_on(async move {
        let data_bytes = put_all(&store, puts).await?;
        let mut loaded = target.loaded.clone();
        let mut rebases = 0;
        loop {
            let change = match catalog_commit(&art, &loaded) {
                Ok(change) => change,
                Err(e) => {
                    remove_all(&store, [std::mem::take(&mut attempt), data].concat()).await;
                    return Err(e);
                }
            };
            let settled = catalog
                .commit_resending(table, &change, target.max_resends)
                .await;
            let error = match settled {
                Settled::Committed(location) => {
                    return Ok(CatalogCommitted {
                        data_bytes,
                        location,
                        snapshot_id: art.snapshot_id,
                        rebases,
                        earlier_attempt: false,
                    })
                }
                Settled::Unknown { error, .. } => {
                    tracing::warn!(%table, error = %error,
                        "the commit's outcome is unknown; its uploaded files are left in place");
                    return Err(error);
                }
                Settled::Refused(error) => error,
            };
            // Refused, and no copy can land: no snapshot can ever name this attempt's
            // manifests.
            remove_all(&store, std::mem::take(&mut attempt)).await;
            let conflict = matches!(error, ShapeError::Conflict(_));
            // A conflict means `main` moved: a write that may not add to a table holding data
            // is not rebuilt onto it (an earlier attempt of its run is still recognized).
            let may_rebuild = conflict && target.append && rebases < target.max_rebases;
            if !may_rebuild && !(conflict && run_id.is_some()) {
                remove_all(&store, data).await;
                return Err(after_rebases(error, rebases));
            }
            if may_rebuild {
                // Spread writers that lost to the same commit, so they do not all meet again.
                tokio::time::sleep(backoff(rebases)).await;
            }
            let now = match load_head(target).await {
                Ok(now) => now,
                Err(e) => {
                    remove_all(&store, data).await;
                    return Err(after_rebases(e, rebases));
                }
            };
            // The writer that moved `main` may be an earlier attempt of this same run.
            if let Some(found) = run_id.and_then(|id| snapshot_of_run(&now, id)) {
                remove_all(&store, data).await;
                tracing::info!(%table, snapshot = found.snapshot_id,
                    "an earlier attempt of this run committed it first; this one's files were removed");
                return Ok(CatalogCommitted {
                    data_bytes: 0,
                    location: None,
                    snapshot_id: found.snapshot_id,
                    rebases: found.rebases,
                    earlier_attempt: true,
                });
            }
            if !may_rebuild {
                remove_all(&store, data).await;
                return Err(after_rebases(error, rebases));
            }
            let next =
                match rebase(&store, base_key, target, &loaded, now, rebuild, rebases + 1).await {
                    Ok(next) => next,
                    Err(e) => {
                        remove_all(&store, data).await;
                        return Err(after_rebases(e, rebases));
                    }
                };
            (loaded, art) = next;
            rebases += 1;
            tracing::info!(%table, rebases, snapshot = art.snapshot_id,
                "another writer committed first; rebuilt this append onto the new head");
            let TablePuts { puts, .. } = table_puts(base_key, Vec::new(), Vec::new(), &mut art)?;
            attempt = keys(&puts);
            if let Err(e) = put_all(&store, puts).await {
                remove_all(&store, data).await;
                return Err(e);
            }
        }
    })
}

/// Where a catalog table's current manifest list is, as the catalog's metadata names it. A
/// URI under the table's location is read under the table's key; any other is read at the
/// key its URI names, which must be in the same store (a table's metadata may live apart
/// from its data, e.g. under `write.metadata.path`). Unlike a table at a path, a catalog
/// table is never read at a relocated copy: the catalog's URIs are where the files are.
fn catalog_manifest_list_key(
    metadata: &serde_json::Value,
    location: &str,
    base_key: &ObjPath,
) -> Result<ObjPath> {
    let current = &metadata["current-snapshot-id"];
    let uri = metadata["snapshots"]
        .as_array()
        .and_then(|snaps| snaps.iter().find(|s| &s["snapshot-id"] == current))
        .and_then(|s| s["manifest-list"].as_str())
        .ok_or_else(|| sink_err("the table's current snapshot names no manifest list"))?;
    let location = location.trim_end_matches('/');
    if let Some(rest) = uri.strip_prefix(location) {
        if rest.starts_with('/') {
            return Ok(child_key(base_key, rest.trim_start_matches('/')));
        }
    }
    let (at, table) = (url::Url::parse(uri), url::Url::parse(location));
    match (at, table) {
        (Ok(at), Ok(table)) if at.scheme() == table.scheme() && at.host() == table.host() => {
            ObjPath::from_url_path(at.path()).map_err(sink_err)
        }
        _ => Err(sink_err(format!(
            "the table's manifest list {uri} is not in the store of its location {location}; \
             this writer reads a table's files from one store"
        ))),
    }
}

/// The table's metadata as it stands now, loaded again after a refusal. A table that is gone
/// is a conflict.
async fn load_head(target: &CatalogTarget) -> Result<serde_json::Value> {
    let table = &target.table;
    let now = target.catalog.load(table).await?.ok_or_else(|| {
        ShapeError::Conflict(format!(
            "{table} was dropped while this write ran; nothing of it was published, and its \
             files were removed"
        ))
    })?;
    Ok(now.metadata)
}

/// Build the append onto the table's new head `now` (rebase number `n`): the table as it now
/// stands, and the new attempt. A table its files no longer fit is a conflict.
async fn rebase(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
    target: &CatalogTarget,
    loaded: &serde_json::Value,
    now: serde_json::Value,
    rebuild: &dyn Fn(PriorTable, u32) -> Result<MetadataArtifacts>,
    n: u32,
) -> Result<(serde_json::Value, MetadataArtifacts)> {
    let table = &target.table;
    if let Some(why) = rebase_blocked(loaded, &now) {
        return Err(ShapeError::Conflict(format!(
            "another writer committed to {table} while this one ran, and {why}, so this append \
             cannot be built onto it; nothing of it was published, and its files were removed. \
             Run it again"
        )));
    }
    let manifests = match main_snapshot(&now) {
        None => Vec::new(),
        Some(_) => {
            let location = now["location"].as_str().unwrap_or_default();
            let key = catalog_manifest_list_key(&now, location, base_key)?;
            read_manifest_list(&get_bytes(store, &key).await?)?
        }
    };
    let art = rebuild(
        PriorTable {
            metadata: now.clone(),
            version: 0,
            manifests,
        },
        n,
    )?;
    Ok((now, art))
}

/// A refusal reported after `rebases` attempts to catch up: say so, so a writer that kept
/// losing reads why its retries did not help.
fn after_rebases(error: ShapeError, rebases: u32) -> ShapeError {
    match error {
        ShapeError::Conflict(msg) if rebases > 0 => ShapeError::Conflict(format!(
            "{msg} (this append was rebuilt onto {rebases} newer snapshot(s) first, and lost \
             each time)"
        )),
        other => other,
    }
}

/// [`moved_on`] for a table a catalog names: a failure while reading the table's files (a
/// merge's key columns, a compaction's data files, an expiry's manifests) is reported as a
/// conflict when `main` has moved since the table was loaded, or the table is gone: another
/// writer's compaction and expiry can remove the very files it was reading. Running it
/// again plans against the new head.
fn moved_on_in_catalog(err: ShapeError, target: &CatalogTarget) -> ShapeError {
    if matches!(err, ShapeError::Conflict(_)) {
        return err;
    }
    let table = &target.table;
    match block_on(target.catalog.load(table)) {
        Ok(Some(now)) if main_snapshot(&now.metadata) != main_snapshot(&target.loaded) => {
            ShapeError::Conflict(format!(
                "another writer committed to {table} while this one ran, and it failed \
                 ({err}); nothing of it was published. Run it again"
            ))
        }
        Ok(None) => ShapeError::Conflict(format!(
            "{table} was dropped while this one ran, and it failed ({err}); nothing of it was \
             published"
        )),
        _ => err,
    }
}

/// Where a catalog table's files are: its location, the store and key prefix there, and the
/// table as loaded (version 0: the catalog numbers its own metadata files), with the
/// manifests of its current snapshot (none while it has no snapshot).
struct CatalogFiles {
    location: String,
    store: Arc<dyn ObjectStore>,
    base_key: ObjPath,
    prior: PriorTable,
}

impl CatalogFiles {
    fn of(metadata: &serde_json::Value, ident: &TableIdent) -> Result<Self> {
        let location = metadata["location"]
            .as_str()
            .ok_or_else(|| sink_err(format!("the catalog's {ident} has no location")))?
            .trim_end_matches('/')
            .to_string();
        let (store, base_key) = build_store(&location)?;
        let manifests = match main_snapshot(metadata) {
            None => Vec::new(),
            Some(_) => {
                let key = catalog_manifest_list_key(metadata, &location, &base_key)?;
                let store = store.clone();
                read_manifest_list(&block_on(async move { get_bytes(&store, &key).await })?)?
            }
        };
        Ok(CatalogFiles {
            location,
            store,
            base_key,
            prior: PriorTable {
                metadata: metadata.clone(),
                version: 0,
                manifests,
            },
        })
    }
}

/// A table a catalog names, loaded for a compaction or an expiry: what the commit goes
/// through and asserts, and the table's files. It must exist.
fn load_catalog_table(
    catalog: &CatalogConnection,
    table: &str,
    options: CatalogOptions,
) -> Result<(CatalogTarget, CatalogFiles)> {
    let ident = TableIdent::parse(table)?;
    let (catalog, loaded) = {
        let ident = &ident;
        let conn = catalog;
        block_on(async move {
            let catalog = RestCatalog::connect(conn, options.timeout).await?;
            let loaded = catalog.load(ident).await?.ok_or_else(|| {
                sink_err(format!(
                    "the catalog at {} has no table {ident}",
                    conn.uri.trim().trim_end_matches('/')
                ))
            })?;
            Ok((catalog, loaded))
        })?
    };
    let files = CatalogFiles::of(&loaded.metadata, &ident)?;
    let target = CatalogTarget {
        catalog,
        table: ident,
        loaded: loaded.metadata,
        max_rebases: 0,
        max_resends: options.max_resends,
        append: false,
    };
    Ok((target, files))
}

impl Sink for ObjectStoreIcebergSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| sink_err("write after finish"))?;
        match &mut self.merge {
            None => writer.write_batch(batch),
            Some(m) => m.write_batch(writer, batch),
        }
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

        // An earlier attempt of this run committed before this one began: its rows are in
        // the table already. Nothing of this attempt was uploaded; its staging goes.
        if let Some(found) = run_in(self.run_id.as_deref(), prior.as_ref()) {
            let _ = std::fs::remove_dir_all(&self.temp_dir);
            return Ok(earlier_attempt_summary(&found));
        }

        // A merge reads the existing table from the store (key columns only, ranged) and
        // stages its delete files beside the data files for the same upload.
        let (delete_files, merge) = match self.merge.take() {
            None => (Vec::new(), None),
            Some(m) => {
                // A catalog's table is read where its URIs say; a table at a path, at the
                // same key under wherever it is now.
                let anchors = match (&self.catalog, &prior) {
                    (Some(_), _) => Anchors::at(&self.base_url),
                    (None, Some(p)) => Anchors::of(&p.metadata),
                    (None, None) => Anchors::default(),
                };
                let files = StoreFiles::open(&self.base_url, anchors)?;
                let out = m
                    .finish(
                        &MergeContext {
                            staging_dir: &self.temp_dir,
                            base_location: &self.base_url,
                            prior: prior.as_ref(),
                            new_files: &data_files,
                            partition_fields: &self.partition_fields,
                            compression: self.compression,
                        },
                        &files,
                    )
                    .map_err(|e| match &self.catalog {
                        Some(target) => moved_on_in_catalog(e, target),
                        None => {
                            let base = prior.as_ref().map_or(0, |p| p.version);
                            moved_on_in_store(e, base, &self.store, &self.base_key)
                        }
                    });
                match out {
                    Ok(out) => (out.delete_files, Some(out.summary)),
                    Err(e) => return self.earlier_attempt_or(e),
                }
            }
        };
        let deletes: Vec<(String, PathBuf)> = delete_files
            .iter()
            .map(|d| (d.data_key.clone(), self.temp_dir.join(&d.data_key)))
            .collect();

        let table_uuid = self.table_uuid.to_string();
        // A catalog append may be built again onto a newer head: keep what it wrote.
        let rebuild_files = self.catalog.as_ref().map(|_| data_files.clone());
        let art = build_metadata_artifacts(&MetadataBuild {
            base_location: &self.base_url,
            table_uuid: &table_uuid,
            ice_fields: &self.ice_fields,
            partition_fields: &self.partition_fields,
            data_files,
            delete_files,
            timestamp_ms: snapshot_timestamp_ms(),
            prior,
            replaces: self.replaces,
            tags: SnapshotTags {
                run_id: self.run_id.clone(),
                rebases: 0,
            },
        })?;
        tracing::debug!(dest = %self.base_url, rows, files = uploads.len(), deletes = deletes.len(), "uploading iceberg table to object store");
        let (bytes, metadata_file, table_version, snapshot_id, rebases, earlier) = match &self
            .catalog
        {
            None => {
                let uri = join_location(&self.base_url, &art.metadata_key);
                let (version, snapshot_id) = (art.version, art.snapshot_id);
                let bytes = match upload_table(&self.store, &self.base_key, uploads, deletes, art) {
                    Ok(bytes) => bytes,
                    Err(e) => return self.earlier_attempt_or(e),
                };
                (bytes, Some(uri), Some(version), snapshot_id, 0, false)
            }
            // The catalog numbers its own metadata files: there is no `vN` to report. Only an
            // append is rebuilt after a refusal (a merge's target allows no rebase), so a
            // rebuilt snapshot carries no deletes.
            Some(target) => {
                let files = rebuild_files.unwrap_or_default();
                let rebuild = |prior: PriorTable, rebases: u32| {
                    build_metadata_artifacts(&MetadataBuild {
                        base_location: &self.base_url,
                        table_uuid: &table_uuid,
                        ice_fields: &self.ice_fields,
                        partition_fields: &self.partition_fields,
                        data_files: files.clone(),
                        delete_files: Vec::new(),
                        timestamp_ms: snapshot_timestamp_ms(),
                        prior: Some(prior),
                        replaces: None,
                        tags: SnapshotTags {
                            run_id: self.run_id.clone(),
                            rebases,
                        },
                    })
                };
                let done = commit_to_catalog(
                    &self.store,
                    &self.base_key,
                    uploads,
                    deletes,
                    art,
                    target,
                    self.run_id.as_deref(),
                    &rebuild,
                )?;
                (
                    done.data_bytes,
                    done.location,
                    None,
                    done.snapshot_id,
                    done.rebases,
                    done.earlier_attempt,
                )
            }
        };
        let _ = std::fs::remove_dir_all(&self.temp_dir);
        Ok(SinkSummary {
            files: metadata_file.into_iter().map(PathBuf::from).collect(),
            // An earlier attempt's snapshot holds the run's rows; this attempt wrote none.
            rows: if earlier { 0 } else { rows },
            bytes,
            merge,
            table_version,
            snapshot_id: Some(snapshot_id),
            rebases,
            earlier_attempt: earlier,
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

/// A staging directory removed however its owner returns.
struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Compact the Iceberg table at `url` — the object-store twin of
/// [`shapeshift_iceberg::compact_table`], with the same semantics. The table is read
/// through [`StoreFiles`] (manifests and delete files whole, data files one row group at a
/// time with ranged GETs). The rewritten files are staged locally and uploaded before the
/// metadata that names them, version hint last. Commits nothing when there is nothing to
/// compact.
pub fn compact_iceberg(url: &str, options: &CompactOptions) -> Result<CompactSummary> {
    let (store, base_key) = build_store(url)?;
    let base_url = url.trim_end_matches('/').to_string();
    let table =
        ObjectStoreIcebergSink::load_prior(&store, &base_key, &base_url)?.ok_or_else(|| {
            sink_err(format!(
                "no Iceberg table at {url} (no metadata/version-hint.text)"
            ))
        })?;
    let files = StoreFiles::open(&base_url, Anchors::of(&table.metadata))?;
    let staging =
        TempDir(std::env::temp_dir().join(format!("shapeshift-compact-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir_all(&staging.0).map_err(sink_err)?;
    let outcome = compact(
        &CompactContext {
            staging_dir: &staging.0,
            base_location: &base_url,
            table: &table,
            options,
        },
        &files,
    )
    .map_err(|e| moved_on_in_store(e, table.version, &store, &base_key))?;
    if let Some(art) = outcome.artifacts {
        let uploads = staged_paths(&staging.0, &outcome.data_files);
        tracing::debug!(dest = %base_url, files = uploads.len(), "uploading a compaction");
        upload_table(&store, &base_key, uploads, Vec::new(), art)?;
    }
    Ok(outcome.summary)
}

/// The keys (relative to `base_key`, `/`-separated) of every object under it: one listing.
async fn list_keys(
    store: &Arc<dyn ObjectStore>,
    base_key: &ObjPath,
) -> Result<std::collections::HashSet<String>> {
    // A table at a bucket's root has an empty key, and its objects' keys no leading `/`.
    let prefix = if base_key.as_ref().is_empty() {
        String::new()
    } else {
        format!("{base_key}/")
    };
    let mut out = std::collections::HashSet::new();
    let mut listed = store.list(Some(base_key));
    while let Some(meta) = std::future::poll_fn(|cx| listed.as_mut().poll_next(cx)).await {
        let meta = meta.map_err(sink_err)?;
        if let Some(key) = meta.location.as_ref().strip_prefix(&prefix) {
            out.insert(key.to_string());
        }
    }
    Ok(out)
}

/// Where an object-store table at `url` is, spelled as its metadata spells its place: a
/// Spark table records `s3a://`, and keeps doing so where it is reached as `s3://`, so
/// relocating it neither rewrites an unmoved table nor changes the client its other readers
/// use.
fn spelled_as_recorded(recorded: &str, url: &str) -> String {
    let recorded = recorded.trim_end_matches('/');
    if !recorded.is_empty() && !is_elsewhere(recorded, url) {
        return recorded.to_string();
    }
    match (recorded.split_once("://"), url.strip_prefix("s3://")) {
        (Some((scheme @ ("s3a" | "s3n"), _)), Some(rest)) => format!("{scheme}://{rest}"),
        _ => url.to_string(),
    }
}

/// Whether a table answers at `root`, a place an object-store table at `here` was written
/// at: its `metadata/` holds a version hint or a metadata file. A place in a store this run
/// cannot reach (no credentials for it) cannot be checked. Nor is a place in another kind of
/// store than `here` probed at all: the place comes from the table's own metadata, and a
/// table in a bucket has no business sending this run to the local disk or another cloud.
async fn store_old_place(root: &str, here: &str) -> OldPlace {
    // `s3a://` (Spark's) is S3, and is probed as `s3://`.
    let root = same_store(root);
    let scheme = |u: &str| {
        same_store(u)
            .split_once("://")
            .map(|(s, _)| s.to_ascii_lowercase())
    };
    if scheme(&root) != scheme(here) {
        return OldPlace::Unknown(format!(
            "it is in another kind of store than {here}, which relocate does not probe"
        ));
    }
    let (store, key) = match build_store(&root) {
        Ok(s) => s,
        Err(e) => return OldPlace::Unknown(e.to_string()),
    };
    let metadata = child_key(&key, "metadata");
    let mut listed = store.list(Some(&metadata));
    loop {
        match std::future::poll_fn(|cx| listed.as_mut().poll_next(cx)).await {
            None => return OldPlace::Gone,
            Some(Err(object_store::Error::NotFound { .. })) => return OldPlace::Gone,
            Some(Err(e)) => return OldPlace::Unknown(e.to_string()),
            Some(Ok(meta)) => {
                let name = meta.location.filename().unwrap_or("");
                if name == "version-hint.text" || name.ends_with(".metadata.json") {
                    return OldPlace::Answers;
                }
            }
        }
    }
}

/// What [`register_in_catalog`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    /// The catalog now names the table, at the metadata file given.
    Registered,
    /// It already did, at that same file: nothing was changed.
    AlreadyRegistered,
}

/// Register the table whose current metadata file is `metadata_location` with the REST
/// catalog `catalog` as `table` (`<namespace>.<name>`): a relocated table's last step
/// (RELOCATE-PLAN R3). The catalog records where the metadata is; the paths inside are the
/// relocation's. A name the catalog already has for another metadata file is refused, never
/// overwritten; the same file again is already done, so a run that relocated and then failed
/// to register can be repeated.
pub fn register_in_catalog(
    catalog: &CatalogConnection,
    table: &str,
    metadata_location: &str,
) -> Result<Registration> {
    let ident = TableIdent::parse(table)?;
    let timeout = CatalogOptions::default().timeout;
    block_on(async move {
        let rest = RestCatalog::connect(catalog, timeout).await?;
        let why = match rest.register(&ident, metadata_location).await? {
            Created::Table(_) => return Ok(Registration::Registered),
            Created::LoadIt(why) => why,
        };
        match rest.load(&ident).await? {
            Some(held) => match held.metadata_location.as_deref() {
                Some(at) if !is_elsewhere(at, metadata_location) => {
                    Ok(Registration::AlreadyRegistered)
                }
                // Registered, and written to through the catalog since: its history passes
                // through this very file. (The table's identity alone would not do: a table
                // moved without a new uuid is still held at its old place under the name.)
                Some(_) if descends_from(&held.metadata, metadata_location) => {
                    Ok(Registration::AlreadyRegistered)
                }
                at => Err(sink_err(format!(
                    "the catalog at {} already has {ident}, at {}, which is neither {metadata_location} \
                     nor a later version of it; relocate never replaces a registration: drop it \
                     in the catalog, or register under another --table",
                    catalog.uri,
                    at.unwrap_or("a metadata file it does not name")
                ))),
            },
            None => Err(sink_err(format!(
                "registering {ident} with the catalog at {}: {why}, and the catalog does not \
                 have it",
                catalog.uri
            ))),
        }
    })
}

/// Whether the table `metadata` describes was written from the metadata file `file`: its
/// `metadata-log` names it. A catalog trims that log (Iceberg keeps the last 100 by default),
/// so a table written to many times since is no longer recognised, and is refused.
fn descends_from(metadata: &serde_json::Value, file: &str) -> bool {
    metadata["metadata-log"].as_array().is_some_and(|log| {
        log.iter().any(|entry| {
            entry["metadata-file"]
                .as_str()
                .is_some_and(|logged| !is_elsewhere(logged, file))
        })
    })
}

/// Relocate the Iceberg table at `url` to where it is — the object-store twin of
/// [`shapeshift_iceberg::relocate_table`], with the same rules. The files a snapshot reads
/// are checked against one listing of the table; the rewritten files are staged locally and
/// uploaded at new names, then the new metadata version is put through the commit guard,
/// version hint last; then the files they supersede are deleted. Data files are neither
/// read nor written.
pub fn relocate_iceberg(url: &str, options: &RelocateOptions) -> Result<RelocateSummary> {
    let (store, base_key) = build_store(url)?;
    let url_here = url.trim_end_matches('/').to_string();
    let (version, metadata, listing) = {
        let (store, base_key) = (store.clone(), base_key.clone());
        let url = url.to_string();
        block_on(async move {
            let version = store_version(&store, &base_key)
                .await?
                .ok_or_else(|| sink_err(format!("no Iceberg table at {url}")))?;
            let text = get_text(&store, &child_key(&base_key, &metadata_key(version))).await?;
            let metadata: serde_json::Value = serde_json::from_str(&text).map_err(sink_err)?;
            Ok((version, metadata, list_keys(&store, &base_key).await?))
        })?
    };
    let here = spelled_as_recorded(metadata["location"].as_str().unwrap_or(""), &url_here);
    let anchors = Anchors::of(&metadata);
    // Probed only when no identity was given: `--moved` or `--copy` sends no request to a
    // place the table's metadata names.
    let elsewhere: Vec<String> = anchors
        .roots()
        .iter()
        .filter(|root| options.identity.is_none() && is_elsewhere(root, &here))
        .cloned()
        .collect();
    let places = block_on({
        let here = here.clone();
        async move {
            let mut places = Vec::new();
            for root in elsewhere {
                let place = store_old_place(&root, &here).await;
                places.push((root, place));
            }
            Ok(places)
        }
    })?;
    let (identity, undecided) = match decide_identity(options.identity, &places) {
        Ok(i) => (i, None),
        Err(why) if options.dry_run => (Identity::Moved, Some(why)),
        Err(why) => return Err(undecided_error(&why)),
    };
    let files = StoreFiles::open(url, anchors.clone())?;
    let staging =
        TempDir(std::env::temp_dir().join(format!("shapeshift-relocate-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir_all(&staging.0).map_err(sink_err)?;
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
            staging_dir: &staging.0,
            new_uuid: identity == Identity::Copy,
            dry_run: options.dry_run,
            now_ms: snapshot_timestamp_ms(),
        },
        &files,
    )?;
    let mut summary = plan.summary;
    summary.undecided = undecided;
    let Some(commit) = plan.commit else {
        return Ok(summary);
    };
    let puts = commit
        .written
        .iter()
        .map(|key| Put::file(child_key(&base_key, key), staging.0.join(key), false))
        .collect::<Result<Vec<_>>>()?;
    let staged: Vec<ObjPath> = puts.iter().map(|p| p.key().clone()).collect();
    let superseded: Vec<ObjPath> = commit
        .superseded
        .iter()
        .map(|key| child_key(&base_key, key))
        .collect();
    // Written through the reads' own client and runtime (see `StoreFiles`).
    let gone = files.rt.block_on(async {
        let (store, base_key) = (&files.store, &files.base_key);
        put_all(store, puts).await?;
        if let Err(nc) =
            publish_metadata(store, base_key, commit.version, commit.metadata_bytes).await
        {
            if nc.refused {
                remove_all(store, staged).await;
            } else {
                tracing::warn!(version = commit.version, error = %nc.error,
                    "the relocation's outcome is unknown; its uploaded files are left in place");
            }
            return Err(nc.error);
        }
        publish_hint(store, base_key, commit.version).await;
        Ok::<_, ShapeError>(remove_all(store, superseded).await)
    });
    let gone = gone.map_err(|e| moved_on_in_store(e, version, &store, &base_key))?;
    summary.committed = true;
    summary.version = Some(commit.version);
    for ok in gone {
        if ok {
            summary.superseded_deleted += 1;
        } else {
            summary.superseded_not_deleted += 1;
        }
    }
    Ok(summary)
}

/// Expire old snapshots of the Iceberg table at `url` — the object-store twin of
/// [`shapeshift_iceberg::expire_table`], with the same rules. The metadata without the
/// expired snapshots is put first, version hint last; then every file only they
/// referenced is deleted through the store's many-delete call (a missing object counts as
/// deleted).
/// A dry run, or a table with nothing to expire, changes nothing.
pub fn expire_iceberg(url: &str, options: &ExpireOptions) -> Result<ExpireSummary> {
    let (store, base_key) = build_store(url)?;
    let base_url = url.trim_end_matches('/').to_string();
    let table =
        ObjectStoreIcebergSink::load_prior(&store, &base_key, &base_url)?.ok_or_else(|| {
            sink_err(format!(
                "no Iceberg table at {url} (no metadata/version-hint.text)"
            ))
        })?;
    let anchors = Anchors::of(&table.metadata);
    let files = StoreFiles::open(&base_url, anchors.clone())?;
    let plan = plan_expiry(
        &table,
        &anchors,
        &base_url,
        &files,
        options,
        snapshot_timestamp_ms(),
    )
    .map_err(|e| moved_on_in_store(e, table.version, &store, &base_key))?;
    let mut summary = plan.summary;
    let Some(commit) = plan.commit.filter(|_| !options.dry_run) else {
        return Ok(summary);
    };
    // Guarded: if another writer committed since the plan was made, nothing is deleted.
    // Written through the reads' own client and runtime (see `StoreFiles`).
    let (store, base_key) = (&files.store, &files.base_key);
    files.rt.block_on(async {
        publish_metadata(store, base_key, commit.version, commit.metadata_bytes)
            .await
            .map_err(|nc| nc.error)?;
        publish_hint(store, base_key, commit.version).await;
        Ok::<_, ShapeError>(())
    })?;
    summary.committed = true;
    summary.version = Some(commit.version);
    delete_expired(&files, &plan.delete, &mut summary);
    Ok(summary)
}

/// Delete the files an expiry that committed no longer needs, through the reads' own client
/// and runtime (see [`StoreFiles`]), [`STORE_WRITERS`] at a time. A file that cannot be
/// deleted is counted, not fatal: the commit stands, and the file is left unreferenced.
fn delete_expired(files: &StoreFiles, delete: &[DeleteTarget], summary: &mut ExpireSummary) {
    let mut keys = Vec::new();
    let mut targets = Vec::new();
    for t in delete {
        match files.key(&t.path) {
            Ok(key) => {
                keys.push(key);
                targets.push(t);
            }
            Err(_) => summary.files_not_deleted += 1,
        }
    }
    let gone = files.rt.block_on(remove_all(&files.store, keys));
    for (t, gone) in targets.into_iter().zip(gone) {
        if gone {
            summary.count_deleted(t);
        } else {
            summary.files_not_deleted += 1;
        }
    }
}

/// Compact `table` (`namespace.name`) in the REST catalog `catalog`: [`compact_iceberg`]
/// for a table a catalog names, with the same semantics. The files are read and written at
/// the location the catalog gives the table, and the `replace` snapshot is committed through
/// the catalog, asserting that the table and `main` are as loaded.
///
/// A refusal is a conflict (exit 75), and the files this compaction wrote are removed. It is
/// not rebuilt onto the other writer's commit: a merge may have deleted rows from the very
/// files it rewrote, and the rewrite would bring them back. Commits nothing when there is
/// nothing to compact.
pub fn compact_in_catalog(
    catalog: &CatalogConnection,
    table: &str,
    options: &CompactOptions,
) -> Result<CompactSummary> {
    compact_through_catalog(catalog, table, options, CatalogOptions::default())
}

fn compact_through_catalog(
    catalog: &CatalogConnection,
    table: &str,
    options: &CompactOptions,
    catalog_options: CatalogOptions,
) -> Result<CompactSummary> {
    let (target, t) = load_catalog_table(catalog, table, catalog_options)?;
    if main_snapshot(&target.loaded).is_none() {
        return Ok(CompactSummary::default());
    }
    let files = StoreFiles::open(&t.location, Anchors::at(&t.location))?;
    let staging =
        TempDir(std::env::temp_dir().join(format!("shapeshift-compact-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir_all(&staging.0).map_err(sink_err)?;
    let outcome = compact(
        &CompactContext {
            staging_dir: &staging.0,
            base_location: &t.location,
            table: &t.prior,
            options,
        },
        &files,
    )
    .map_err(|e| moved_on_in_catalog(e, &target))?;
    let mut summary = outcome.summary;
    if let Some(art) = outcome.artifacts {
        let uploads = staged_paths(&staging.0, &outcome.data_files);
        tracing::debug!(%table, files = uploads.len(), "uploading a compaction");
        let never = |_: PriorTable, _: u32| -> Result<MetadataArtifacts> {
            Err(sink_err("internal error: a compaction is never rebuilt"))
        };
        let done = commit_to_catalog(
            &t.store,
            &t.base_key,
            uploads,
            Vec::new(),
            art,
            &target,
            None,
            &never,
        )?;
        summary.snapshot_id = Some(done.snapshot_id);
        // The catalog numbers its own metadata files: there is no `vN` to report.
        summary.version = None;
    }
    Ok(summary)
}

/// Expire old snapshots of `table` (`namespace.name`) in the REST catalog `catalog`:
/// [`expire_iceberg`] for a table a catalog names, with the same rules. The plan is made
/// against the table as loaded, and committed as a `remove-snapshots` asserting that the
/// table, `main` and every other ref are as loaded (see
/// [`expiry_commit`](shapeshift_iceberg::expiry_commit)). Files are deleted only once the
/// catalog has taken the commit.
///
/// A refusal is a conflict (exit 75) and deletes nothing; it is not planned again. A commit
/// whose outcome stays unknown deletes nothing either (exit 1). A dry run, or a table with
/// nothing to expire, changes nothing.
pub fn expire_in_catalog(
    catalog: &CatalogConnection,
    table: &str,
    options: &ExpireOptions,
) -> Result<ExpireSummary> {
    expire_through_catalog(catalog, table, options, CatalogOptions::default())
}

fn expire_through_catalog(
    catalog: &CatalogConnection,
    table: &str,
    options: &ExpireOptions,
    catalog_options: CatalogOptions,
) -> Result<ExpireSummary> {
    let (target, t) = load_catalog_table(catalog, table, catalog_options)?;
    let some = target.loaded["snapshots"]
        .as_array()
        .is_some_and(|s| !s.is_empty());
    if !some {
        return Ok(ExpireSummary::default());
    }
    let anchors = Anchors::at(&t.location);
    let files = StoreFiles::open(&t.location, anchors.clone())?;
    let plan = plan_expiry(
        &t.prior,
        &anchors,
        &t.location,
        &files,
        options,
        snapshot_timestamp_ms(),
    )
    .map_err(|e| moved_on_in_catalog(e, &target))?;
    let mut summary = plan.summary;
    if plan.commit.is_none() || options.dry_run {
        return Ok(summary);
    }
    let change = expiry_commit(&target.loaded, &summary.expired_snapshots)?;
    let settled = block_on(async {
        Ok(target
            .catalog
            .commit_resending(&target.table, &change, target.max_resends)
            .await)
    })?;
    match settled {
        Settled::Committed(_) => {}
        Settled::Refused(error) | Settled::Unknown { error, .. } => return Err(error),
    }
    summary.committed = true;
    // The catalog numbers its own metadata files: there is no `vN` to report.
    summary.version = None;
    delete_expired(&files, &plan.delete, &mut summary);
    Ok(summary)
}

/// Read an Iceberg table's current metadata from an object store (via
/// `version-hint.text`) and summarize it — the object-store analog of
/// [`shapeshift_iceberg::inspect`].
pub fn inspect_iceberg(url: &str) -> Result<IcebergInfo> {
    let (store, base_key) = build_store(url)?;
    block_on(async move {
        let version = store_version(&store, &base_key)
            .await?
            .ok_or_else(|| sink_err(format!("no Iceberg table at {url}")))?;
        let text = get_text(&store, &child_key(&base_key, &metadata_key(version))).await?;
        let mut info = parse_metadata_json(&text)?;
        info.moved = is_elsewhere(&info.location, url);
        Ok(info)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An error with a cause, as reqwest's "builder error" has.
    #[derive(Debug)]
    struct Caused(&'static str, Option<Box<Caused>>);
    impl std::fmt::Display for Caused {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for Caused {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|c| c as _)
        }
    }

    /// A client that cannot start says why: missing CA certificates by name, with what to
    /// do; any other cause as its whole chain, not only "builder error".
    #[test]
    fn a_client_that_cannot_start_says_why() {
        let no_roots = Caused(
            "builder error",
            Some(Box::new(Caused(
                "unexpected error: No CA certificates were loaded from the system",
                None,
            ))),
        );
        let shown = client_err(no_roots).to_string();
        assert!(
            shown.contains("no CA certificates were found") && shown.contains("SSL_CERT_FILE"),
            "{shown}"
        );
        let other = Caused(
            "builder error",
            Some(Box::new(Caused("invalid proxy URL", None))),
        );
        assert_eq!(
            client_err(other).to_string(),
            "sink error: builder error: invalid proxy URL"
        );
        // A store's configuration error, as object_store words it: the cause is already
        // in the message, and is not repeated.
        let config = Caused(
            "Generic S3 error: Missing bucket name",
            Some(Box::new(Caused("Missing bucket name", None))),
        );
        assert_eq!(
            client_err(config).to_string(),
            "sink error: Generic S3 error: Missing bucket name"
        );
    }

    /// A table Spark recorded as `s3a://` (or `s3n://`) opens as S3, as a catalog's
    /// location: a commit, compaction or expiry through the catalog reaches it.
    #[test]
    fn spark_spellings_of_s3_open_as_s3() {
        for dest in ["s3a://lake/wh/t", "s3n://lake/wh/t", "s3://lake/wh/t"] {
            let (_, key) = build_store(dest).unwrap_or_else(|e| panic!("{dest}: {e}"));
            assert_eq!(key.as_ref(), "wh/t", "{dest}");
        }
    }

    #[test]
    fn a_relocated_table_keeps_its_own_spelling_of_s3() {
        // Unmoved, reached as s3:// but recorded as s3a://: its place as recorded.
        assert_eq!(
            spelled_as_recorded("s3a://lake/t/", "s3://lake/t"),
            "s3a://lake/t"
        );
        // Moved: the new place, in the spelling its other readers use.
        assert_eq!(
            spelled_as_recorded("s3a://old/t", "s3://new/t"),
            "s3a://new/t"
        );
        assert_eq!(
            spelled_as_recorded("s3://old/t", "s3://new/t"),
            "s3://new/t"
        );
        assert_eq!(spelled_as_recorded("", "s3://new/t"), "s3://new/t");
        assert_eq!(
            spelled_as_recorded("s3a://old/t", "gs://new/t"),
            "gs://new/t"
        );
    }
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ss-objstore-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn run_bounded_keeps_order_and_both_bounds() {
        // (cost, sleep ms): small jobs, then jobs a part each, then one past the budget.
        let mut jobs: Vec<(u64, u64)> = (0..20).map(|i| (1024, 20 - i)).collect();
        jobs.extend((0..6).map(|i| (PART_SIZE as u64, 6 - i)));
        jobs.push((3 * PART_SIZE as u64, 2));
        jobs.extend((0..4).map(|_| (1024, 1)));
        let now = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let held = Arc::new(AtomicU64::new(0));
        let max_held = Arc::new(AtomicU64::new(0));
        let n = jobs.len();
        let results = block_on(async {
            let r = run_bounded(
                jobs.clone(),
                |j| j.0,
                |(cost, ms)| {
                    let (now, max, held, max_held) =
                        (now.clone(), max.clone(), held.clone(), max_held.clone());
                    async move {
                        max.fetch_max(now.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                        let h = held.fetch_add(cost, Ordering::SeqCst) + cost;
                        // Past the budget only as the one oversized job, alone.
                        if h > WRITE_BUDGET && h != cost {
                            max_held.fetch_max(u64::MAX, Ordering::SeqCst);
                        }
                        max_held.fetch_max(h, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        held.fetch_sub(cost, Ordering::SeqCst);
                        now.fetch_sub(1, Ordering::SeqCst);
                        Ok(cost * 1000 + ms)
                    }
                },
            )
            .await;
            Ok(r)
        })
        .unwrap();
        let got: Vec<u64> = results.into_iter().map(|r| r.unwrap().unwrap()).collect();
        let want: Vec<u64> = jobs.iter().map(|(c, ms)| c * 1000 + ms).collect();
        assert_eq!(got, want, "results in job order");
        assert_eq!(got.len(), n);
        let max = max.load(Ordering::SeqCst);
        assert!(max > 1 && max <= STORE_WRITERS, "{max} in flight");
        // The one oversized job ran alone; everything else stayed within the budget.
        assert_eq!(max_held.load(Ordering::SeqCst), 3 * PART_SIZE as u64);
    }

    #[test]
    fn run_bounded_starts_nothing_after_a_failure() {
        let started = Arc::new(AtomicUsize::new(0));
        let results = block_on(async {
            let r = run_bounded(
                (0..100u64).collect(),
                |_| 0,
                |i| {
                    let started = started.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(if i == 3 { 1 } else { 5 })).await;
                        if i == 3 {
                            Err(sink_err("job 3"))
                        } else {
                            Ok(i)
                        }
                    }
                },
            )
            .await;
            Ok(r)
        })
        .unwrap();
        let started = started.load(Ordering::SeqCst);
        assert!((4..100).contains(&started), "{started} started");
        assert!(results[3].as_ref().unwrap().is_err());
        // Every job that started finished and is reported; the rest never started.
        assert_eq!(results.iter().filter(|r| r.is_some()).count(), started);
        assert!(results[99].is_none());
    }

    #[test]
    fn files_round_trip_on_both_sides_of_the_part_size() {
        let dir = tmp("roundtrip");
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let part = PART_SIZE as u64;
        let sizes = [0, 1, part - 1, part, part + 1, 2 * part + 12345];
        let puts: Vec<Put> = sizes
            .iter()
            .map(|&size| {
                let path = dir.join(format!("f{size}"));
                let bytes: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
                std::fs::write(&path, bytes).unwrap();
                Put::file(ObjPath::from(format!("t/f{size}")), path, true).unwrap()
            })
            .collect();
        let total = block_on(put_all(&store, puts)).unwrap();
        assert_eq!(total, sizes.iter().sum::<u64>());
        for &size in &sizes {
            let got = block_on(get_bytes(&store, &ObjPath::from(format!("t/f{size}")))).unwrap();
            assert_eq!(
                got,
                std::fs::read(dir.join(format!("f{size}"))).unwrap(),
                "{size}"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An in-memory store that counts its writes, and how many overlap (each one waits
    /// a few milliseconds, as a request to a real store would).
    #[derive(Debug, Default)]
    struct Counting {
        inner: object_store::memory::InMemory,
        puts: AtomicU64,
        multiparts: AtomicU64,
        now: AtomicUsize,
        max: AtomicUsize,
        batch_deletes: AtomicU64,
        /// Its first many-delete call deletes and confirms nothing (a bulk request that
        /// failed), so the single deletes after it must do the work. (A single `delete`
        /// is itself a one-key many-delete call.)
        batch_fails: bool,
        /// A conditional create (`PutMode::Create`) fails: after the store applied it (a
        /// lost response), or before.
        create_fails: Option<Applied>,
        /// Every read fails.
        get_fails: bool,
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Applied {
        Yes,
        No,
    }

    impl std::fmt::Display for Counting {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Counting")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.max.fetch_max(
                self.now.fetch_add(1, Ordering::SeqCst) + 1,
                Ordering::SeqCst,
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.now.fetch_sub(1, Ordering::SeqCst);
            if let (Some(applied), object_store::PutMode::Create) = (self.create_fails, &opts.mode)
            {
                if applied == Applied::Yes {
                    self.inner.put_opts(location, payload, opts).await?;
                }
                return Err(object_store::Error::Generic {
                    store: "Counting",
                    source: "the connection was reset".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.multiparts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if self.get_fails {
                return Err(object_store::Error::Generic {
                    store: "Counting",
                    source: "the read timed out".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_core::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_core::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            if self.batch_deletes.fetch_add(1, Ordering::SeqCst) == 0 && self.batch_fails {
                return Box::pin(Ready(std::iter::empty()));
            }
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_core::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn a_file_that_fits_in_a_part_is_one_put_and_the_puts_overlap() {
        let dir = tmp("requests");
        let counting = Arc::new(Counting::default());
        let store: Arc<dyn ObjectStore> = counting.clone();
        let part = PART_SIZE as u64;
        let mut puts = Vec::new();
        for (i, size) in [1, 1000, part, part + 1]
            .into_iter()
            .chain([500; 16])
            .enumerate()
        {
            let path = dir.join(format!("f{i}"));
            std::fs::write(&path, vec![7u8; size as usize]).unwrap();
            puts.push(Put::file(ObjPath::from(format!("t/f{i}")), path, true).unwrap());
        }
        block_on(put_all(&store, puts)).unwrap();
        // One multipart upload (the file past a part: a create, then parts that are not
        // puts), and one put for each of the other 19 files.
        assert_eq!(counting.multiparts.load(Ordering::SeqCst), 1);
        assert_eq!(counting.puts.load(Ordering::SeqCst), 19);
        let max = counting.max.load(Ordering::SeqCst);
        assert!(max > 1 && max <= STORE_WRITERS, "{max} puts in flight");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn remove_all_reports_each_key_and_falls_back_to_single_deletes() {
        for batch_fails in [false, true] {
            let counting = Arc::new(Counting {
                batch_fails,
                ..Counting::default()
            });
            let store: Arc<dyn ObjectStore> = counting.clone();
            let present: Vec<ObjPath> =
                (0..30).map(|i| ObjPath::from(format!("t/d/{i}"))).collect();
            for k in &present {
                block_on(put_bytes(&store, k, vec![1])).unwrap();
            }
            let mut keys = present.clone();
            keys.insert(5, ObjPath::from("t/d/never-there"));
            let gone = block_on(async { Ok(remove_all(&store, keys.clone()).await) }).unwrap();
            assert_eq!(gone, vec![true; keys.len()], "batch fails: {batch_fails}");
            // One call for every key; then, only if it failed, one per key.
            let calls = if batch_fails { 1 + keys.len() } else { 1 };
            assert_eq!(counting.batch_deletes.load(Ordering::SeqCst), calls as u64);
            for k in &present {
                let left = block_on(async { Ok(store.head(k).await) }).unwrap();
                assert!(
                    matches!(left, Err(object_store::Error::NotFound { .. })),
                    "{k}"
                );
            }
        }
    }

    fn exists(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> bool {
        block_on(async { Ok(store.head(key).await.is_ok()) }).unwrap()
    }

    #[test]
    fn a_commit_is_judged_by_what_the_store_holds_not_by_the_error() {
        let base = ObjPath::from("t");
        let ours = br#"{"format-version":2,"ours":true}"#.to_vec();
        let publish = |store: &Arc<dyn ObjectStore>| {
            let (store, base, ours) = (store.clone(), base.clone(), ours.clone());
            block_on(async move { Ok(publish_metadata(&store, &base, 3, ours).await) }).unwrap()
        };
        let key = child_key(&base, &metadata_key(3));

        // The put landed and its response was lost: committed.
        let store: Arc<dyn ObjectStore> = Arc::new(Counting {
            create_fails: Some(Applied::Yes),
            ..Counting::default()
        });
        assert!(publish(&store).is_ok());
        // A retry that finds this write's own bytes in place: committed.
        assert!(
            publish(&store).is_ok(),
            "our own metadata is not a conflict"
        );

        // Another writer's metadata holds the version: refused, safe to clean up.
        let store: Arc<dyn ObjectStore> = Arc::new(Counting::default());
        block_on(put_bytes(&store, &key, b"{}".to_vec())).unwrap();
        let nc = publish(&store).expect_err("refused");
        assert!(nc.refused, "{}", nc.error);

        // The put failed and nothing is there: not committed, and not known to be refused.
        let store: Arc<dyn ObjectStore> = Arc::new(Counting {
            create_fails: Some(Applied::No),
            ..Counting::default()
        });
        let nc = publish(&store).expect_err("not committed");
        assert!(!nc.refused, "{}", nc.error);
        assert!(!exists(&store, &key));

        // The version exists (it is ours) but cannot be read back: unknown, not refused.
        let store: Arc<dyn ObjectStore> = Arc::new(Counting {
            get_fails: true,
            ..Counting::default()
        });
        block_on(put_bytes(&store, &key, ours.clone())).unwrap();
        let nc = publish(&store).expect_err("unknown");
        assert!(!nc.refused, "{}", nc.error);
        assert!(
            !matches!(nc.error, ShapeError::Conflict(_)),
            "not a retry: {}",
            nc.error
        );
    }

    #[test]
    fn a_write_removes_its_files_only_when_another_writer_holds_the_version() {
        let dir = tmp("commit-outcome");
        let base = ObjPath::from("t");
        let run = |store: &Arc<dyn ObjectStore>| {
            let data = dir.join("d.parquet");
            std::fs::write(&data, b"data").unwrap();
            let art = MetadataArtifacts {
                manifest_key: "metadata/m.avro".into(),
                manifest_bytes: vec![1],
                manifest_list_key: "metadata/snap-1.avro".into(),
                manifest_list_bytes: vec![2],
                metadata_key: metadata_key(1),
                metadata_bytes: br#"{"ours":true}"#.to_vec(),
                version_hint_key: "metadata/version-hint.text".into(),
                version_hint_bytes: b"1".to_vec(),
                delete_manifest: None,
                snapshot_id: 1,
                version: 1,
            };
            let result = upload_table(
                store,
                &base,
                vec![("data/d.parquet".into(), data)],
                Vec::new(),
                art,
            );
            (result, exists(store, &child_key(&base, "data/d.parquet")))
        };

        // A lost response on a commit that landed: success, files in place.
        let store: Arc<dyn ObjectStore> = Arc::new(Counting {
            create_fails: Some(Applied::Yes),
            ..Counting::default()
        });
        let (result, kept) = run(&store);
        assert!(result.is_ok() && kept, "{result:?}");

        // Unknown outcome: an error, and the files stay (a committed snapshot may name them).
        let store: Arc<dyn ObjectStore> = Arc::new(Counting {
            create_fails: Some(Applied::No),
            ..Counting::default()
        });
        let (result, kept) = run(&store);
        assert!(result.is_err() && kept, "{result:?}");

        // Our own metadata in place but unreadable: an error, and the files stay. (This
        // store fails every read, its own HEAD included, so ask the store behind it.)
        let counting = Arc::new(Counting {
            get_fails: true,
            ..Counting::default()
        });
        let store: Arc<dyn ObjectStore> = counting.clone();
        block_on(put_bytes(
            &store,
            &child_key(&base, &metadata_key(1)),
            br#"{"ours":true}"#.to_vec(),
        ))
        .unwrap();
        let (result, _) = run(&store);
        let behind: Arc<dyn ObjectStore> = Arc::new(counting.inner.clone());
        let kept = exists(&behind, &child_key(&base, "data/d.parquet"));
        assert!(result.is_err() && kept, "{result:?}");

        // Refused by another writer's metadata: an error, and the files are removed.
        let store: Arc<dyn ObjectStore> = Arc::new(Counting::default());
        block_on(put_bytes(
            &store,
            &child_key(&base, &metadata_key(1)),
            b"{}".to_vec(),
        ))
        .unwrap();
        let (result, kept) = run(&store);
        assert!(result.is_err() && !kept, "{result:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_failed_batch_leaves_nothing_behind() {
        let dir = tmp("cleanup");
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut puts = Vec::new();
        for i in 0..12 {
            let path = dir.join(format!("f{i}"));
            std::fs::write(&path, vec![i as u8; 1000 + i]).unwrap();
            puts.push(Put::file(ObjPath::from(format!("t/data/f{i}")), path, true).unwrap());
        }
        puts.push(Put::Bytes {
            key: ObjPath::from("t/metadata/m.avro"),
            bytes: vec![1, 2, 3],
        });
        let keys: Vec<ObjPath> = puts.iter().map(|p| p.key().clone()).collect();
        // A staged file that vanished before its upload.
        std::fs::remove_file(dir.join("f7")).unwrap();
        let err = block_on(put_all(&store, puts)).unwrap_err();
        assert!(err.to_string().contains("f7"), "{err}");
        for key in &keys {
            let left = block_on(async { Ok(store.head(key).await) }).unwrap();
            assert!(
                matches!(left, Err(object_store::Error::NotFound { .. })),
                "{key} was left behind"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
