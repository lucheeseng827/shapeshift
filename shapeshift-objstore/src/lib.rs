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
//! prefix; a REST catalog (relative-path relocation, multi-writer commits) is out of
//! scope.
//!
//! **Merge** ([`ObjectStoreIcebergSink::create_merge`]) runs the same upsert as a local
//! table. The existing table is read through [`StoreFiles`]: manifests and delete files
//! whole, and a data file's footer plus its key-column chunks with ranged GETs, one row
//! group at a time. The delete files are staged and uploaded with the data files.
//! [`compact_iceberg`] compacts an object-store table through the same seam, and
//! [`expire_iceberg`] expires its old snapshots.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::path::Path as ObjPath;
// `ObjectStoreExt` carries the convenience methods (`put_multipart`, …); the base
// `ObjectStore` trait only exposes the `_opts` variants.
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt};
use tokio::io::AsyncReadExt;

use shapeshift_core::{Compression, RecordBatch, Result, SchemaRef, ShapeError, Sink, SinkSummary};
use shapeshift_iceberg::{
    build_metadata_artifacts, compact, continues_from, current_manifest_list_key,
    ensure_partition_compatible, evolve_fields, ice_fields_from_schema, join_location,
    metadata_key, moved_on, parse_metadata_json, plan_expiry, read_manifest_list,
    resolve_partition_fields, snapshot_timestamp_ms, staged_paths, version_taken, CompactContext,
    CompactOptions, CompactSummary, ExpireOptions, ExpireSummary, IceField, IcebergInfo,
    MergeContext, MergeWriter, MetadataArtifacts, MetadataBuild, PartitionField, PartitionedWriter,
    PriorTable, TableFiles,
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

/// An object-store table's existing files, for a merge to read. A path recorded under the
/// table's `location` resolves under the table's current key (so a copied table reads its
/// own files); any other path is read at the key its URL names in the same store.
///
/// It has **its own client**, used only on its own runtime. A pooled keep-alive connection
/// belongs to the runtime it was opened on: reused from another runtime while that one is
/// not being driven, its request is never sent and hangs until it times out. (Every other
/// call in this crate runs on a runtime of its own that ends with it, which closes its
/// connections; this runtime outlives many reads, so it must not share them.)
pub struct StoreFiles {
    store: Arc<dyn ObjectStore>,
    base_key: ObjPath,
    location: String,
    /// One runtime for every read of the merge, rather than one per request. Readers on
    /// several threads share it: the first to block on it drives the I/O for all of them.
    rt: tokio::runtime::Runtime,
}

impl StoreFiles {
    /// The table at `url`; `location` is what its metadata records.
    pub fn open(url: &str, location: &str) -> Result<Self> {
        let (store, base_key) = build_store(url)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(sink_err)?;
        Ok(StoreFiles {
            store,
            base_key,
            location: location.trim_end_matches('/').to_string(),
            rt,
        })
    }

    fn key(&self, path: &str) -> Result<ObjPath> {
        if !self.location.is_empty() {
            if let Some(rest) = path.strip_prefix(&self.location) {
                if rest.is_empty() || rest.starts_with('/') {
                    return Ok(child_key(&self.base_key, rest.trim_start_matches('/')));
                }
            }
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
            compression,
            prior,
            replaces,
            merge,
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
            let Some(version) = store_version(&store, &base_key).await? else {
                return Ok(None); // first write — no table to append to
            };
            let meta_text = get_text(&store, &child_key(&base_key, &metadata_key(version))).await?;
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
    art: MetadataArtifacts,
) -> Result<u64> {
    let mut puts = Vec::new();
    for (key, local) in uploads {
        puts.push(Put::file(child_key(base_key, &key), local, true)?);
    }
    for (key, local) in deletes {
        puts.push(Put::file(child_key(base_key, &key), local, false)?);
    }
    puts.push(Put::Bytes {
        key: child_key(base_key, &art.manifest_key),
        bytes: art.manifest_bytes,
    });
    if let Some((key, bytes)) = art.delete_manifest {
        puts.push(Put::Bytes {
            key: child_key(base_key, &key),
            bytes,
        });
    }
    puts.push(Put::Bytes {
        key: child_key(base_key, &art.manifest_list_key),
        bytes: art.manifest_list_bytes,
    });
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

        // A merge reads the existing table from the store (key columns only, ranged) and
        // stages its delete files beside the data files for the same upload.
        let (delete_files, merge) = match self.merge.take() {
            None => (Vec::new(), None),
            Some(m) => {
                let location = prior
                    .as_ref()
                    .and_then(|p| p.metadata["location"].as_str())
                    .unwrap_or(&self.base_url);
                let files = StoreFiles::open(&self.base_url, location)?;
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
                    .map_err(|e| {
                        let base = prior.as_ref().map_or(0, |p| p.version);
                        moved_on_in_store(e, base, &self.store, &self.base_key)
                    })?;
                (out.delete_files, Some(out.summary))
            }
        };
        let deletes: Vec<(String, PathBuf)> = delete_files
            .iter()
            .map(|d| (d.data_key.clone(), self.temp_dir.join(&d.data_key)))
            .collect();

        let table_uuid = self.table_uuid.to_string();
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
        })?;
        let metadata_uri = join_location(&self.base_url, &art.metadata_key);
        let (table_version, snapshot_id) = (art.version, art.snapshot_id);
        tracing::debug!(dest = %self.base_url, rows, files = uploads.len(), deletes = deletes.len(), "uploading iceberg table to object store");
        let bytes = upload_table(&self.store, &self.base_key, uploads, deletes, art)?;
        let _ = std::fs::remove_dir_all(&self.temp_dir);
        Ok(SinkSummary {
            files: vec![PathBuf::from(metadata_uri)],
            rows,
            bytes,
            merge,
            table_version: Some(table_version),
            snapshot_id: Some(snapshot_id),
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
    let location = table.metadata["location"]
        .as_str()
        .unwrap_or(&base_url)
        .to_string();
    let files = StoreFiles::open(&base_url, &location)?;
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
    let location = table.metadata["location"]
        .as_str()
        .unwrap_or(&base_url)
        .to_string();
    let files = StoreFiles::open(&base_url, &location)?;
    let plan = plan_expiry(&table, &base_url, &files, options, snapshot_timestamp_ms())
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
    // Deleted STORE_WRITERS at a time; a file that cannot be deleted is counted, not fatal.
    let mut keys = Vec::new();
    let mut targets = Vec::new();
    for t in &plan.delete {
        match files.key(&t.path) {
            Ok(key) => {
                keys.push(key);
                targets.push(t);
            }
            Err(_) => summary.files_not_deleted += 1,
        }
    }
    let gone = files.rt.block_on(remove_all(store, keys));
    for (t, gone) in targets.into_iter().zip(gone) {
        if gone {
            summary.count_deleted(t);
        } else {
            summary.files_not_deleted += 1;
        }
    }
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
        parse_metadata_json(&text)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
