//! Reading a table's existing files, wherever the table lives.
//!
//! A merge reads three kinds of existing file: manifests and position-delete files
//! (small, read whole), and the **key columns** of data files (potentially large).
//! [`TableFiles`] is the seam. A local table answers from disk, and an object-store table
//! answers with GETs and ranged GETs.
//!
//! Parquet is read through a [`RangeCache`] that already holds exactly the bytes the read
//! will touch: the footer first, then **one row group's projected column chunks at a
//! time**. Against an object store that is one footer GET and one (coalesced) ranged GET
//! per row group — the file's size comes from the manifest, so no size lookup unless the
//! manifest's size proves wrong — and a file small enough for the footer's read to hold
//! all of it costs that one GET. Parquet's own reader would issue a request per page
//! header. Memory stays bounded to one row group's key chunks per file being read, never
//! the file.
//!
//! Files are read [`TableFiles::readers`] at a time ([`read_each`]): an object store
//! answers each request in tens of milliseconds however many are in flight, so a merge
//! that reads one file after another spends its time waiting. A compaction, which must
//! write what it reads in order, reads ahead of its writer instead ([`read_ahead`]).
//! Local tables take the same path, so the code an object store runs is the code the local
//! tests exercise.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use bytes::{Buf, Bytes};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::{ChunkReader, Length};
use parquet::schema::types::SchemaDescriptor;

use shapeshift_core::{Result, SchemaRef, ShapeError};

use crate::anchor::Anchors;

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// A data or delete file's rows failed to decode. When the reason is a Parquet codec this
/// build was compiled without, say so and which build reads it: zstd is the default of
/// PyIceberg and of Iceberg's Java writers since 1.4, and the lean binary leaves it out to
/// stay free of C code. Any other failure is reported as it came, with the file's path.
pub(crate) fn batch_err(path: &str, e: impl std::fmt::Display) -> ShapeError {
    let text = e.to_string();
    // parquet 54's wording (`compression::create_codec`); its test pins it, so a parquet
    // upgrade that rewords it fails there rather than silently losing the explanation.
    let Some(codec) = text.split("Disabled feature at compile time: ").nth(1) else {
        return err(format!("{path}: {text}"));
    };
    let codec: String = codec
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    err(if codec == "zstd" {
        format!(
            "{path} is zstd-compressed Parquet (the default of PyIceberg and of Iceberg's \
             Java writers since 1.4), and this build reads only uncompressed and Snappy \
             Parquet. Reading it takes a build with `--features zstd`"
        )
    } else {
        format!(
            "{path} is Parquet compressed with `{codec}`, which this build does not read \
             (it reads uncompressed and Snappy Parquet, and zstd with `--features zstd`)"
        )
    })
}

/// Read access to a table's existing files. Every `path` is a file's path exactly as the
/// table's metadata records it (usually absolute: `/warehouse/t/data/…`,
/// `s3://bucket/t/data/…`). The implementation resolves it against where the table lives
/// now. Reads may come from several threads at once (see [`TableFiles::readers`]).
pub trait TableFiles: Sync {
    /// A whole file. Meant for small ones: manifests, position-delete files.
    fn read(&self, path: &str) -> Result<Vec<u8>>;
    /// A file's size in bytes.
    fn size(&self, path: &str) -> Result<u64>;
    /// Byte ranges of one file, returned in the order asked. An implementation may
    /// coalesce nearby ranges into one request.
    fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>>;
    /// How many files are worth reading at once. Each reader holds one row group's
    /// projected chunks (or one small file), so this also bounds what reading costs in
    /// memory. `SHAPESHIFT_READERS` overrides it.
    fn readers(&self) -> usize {
        1
    }
}

/// Environment variable that overrides [`TableFiles::readers`] (a positive integer).
pub const READERS_ENV: &str = "SHAPESHIFT_READERS";

/// Most readers a local table uses: past a few, decoding key columns stops scaling.
const LOCAL_READERS: usize = 4;

/// How many files to read at once from `files`.
pub(crate) fn readers_for<F: TableFiles>(files: &F) -> usize {
    std::env::var(READERS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| files.readers())
        .max(1)
}

/// `f` over every item, up to `files`' readers at a time; the results come back in item
/// order, so what a read produces never depends on which finished first. The first error
/// stops new work from starting and is returned (the earliest item's, if several failed).
pub(crate) fn read_each<F, T, R>(
    files: &F,
    items: &[T],
    f: impl Fn(&T) -> Result<R> + Sync,
) -> Result<Vec<R>>
where
    F: TableFiles,
    T: Sync,
    R: Send,
{
    let workers = readers_for(files).min(items.len());
    if workers <= 1 {
        return items.iter().map(f).collect();
    }
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let mut done: Vec<(usize, Result<R>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut out = Vec::new();
                    while !failed.load(Ordering::Relaxed) {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(item) = items.get(i) else { break };
                        let r = f(item);
                        if r.is_err() {
                            failed.store(true, Ordering::Relaxed);
                        }
                        out.push((i, r));
                    }
                    out
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            .collect()
    });
    done.sort_unstable_by_key(|(i, _)| *i);
    // Every item ran unless one failed, and then the earliest error is what is returned.
    done.into_iter().map(|(_, r)| r).collect()
}

/// Bytes [`read_ahead`] may hold for its caller: footers opened and row groups fetched that
/// the caller has not taken yet. A row group bigger than this is still read, alone.
pub(crate) const READ_AHEAD_BYTES: u64 = 16 * 1024 * 1024;

/// One Parquet file being handed over by [`read_ahead`]: its schema, then its row groups in
/// file order.
pub(crate) struct AheadFile<'s, 'a, F: TableFiles> {
    schema: SchemaRef,
    row_groups: usize,
    next: usize,
    from: AheadSource<'s, 'a, F>,
}

enum AheadSource<'s, 'a, F: TableFiles> {
    /// One reader: the caller's own thread reads each row group when it asks for it.
    Inline(RangedParquet<'a, F>, &'s ProjectionMask),
    /// Readers on threads of their own; the row groups wait in `Ahead`.
    Shared(&'s Ahead, usize),
}

impl<F: TableFiles> AheadFile<'_, '_, F> {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The file's next row group, or `None` after its last.
    pub fn next_group(&mut self) -> Result<Option<ParquetRecordBatchReader>> {
        if self.next == self.row_groups {
            return Ok(None);
        }
        self.next += 1;
        match &self.from {
            AheadSource::Inline(pq, mask) => pq.row_group(self.next - 1, mask).map(Some),
            AheadSource::Shared(ahead, file) => match ahead.take((*file, self.next))? {
                Got::Group(reader) => Ok(Some(reader)),
                Got::Opened(..) => Err(err("internal error: a file opened twice")),
            },
        }
    }
}

/// What a reader hands over, keyed by (file, 0) for the file opened and (file, n) for its
/// row group n - 1.
enum Got {
    Opened(SchemaRef, usize),
    Group(ParquetRecordBatchReader),
}

struct Handed {
    got: Result<Got>,
    /// Bytes counted against the budget until the caller takes it.
    cost: u64,
}

#[derive(Default)]
struct AheadState {
    /// The next file a reader takes.
    next_file: usize,
    ready: HashMap<(usize, usize), Handed>,
    held: u64,
    /// What the caller waits for: always let through, whatever the budget.
    want: (usize, usize),
    /// A read failed: no reader starts another file.
    failed: bool,
    /// The caller is gone, or a reader panicked: every reader stops.
    stop: bool,
}

/// The readers' and the caller's shared view of [`read_ahead`].
struct Ahead {
    state: Mutex<AheadState>,
    changed: Condvar,
    budget: u64,
}

impl Ahead {
    fn lock(&self) -> MutexGuard<'_, AheadState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn wait<'g>(&self, guard: MutexGuard<'g, AheadState>) -> MutexGuard<'g, AheadState> {
        self.changed.wait(guard).unwrap_or_else(|p| p.into_inner())
    }

    /// Wait until `key`'s `cost` fits the budget, or the caller needs `key` next. False if
    /// the reader is to stop instead.
    fn admit(&self, key: (usize, usize), cost: u64) -> bool {
        let mut st = self.lock();
        loop {
            if st.stop {
                return false;
            }
            if st.held + cost <= self.budget || st.want == key {
                st.held += cost;
                return true;
            }
            st = self.wait(st);
        }
    }

    /// Hand `got` over for `key`. It was admitted at `admitted` bytes and holds `cost`
    /// (they differ for a footer longer than its first read), which is what stays charged
    /// until the caller takes it.
    fn hand(&self, key: (usize, usize), got: Result<Got>, admitted: u64, cost: u64) {
        let mut st = self.lock();
        st.failed |= got.is_err();
        st.held = st.held - admitted + cost;
        st.ready.insert(key, Handed { got, cost });
        self.changed.notify_all();
    }

    /// What a reader handed over for `key`, once it has.
    fn take(&self, key: (usize, usize)) -> Result<Got> {
        let mut st = self.lock();
        st.want = key;
        self.changed.notify_all();
        loop {
            if let Some(h) = st.ready.remove(&key) {
                st.held -= h.cost;
                self.changed.notify_all();
                return h.got;
            }
            if st.stop {
                return Err(err("a reader stopped before handing over its file"));
            }
            st = self.wait(st);
        }
    }

    fn stop(&self) {
        self.lock().stop = true;
        self.changed.notify_all();
    }
}

/// Stops every reader if the one that owns it panics, so the caller is not left waiting
/// for a file that will never come (the panic is then raised where the readers are joined).
struct StopOnPanic<'s>(&'s Ahead);

impl Drop for StopOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.stop();
        }
    }
}

/// Stops every reader when the caller is done with them, however it leaves: returning, failing,
/// or unwinding from a panic in `consume` (a reader waiting on the budget would otherwise wait
/// forever, and the scope that joins it with it).
struct StopWhenDone<'s>(&'s Ahead);

impl Drop for StopWhenDone<'_> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// One reader: take the next file, open it, fetch its row groups in order, repeat.
fn read_files<F: TableFiles>(
    ahead: &Ahead,
    files: &F,
    inputs: &[(&str, Option<u64>)],
    mask: &ProjectionMask,
) {
    let _stop = StopOnPanic(ahead);
    loop {
        let i = {
            let mut st = ahead.lock();
            if st.stop || st.failed || st.next_file == inputs.len() {
                return;
            }
            st.next_file += 1;
            st.next_file - 1
        };
        let (path, size) = inputs[i];
        // The footer's first read, as `RangedParquet::open` makes it; a longer footer is
        // charged in full once read.
        let footer = size.unwrap_or(FOOTER_PREFETCH).min(FOOTER_PREFETCH);
        if !ahead.admit((i, 0), footer) {
            return;
        }
        let pq = match RangedParquet::open(files, path, size) {
            Ok(pq) => pq,
            Err(e) => return ahead.hand((i, 0), Err(e), footer, footer),
        };
        let groups = pq.num_row_groups();
        let opened = Ok(Got::Opened(pq.schema().clone(), groups));
        ahead.hand((i, 0), opened, footer, pq.footer_bytes());
        for rg in 0..groups {
            let cost = pq.fetch_cost(rg, mask);
            if !ahead.admit((i, rg + 1), cost) {
                return;
            }
            let got = pq.row_group(rg, mask);
            let failed = got.is_err();
            ahead.hand((i, rg + 1), got.map(Got::Group), cost, cost);
            if failed {
                return;
            }
        }
    }
}

/// Read the Parquet files `inputs` (each a path and the size its manifest records) ahead of
/// `consume`, which gets them one at a time, in the order given, each with its row groups
/// (projected to `mask`) in file order. Up to `files`' readers take the next file and fetch
/// its footer and row groups while `consume` works through the earlier ones. About `budget`
/// bytes of fetched data (footer reads and compressed column chunks, as fetched) wait for it,
/// and the row group it needs next is always fetched, however large. What `consume` sees never
/// depends on how many readers ran or which finished first. A failed read is returned when
/// `consume` reaches it; no file after it is handed over. `consume` need not read every row
/// group of a file: the rest are skipped (with one reader, never fetched).
pub(crate) fn read_ahead<'a, F: TableFiles>(
    files: &'a F,
    inputs: &[(&'a str, Option<u64>)],
    mask: &ProjectionMask,
    budget: u64,
    mut consume: impl FnMut(usize, &mut AheadFile<'_, 'a, F>) -> Result<()>,
) -> Result<()> {
    let workers = readers_for(files).min(inputs.len());
    if workers <= 1 {
        for (i, &(path, size)) in inputs.iter().enumerate() {
            let pq = RangedParquet::open(files, path, size)?;
            let mut file = AheadFile {
                schema: pq.schema().clone(),
                row_groups: pq.num_row_groups(),
                next: 0,
                from: AheadSource::Inline(pq, mask),
            };
            consume(i, &mut file)?;
        }
        return Ok(());
    }
    let ahead = Ahead {
        state: Mutex::new(AheadState::default()),
        changed: Condvar::new(),
        budget,
    };
    std::thread::scope(|scope| {
        // Done, failed or panicking, the readers stop; one that panicked panics the scope.
        let _stop = StopWhenDone(&ahead);
        for _ in 0..workers {
            scope.spawn(|| read_files(&ahead, files, inputs, mask));
        }
        (|| {
            for i in 0..inputs.len() {
                let (schema, row_groups) = match ahead.take((i, 0))? {
                    Got::Opened(schema, n) => (schema, n),
                    Got::Group(_) => {
                        return Err(err("internal error: a row group before its file"))
                    }
                };
                let mut file = AheadFile {
                    schema,
                    row_groups,
                    next: 0,
                    from: AheadSource::Shared(&ahead, i),
                };
                consume(i, &mut file)?;
                // What the readers fetched and `consume` left releases its budget.
                while file.next_group()?.is_some() {}
            }
            Ok(())
        })()
    })
}

/// A local table's files. A path recorded under any directory the table has lived at (its
/// [`Anchors`]: a moved or copied table, or one moved, written to and moved again) is read
/// at the same key under the table's current directory.
pub struct LocalTableFiles {
    dir: PathBuf,
    anchors: Anchors,
}

impl LocalTableFiles {
    /// `dir` is where the table is now; `anchors` are the directories its file paths were
    /// written against ([`Anchors::of`] its metadata).
    pub fn new(dir: impl AsRef<Path>, anchors: Anchors) -> Self {
        LocalTableFiles {
            dir: dir.as_ref().to_path_buf(),
            anchors,
        }
    }

    /// The local path a recorded file path resolves to: under the table's directory for a
    /// path under one of its anchors, else the path as recorded. A path that would lead out
    /// of the table is refused (see [`Anchors::key`]).
    pub fn resolve(&self, path: &str) -> Result<PathBuf> {
        Ok(match self.anchors.key(path)? {
            Some(key) => self.dir.join(key),
            None => PathBuf::from(path),
        })
    }
}

impl TableFiles for LocalTableFiles {
    fn readers(&self) -> usize {
        std::thread::available_parallelism().map_or(1, |n| n.get().min(LOCAL_READERS))
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let p = self.resolve(path)?;
        std::fs::read(&p).map_err(|e| err(format!("{}: {e}", p.display())))
    }

    fn size(&self, path: &str) -> Result<u64> {
        let p = self.resolve(path)?;
        Ok(std::fs::metadata(&p)
            .map_err(|e| err(format!("{}: {e}", p.display())))?
            .len())
    }

    fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let p = self.resolve(path)?;
        let mut f = std::fs::File::open(&p).map_err(|e| err(format!("{}: {e}", p.display())))?;
        ranges
            .iter()
            .map(|r| {
                let mut buf = vec![0u8; (r.end - r.start) as usize];
                f.seek(SeekFrom::Start(r.start)).map_err(err)?;
                f.read_exact(&mut buf)
                    .map_err(|e| err(format!("{}: {e}", p.display())))?;
                Ok(Bytes::from(buf))
            })
            .collect()
    }
}

/// A Parquet file's bytes, held only for the ranges a read will touch. Parquet's reader
/// asks for bytes by offset; each request is served from the segment that covers it, and
/// a request outside every segment is an error (a range the caller forgot to prefetch).
#[derive(Clone)]
pub(crate) struct RangeCache {
    len: u64,
    segments: Vec<(u64, Bytes)>,
}

impl RangeCache {
    /// Whether one segment holds all of `r`.
    fn covers(&self, r: &Range<u64>) -> bool {
        self.segments
            .iter()
            .any(|(at, b)| *at <= r.start && r.end <= *at + b.len() as u64)
    }

    /// The bytes from `start` to the end of the segment that holds `start..start + len`.
    /// `start` must lie strictly inside the segment: when two fetched column chunks sit
    /// back to back, the first one's segment *ends* at the second's first byte, and
    /// handing that empty tail to parquet's page reader makes it decode a page header out
    /// of nothing (an EOF at best, a garbage length and a runaway allocation at worst).
    /// Of the segments that qualify — the footer prefetch can overlap a chunk — the one
    /// reaching furthest wins, so a reader never stops short.
    fn segment(&self, start: u64, len: u64) -> parquet::errors::Result<Bytes> {
        self.segments
            .iter()
            .filter(|(at, b)| {
                let end = *at + b.len() as u64;
                *at <= start && start < end && start + len <= end
            })
            .max_by_key(|(at, b)| *at + b.len() as u64)
            .map(|(at, b)| b.slice((start - at) as usize..))
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "internal error: bytes {start}..{} were not fetched",
                    start + len
                ))
            })
    }
}

impl Length for RangeCache {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for RangeCache {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        // A reader runs to the end of its segment: a page header and its page, or the
        // footer's last 8 bytes — always within one fetched column chunk or the tail.
        Ok(self.segment(start, 0)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        Ok(self.segment(start, length as u64)?.slice(..length))
    }
}

/// How much of a file's tail to fetch for its footer. Most footers fit; a bigger one costs
/// one more request.
const FOOTER_PREFETCH: u64 = 64 * 1024;

/// A Parquet file opened through [`TableFiles`]: its footer is read, nothing else yet.
pub(crate) struct RangedParquet<'a, F: TableFiles> {
    files: &'a F,
    path: &'a str,
    footer: RangeCache,
    meta: ArrowReaderMetadata,
}

impl<'a, F: TableFiles> RangedParquet<'a, F> {
    /// Fetch and parse the footer. `recorded_size` is the size the manifest records for the
    /// file: with it the footer is the first request. If the file does not open at that
    /// size (a writer recorded it wrong), its actual size is asked for and, if it differs,
    /// the file is opened at that.
    pub fn open(files: &'a F, path: &'a str, recorded_size: Option<u64>) -> Result<Self> {
        match recorded_size.filter(|&s| s > 0) {
            Some(size) => Self::open_at(files, path, size).or_else(|e| {
                let actual = files.size(path)?;
                if actual == size {
                    Err(e)
                } else {
                    Self::open_at(files, path, actual)
                }
            }),
            None => Self::open_at(files, path, files.size(path)?),
        }
    }

    fn open_at(files: &'a F, path: &'a str, size: u64) -> Result<Self> {
        let bad = |what: &str| err(format!("{path}: {what}"));
        if size < 12 {
            return Err(bad("too short to be a Parquet file"));
        }
        let fetch = |from: u64| -> Result<RangeCache> {
            let mut got = files.read_ranges(path, std::slice::from_ref(&(from..size)))?;
            let bytes = got.pop().ok_or_else(|| bad("no bytes returned"))?;
            Ok(RangeCache {
                len: size,
                segments: vec![(from, bytes)],
            })
        };
        let mut footer = fetch(size - FOOTER_PREFETCH.min(size))?;
        let tail = footer.get_bytes(size - 8, 8).map_err(err)?;
        if &tail[4..] != b"PAR1" {
            return Err(bad("not a Parquet file (no PAR1 footer)"));
        }
        let needed = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64 + 8;
        if needed > size {
            return Err(bad("Parquet footer is longer than the file"));
        }
        if needed > FOOTER_PREFETCH.min(size) {
            footer = fetch(size - needed)?;
        }
        let meta = ParquetMetaDataReader::new()
            .parse_and_finish(&footer)
            .map_err(|e| bad(&e.to_string()))?;
        let meta = ArrowReaderMetadata::try_new(Arc::new(meta), ArrowReaderOptions::new())
            .map_err(|e| bad(&e.to_string()))?;
        Ok(RangedParquet {
            files,
            path,
            footer,
            meta,
        })
    }

    /// The file's Arrow schema (with `PARQUET:field_id` in each field's metadata).
    pub fn schema(&self) -> &SchemaRef {
        self.meta.schema()
    }

    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.meta.metadata().file_metadata().schema_descr()
    }

    pub fn num_row_groups(&self) -> usize {
        self.meta.metadata().num_row_groups()
    }

    /// The file's own key-value metadata (an engine's `iceberg.schema`, the Arrow schema).
    pub fn key_value_metadata(&self) -> Option<&Vec<parquet::file::metadata::KeyValue>> {
        self.meta.metadata().file_metadata().key_value_metadata()
    }

    /// Bytes the footer's read keeps in memory for as long as the file is open.
    pub fn footer_bytes(&self) -> u64 {
        self.footer
            .segments
            .iter()
            .map(|(_, b)| b.len() as u64)
            .sum()
    }

    /// The projected column chunks of row group `rg` that the footer's read did not
    /// already bring (a small file's footer read is the whole file).
    fn missing(&self, rg: usize, mask: &ProjectionMask) -> Vec<Range<u64>> {
        let group = self.meta.metadata().row_group(rg);
        (0..group.num_columns())
            .filter(|&leaf| mask.leaf_included(leaf))
            .map(|leaf| {
                let (start, len) = group.column(leaf).byte_range();
                start..start + len
            })
            .filter(|r| !self.footer.covers(r))
            .collect()
    }

    /// The bytes reading row group `rg` (projected to `mask`) will fetch.
    pub fn fetch_cost(&self, rg: usize, mask: &ProjectionMask) -> u64 {
        self.missing(rg, mask).iter().map(|r| r.end - r.start).sum()
    }

    /// Read row group `rg`, projected to `mask`, fetching only its projected column
    /// chunks (none the footer's read already holds). Rows come back in file order.
    pub fn row_group(&self, rg: usize, mask: &ProjectionMask) -> Result<ParquetRecordBatchReader> {
        let ranges = self.missing(rg, mask);
        let mut cache = self.footer.clone();
        if !ranges.is_empty() {
            let fetched = self.files.read_ranges(self.path, &ranges)?;
            cache
                .segments
                .extend(ranges.iter().map(|r| r.start).zip(fetched));
        }
        ParquetRecordBatchReaderBuilder::new_with_metadata(cache, self.meta.clone())
            .with_projection(mask.clone())
            .with_row_groups(vec![rg])
            .with_batch_size(8192)
            .build()
            .map_err(|e| err(format!("{}: {e}", self.path)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::atomic::AtomicU64;

    /// Local files that count the bytes and requests a read costs.
    #[derive(Default)]
    pub(crate) struct Counting<'a> {
        pub inner: Option<&'a LocalTableFiles>,
        pub requests: AtomicU64,
        pub sizes: AtomicU64,
        pub bytes: AtomicU64,
    }

    impl<'a> Counting<'a> {
        fn new(inner: &'a LocalTableFiles) -> Self {
            Counting {
                inner: Some(inner),
                ..Counting::default()
            }
        }
        fn inner(&self) -> &LocalTableFiles {
            self.inner.expect("a backing table")
        }
        fn get(n: &AtomicU64) -> u64 {
            n.load(Ordering::Relaxed)
        }
    }

    impl TableFiles for Counting<'_> {
        fn read(&self, path: &str) -> Result<Vec<u8>> {
            let b = self.inner().read(path)?;
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
            Ok(b)
        }
        fn size(&self, path: &str) -> Result<u64> {
            self.sizes.fetch_add(1, Ordering::Relaxed);
            self.inner().size(path)
        }
        fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            let b = self.inner().read_ranges(path, ranges)?;
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.bytes
                .fetch_add(b.iter().map(|b| b.len() as u64).sum(), Ordering::Relaxed);
            Ok(b)
        }
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ss-files-{}-{name}", std::process::id()))
    }

    #[test]
    fn a_codec_this_build_lacks_is_named_with_the_build_that_reads_it() {
        // What parquet says when a codec was left out at compile time.
        let zstd = "Parquet argument error: Parquet error: Disabled feature at compile time: zstd";
        let e = batch_err("s3://b/t/data/a.parquet", zstd).to_string();
        assert!(
            e.contains("s3://b/t/data/a.parquet is zstd-compressed")
                && e.contains("`--features zstd`"),
            "{e}"
        );
        let e = batch_err("a.parquet", "Disabled feature at compile time: brotli").to_string();
        assert!(e.contains("compressed with `brotli`"), "{e}");
        // Any other failure is passed on as it was, with the file named.
        let e = batch_err("a.parquet", "EOF: unexpected end").to_string();
        assert!(e.contains("a.parquet: EOF: unexpected end"), "{e}");
    }

    /// parquet's own words for a codec left out, as this build gets them: a parquet upgrade
    /// that rewords them fails here instead of quietly losing the explanation.
    #[cfg(not(feature = "zstd"))]
    #[test]
    fn parquets_own_error_for_a_missing_codec_is_explained() {
        use parquet::basic::{Compression, ZstdLevel};
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        // The writer meets the missing codec as the reader does, in `create_codec`, and
        // panics with its error rather than returning it; the message is what is pinned.
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let mut w = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
            w.write(&batch).and_then(|_| w.close().map(drop))
        }));
        let text = match failed {
            Ok(r) => format!("{:?}", r.expect_err("zstd is not in this build")),
            Err(panic) => panic.downcast_ref::<String>().cloned().unwrap_or_default(),
        };
        let e = batch_err("t/data/a.parquet", text).to_string();
        assert!(e.contains("is zstd-compressed Parquet"), "{e}");
    }

    #[test]
    fn a_projected_read_fetches_the_footer_and_only_the_projected_chunks() {
        // A narrow key column next to a wide payload, in three row groups.
        let path = tmp("ranged.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .build();
        let mut w = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        for g in 0..3i64 {
            let ids: Vec<i64> = (g * 1000..(g + 1) * 1000).collect();
            let payload: Vec<String> = ids.iter().map(|i| format!("{i:0>200}")).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(payload)),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
        let file_size = std::fs::metadata(&path).unwrap().len();

        let local = LocalTableFiles::new("/", Anchors::default());
        let files = Counting::new(&local);
        let p = path.to_string_lossy().into_owned();
        let pq = RangedParquet::open(&files, &p, None).unwrap();
        assert_eq!(pq.num_row_groups(), 3);
        let mask = ProjectionMask::roots(pq.parquet_schema(), [0]);
        let mut ids = Vec::new();
        for rg in 0..pq.num_row_groups() {
            for batch in pq.row_group(rg, &mask).unwrap() {
                let batch = batch.unwrap();
                assert_eq!(
                    batch.num_columns(),
                    1,
                    "only the projected column is decoded"
                );
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.extend(col.values().iter().copied());
            }
        }
        assert_eq!(
            ids,
            (0..3000).collect::<Vec<i64>>(),
            "every row, in file order"
        );
        assert_eq!(
            Counting::get(&files.requests),
            4,
            "one footer fetch, then one per row group"
        );
        assert_eq!(
            Counting::get(&files.sizes),
            1,
            "no size recorded: one lookup"
        );
        let fetched = Counting::get(&files.bytes);
        assert!(
            fetched * 4 < file_size,
            "a key-only read fetched {fetched} of {file_size} bytes"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn adjacent_projected_chunks_read_back_exactly() {
        // Two projected columns whose chunks sit back to back, in a file too big for the
        // footer prefetch to cover it, with several pages per chunk. A page header read
        // at the second chunk's first byte must come from that chunk — the first chunk's
        // segment also "covers" that offset, with nothing left in it.
        let path = tmp("adjacent.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(1000)
            .set_write_batch_size(1000)
            .build();
        let mut w = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        let n = 40_000i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|i| format!("/t/data/{}.parquet", i % 7))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..n).map(|i| i * 7919 % n).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        assert!(
            std::fs::metadata(&path).unwrap().len() > FOOTER_PREFETCH,
            "the file must outgrow the footer prefetch to exercise chunk segments"
        );

        let expected: Vec<RecordBatch> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap())
                .collect();
        let local = LocalTableFiles::new("/", Anchors::default());
        let p = path.to_string_lossy().into_owned();
        let pq = RangedParquet::open(&local, &p, None).unwrap();
        let mut got = Vec::new();
        for rg in 0..pq.num_row_groups() {
            for b in pq.row_group(rg, &ProjectionMask::all()).unwrap() {
                got.push(b.unwrap());
            }
        }
        let concat =
            |bs: &[RecordBatch]| arrow_select::concat::concat_batches(&bs[0].schema(), bs).unwrap();
        assert_eq!(concat(&got), concat(&expected));
        std::fs::remove_file(&path).ok();
    }

    /// A source that reads nothing but says how many readers it wants.
    struct Wants(usize);

    impl TableFiles for Wants {
        fn read(&self, _: &str) -> Result<Vec<u8>> {
            unreachable!()
        }
        fn size(&self, _: &str) -> Result<u64> {
            unreachable!()
        }
        fn read_ranges(&self, _: &str, _: &[Range<u64>]) -> Result<Vec<Bytes>> {
            unreachable!()
        }
        fn readers(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn read_each_keeps_item_order_and_its_reader_bound() {
        let files = Wants(4);
        let bound = readers_for(&files);
        let (now, max) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let items: Vec<u64> = (0..64).collect();
        let got = read_each(&files, &items, |&i| {
            let n = now.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(n, Ordering::SeqCst);
            // Later items finish first, so completion order is not item order.
            std::thread::sleep(std::time::Duration::from_micros(200 * (64 - i) % 3000));
            now.fetch_sub(1, Ordering::SeqCst);
            Ok(i * 10)
        })
        .unwrap();
        assert_eq!(got, items.iter().map(|i| i * 10).collect::<Vec<_>>());
        let max = max.load(Ordering::SeqCst);
        assert!(max <= bound, "{max} in flight, bound {bound}");
        if bound > 1 {
            assert!(max > 1, "the reads never overlapped");
        }
        assert!(read_each(&files, &[] as &[u64], |&i| Ok(i))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn read_each_returns_the_earliest_error_and_stops_starting_work() {
        let files = Wants(4);
        let calls = AtomicUsize::new(0);
        let items: Vec<u64> = (0..1000).collect();
        let e = read_each(&files, &items, |&i| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_micros(100));
            if i == 10 || i == 12 {
                Err(err(format!("item {i}")))
            } else {
                Ok(i)
            }
        })
        .unwrap_err();
        assert_eq!(e.to_string(), "sink error: item 10");
        let calls = calls.load(Ordering::SeqCst);
        assert!(calls < 100, "{calls} items ran after the failure");
    }

    #[test]
    fn a_recorded_size_skips_the_size_lookup_and_a_wrong_one_falls_back() {
        let path = tmp("sized.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut w =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None)
                .unwrap();
        w.write(
            &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..500))])
                .unwrap(),
        )
        .unwrap();
        w.close().unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        let local = LocalTableFiles::new("/", Anchors::default());
        let p = path.to_string_lossy().into_owned();
        let rows = |pq: &RangedParquet<Counting>| -> usize {
            let mask = ProjectionMask::all();
            (0..pq.num_row_groups())
                .flat_map(|rg| pq.row_group(rg, &mask).unwrap())
                .map(|b| b.unwrap().num_rows())
                .sum()
        };

        let files = Counting::new(&local);
        let pq = RangedParquet::open(&files, &p, Some(size)).unwrap();
        assert_eq!(rows(&pq), 500);
        assert_eq!(Counting::get(&files.sizes), 0, "the recorded size is used");
        assert!(size < FOOTER_PREFETCH);
        assert_eq!(
            Counting::get(&files.requests),
            1,
            "a file the footer's read holds whole is read once"
        );

        for wrong in [size + 100, size - 100, 13] {
            let files = Counting::new(&local);
            let pq = RangedParquet::open(&files, &p, Some(wrong)).unwrap();
            assert_eq!(rows(&pq), 500, "recorded {wrong}, actual {size}");
            assert_eq!(
                Counting::get(&files.sizes),
                1,
                "one lookup when it is wrong"
            );
        }

        // Not a Parquet file at its true size: the original error, one lookup to be sure.
        let junk = tmp("junk.parquet");
        std::fs::write(&junk, vec![7u8; 4096]).unwrap();
        let files = Counting::new(&local);
        let j = junk.to_string_lossy().into_owned();
        let e = RangedParquet::open(&files, &j, Some(4096)).err().unwrap();
        assert!(e.to_string().contains("no PAR1 footer"), "{e}");
        assert_eq!(Counting::get(&files.sizes), 1);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&junk).ok();
    }

    #[test]
    fn a_read_starts_in_the_segment_that_holds_its_first_byte() {
        // Back-to-back segments: offset 10 is the end of the first and the start of the
        // second. The reader must come from the second, not be an empty tail of the first.
        let cache = RangeCache {
            len: 20,
            segments: vec![
                (0, Bytes::from_static(b"AAAAAAAAAA")),
                (10, Bytes::from_static(b"BBBBBBBBBB")),
            ],
        };
        let mut first = [0u8; 3];
        cache.get_read(10).unwrap().read_exact(&mut first).unwrap();
        assert_eq!(&first, b"BBB");
        assert_eq!(&cache.get_bytes(10, 4).unwrap()[..], b"BBBB");
        assert_eq!(&cache.get_bytes(6, 4).unwrap()[..], b"AAAA");
    }

    #[test]
    fn a_range_that_was_not_fetched_is_an_error_not_a_guess() {
        let cache = RangeCache {
            len: 100,
            segments: vec![(10, Bytes::from_static(b"0123456789"))],
        };
        assert_eq!(&cache.get_bytes(12, 3).unwrap()[..], b"234");
        assert!(cache.get_bytes(18, 5).is_err(), "runs past the segment");
        assert!(cache.get_read(50).is_err());
    }

    /// Local files read `readers` at a time, each request taking a moment (longer for some
    /// files, so reads finish out of order), counting the most in flight. Until two reads have
    /// overlapped once, a read waits up to 20 ms for company, so whether they can overlap does
    /// not hang on the scheduler. A path containing `panic` panics its reader.
    struct Slow<'a> {
        inner: &'a LocalTableFiles,
        readers: usize,
        now: AtomicUsize,
        max: AtomicUsize,
    }

    impl<'a> Slow<'a> {
        fn new(inner: &'a LocalTableFiles, readers: usize) -> Self {
            Slow {
                inner,
                readers,
                now: AtomicUsize::new(0),
                max: AtomicUsize::new(0),
            }
        }
    }

    impl TableFiles for Slow<'_> {
        fn read(&self, path: &str) -> Result<Vec<u8>> {
            self.inner.read(path)
        }
        fn size(&self, path: &str) -> Result<u64> {
            self.inner.size(path)
        }
        fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            assert!(!path.contains("panic"), "a reader panics");
            let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(n, Ordering::SeqCst);
            let until = std::time::Instant::now() + std::time::Duration::from_millis(20);
            while self.max.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < until {
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            let ms = 1 + path.len() as u64 % 3;
            std::thread::sleep(std::time::Duration::from_millis(ms));
            let got = self.inner.read_ranges(path, ranges);
            self.now.fetch_sub(1, Ordering::SeqCst);
            got
        }
        fn readers(&self) -> usize {
            self.readers
        }
    }

    /// Parquet files of `rows[i]` rows each, in row groups of `group` rows; file `i`'s ids
    /// start at `i * 1_000_000`. A file of 0 rows has no row group.
    fn parquet_files(name: &str, rows: &[usize], group: usize) -> Vec<String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("pad", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_size(group)
            .build();
        rows.iter()
            .enumerate()
            .map(|(i, &n)| {
                let path = tmp(&format!("{name}-{i}.parquet"));
                let file = std::fs::File::create(&path).unwrap();
                let mut w =
                    ArrowWriter::try_new(file, schema.clone(), Some(props.clone())).unwrap();
                let base = i as i64 * 1_000_000;
                if n > 0 {
                    let ids = Int64Array::from_iter_values(base..base + n as i64);
                    let pad = StringArray::from_iter_values((0..n).map(|k| format!("{k:032}")));
                    let batch =
                        RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(pad)])
                            .unwrap();
                    w.write(&batch).unwrap();
                }
                w.close().unwrap();
                path.to_string_lossy().into_owned()
            })
            .collect()
    }

    /// Every file's ids, grouped by row group, as `read_ahead` hands them over.
    type Seen = Vec<(usize, Vec<Vec<i64>>)>;

    /// `f` on a thread of its own: its result, or its panic. A run that has not finished in
    /// 30 s fails the test rather than hanging it (a stalled `read_ahead` never returns).
    fn within<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> std::thread::Result<T> {
        let (done, finished) = std::sync::mpsc::channel::<()>();
        let run = std::thread::spawn(move || {
            let _done = done;
            f()
        });
        let waited = finished.recv_timeout(std::time::Duration::from_secs(30));
        if waited == Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
            panic!("read_ahead stalled");
        }
        run.join()
    }

    /// `read_all` with `readers` readers, on a thread of its own: what was handed over, the
    /// most reads in flight, and how many readers ran (`SHAPESHIFT_READERS` can say otherwise).
    fn read_with(readers: usize, paths: &[String], budget: u64) -> (Seen, usize, usize) {
        let paths = paths.to_vec();
        within(move || {
            let local = LocalTableFiles::new("/", Anchors::default());
            let files = Slow::new(&local, readers);
            let seen = read_all(&files, &paths, budget).unwrap();
            (seen, files.max.load(Ordering::SeqCst), readers_for(&files))
        })
        .unwrap()
    }

    fn read_all(files: &Slow, paths: &[String], budget: u64) -> Result<Seen> {
        let inputs: Vec<(&str, Option<u64>)> = paths
            .iter()
            .map(|p| (p.as_str(), std::fs::metadata(p).ok().map(|m| m.len())))
            .collect();
        let mut seen = Seen::new();
        read_ahead(files, &inputs, &ProjectionMask::all(), budget, |i, file| {
            assert_eq!(file.schema().field(0).name(), "id");
            let mut groups = Vec::new();
            while let Some(group) = file.next_group()? {
                let mut ids = Vec::new();
                for b in group {
                    let b = b.map_err(err)?;
                    let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    ids.extend(col.values().iter().copied());
                }
                groups.push(ids);
            }
            seen.push((i, groups));
            Ok(())
        })?;
        Ok(seen)
    }

    #[test]
    fn read_ahead_hands_files_over_in_order_however_many_read_them() {
        // Small files the footer's read holds whole, big ones read a row group at a time,
        // and one with no row group at all.
        let rows = [3000, 10, 0, 5000, 1, 2500, 40, 4000, 7, 3000, 0, 20];
        let paths = parquet_files("ahead", &rows, 1000);
        assert!(std::fs::metadata(&paths[0]).unwrap().len() > FOOTER_PREFETCH);
        let (one, ..) = read_with(1, &paths, READ_AHEAD_BYTES);
        assert_eq!(one.len(), rows.len());
        for (i, groups) in &one {
            assert_eq!(groups.len(), rows[*i].div_ceil(1000), "file {i}");
            let ids: Vec<i64> = groups.iter().flatten().copied().collect();
            let base = *i as i64 * 1_000_000;
            assert_eq!(ids, (base..base + rows[*i] as i64).collect::<Vec<_>>());
        }
        for readers in [2, 3, 8, 64] {
            let (seen, max, ran) = read_with(readers, &paths, READ_AHEAD_BYTES);
            assert_eq!(seen, one, "{readers} readers");
            assert!(max <= ran, "{max} reads in flight, {ran} readers");
            if ran > 1 {
                assert!(max > 1, "{ran} readers, but the reads never overlapped");
            }
        }
        // A budget smaller than any read: only what the caller waits for is let through
        // past it, and still every file comes, in order.
        assert_eq!(read_with(8, &paths, 1).0, one);
        for p in &paths {
            std::fs::remove_file(p).ok();
        }
    }

    #[test]
    fn read_ahead_stops_at_a_failed_read_after_handing_over_what_came_before() {
        let mut paths = parquet_files("ahead-fail", &[2000, 10, 3000, 5, 4000, 6, 7, 8], 1000);
        let junk = tmp("ahead-fail-junk.parquet");
        std::fs::write(&junk, vec![7u8; 100_000]).unwrap();
        paths[5] = junk.to_string_lossy().into_owned();
        let run = |readers: usize, give_up_at: Option<usize>| {
            let paths = paths.clone();
            within(move || {
                let local = LocalTableFiles::new("/", Anchors::default());
                let inputs: Vec<(&str, Option<u64>)> =
                    paths.iter().map(|p| (p.as_str(), None)).collect();
                let mut seen = Vec::new();
                let e = read_ahead(
                    &Slow::new(&local, readers),
                    &inputs,
                    &ProjectionMask::all(),
                    1 << 20,
                    |i, file| {
                        if Some(i) == give_up_at {
                            return Err(err("the caller gives up"));
                        }
                        while file.next_group()?.is_some() {}
                        seen.push(i);
                        Ok(())
                    },
                )
                .unwrap_err();
                (e.to_string(), seen)
            })
            .unwrap()
        };
        for readers in [1, 4] {
            let (e, seen) = run(readers, None);
            assert!(e.contains("ahead-fail-junk"), "{e}");
            assert_eq!(seen, vec![0, 1, 2, 3, 4], "{readers} readers");
            // The caller failing stops the readers too.
            let (e, seen) = run(readers, Some(2));
            assert_eq!(e, "sink error: the caller gives up");
            assert_eq!(seen, vec![0, 1]);
        }
        for p in &paths {
            std::fs::remove_file(p).ok();
        }
    }

    #[test]
    fn a_caller_that_panics_stops_the_readers_instead_of_hanging() {
        // A budget of one byte: readers soon wait on it, and only the caller's next read
        // would let them go.
        let paths = parquet_files("ahead-cp", &[2000, 3000, 4000, 5000, 1000, 2000], 1000);
        let all = paths.clone();
        let outcome = within(move || {
            let local = LocalTableFiles::new("/", Anchors::default());
            let inputs: Vec<(&str, Option<u64>)> =
                paths.iter().map(|p| (p.as_str(), None)).collect();
            read_ahead(
                &Slow::new(&local, 4),
                &inputs,
                &ProjectionMask::all(),
                1,
                |i, _| {
                    assert!(i < 2, "the caller panics");
                    Ok(())
                },
            )
        });
        assert!(outcome.is_err(), "the caller's panic propagates");
        for p in &all {
            std::fs::remove_file(p).ok();
        }
    }

    #[test]
    fn a_footer_longer_than_its_first_read_is_charged_in_full() {
        // Thousands of one-row row groups: a footer well past the first 64 KiB read. Writing
        // them takes more stack than a test thread has (in a debug build); opening does not.
        let path = std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(|| parquet_files("ahead-wide", &[3000], 1).pop().unwrap())
            .unwrap()
            .join()
            .unwrap();
        let local = LocalTableFiles::new("/", Anchors::default());
        let pq = RangedParquet::open(&local, &path, None).unwrap();
        assert_eq!(pq.num_row_groups(), 3000);
        let kept = pq.footer_bytes();
        assert!(kept > FOOTER_PREFETCH, "{kept} bytes of footer");
        // Admitted at the first read's size, it is charged what it keeps until taken.
        let ahead = Ahead {
            state: Mutex::new(AheadState::default()),
            changed: Condvar::new(),
            budget: READ_AHEAD_BYTES,
        };
        assert!(ahead.admit((0, 0), FOOTER_PREFETCH));
        let opened = Ok(Got::Opened(pq.schema().clone(), 3000));
        ahead.hand((0, 0), opened, FOOTER_PREFETCH, kept);
        assert_eq!(ahead.lock().held, kept);
        assert!(ahead.take((0, 0)).is_ok());
        assert_eq!(ahead.lock().held, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_reader_that_panics_panics_the_caller_instead_of_leaving_it_waiting() {
        let mut paths = parquet_files("ahead-p", &[2000, 3000, 4000], 1000);
        let bad = tmp("ahead-p-panic.parquet");
        std::fs::copy(&paths[2], &bad).unwrap();
        paths[1] = bad.to_string_lossy().into_owned();
        let all = paths.clone();
        let outcome = within(move || {
            let local = LocalTableFiles::new("/", Anchors::default());
            read_all(&Slow::new(&local, 4), &paths, READ_AHEAD_BYTES)
        });
        assert!(outcome.is_err(), "the reader's panic reaches the caller");
        for p in &all {
            std::fs::remove_file(p).ok();
        }
    }
}
