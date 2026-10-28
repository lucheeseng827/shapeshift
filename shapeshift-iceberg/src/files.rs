//! Reading a table's existing files, wherever the table lives.
//!
//! A merge reads three kinds of existing file: manifests and position-delete files
//! (small, read whole), and the **key columns** of data files (potentially large).
//! [`TableFiles`] is the seam. A local table answers from disk, and an object-store table
//! answers with GETs and ranged GETs.
//!
//! Parquet is read through a [`RangeCache`] that already holds exactly the bytes the read
//! will touch: the footer first, then **one row group's projected column chunks at a
//! time**. Against an object store that is one size lookup, one footer GET, and one
//! (coalesced) ranged GET per row group. Parquet's own reader would issue a request per
//! page header. Memory stays bounded to one row group's key chunks, never the file.
//! Local tables take the same path, so the code an object store runs is the code the
//! local tests exercise.

use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

use crate::reanchor;

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// Read access to a table's existing files. Every `path` is a file's path exactly as the
/// table's metadata records it (usually absolute: `/warehouse/t/data/…`,
/// `s3://bucket/t/data/…`). The implementation resolves it against where the table lives
/// now.
pub trait TableFiles {
    /// A whole file. Meant for small ones: manifests, position-delete files.
    fn read(&self, path: &str) -> Result<Vec<u8>>;
    /// A file's size in bytes.
    fn size(&self, path: &str) -> Result<u64>;
    /// Byte ranges of one file, returned in the order asked. An implementation may
    /// coalesce nearby ranges into one request.
    fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>>;
}

/// A local table's files. Paths recorded against an older `location` (a moved or copied
/// table) are re-anchored to the table's current directory.
pub struct LocalTableFiles {
    dir: PathBuf,
    location: String,
}

impl LocalTableFiles {
    /// `dir` is where the table is now; `location` is the `location` its metadata
    /// records (what its file paths were written against).
    pub fn new(dir: impl AsRef<Path>, location: impl Into<String>) -> Self {
        LocalTableFiles {
            dir: dir.as_ref().to_path_buf(),
            location: location.into(),
        }
    }

    /// The local path a recorded file path resolves to.
    pub fn resolve(&self, path: &str) -> PathBuf {
        reanchor(&self.dir, &self.location, path)
    }
}

impl TableFiles for LocalTableFiles {
    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let p = self.resolve(path);
        std::fs::read(&p).map_err(|e| err(format!("{}: {e}", p.display())))
    }

    fn size(&self, path: &str) -> Result<u64> {
        let p = self.resolve(path);
        Ok(std::fs::metadata(&p)
            .map_err(|e| err(format!("{}: {e}", p.display())))?
            .len())
    }

    fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let p = self.resolve(path);
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
    /// Fetch and parse the footer.
    pub fn open(files: &'a F, path: &'a str) -> Result<Self> {
        let bad = |what: &str| err(format!("{path}: {what}"));
        let size = files.size(path)?;
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

    /// Read row group `rg`, projected to `mask`, fetching only its projected column
    /// chunks. Rows come back in file order.
    pub fn row_group(&self, rg: usize, mask: &ProjectionMask) -> Result<ParquetRecordBatchReader> {
        let group = self.meta.metadata().row_group(rg);
        let ranges: Vec<Range<u64>> = (0..group.num_columns())
            .filter(|&leaf| mask.leaf_included(leaf))
            .map(|leaf| {
                let (start, len) = group.column(leaf).byte_range();
                start..start + len
            })
            .collect();
        let mut cache = self.footer.clone();
        let fetched = self.files.read_ranges(self.path, &ranges)?;
        cache
            .segments
            .extend(ranges.iter().map(|r| r.start).zip(fetched));
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
    use std::cell::RefCell;

    /// Local files that count the bytes and requests a read costs.
    pub(crate) struct Counting<'a> {
        pub inner: &'a LocalTableFiles,
        pub requests: RefCell<u64>,
        pub bytes: RefCell<u64>,
    }

    impl TableFiles for Counting<'_> {
        fn read(&self, path: &str) -> Result<Vec<u8>> {
            let b = self.inner.read(path)?;
            *self.requests.borrow_mut() += 1;
            *self.bytes.borrow_mut() += b.len() as u64;
            Ok(b)
        }
        fn size(&self, path: &str) -> Result<u64> {
            self.inner.size(path)
        }
        fn read_ranges(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            let b = self.inner.read_ranges(path, ranges)?;
            *self.requests.borrow_mut() += 1;
            *self.bytes.borrow_mut() += b.iter().map(|b| b.len() as u64).sum::<u64>();
            Ok(b)
        }
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ss-files-{}-{name}", std::process::id()))
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

        let local = LocalTableFiles::new("/", "");
        let files = Counting {
            inner: &local,
            requests: RefCell::new(0),
            bytes: RefCell::new(0),
        };
        let p = path.to_string_lossy().into_owned();
        let pq = RangedParquet::open(&files, &p).unwrap();
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
            *files.requests.borrow(),
            4,
            "one footer fetch, then one per row group"
        );
        let fetched = *files.bytes.borrow();
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
        let local = LocalTableFiles::new("/", "");
        let p = path.to_string_lossy().into_owned();
        let pq = RangedParquet::open(&local, &p).unwrap();
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
}
