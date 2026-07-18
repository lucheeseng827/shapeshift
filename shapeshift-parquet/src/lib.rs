//! # shapeshift-parquet — the Parquet sink
//!
//! Streams Arrow RecordBatches to one Parquet file via `parquet::arrow::ArrowWriter`,
//! one row group per batch, so a 100M-row shape never holds more than one row group
//! in RAM. Snappy by default (pure Rust) keeps the build musl-static — the arrow/
//! parquet stack is compiled with `default-features = false, features = ["arrow",
//! "snap"]`, so the zstd/brotli C codecs are never linked.
//!
//! Also exposes [`inspect`], a read-back that reports schema + row count — used by
//! `shapeshift inspect` and as a self-contained round-trip check.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::file::properties::WriterProperties;

use shapeshift_core::{Compression, Result, ShapeError, Sink, SinkSummary};

fn sink_err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// Map the spec's compression choice onto the codecs actually compiled in. Zstd is
/// not linked by default (it would pull a C codec and break the musl-static build),
/// so without the `zstd` feature it is a clear error rather than a silent surprise;
/// a `--features zstd` "fat build" links the codec and makes it work.
pub(crate) fn pq_compression(c: Compression) -> Result<PqCompression> {
    match c {
        Compression::Snappy => Ok(PqCompression::SNAPPY),
        Compression::Uncompressed => Ok(PqCompression::UNCOMPRESSED),
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

/// Build `WriterProperties` for a compression choice.
pub(crate) fn writer_props(c: Compression) -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(pq_compression(c)?)
        .build())
}

/// A write-once Parquet file sink over a fixed Arrow schema.
pub struct ParquetSink {
    writer: Option<ArrowWriter<File>>,
    path: PathBuf,
    rows: u64,
}

impl ParquetSink {
    /// Create a Parquet file at `path` for `schema`. Parent directories are created.
    pub fn create(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        compression: Compression,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Validate the codec BEFORE touching the filesystem: `File::create` truncates,
        // and a rejected compression choice (zstd on the lean build) must not clobber
        // an existing file at the target path.
        let props = writer_props(compression)?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(sink_err)?;
            }
        }
        let file = File::create(&path).map_err(sink_err)?;
        let writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(sink_err)?;
        Ok(ParquetSink {
            writer: Some(writer),
            path,
            rows: 0,
        })
    }
}

impl Sink for ParquetSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| ShapeError::Sink("write after finish".into()))?;
        w.write(batch).map_err(sink_err)?;
        // Close this batch's row group now, so the writer never buffers more than
        // one batch — bounding RAM regardless of input size.
        w.flush().map_err(sink_err)?;
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    fn finish(&mut self) -> Result<SinkSummary> {
        let w = self
            .writer
            .take()
            .ok_or_else(|| ShapeError::Sink("finish called twice".into()))?;
        w.close().map_err(sink_err)?;
        let bytes = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        Ok(SinkSummary {
            files: vec![self.path.clone()],
            rows: self.rows,
            bytes,
        })
    }
}

/// What a Parquet file contains, read back from its footer.
#[derive(Debug, Clone)]
pub struct ParquetInfo {
    pub schema: SchemaRef,
    pub rows: i64,
    pub row_groups: usize,
    /// `(column name, Arrow type string)` in schema order.
    pub columns: Vec<(String, String)>,
}

/// Read a Parquet file's schema + row/row-group counts without scanning the data.
pub fn inspect(path: impl AsRef<Path>) -> Result<ParquetInfo> {
    let file = File::open(path.as_ref()).map_err(sink_err)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(sink_err)?;
    let schema = builder.schema().clone();
    let meta = builder.metadata();
    let rows = meta.file_metadata().num_rows();
    let row_groups = meta.num_row_groups();
    let columns = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), format!("{:?}", f.data_type())))
        .collect();
    Ok(ParquetInfo {
        schema,
        rows,
        row_groups,
        columns,
    })
}
