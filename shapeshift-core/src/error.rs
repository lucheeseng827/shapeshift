//! The one error type the engine surfaces.

use thiserror::Error;

/// Every fallible engine operation returns this.
#[derive(Debug, Error)]
pub enum ShapeError {
    /// The dataset spec could not be parsed or is internally inconsistent.
    #[error("spec error: {0}")]
    Spec(String),

    /// A declared column referenced a type / transform combination the engine
    /// cannot honor.
    #[error("schema error: {0}")]
    Schema(String),

    /// A record could not be shaped under a strict policy (the offending path +
    /// reason). Under the default lenient policy these become row rejections, not
    /// errors.
    #[error("record error: {0}")]
    Record(String),

    /// An Arrow builder / RecordBatch assembly failure.
    #[error("arrow error: {0}")]
    Arrow(String),

    /// A sink (Parquet / Iceberg) I/O or encode failure.
    #[error("sink error: {0}")]
    Sink(String),

    /// A source (JSON reader) failure bubbled up through a boxed dyn error.
    #[error("source error: {0}")]
    Source(String),
}

impl From<arrow_schema::ArrowError> for ShapeError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        ShapeError::Arrow(e.to_string())
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, ShapeError>;
