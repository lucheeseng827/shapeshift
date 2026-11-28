//! # shapeshift-json — the streaming JSON source
//!
//! Bounded-RAM readers that yield `serde_json::Value` records for the shaper:
//!
//! - [`JsonlReader`] — one JSON object per line (the streaming path). A line at a
//!   time enters memory; a 100M-line file never materializes.
//! - [`JsonArrayReader`] — a single top-level JSON array, **streamed**: a small
//!   depth- and string-aware scanner finds element boundaries, so one element at
//!   a time enters memory — a multi-GB array never materializes either.
//!
//! - [`CsvReader`] and [`XmlReader`] — CSV/TSV rows and XML elements as records, streamed
//!   the same way; [`open_source`] opens any format as a spec's `source` describes it, and
//!   [`OwnedTape`] puts their records on a tape for the shape loop.
//!
//! The JSON readers drive the [simd-json](https://docs.rs/simd-json) SIMD lexer via its serde
//! bridge, landing straight in the ubiquitous `serde_json::Value` so everything
//! downstream stays on one value model.
//!
//! ```
//! use std::io::Cursor;
//! use shapeshift_json::JsonlReader;
//!
//! let data = "{\"id\":1}\n\n{\"id\":2}\n";
//! let rows: Vec<_> = JsonlReader::new(Cursor::new(data))
//!     .collect::<Result<_, _>>()
//!     .unwrap();
//! assert_eq!(rows.len(), 2); // the blank line is skipped
//! assert_eq!(rows[0]["id"], 1);
//! ```

mod cap;
mod csv;
mod error;
mod source;
mod tape;
mod xml;

pub use error::JsonError;
pub use source::{
    is_rereadable, is_stdin, open_reader, open_source, JsonArrayReader, JsonlReader,
    RecordSource,
};
pub use tape::{
    OwnedTape, TapeJsonArrayReader, TapeJsonlReader, TapeRecord, TapeSource,
};
pub use crate::csv::CsvReader;
pub use crate::xml::XmlReader;
