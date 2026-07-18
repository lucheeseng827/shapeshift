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
//! Both drive the [simd-json](https://docs.rs/simd-json) SIMD lexer via its serde
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

mod error;
mod source;

pub use error::JsonError;
pub use source::{open_reader, JsonArrayReader, JsonlReader, RecordSource};
