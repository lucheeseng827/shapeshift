//! A byte ceiling on one record, for the readers whose records are not lines.
//!
//! The JSON readers refuse a record past [`crate::source::DEFAULT_MAX_LINE`] before they
//! buffer it. CSV and XML parsers buffer inside their own crates (a quoted field that never
//! closes, one enormous text node), so the ceiling sits under them instead: the input is
//! read through [`Capped`], which counts what has been taken, and the reader moves the mark
//! forward at every record boundary. Past the ceiling since the last mark, the next read
//! fails, and the run with it, instead of the process growing until it is killed.

use std::io::{self, BufRead, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The input, counted. `mark` is the byte offset of the current record's start, set by
/// the reader that owns the parser.
pub(crate) struct Capped<R> {
    inner: R,
    consumed: u64,
    mark: Arc<AtomicU64>,
    limit: u64,
    what: &'static str,
}

impl<R> Capped<R> {
    /// `inner`, failing once more than `limit` bytes pass without the mark moving. `what`
    /// names a record in the error ("a CSV row", "an XML record").
    pub(crate) fn new(inner: R, limit: u64, what: &'static str) -> (Self, Arc<AtomicU64>) {
        let mark = Arc::new(AtomicU64::new(0));
        (
            Capped {
                inner,
                consumed: 0,
                mark: mark.clone(),
                limit,
                what,
            },
            mark,
        )
    }

    fn check(&self) -> io::Result<()> {
        let since = self
            .consumed
            .saturating_sub(self.mark.load(Ordering::Relaxed));
        if since > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} runs past {} bytes, the most one record may have (an unclosed quote, or a \
                     record path that matches too much?)",
                    self.what, self.limit
                ),
            ));
        }
        Ok(())
    }
}

impl<R: Read> Read for Capped<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.check()?;
        let n = self.inner.read(buf)?;
        self.consumed += n as u64;
        Ok(n)
    }
}

impl<R: BufRead> BufRead for Capped<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.check()?;
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.consumed += amt as u64;
        self.inner.consume(amt);
    }
}
