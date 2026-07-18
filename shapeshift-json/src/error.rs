use std::fmt;

/// A source read/parse failure. Parse errors carry the 1-based line number so the
/// CLI can sidecar the offending line for repair + re-ingest.
#[derive(Debug)]
pub enum JsonError {
    /// Underlying I/O error.
    Io(std::io::Error),
    /// A record failed to parse. `line` is 1-based (0 for array mode). `raw` is the
    /// offending text (lossy UTF-8), preserved so the CLI can sidecar it for repair.
    Parse {
        line: u64,
        message: String,
        raw: String,
    },
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonError::Io(e) => write!(f, "io: {e}"),
            JsonError::Parse { line, message, .. } => {
                write!(f, "parse error at line {line}: {message}")
            }
        }
    }
}

impl std::error::Error for JsonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JsonError::Io(e) => Some(e),
            JsonError::Parse { .. } => None,
        }
    }
}

impl From<std::io::Error> for JsonError {
    fn from(e: std::io::Error) -> Self {
        JsonError::Io(e)
    }
}
