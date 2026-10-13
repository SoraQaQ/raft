use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendError {
    LogMismatch {
        prev_log_index: u64,
        prev_log_term: u64,
    },

    NonContiguous {
        offset: u64,
        expected: u64,
        got: u64,
    },

    OverwriteCommitted {
        index: u64,
        committed: u64,
    },
}

impl Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::LogMismatch {
                prev_log_index,
                prev_log_term,
            } => write!(
                f,
                "prev log entry mismatch: index {prev_log_index}, term {prev_log_term}"
            ),
            AppendError::NonContiguous {
                offset,
                expected,
                got,
            } => write!(
                f,
                "non-contiguous entries at offset {offset}: expected at {expected}, got {got}"
            ),
            AppendError::OverwriteCommitted { index, committed } => write!(
                f,
                "refusing to overwrite committed entry at index {index}(committed {committed})"
            ),
        }
    }
}

impl std::error::Error for AppendError {}
