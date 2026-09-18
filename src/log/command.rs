use tokio::sync::oneshot;

use crate::log::logs::LogEntry;

pub enum LogCommand {
    /// raft core: append / overwrite logs
    /// return true indicates that the consistency check has been passed.
    Append {
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        reply: oneshot::Sender<bool>,
    },

    /// delete from (and including) the index from_index.
    /// return the number of delete items.
    TruncateFrom {
        from_index: u64,
        reply: oneshot::Sender<usize>,
    },

    /// Retain up to and including the upto_index. and delete the rest.
    /// return the number of deleted items.
    TruncateSuffix {
        upto_index: u64,
        reply: oneshot::Sender<usize>,
    },

    /// get last entry index / term
    LastInfo { reply: oneshot::Sender<(u64, u64)> },

    /// get log by index
    Get {
        index: u64,
        reply: oneshot::Sender<Option<LogEntry>>,
    },

    /// get log slice by [from, to]
    Slice {
        from: u64,
        to: u64,
        reply: oneshot::Sender<Vec<LogEntry>>,
    },

    /// Consistency check (without modification)
    Matches {
        prev_log_index: u64,
        prev_log_term: u64,
        reply: oneshot::Sender<bool>,
    },
}
