//! Boundary between Raft and the application state machine.

use std::collections::HashMap;

use crate::log::raft_log::LogEntry;

/// Applied by the node actor in commit order, so implementations do not need
/// to worry about concurrency or reordering.
pub trait StateMachine: Send {
    /// Apply one committed, non-empty entry. `index` is the log index it was
    /// committed at; implementations may ignore it.
    fn apply(&mut self, index: u64, entry: &LogEntry);

    /// Read-only query. Request/response encoding belongs to the
    /// implementation; Raft only forwards bytes.
    fn query(&self, request: &[u8]) -> Vec<u8>;
}

/// Minimal key-value state machine used by tests and examples.
///
/// Entries are encoded as `key=value`; entries without `=` are ignored.
/// Queries are keys and return the stored value (empty if absent).
#[derive(Default)]
pub struct KvStateMachine {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl StateMachine for KvStateMachine {
    fn apply(&mut self, _index: u64, entry: &LogEntry) {
        let Some(pos) = entry.data.iter().position(|&b| b == b'=') else {
            return;
        };
        let key = &entry.data[..pos];
        let value = &entry.data[pos + 1..];
        self.data.insert(key.to_vec(), value.to_vec());
    }

    fn query(&self, request: &[u8]) -> Vec<u8> {
        self.data.get(request).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_put_and_get() {
        let mut sm = KvStateMachine::default();

        sm.apply(1, &LogEntry::normal(1, 1, b"k=v".to_vec()));
        assert_eq!(sm.query(b"k"), b"v");
        assert!(sm.query(b"missing").is_empty());

        sm.apply(2, &LogEntry::normal(2, 1, b"no equals sign".to_vec()));
        assert!(sm.query(b"no equals sign").is_empty());
    }
}
