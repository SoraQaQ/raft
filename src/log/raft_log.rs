use serde::{Deserialize, Serialize};

use crate::error::append_error::AppendError;

// ============================================================
// LogEntry
// ============================================================
#[derive(Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Clone)]
pub enum EntryType {
    /// normal log, append data
    Normal,
    /// empty log, heartbeat
    Empty,
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub entry_type: EntryType,
}

impl LogEntry {
    pub fn normal(index: u64, term: u64, data: Vec<u8>) -> Self {
        Self {
            index,
            term,
            data,
            entry_type: EntryType::Normal,
        }
    }

    pub fn empty(index: u64, term: u64) -> Self {
        Self {
            index,
            term,
            data: Vec::new(),
            entry_type: EntryType::Empty,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entry_type == EntryType::Empty
    }
}

// ============================================================
// RaftLog
// ============================================================

#[derive(Debug, Default, Clone)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
    offset: u64,
}

pub trait Log {
    fn new() -> Self;

    fn last_index(&self) -> u64;

    /// return last log term, if empty log return 0
    fn last_term(&self) -> u64;

    fn is_empty(&self) -> bool;

    fn len(&self) -> usize;

    /// retrieve logs using the index
    fn get(&self, index: u64) -> Option<&LogEntry>;

    /// term at index: index return None if the index out of bounds
    /// the sentinel at index 0 return 0
    /// (used for the prev_log_term check)
    fn term_at(&self, index: u64) -> Option<u64>;

    /// returns the closed interval `[from, to]`
    /// returns empty if `from > to` or if the range is out of bounds
    fn slice(&self, from: u64, to: u64) -> Vec<LogEntry>;

    /// Consistency check whether `prev_log_index` / `prev_log_term` match
    /// `prev_log_index == 0` always matches (sentinel)
    /// otherwise, the index must match exist locally and ther term must match
    fn matches(&self, prev_log_index: u64, prev_log_term: u64) -> bool;

    ///Deletes all logs starting from `from_index` (inclusive)
    /// Returns the number of deleted entries
    ///
    /// `from_index < offset` no-op
    /// `from_index` > last_index: no-op
    fn truncate_from(&mut self, from_index: u64) -> usize;

    /// delete single logs(by index)
    fn remove(&mut self, index: u64) -> Option<LogEntry>;

    /// append/overwrite log entries
    ///
    /// steps:
    /// 1. consistency check (`prev_log_index / prev_log_term`); return Err and leave state unchanged if it fails
    ///
    /// 2. find the fist point if conflict(local entry has the same index but a different term) and truncate starting from that point
    ///
    /// 3. append entries not present locally(idempotent)
    ///
    /// 4. check committed index, if truncate idx <= committed return Err
    ///
    /// return `Ok` to indicate a successful write
    fn append(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        committed: u64,
    ) -> Result<(), AppendError>;
}

impl Log for RaftLog {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            offset: 1,
        }
    }

    fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.offset - 1)
    }

    fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn get(&self, index: u64) -> Option<&LogEntry> {
        if index < self.offset {
            return None;
        }
        self.entries.get((index - self.offset) as usize)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.get(index).map(|e| e.term)
    }

    fn slice(&self, from: u64, to: u64) -> Vec<LogEntry> {
        if from > to || from < self.offset {
            return Vec::new();
        }

        let start = (from - self.offset) as usize;
        if start >= self.entries.len() {
            return Vec::new();
        }

        let end = ((to - self.offset) as usize + 1).min(self.entries.len());
        self.entries[start..end].to_vec()
    }

    fn matches(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true;
        }

        matches!(self.get(prev_log_index), Some(e) if e.term == prev_log_term)
    }

    fn truncate_from(&mut self, from_index: u64) -> usize {
        if from_index < self.offset {
            return 0;
        }

        if from_index > self.last_index() {
            return 0;
        }
        let pos = (from_index - self.offset) as usize;
        let removed = self.entries.len() - pos;
        self.entries.truncate(pos);
        removed
    }

    fn remove(&mut self, index: u64) -> Option<LogEntry> {
        if index < self.offset || index > self.last_index() {
            return None;
        }
        Some(self.entries.remove((index - self.offset) as usize))
    }

    fn append(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        committed: u64,
    ) -> Result<(), AppendError> {
        if !self.matches(prev_log_index, prev_log_term) {
            return Err(AppendError::LogMismatch {
                prev_log_index,
                prev_log_term,
            });
        }

        if let Some((offset, entry)) = entries
            .iter()
            .enumerate()
            .find(|(i, x)| x.index != prev_log_index + 1 + *i as u64)
        {
            return Err(AppendError::NonContiguous {
                offset: offset as u64,
                expected: prev_log_index + 1 + offset as u64,
                got: entry.index,
            });
        }

        // 1. find the first point of conflict
        let mut conflict_at: Option<u64> = None;
        for entry in &entries {
            match self.get(entry.index) {
                Some(local) if local.term != entry.term => {
                    conflict_at = Some(entry.index);
                    break;
                }
                Some(_) => {}
                None => break,
            }
        }

        if let Some(idx) = conflict_at {
            if committed >= idx {
                return Err(AppendError::OverwriteCommitted {
                    index: idx,
                    committed,
                });
            }
            self.truncate_from(idx);
        }

        for entry in entries {
            if entry.index > self.last_index() {
                self.entries.push(entry);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normal(i: u64, t: u64, s: &[u8]) -> LogEntry {
        LogEntry::normal(i, t, s.to_vec())
    }

    // ------------LogEntry-------------

    #[test]
    fn entry_helper() {
        let n = LogEntry::normal(1, 2, b"x".to_vec());
        assert_eq!(n.entry_type, EntryType::Normal);
        assert!(!n.is_empty());

        let e = LogEntry::empty(3, 4);
        assert_eq!(e.entry_type, EntryType::Empty);
        assert!(e.is_empty());
        assert!(e.data.is_empty());
    }

    #[test]
    fn empty_log() {
        let log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert!(log.get(1).is_none());
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), None);
    }

    #[test]
    fn append_from_scratch() {
        let mut log = RaftLog::new();
        let result = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")], 0);
        assert!(result.is_ok());
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.get(1).unwrap().data, b"a");
        assert_eq!(log.get(2).unwrap().data, b"b");
    }

    #[test]
    fn append_rejects_bad_prev() {
        let mut log = RaftLog::new();

        let result = log.append(0, 0, vec![normal(1, 1, b"a")], 0);
        assert!(result.is_ok());

        assert_eq!(
            log.append(1, 99, vec![normal(2, 1, b"b")], 0).unwrap_err(),
            AppendError::LogMismatch {
                prev_log_index: 1,
                prev_log_term: 99,
            }
        );

        assert_eq!(
            log.append(5, 1, vec![normal(6, 1, b"b")], 0).unwrap_err(),
            AppendError::LogMismatch {
                prev_log_index: 5,
                prev_log_term: 1,
            }
        );
        assert_eq!(log.entries, vec![normal(1, 1, b"a")]);

        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn truncate_from_middle() {
        let mut log = RaftLog::new();
        let result = log.append(
            0,
            0,
            vec![
                normal(1, 1, b"a"),
                normal(2, 1, b"b"),
                normal(3, 1, b"c"),
                normal(4, 2, b"d"),
            ],
            0,
        );

        assert!(result.is_ok());

        assert_eq!(log.last_index(), 4);

        let removed = log.truncate_from(3);
        assert_eq!(removed, 2);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.get(3), None);
        assert_eq!(log.get(4), None);
    }

    #[test]
    fn truncate_from_all() {
        let mut log = RaftLog::new();
        let result = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")], 0);
        assert!(result.is_ok());

        let removed = log.truncate_from(log.offset);
        assert_eq!(removed, 2);
        assert!(log.is_empty());
        assert!(log.entries.is_empty());
    }

    #[test]
    fn truncate_from_out_of_range_is_noop() {
        let mut log = RaftLog::new();
        let result = log.append(0, 0, vec![normal(1, 1, b"a")], 0);
        assert!(result.is_ok());

        assert_eq!(log.truncate_from(99), 0);
        assert_eq!(log.truncate_from(0), 0);
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn append_refuses_to_truncate_committed() {
        let mut log = RaftLog::new();
        let reesult = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")], 0);
        assert!(reesult.is_ok());
        let result = log.append(1, 1, vec![normal(2, 2, b"b")], 2);
        assert_eq!(
            result.unwrap_err(),
            AppendError::OverwriteCommitted {
                index: 2,
                committed: 2,
            }
        );
        assert_eq!(log.entries, vec![normal(1, 1, b"a"), normal(2, 1, b"b")]);
    }

    #[test]
    fn append_rejects_non_contiguous_entries_without_changes() {
        let mut log = RaftLog::new();
        let entries = vec![normal(1, 1, b"a"), normal(2, 1, b"b")];
        log.append(0, 0, entries.clone(), 0).unwrap();

        let error = log
            .append(1, 1, vec![normal(2, 2, b"B"), normal(4, 2, b"D")], 0)
            .unwrap_err();
        assert_eq!(
            error,
            AppendError::NonContiguous {
                offset: 1,
                expected: 3,
                got: 4,
            }
        );
        assert_eq!(log.entries, entries);
    }

    #[test]
    fn append_overwriters_on_term_conflict() {
        let mut log = RaftLog::new();
        let result = log.append(
            0,
            0,
            vec![normal(1, 1, b"a"), normal(2, 1, b"b"), normal(3, 1, b"c")],
            0,
        );

        assert!(result.is_ok());

        let result = log.append(1, 1, vec![normal(2, 2, b"B"), normal(3, 2, b"C")], 0);
        assert!(result.is_ok());
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.get(2).unwrap().data, b"B");
        assert_eq!(log.get(3).unwrap().data, b"C");
    }

    #[test]
    fn append_is_idempotent_on_same_entries() {
        let mut log = RaftLog::new();
        let entries = vec![normal(1, 1, b"a"), normal(2, 1, b"b")];
        let result = log.append(0, 0, entries.clone(), 0);
        assert!(result.is_ok());

        let result = log.append(0, 0, entries, 0);
        assert!(result.is_ok());
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn append_partial_overlap() {
        let mut log = RaftLog::new();
        let result = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")], 0);
        assert!(result.is_ok());

        let result = log.append(
            1,
            1,
            vec![normal(2, 1, b"b"), normal(3, 1, b"c"), normal(4, 1, b"d")],
            0,
        );

        assert!(result.is_ok());
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.len(), 4);
    }

    #[test]
    fn append_extends_with_higher_term() {
        let mut log = RaftLog::new();
        let result = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")], 0);
        assert!(result.is_ok());

        let result = log.append(2, 1, vec![normal(3, 2, b"c"), normal(4, 2, b"d")], 0);
        assert!(result.is_ok());
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn slice_closed_interval() {
        let mut log = RaftLog::new();
        let result = log.append(
            0,
            0,
            vec![
                normal(1, 1, b"a"),
                normal(2, 1, b"b"),
                normal(3, 2, b"c"),
                normal(4, 2, b"d"),
            ],
            0,
        );
        assert!(result.is_ok());
        let s = log.slice(2, 3);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].index, 2);
        assert_eq!(s[1].index, 3);

        let s = log.slice(3, 3);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].index, 3);

        assert!(log.slice(10, 20).is_empty());
        assert!(log.slice(0, 2).is_empty());
        assert!(log.slice(3, 2).is_empty());

        let s = log.slice(3, 100);
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn matches_sentinel_and_real() {
        let mut log = RaftLog::new();
        let result = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 2, b"b")], 0);
        assert!(result.is_ok());

        assert!(log.matches(0, 0));
        assert!(log.matches(0, 999));
        assert!(log.matches(1, 1));
        assert!(log.matches(2, 2));
        assert!(!log.matches(1, 2));
        assert!(!log.matches(3, 2));
    }

    #[test]
    fn follower_overwrite_scenario() {
        let mut log = RaftLog::new();
        let result = log.append(
            0,
            0,
            vec![
                normal(1, 1, b"a"),
                normal(2, 1, b"b"),
                normal(3, 1, b"stale"),
            ],
            0,
        );

        assert!(result.is_ok());

        let result = log.append(
            1,
            1,
            vec![normal(2, 2, b"B"), normal(3, 2, b"C"), normal(4, 2, b"D")],
            0,
        );
        assert!(result.is_ok());
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.get(1).unwrap().term, 1);
        assert_eq!(log.get(2).unwrap().term, 2);
        assert_eq!(log.get(3).unwrap().term, 2);
        assert_eq!(log.get(4).unwrap().term, 2);
        assert_eq!(log.get(3).unwrap().data, b"C");
    }
}
