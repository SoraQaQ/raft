use serde::{Deserialize, Serialize};

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

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            offset: 1,
        }
    }

    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.offset - 1)
    }

    /// return last log term, if empty log return 0
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// retrieve logs using the index
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index < self.offset {
            return None;
        }
        self.entries.get((index - self.offset) as usize)
    }

    /// term at index: index return None if the index out of bounds
    /// the sentinel at index 0 return 0
    /// (used for the prev_log_term check)
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.get(index).map(|e| e.term)
    }

    /// returns the closed interval `[from, to]`
    /// returns empty if `from > to` or if the range is out of bounds
    pub fn slice(&self, from: u64, to: u64) -> Vec<LogEntry> {
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

    /// Consistency check whether `prev_log_index` / `prev_log_term` match
    /// `prev_log_index == 0` always matches (sentinel)
    /// otherwise, the index must match exist locally and ther term must match
    pub fn matches(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true;
        }

        matches!(self.get(prev_log_index), Some(e) if e.term == prev_log_term)
    }

    ///Deletes all logs starting from `from_index` (inclusive)
    /// Returns the number of deleted entries
    ///
    /// `from_index < offset` no-op
    /// `from_index` > last_index: no-op
    pub fn truncate_from(&mut self, from_index: u64) -> usize {
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

    /// retain `[offset, upto_index]`, and delete everything after it.
    /// returns the number of deleted items.
    ///
    /// equivalent to `truncate_from(upto_index+1)`
    pub fn truncate_suffix(&mut self, upto_index: u64) -> usize {
        self.truncate_from(upto_index.saturating_add(1))
    }

    /// delete single logs(by index)
    pub fn remove(&mut self, index: u64) -> Option<LogEntry> {
        if index < self.offset || index > self.last_index() {
            return None;
        }
        Some(self.entries.remove((index - self.offset) as usize))
    }

    /// append/overwrite log entries
    ///
    /// steps:
    /// 1. consistency check (`prev_log_index / prev_log_term`);
    /// return false and leave state unchanged if it fails
    ///
    /// 2. find the fist point if conflict(local entry has the same index but a different term)
    /// and truncate starting from that point
    ///
    /// 3. append entries not present locally(idempotent)
    ///
    /// return `true` to indicate a successful write
    pub fn append(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
    ) -> bool {
        if !self.matches(prev_log_index, prev_log_term) {
            return false;
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
            self.truncate_from(idx);
        }

        for entry in entries {
            if entry.index > self.last_index() {
                debug_assert_eq!(
                    entry.index,
                    self.last_index() + 1,
                    "append gap: excepted{}. got {}",
                    self.last_index() + 1,
                    entry.index
                );
                self.entries.push(entry);
            }
        }

        true
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
        let ok = log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")]);
        assert!(ok);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.get(1).unwrap().data, b"a");
        assert_eq!(log.get(2).unwrap().data, b"b");
    }

    #[test]
    fn append_rejects_bad_prev() {
        let mut log = RaftLog::new();

        log.append(0, 0, vec![normal(1, 1, b"a")]);

        assert!(!log.append(1, 99, vec![normal(2, 1, b"b")]));

        assert!(!log.append(5, 1, vec![normal(6, 1, b"b")]));

        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn truncate_from_middle() {
        let mut log = RaftLog::new();
        log.append(
            0,
            0,
            vec![
                normal(1, 1, b"a"),
                normal(2, 1, b"b"),
                normal(3, 1, b"c"),
                normal(4, 2, b"d"),
            ],
        );

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
        log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")]);

        let removed = log.truncate_from(log.offset);
        assert_eq!(removed, 2);
        assert!(log.is_empty());
        assert!(log.entries.is_empty());
    }

    #[test]
    fn truncate_from_out_of_range_is_noop() {
        let mut log = RaftLog::new();
        log.append(0, 0, vec![normal(1, 1, b"a")]);

        assert_eq!(log.truncate_from(99), 0);
        assert_eq!(log.truncate_from(0), 0);
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn truncate_suffix_keeps_prefix() {
        let mut log = RaftLog::new();
        log.append(
            0,
            0,
            vec![normal(1, 1, b"a"), normal(2, 1, b"b"), normal(3, 1, b"c")],
        );

        let removed = log.truncate_suffix(1);
        assert_eq!(removed, 2);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.get(1).unwrap().data, b"a");
    }

    #[test]
    fn append_overwriters_on_term_conflict() {
        let mut log = RaftLog::new();
        log.append(
            0,
            0,
            vec![normal(1, 1, b"a"), normal(2, 1, b"b"), normal(3, 1, b"c")],
        );

        let ok = log.append(1, 1, vec![normal(2, 2, b"B"), normal(3, 2, b"C")]);
        assert!(ok);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.get(2).unwrap().data, b"B");
        assert_eq!(log.get(3).unwrap().data, b"C");
    }

    #[test]
    fn append_is_idempotent_on_same_entries() {
        let mut log = RaftLog::new();
        let entries = vec![normal(1, 1, b"a"), normal(2, 1, b"b")];
        log.append(0, 0, entries.clone());

        let ok = log.append(0, 0, entries);
        assert!(ok);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn append_partial_overlap() {
        let mut log = RaftLog::new();
        log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")]);

        let ok = log.append(
            1,
            1,
            vec![normal(2, 1, b"b"), normal(3, 1, b"c"), normal(4, 1, b"d")],
        );

        assert!(ok);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.len(), 4);
    }

    #[test]
    fn append_extends_with_higher_term() {
        let mut log = RaftLog::new();
        log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")]);

        let ok = log.append(2, 1, vec![normal(3, 2, b"c"), normal(4, 2, b"d")]);
        assert!(ok);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn slice_closed_interval() {
        let mut log = RaftLog::new();
        log.append(
            0,
            0,
            vec![
                normal(1, 1, b"a"),
                normal(2, 1, b"b"),
                normal(3, 2, b"c"),
                normal(4, 2, b"d"),
            ],
        );
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
        log.append(0, 0, vec![normal(1, 1, b"a"), normal(2, 2, b"b")]);

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
        log.append(
            0,
            0,
            vec![
                normal(1, 1, b"a"),
                normal(2, 1, b"b"),
                normal(3, 1, b"stale"),
            ],
        );

        let ok = log.append(
            1,
            1,
            vec![normal(2, 2, b"B"), normal(3, 2, b"C"), normal(4, 2, b"D")],
        );
        assert!(ok);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.get(1).unwrap().term, 1);
        assert_eq!(log.get(2).unwrap().term, 2);
        assert_eq!(log.get(3).unwrap().term, 2);
        assert_eq!(log.get(4).unwrap().term, 2);
        assert_eq!(log.get(3).unwrap().data, b"C");
    }
}
