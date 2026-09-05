#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Entry {
    term: u64,
    index: u64,
    command: Vec<u8>,
}

impl Entry {
    pub fn new(term: u64, index: u64, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
        }
    }
}

//use the offset to set index start at 1
#[derive(Debug)]
pub struct RaftLog {
    entries: Vec<Entry>,
    offset: u64,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            offset: 1,
        }
    }

    pub fn get_entry(&self, index: u64) -> Option<&Entry> {
        match index {
            0 => None,
            _ if index < self.offset => None,
            _ => self.entries.get((index - self.offset) as usize),
        }
    }

    pub fn append(&mut self, mut entries: Vec<Entry>) {
        let start = self.offset + self.entries.len() as u64;
        for (i, entry) in entries.iter_mut().enumerate() {
            entry.index = start + i as u64;
        }
        self.entries.extend(entries);
    }

    pub fn truncate(&mut self, up_to: u64) {
        if up_to < self.offset {
            self.entries.clear();
            return;
        }

        let pos = (up_to - self.offset) as usize + 1;

        if pos < self.entries.len() {
            self.entries.truncate(pos);
        }
    }

    pub fn get_last_entry(&self) -> Option<&Entry> {
        self.get_entry(self.entries.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use crate::raft_log::{Entry, RaftLog};

    #[test]
    fn raft_log() {
        let mut raft_log = RaftLog::new();
        let entry = raft_log.get_entry(1);
        assert!(entry.is_none());

        let mut entrys = vec![Entry::new(1, 1, vec![0x48])];
        raft_log.append(entrys);

        let entry = raft_log.get_entry(0);

        assert!(entry.is_none());

        let entry = raft_log.get_entry(1);

        let entry_test = Entry::new(1, 1, vec![0x48]);
        assert!(entry.unwrap() == &entry_test);
        let mut append_entrys = Vec::<Entry>::new();
        for i in 2..=10 {
            let entry = Entry::new(i, i, vec![0x48]);
            append_entrys.push(entry);
        }

        raft_log.append(append_entrys);

        let entry = raft_log.get_last_entry();
        assert!(entry.unwrap().index == 10);

        raft_log.truncate(5);

        println!("{:?}", raft_log);

        let entry = raft_log.get_last_entry();
        assert!(entry.unwrap().index == 5);
    }
}
