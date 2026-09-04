#[derive(Clone, Debug)]
pub struct Entry {
    term: u64,
    index: u64,
    command: Vec<u8>,
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
            self.offset = up_to;
            return;
        }

        let pos = (up_to - self.offset) as usize;
        self.entries.truncate(pos);
    }
}
