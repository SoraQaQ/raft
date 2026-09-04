use crate::{peer::peer, raft_log::RaftLog};

#[derive(Debug)]
pub enum RaftState {
    Master,
    Candidate,
    Follower,
}

#[derive(Debug)]
pub struct Raft {
    peers: Vec<peer>,
    state: RaftState,
    current_term: u64,
    voted_for: u64,
    log: RaftLog,
    commit_index: u64,
    last_applied: u64,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    me: u64,
}

impl Raft {
    pub fn new(me: u64, peers: Vec<peer>) -> Self {
        Self {
            peers,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: 0,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            me,
        }
    }
}
