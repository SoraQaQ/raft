use std::collections::{HashMap, HashSet};

use crate::raft::role::RaftRole;

pub type NodeId = u64;

#[derive(Debug, Clone)]
pub struct LeaderState {
    pub next_index: HashMap<NodeId, u64>,
    pub match_index: HashMap<NodeId, u64>,
}

impl LeaderState {
    pub fn new(peers: &[NodeId], last_log_index: u64) -> Self {
        let next_index = peers.iter().map(|p| (*p, last_log_index + 1)).collect();
        let match_index = peers.iter().map(|p| (*p, 0)).collect();
        Self {
            next_index,
            match_index,
        }
    }
}

pub struct CandidateState {
    pub votes: HashSet<NodeId>,
}
