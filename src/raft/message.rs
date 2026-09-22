use serde::{Deserialize, Serialize};

use crate::{log::raft_log::LogEntry, raft::state::NodeId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntires {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntiresResp {
        term: u64,
        success: bool,
        match_index: u64,
    },

    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },

    VoteResp {
        term: u64,
        vote_granted: bool,
    },
}
