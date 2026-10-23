use serde::{Deserialize, Serialize};

use crate::{log::raft_log::LogEntry, raft::state::NodeId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResp {
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

/// A raft message tagged with the sender's id at the transport boundary.
/// Responses carry no sender in their payload, so a per-node inbox needs
/// this to attribute votes and replication progress to the right peer.
pub type PeerMessage = (NodeId, RaftMessage);
