use tokio::sync::oneshot;

use crate::raft::{message::RaftMessage, role::RaftRole, state::NodeId};

pub enum NodeCommand {
    Raft {
        from: NodeId,
        msg: RaftMessage,
    },

    ClientPropose {
        data: Vec<u8>,
        reply: oneshot::Sender<ProposeResult>,
    },

    QueryLocal {
        request: Vec<u8>,
        reply: oneshot::Sender<Vec<u8>>,
    },

    Inspect {
        reply: oneshot::Sender<InspectResult>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeResult {
    pub accepted: bool,
    pub index: Option<u64>,
}

impl ProposeResult {
    pub fn rejected() -> Self {
        Self {
            accepted: false,
            index: None,
        }
    }

    pub fn accepted(index: u64) -> Self {
        Self {
            accepted: true,
            index: Some(index),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectResult {
    pub role: RaftRole,
    pub term: u64,
    pub commit_index: u64,
    pub last_index: u64,
    pub last_applied: u64,
    pub voted_for: Option<NodeId>,
}
