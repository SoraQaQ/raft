use tokio::sync::oneshot;

use crate::raft::{message::RaftMessage, role::RaftRole, state::NodeId};

pub enum NodeCommand {
    Raft {
        from: NodeId,
        msg: RaftMessage,
    },

    ClientPropose {
        data: Vec<u8>,
        reply: oneshot::Sender<ProposeResut>,
    },

    ElectionTick,

    HeartbeatTick,

    ResetElectionTimer,

    Inspect {
        reply: oneshot::Sender<InspectResult>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeResut {
    pub accepted: bool,
    pub index: Option<u64>,
}

impl ProposeResut {
    pub fn rejectd() -> Self {
        Self {
            accepted: false,
            index: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectResult {
    pub role: RaftRole,
    pub term: u64,
    pub commit_index: u64,
    pub last_index: u64,
    pub voted_for: Option<NodeId>,
}
