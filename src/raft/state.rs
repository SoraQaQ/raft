use std::collections::{HashMap, HashSet};

use crate::raft::role::RaftRole;

pub type NodeId = u64;

// ============================================================
// LeaderState
// ============================================================

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

// ============================================================
// CandidateState
// ============================================================

#[derive(Debug, Clone, Default)]
pub struct CandidateState {
    pub votes: HashSet<NodeId>,
}

impl CandidateState {
    /// Initialize when changing to candidate:
    /// vote for yourself
    pub fn new(id: NodeId) -> Self {
        let mut votes = HashSet::new();
        votes.insert(id);
        Self { votes }
    }

    pub fn grant(&mut self, from: NodeId) -> usize {
        self.votes.insert(from);
        self.votes.len()
    }

    pub fn count(&self) -> usize {
        self.votes.len()
    }

    pub fn is_majority(&self, cluster_size: usize) -> bool {
        self.count() >= cluster_size / 2 + 1
    }
}

pub struct RaftState {
    pub role: RaftRole,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader: Option<LeaderState>,
    pub candidate: Option<CandidateState>,
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: Option::None,
            commit_index: 0,
            last_applied: 0,
            leader: Option::None,
            candidate: Option::None,
        }
    }

    pub fn become_follower(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = RaftRole::Follower;
        self.leader = None;
        self.candidate = None;
    }

    pub fn become_candidate(&mut self, id: NodeId) {
        self.current_term += 1;
        self.voted_for = Some(id);
        self.role = RaftRole::Candidate;
        self.leader = None;
        self.candidate = Some(CandidateState::new(id))
    }

    pub fn become_leader(&mut self, peers: &[NodeId], last_log_index: u64) {
        self.role = RaftRole::Leader;
        self.candidate = None;
        self.leader = Some(LeaderState::new(peers, last_log_index))
    }

    pub fn as_leader(&self) -> Option<&LeaderState> {
        self.leader.as_ref()
    }

    pub fn as_leader_mut(&mut self) -> Option<&mut LeaderState> {
        self.leader.as_mut()
    }

    pub fn as_candidate(&self) -> Option<&CandidateState> {
        self.candidate.as_ref()
    }
    pub fn as_candidate_mut(&mut self) -> Option<&mut CandidateState> {
        self.candidate.as_mut()
    }

    pub fn check_invariants(&self) {
        match self.role {
            RaftRole::Follower => {
                assert!(
                    self.candidate.is_none(),
                    "follower state but candidate state is some"
                );

                assert!(
                    self.leader.is_none(),
                    "follower state but leader state is some"
                );
            }
            RaftRole::Candidate => {
                assert!(
                    self.candidate.is_some(),
                    "candidate state but candidate state is none"
                );

                assert!(
                    self.leader.is_none(),
                    "candidate state but leader state is some"
                );
            }
            RaftRole::Leader => {
                assert!(
                    self.leader.is_none(),
                    "leader state but leader state is none"
                );

                assert!(
                    self.candidate.is_some(),
                    "leader state but candidate is some"
                );
            }
        }
    }
}
