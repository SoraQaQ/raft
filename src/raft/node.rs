//! Raft node state, role transitions and the module tree. Protocol logic lives
//! in the sibling modules; the actor shell (command dispatch, drive loop and
//! [`NodeHandle`]) lives in [`actor`].

use std::collections::HashMap;

use rand::Rng;
use tokio::sync::mpsc;

use crate::{
    log::{log_actor::LogHandle, raft_log::LogEntry},
    raft::{
        message::{PeerMessage, RaftMessage},
        role::RaftRole,
        state::{NodeId, RaftState},
    },
    state_machine::StateMachine,
};

mod actor;
mod apply;
mod client;
mod election;
mod replication;
mod tick;

#[cfg(test)]
mod tests;

pub use actor::NodeHandle;

/// Clock granularity. Followers count ticks for the election timeout,
/// the leader sends one heartbeat per tick.
const TICK_MS: u64 = 100;

/// Election timeout is randomized in this range (in ticks), so nodes
/// that start at the same time do not split the vote forever.
const ELECTION_TIMEOUT_MIN_TICKS: u32 = 5; // 500ms
const ELECTION_TIMEOUT_MAX_TICKS: u32 = 10; // 1s

pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>,

    state: RaftState,
    log: LogHandle,
    peer_txs: HashMap<NodeId, mpsc::Sender<PeerMessage>>,
    state_machine: Box<dyn StateMachine>,

    /// Ticks elapsed since the last reset. Only meaningful when not leader:
    /// leader ignores the election countdown, followers/candidates ignore
    /// the heartbeat cadence, so one counter is enough.
    timer_elapsed: u32,
    election_timeout: u32,
}

impl RaftNode {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        log: LogHandle,
        peer_txs: HashMap<NodeId, mpsc::Sender<PeerMessage>>,
        state_machine: Box<dyn StateMachine>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        Self {
            id,
            peers,
            state: RaftState::new(),
            log,
            peer_txs,
            state_machine,
            timer_elapsed: 0,
            election_timeout: rng
                .gen_range(ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS),
        }
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    fn is_leader(&self) -> bool {
        self.state.role == RaftRole::Leader
    }

    fn is_candidate(&self) -> bool {
        self.state.role == RaftRole::Candidate
    }

    fn broadcast(&self, msg: RaftMessage) {
        for tx in self.peer_txs.values() {
            let tx = tx.clone();
            let msg = (self.id, msg.clone());

            tokio::spawn(async move {
                let _ = tx.send(msg).await;
            });
        }
    }

    fn send_to(&self, to: NodeId, msg: RaftMessage) {
        if let Some(tx) = self.peer_txs.get(&to) {
            let tx = tx.clone();
            let msg = (self.id, msg);
            tokio::spawn(async move {
                let _ = tx.send(msg).await;
            });
        }
    }

    fn become_follower(&mut self, term: u64) {
        self.state.become_follower(term);
        self.reset_timer();
    }

    async fn become_candidate(&mut self) {
        self.state.become_candidate(self.id);

        let (last_log_index, last_log_term) = self.log.last_info().await;
        let msg = RaftMessage::RequestVote {
            term: self.state.current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };
        self.broadcast(msg);
        self.check_election_won().await;
        self.reset_timer();
    }

    async fn become_leader(&mut self) {
        let (last_log_index, last_log_term) = self.log.last_info().await;

        // Commit an entry from the new term so entries left over from previous
        // terms can be committed as well (Raft §5.4.2). Without it an idle
        // cluster would never advance commit_index after a leader change.
        let noop = LogEntry::empty(last_log_index + 1, self.state.current_term);
        let _ = self
            .log
            .append(
                last_log_index,
                last_log_term,
                vec![noop],
                self.state.commit_index,
            )
            .await;

        // `last_log_index` points just before the no-op, so every peer's
        // next_index starts at the no-op and the sends below ship it.
        self.state.become_leader(&self.peers, last_log_index);
        self.maybe_advance_commit().await;
        for peer in self.peers.clone() {
            self.send_append_entries_to(peer).await;
        }
        self.reset_timer();
    }
}
