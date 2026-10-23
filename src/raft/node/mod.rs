//! Raft node actor: state ownership, role transitions, RPC dispatch and the
//! drive loop. Protocol logic lives in the sibling modules:
//!
//! - [`election`]: RequestVote handling and election bookkeeping
//! - [`replication`]: AppendEntries, commit advancement, heartbeats
//! - [`client`]: client proposals
//! - [`tick`]: election countdown and heartbeat cadence

use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, oneshot};

use crate::{
    log::{log_actor::LogHandle, raft_log::LogEntry},
    raft::{
        command::{InspectResult, NodeCommand, ProposeResult},
        message::{PeerMessage, RaftMessage},
        role::RaftRole,
        state::{NodeId, RaftState},
    },
};

mod client;
mod election;
mod replication;
mod tick;

#[cfg(test)]
mod tests;

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
    ) -> Self {
        let mut rng = rand::thread_rng();
        Self {
            id,
            peers,
            state: RaftState::new(),
            log,
            peer_txs,
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

    async fn broadcast(&self, msg: RaftMessage) {
        for tx in self.peer_txs.values() {
            let tx = tx.clone();
            let msg = (self.id, msg.clone());

            tokio::spawn(async move {
                let _ = tx.send(msg).await;
            });
        }
    }

    async fn send_to(&self, to: NodeId, msg: RaftMessage) {
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
        self.broadcast(msg).await;
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
        for &peer in &self.peers {
            self.send_append_entries_to(peer).await;
        }
        self.reset_timer();
    }

    // ========================================================
    // RPC dispatch
    // ========================================================

    async fn handle_raft(&mut self, from: NodeId, msg: RaftMessage) {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(from, term, candidate_id, last_log_index, last_log_term)
                    .await;
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    from,
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
                .await;
            }
            RaftMessage::AppendEntriesResp {
                term,
                success,
                match_index,
            } => {
                self.handle_append_entries_resp(from, term, success, match_index)
                    .await;
            }
            RaftMessage::VoteResp { term, vote_granted } => {
                self.handle_request_vote_resp(from, term, vote_granted)
                    .await;
            }
        }
    }

    // ========================================================
    // actor loop
    // ========================================================

    pub async fn run(mut self, mut rx: mpsc::Receiver<NodeCommand>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));
        ticker.tick().await; // the first tick of `interval` is immediately ready

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    match cmd {
                        NodeCommand::Raft { from, msg } => {
                            self.handle_raft(from, msg).await;
                        }
                        NodeCommand::ClientPropose { data, reply } => {
                            let result = self.handle_propose(data).await;
                            let _ = reply.send(result);
                        }
                        NodeCommand::Inspect { reply } => {
                            let (last_index, _) = self.log.last_info().await;
                            let _ = reply.send(InspectResult {
                                role: self.state.role,
                                term: self.state.current_term,
                                commit_index: self.state.commit_index,
                                last_index,
                                voted_for: self.state.voted_for,
                            });
                        }
                    }
                }
                _ = ticker.tick() => {
                    self.handle_tick().await;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct NodeHandle {
    pub id: NodeId,
    tx: mpsc::Sender<NodeCommand>,
}

impl NodeHandle {
    pub fn spawn(
        id: NodeId,
        peers: Vec<NodeId>,
        log: LogHandle,
        peer_txs: HashMap<NodeId, mpsc::Sender<PeerMessage>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let node = RaftNode::new(id, peers, log, peer_txs);
        tokio::spawn(node.run(rx));
        Self { id, tx }
    }

    pub async fn send_raft(&self, from: NodeId, msg: RaftMessage) {
        let _ = self.tx.send(NodeCommand::Raft { from, msg }).await;
    }

    pub async fn propose(&self, data: Vec<u8>) -> ProposeResult {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .tx
            .send(NodeCommand::ClientPropose {
                data,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return ProposeResult::rejected();
        }
        reply_rx.await.unwrap_or_else(|_| ProposeResult::rejected())
    }

    pub async fn inspect(&self) -> InspectResult {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.tx.send(NodeCommand::Inspect { reply: reply_tx }).await;

        reply_rx.await.unwrap_or(InspectResult {
            role: RaftRole::Follower,
            term: 0,
            commit_index: 0,
            last_index: 0,
            voted_for: None,
        })
    }
}
