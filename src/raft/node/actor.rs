//! Actor shell: command dispatch, the select! drive loop and the public
//! [`NodeHandle`] used by the transport and clients.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::{
    error::query_error::QueryError,
    log::log_actor::LogHandle,
    raft::{
        command::{InspectResult, NodeCommand, ProposeResult},
        message::{PeerMessage, RaftMessage},
        role::RaftRole,
        state::NodeId,
    },
    state_machine::StateMachine,
};

use super::{RaftNode, TICK_MS};

impl RaftNode {
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
                        NodeCommand::QueryLocal { request, reply } => {
                            let _ = reply.send(self.state_machine.query(&request));
                        }
                        NodeCommand::Inspect { reply } => {
                            let (last_index, _) = self.log.last_info().await;
                            let _ = reply.send(InspectResult {
                                role: self.state.role,
                                term: self.state.current_term,
                                commit_index: self.state.commit_index,
                                last_index,
                                last_applied: self.state.last_applied,
                                voted_for: self.state.voted_for,
                            });
                        }
                    }
                }
                _ = ticker.tick() => {
                    self.handle_tick().await;
                }
            }

            // Apply after every command so commit progress is visible to the
            // next command (including reads) without extra bookkeeping.
            self.advance_apply().await;
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
        state_machine: Box<dyn StateMachine>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let node = RaftNode::new(id, peers, log, peer_txs, state_machine);
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

    /// Read this node's applied state without contacting peers.
    /// May return stale data on any node, including a leader.
    /// Returns an error if the node cannot receive or answer the query.
    pub async fn query_local(&self, request: Vec<u8>) -> Result<Vec<u8>, QueryError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(NodeCommand::QueryLocal {
                request,
                reply: reply_tx,
            })
            .await
            .map_err(|_| QueryError::NodeUnavailable)?;

        reply_rx.await.map_err(|_| QueryError::ResponseDropped)
    }

    pub async fn inspect(&self) -> InspectResult {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.tx.send(NodeCommand::Inspect { reply: reply_tx }).await;

        reply_rx.await.unwrap_or(InspectResult {
            role: RaftRole::Follower,
            term: 0,
            commit_index: 0,
            last_index: 0,
            last_applied: 0,
            voted_for: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn query_local_errors_when_node_is_unavailable() {
        let (tx, rx) = mpsc::channel(1);
        let handle = NodeHandle { id: 1, tx };
        drop(rx);

        assert_eq!(
            handle.query_local(b"k".to_vec()).await,
            Err(QueryError::NodeUnavailable)
        );
    }

    #[tokio::test]
    async fn query_local_errors_when_reply_is_dropped() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = NodeHandle { id: 1, tx };

        let (result, ()) = tokio::join!(handle.query_local(b"k".to_vec()), async {
            drop(rx.recv().await.unwrap());
        });

        assert_eq!(result, Err(QueryError::ResponseDropped));
    }
}
