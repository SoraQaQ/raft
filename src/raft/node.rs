use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::{
    log::{log_actor::LogHandle, raft_log::LogEntry},
    raft::{
        message::RaftMessage,
        role::RaftRole,
        state::{NodeId, RaftState},
    },
};

pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>,

    state: RaftState,
    log: LogHandle,
    peer_txs: HashMap<NodeId, mpsc::Sender<RaftMessage>>,
}

impl RaftNode {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        log: LogHandle,
        peer_txs: HashMap<NodeId, mpsc::Sender<RaftMessage>>,
    ) -> Self {
        Self {
            id,
            peers,
            state: RaftState::new(),
            log,
            peer_txs,
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
        for (_, tx) in &self.peer_txs {
            let tx = tx.clone();
            let msg = msg.clone();

            tokio::spawn(async move {
                let _ = tx.send(msg).await;
            });
        }
    }

    async fn send_to(&self, to: NodeId, msg: RaftMessage) {
        if let Some(tx) = self.peer_txs.get(&to) {
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(msg).await;
            });
        }
    }

    fn become_follower(&mut self, term: u64) {
        self.state.become_follower(term);
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
    }

    async fn become_leader(&mut self) {
        let last_log_index = self.log.last_info().await.0;
        self.state.become_leader(&self.peers, last_log_index);
        self.broadcast_heartbeat().await;
    }

    ///check election won
    async fn check_election_won(&mut self) {
        if !self.is_candidate() {
            return;
        }

        let count = self.state.as_candidate().map(|c| c.count()).unwrap_or(0);
        if count >= self.majority() {
            self.become_leader().await;
        }
    }

    /// send heartbeat(empty entries) to all peer
    async fn broadcast_heartbeat(&self) {
        let (last_log_index, last_log_term) = self.log.last_info().await;
        let msg = RaftMessage::AppendEntires {
            term: self.state.current_term,
            leader_id: self.id,
            prev_log_index: last_log_index,
            prev_log_term: last_log_term,
            entries: vec![],
            leader_commit: self.state.commit_index,
        };
        self.broadcast(msg).await;
    }

    // ========================================================
    // RequestVote
    // ========================================================
    async fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) {
        if term < self.state.current_term {
            let msg = RaftMessage::VoteResp {
                term,
                vote_granted: false,
            };

            self.send_to(from, msg).await;
            return;
        }

        if term > self.state.current_term {
            self.become_follower(term);
        }

        let mut grant = false;

        let can_vote = self.state.voted_for.is_none() || self.state.voted_for == Some(candidate_id);

        if can_vote {
            let (my_last_index, my_last_term) = self.log.last_info().await;
            let up_to_date = last_log_term > my_last_term
                || (last_log_term == my_last_term && last_log_index > my_last_index);

            if up_to_date {
                grant = true;
                self.state.voted_for = Some(candidate_id);
                self.reset_election_timer();
            }
        }

        self.send_to(
            from,
            RaftMessage::VoteResp {
                term,
                vote_granted: grant,
            },
        )
        .await;
    }

    async fn handle_request_vote_resp(&mut self, from: NodeId, term: u64, vote_granted: bool) {
        if term > self.state.current_term {
            self.become_follower(term);
            return;
        }

        if !self.is_candidate() || term != self.state.current_term {
            return;
        }

        if vote_granted {
            let count = self
                .state
                .as_candidate_mut()
                .map(|c| c.grant(from))
                .unwrap_or(0);
            let _ = count;
            self.check_election_won().await;
        }
    }

    // ========================================================
    // AppendEntries
    // ========================================================

    #[allow(clippy::too_many_arguments)]
    async fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: u64,
        _leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) {
        if term < self.state.current_term {
            let match_index = self.log.last_info().await.0;
            self.send_to(
                from,
                RaftMessage::AppendEntiresResp {
                    term,
                    success: false,
                    match_index: match_index,
                },
            )
            .await;
            return;
        }

        if term > self.state.current_term {
            self.become_follower(term);
        } else {
            if self.state.role != RaftRole::Follower {
                self.become_follower(term);
            }
        }

        let matches = self.log.matches(prev_log_index, prev_log_term).await;

        if !matches {
            let match_index = self.log.last_info().await.0;
            self.send_to(
                from,
                RaftMessage::AppendEntiresResp {
                    term,
                    success: false,
                    match_index: match_index,
                },
            )
            .await;
            return;
        }

        let entries_len = entries.len() as u64;
        self.log
            .append(prev_log_index, prev_log_term, entries)
            .await;

        let last_new_index = prev_log_index + entries_len;
        if leader_commit > self.state.commit_index {
            self.state.commit_index = leader_commit.min(last_new_index);
            //TODO: apply log
        }

        self.reset_election_timer();

        let match_index = self.log.last_info().await.0;
        self.send_to(
            from,
            RaftMessage::AppendEntiresResp {
                term,
                success: true,
                match_index: match_index,
            },
        )
        .await;
    }

    ///Received AppendEntriesResp (leader)
    async fn handle_append_entries_resp(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
    ) {
        if term > self.state.current_term {
            self.become_follower(term);
        }

        if !self.is_leader() || term != self.state.current_term {
            return;
        }

        // update match index, next index
        if success {
            if let Some(leader) = self.state.as_leader_mut() {
                leader.match_index.insert(from, match_index);
                leader.next_index.insert(from, match_index + 1);
            }
            self.maybe_advance_commit().await;
        } else {
            // now sub 1, maybe Optimize
            if let Some(leader) = self.state.as_leader_mut() {
                let new_next = leader.next_index.entry(from).or_insert(1);
                *new_next = (*new_next).saturating_sub(1).max(1);
            } else {
                return;
            };
            self.send_append_entries_to(from).await;
        }
    }

    async fn maybe_advance_commit(&mut self) {
        if !self.is_leader() {
            return;
        }

        let last_index = self.log.last_info().await.0;
        let mut indices = vec![last_index];

        if let Some(leader) = self.state.as_leader() {
            for peer in &self.peers {
                let m = leader.match_index.get(peer).copied().unwrap_or(0);
                indices.push(m);
            }
        }

        indices.sort_unstable();
        let majority_index = indices[indices.len() - self.majority()];

        if majority_index <= self.state.commit_index {
            return;
        }

        let term_at = self.log.term_at(majority_index).await.unwrap_or(0);
        if self.state.current_term != term_at {
            return;
        }

        self.state.current_term = term_at;

        //TODO: apply log
    }

    async fn send_append_entries_to(&self, peer: NodeId) {
        let next_index = match self.state.as_leader() {
            Some(l) => match l.next_index.get(&peer) {
                Some(&n) => n,
                None => return,
            },
            None => return,
        };

        let last_index = self.log.last_info().await.0;

        let prev_log_index = next_index - 1;
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            match self.log.term_at(prev_log_index).await {
                Some(t) => t,
                None => return,
            }
        };

        // sliec[next_index, last_index]
        let entries = if last_index >= next_index {
            self.log.slice(last_index, next_index).await
        } else {
            vec![]
        };

        self.send_to(
            peer,
            RaftMessage::AppendEntires {
                term: self.state.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.state.current_term,
            },
        )
        .await;
    }

    fn reset_election_timer(&self) {
        unimplemented!();
    }
}
