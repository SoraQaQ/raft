//! AppendEntries handling, replication progress, commit advancement and
//! heartbeats.

use crate::{
    error::append_error::AppendError,
    log::raft_log::LogEntry,
    raft::{message::RaftMessage, role::RaftRole, state::NodeId},
};

use super::RaftNode;

impl RaftNode {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_append_entries(
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
                RaftMessage::AppendEntriesResp {
                    term: self.state.current_term,
                    success: false,
                    match_index,
                },
            )
            .await;
            return;
        }

        if term > self.state.current_term || self.state.role != RaftRole::Follower {
            self.become_follower(term);
        }

        self.reset_timer();

        let entries_len = entries.len() as u64;

        if let Err(err) = self
            .log
            .append(
                prev_log_index,
                prev_log_term,
                entries,
                self.state.commit_index,
            )
            .await
        {
            match &err {
                AppendError::LogMismatch { .. } => {}
                AppendError::NonContiguous { .. } | AppendError::OverwriteCommitted { .. } => {
                    eprintln!("AppendEntries from {from} rejecet {err}");
                }
            }

            let (last_index, _) = self.log.last_info().await;
            self.send_to(
                from,
                RaftMessage::AppendEntriesResp {
                    term: self.state.current_term,
                    success: false,
                    match_index: last_index,
                },
            )
            .await;
            return;
        }

        let last_new_index = prev_log_index + entries_len;

        // The commit index must be monotonically non-decreasing.
        if leader_commit > self.state.commit_index {
            let new_commit_index = leader_commit.min(last_new_index);
            self.state.commit_index = self.state.commit_index.max(new_commit_index);
            //TODO: apply log
        }

        self.send_to(
            from,
            RaftMessage::AppendEntriesResp {
                term: self.state.current_term,
                success: true,
                match_index: last_new_index,
            },
        )
        .await;
    }

    /// Received AppendEntriesResp (leader).
    pub(super) async fn handle_append_entries_resp(
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
            // The (match_index/next_index) must be monotonically non-decreasing.
            if let Some(leader) = self.state.as_leader_mut() {
                let matched = leader.match_index.entry(from).or_insert(0);
                *matched = (*matched).max(match_index);

                let next = leader.next_index.entry(from).or_insert(1);
                *next = (*next).max(match_index + 1);
            }
            self.maybe_advance_commit().await;
        } else {
            // now sub 1, maybe Optimize
            if let Some(leader) = self.state.as_leader_mut() {
                let matched = leader.match_index.get(&from).cloned().unwrap_or(0);
                let new_next = leader.next_index.entry(from).or_insert(1);
                *new_next = match_index
                    .saturating_add(1)
                    .min(new_next.saturating_sub(1))
                    .max(matched + 1)
                    .max(1);
            } else {
                return;
            };
            self.send_append_entries_to(from).await;
        }
    }

    // ========================================================
    // Leader
    // ========================================================

    /// Send a heartbeat (empty entries) to every peer.
    pub(super) async fn broadcast_heartbeat(&self) {
        let (last_log_index, last_log_term) = self.log.last_info().await;
        let msg = RaftMessage::AppendEntries {
            term: self.state.current_term,
            leader_id: self.id,
            prev_log_index: last_log_index,
            prev_log_term: last_log_term,
            entries: vec![],
            leader_commit: self.state.commit_index,
        };
        self.broadcast(msg).await;
    }

    /// Try to advance commit_index according to the majority.
    pub(super) async fn maybe_advance_commit(&mut self) {
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

        self.state.commit_index = majority_index;
        //TODO: apply log
    }

    /// Send AppendEntries to a single peer, deciding what to send based on its
    /// next_index.
    pub(super) async fn send_append_entries_to(&self, peer: NodeId) {
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

        // slice[next_index, last_index]
        let entries = if last_index >= next_index {
            self.log.slice(next_index, last_index).await
        } else {
            vec![]
        };

        self.send_to(
            peer,
            RaftMessage::AppendEntries {
                term: self.state.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.state.commit_index,
            },
        )
        .await;
    }
}
