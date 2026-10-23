//! Client-facing proposals.

use crate::{log::raft_log::LogEntry, raft::command::ProposeResult};

use super::RaftNode;

impl RaftNode {
    pub(super) async fn handle_propose(&mut self, data: Vec<u8>) -> ProposeResult {
        if !self.is_leader() {
            return ProposeResult::rejected();
        }

        let (last_index, last_term) = self.log.last_info().await;
        let new_index = last_index + 1;
        let entry = LogEntry::normal(new_index, self.state.current_term, data);

        if self
            .log
            .append(last_index, last_term, vec![entry], self.state.commit_index)
            .await
            .is_err()
        {
            return ProposeResult::rejected();
        }

        self.maybe_advance_commit().await;

        for &peer in &self.peers {
            self.send_append_entries_to(peer).await;
        }

        ProposeResult::accepted(new_index)
    }
}
