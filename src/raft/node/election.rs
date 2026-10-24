//! RequestVote handling and election bookkeeping.

use crate::raft::{message::RaftMessage, state::NodeId};

use super::RaftNode;

impl RaftNode {
    /// Check whether enough votes have been gathered to become leader.
    pub(super) async fn check_election_won(&mut self) {
        if !self.is_candidate() {
            return;
        }

        let count = self.state.as_candidate().map(|c| c.count()).unwrap_or(0);
        if count >= self.majority() {
            self.become_leader().await;
        }
    }

    pub(super) async fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) {
        if term < self.state.current_term {
            let msg = RaftMessage::VoteResp {
                term: self.state.current_term,
                vote_granted: false,
            };

            self.send_to(from, msg);
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
                || (last_log_term == my_last_term && last_log_index >= my_last_index);

            if up_to_date {
                grant = true;
                self.state.voted_for = Some(candidate_id);
                self.reset_timer();
            }
        }

        self.send_to(
            from,
            RaftMessage::VoteResp {
                term: self.state.current_term,
                vote_granted: grant,
            },
        );
    }

    pub(super) async fn handle_request_vote_resp(
        &mut self,
        from: NodeId,
        term: u64,
        vote_granted: bool,
    ) {
        if term > self.state.current_term {
            self.become_follower(term);
            return;
        }

        if !self.is_candidate() || term != self.state.current_term {
            return;
        }

        if vote_granted {
            self.state
                .as_candidate_mut()
                .map(|c| c.grant(from))
                .unwrap_or(0);
            self.check_election_won().await;
        }
    }
}
