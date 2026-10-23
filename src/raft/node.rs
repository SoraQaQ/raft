use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, oneshot};

use crate::{
    log::{log_actor::LogHandle, raft_log::LogEntry},
    raft::{
        command::{InspectResult, NodeCommand, ProposeResult},
        message::RaftMessage,
        role::RaftRole,
        state::{NodeId, RaftState},
    },
};

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
    peer_txs: HashMap<NodeId, mpsc::Sender<RaftMessage>>,

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
        peer_txs: HashMap<NodeId, mpsc::Sender<RaftMessage>>,
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
        let last_log_index = self.log.last_info().await.0;
        self.state.become_leader(&self.peers, last_log_index);
        self.broadcast_heartbeat().await;
        self.reset_timer();
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

    // ========================================================
    // RPC handler
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
                term: self.state.current_term,
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
                crate::error::append_error::AppendError::LogMismatch { .. } => {}
                crate::error::append_error::AppendError::NonContiguous { .. }
                | crate::error::append_error::AppendError::OverwriteCommitted { .. } => {
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

    /// send heartbeat(empty entries) to all peer
    async fn broadcast_heartbeat(&self) {
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

    /// try to advance commit_index according to the majority
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

        self.state.commit_index = majority_index;
        //TODO: apply log
    }

    /// send appendentries to a single peer(decide what to send based on next_index)
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

    // ========================================================
    // Client Proposal
    // ========================================================

    async fn handle_propose(&mut self, data: Vec<u8>) -> ProposeResult {
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

    // ========================================================
    // Timer
    // ========================================================

    /// Restart the countdown and pick a fresh random election timeout.
    fn reset_timer(&mut self) {
        self.timer_elapsed = 0;
        self.election_timeout =
            rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS);
    }

    /// One clock tick: the leader sends heartbeats, everyone else runs
    /// the election countdown.
    async fn handle_tick(&mut self) {
        if self.is_leader() {
            self.broadcast_heartbeat().await;
            return;
        }

        self.timer_elapsed += 1;
        if self.timer_elapsed >= self.election_timeout {
            self.become_candidate().await;
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

pub struct NodeHandle {
    pub id: NodeId,
    tx: mpsc::Sender<NodeCommand>,
}

impl NodeHandle {
    pub fn spawn(
        id: NodeId,
        peers: Vec<NodeId>,
        log: LogHandle,
        peer_txs: HashMap<NodeId, mpsc::Sender<RaftMessage>>,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::{ELECTION_TIMEOUT_MAX_TICKS, ELECTION_TIMEOUT_MIN_TICKS, RaftNode};
    use crate::{
        log::log_actor::LogHandle,
        raft::{message::RaftMessage, role::RaftRole},
    };

    fn new_node(peers: Vec<u64>, peer_txs: HashMap<u64, mpsc::Sender<RaftMessage>>) -> RaftNode {
        RaftNode::new(1, peers, LogHandle::spawn(), peer_txs)
    }

    /// Run one election round without any clock: drive `handle_tick` by hand.
    async fn campaign(node: &mut RaftNode) {
        for _ in 0..node.election_timeout {
            node.handle_tick().await;
        }
    }

    async fn recv(rx: &mut mpsc::Receiver<RaftMessage>) -> Option<RaftMessage> {
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .ok()
            .flatten()
    }

    async fn drain(rx: &mut mpsc::Receiver<RaftMessage>) {
        while tokio::time::timeout(Duration::from_millis(20), rx.recv())
            .await
            .ok()
            .flatten()
            .is_some()
        {}
    }

    #[tokio::test]
    async fn initial_timeout_in_range() {
        let node = new_node(vec![2, 3], HashMap::new());
        assert!(
            (ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS)
                .contains(&node.election_timeout)
        );
        assert_eq!(node.timer_elapsed, 0);
    }

    #[tokio::test]
    async fn follower_becomes_candidate_after_timeout() {
        let mut node = new_node(vec![2, 3], HashMap::new());

        for _ in 0..node.election_timeout - 1 {
            node.handle_tick().await;
        }
        assert_eq!(node.state.role, RaftRole::Follower);
        assert_eq!(node.timer_elapsed, node.election_timeout - 1);

        node.handle_tick().await;
        assert_eq!(node.state.role, RaftRole::Candidate);
        assert_eq!(node.state.current_term, 1);
        assert_eq!(node.state.voted_for, Some(1));
        assert_eq!(node.timer_elapsed, 0, "a campaign restarts the countdown");
    }

    #[tokio::test]
    async fn single_node_wins_immediately() {
        let mut node = new_node(vec![], HashMap::new());
        campaign(&mut node).await;

        assert_eq!(node.state.role, RaftRole::Leader);
        assert_eq!(node.state.current_term, 1);
    }

    #[tokio::test]
    async fn candidate_re_campaigns_after_timeout() {
        let mut node = new_node(vec![2, 3], HashMap::new());
        campaign(&mut node).await;
        assert_eq!(node.state.role, RaftRole::Candidate);
        assert_eq!(node.state.current_term, 1);

        campaign(&mut node).await;
        assert_eq!(node.state.role, RaftRole::Candidate);
        assert_eq!(node.state.current_term, 2);
        assert_eq!(node.state.voted_for, Some(1));
        assert_eq!(node.state.as_candidate().unwrap().count(), 1);
    }

    #[tokio::test]
    async fn leader_does_not_run_election_timer() {
        let mut node = new_node(vec![], HashMap::new());
        campaign(&mut node).await;
        assert_eq!(node.state.role, RaftRole::Leader);
        let term = node.state.current_term;

        for _ in 0..(ELECTION_TIMEOUT_MAX_TICKS * 3) {
            node.handle_tick().await;
        }
        assert_eq!(node.state.role, RaftRole::Leader);
        assert_eq!(node.state.current_term, term);
    }

    #[tokio::test]
    async fn append_entries_resets_countdown() {
        let mut node = new_node(vec![2, 3], HashMap::new());
        for _ in 0..node.election_timeout - 1 {
            node.handle_tick().await;
        }

        node.handle_append_entries(2, 1, 2, 0, 0, vec![], 0).await;
        assert_eq!(node.state.role, RaftRole::Follower);
        assert_eq!(node.state.current_term, 1);
        assert_eq!(node.timer_elapsed, 0);
        assert!(
            (ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS)
                .contains(&node.election_timeout)
        );
    }

    #[tokio::test]
    async fn leader_broadcasts_heartbeat_every_tick() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut peer_txs = HashMap::new();
        peer_txs.insert(2, tx);
        let mut node = new_node(vec![2], peer_txs);

        campaign(&mut node).await;
        assert_eq!(node.state.role, RaftRole::Candidate);

        let term = node.state.current_term;
        node.handle_request_vote_resp(2, term, true).await;
        assert_eq!(node.state.role, RaftRole::Leader);
        drain(&mut rx).await; // drop the heartbeat sent on election

        for _ in 0..3 {
            node.handle_tick().await;
            let msg = recv(&mut rx).await;
            assert!(matches!(msg, Some(RaftMessage::AppendEntries { .. })));
        }
    }
}
