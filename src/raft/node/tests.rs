use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use super::{ELECTION_TIMEOUT_MAX_TICKS, ELECTION_TIMEOUT_MIN_TICKS, RaftNode};
use crate::{
    log::{log_actor::LogHandle, raft_log::LogEntry},
    raft::{
        message::{PeerMessage, RaftMessage},
        role::RaftRole,
    },
    state_machine::{KvStateMachine, StateMachine},
};

fn new_node(peers: Vec<u64>, peer_txs: HashMap<u64, mpsc::Sender<PeerMessage>>) -> RaftNode {
    RaftNode::new(
        1,
        peers,
        LogHandle::spawn(),
        peer_txs,
        Box::new(KvStateMachine::default()),
    )
}

type Applied = Arc<Mutex<Vec<(u64, Vec<u8>)>>>;

/// Records every entry handed to the state machine.
#[derive(Default, Clone)]
struct RecordingSm(Applied);

impl StateMachine for RecordingSm {
    fn apply(&mut self, index: u64, entry: &LogEntry) {
        self.0.lock().unwrap().push((index, entry.data.clone()));
    }

    fn query(&self, _request: &[u8]) -> Vec<u8> {
        Vec::new()
    }
}

/// Run one election round without any clock: drive `handle_tick` by hand.
async fn campaign(node: &mut RaftNode) {
    for _ in 0..node.election_timeout {
        node.handle_tick().await;
    }
}

async fn recv(rx: &mut mpsc::Receiver<PeerMessage>) -> Option<PeerMessage> {
    tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .ok()
        .flatten()
}

async fn drain(rx: &mut mpsc::Receiver<PeerMessage>) {
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
        (ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS).contains(&node.election_timeout)
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

    // the leader appended a no-op and, being the whole cluster, committed it
    assert_eq!(node.log.last_info().await, (1, 1));
    assert_eq!(node.state.commit_index, 1);
}

#[tokio::test]
async fn leader_appends_noop_entry() {
    let mut node = new_node(vec![2, 3], HashMap::new());
    campaign(&mut node).await;
    let term = node.state.current_term;

    node.handle_request_vote_resp(2, term, true).await;
    assert_eq!(node.state.role, RaftRole::Leader);

    let (last_index, last_term) = node.log.last_info().await;
    assert_eq!(last_index, 1);
    assert_eq!(last_term, term);
    assert!(node.log.get(1).await.unwrap().is_empty());
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
        (ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS).contains(&node.election_timeout)
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
        assert!(matches!(msg, Some((1, RaftMessage::AppendEntries { .. }))));
    }
}

#[tokio::test]
async fn applies_committed_entries_in_order() {
    let recording = RecordingSm::default();
    let seen = recording.clone();
    let mut node = RaftNode::new(
        1,
        vec![],
        LogHandle::spawn(),
        HashMap::new(),
        Box::new(recording),
    );

    // Single node: it becomes leader, appends and commits a no-op.
    campaign(&mut node).await;
    node.advance_apply().await;
    assert_eq!(node.state.last_applied, 1);
    assert!(
        seen.0.lock().unwrap().is_empty(),
        "the no-op must not reach the state machine"
    );

    node.handle_propose(b"a".to_vec()).await;
    node.handle_propose(b"b".to_vec()).await;
    node.advance_apply().await;

    assert_eq!(node.state.last_applied, 3);
    assert_eq!(
        *seen.0.lock().unwrap(),
        vec![(2, b"a".to_vec()), (3, b"b".to_vec())]
    );
}

#[tokio::test]
async fn follower_applies_entries_from_leader() {
    let recording = RecordingSm::default();
    let seen = recording.clone();
    let mut node = RaftNode::new(
        1,
        vec![2],
        LogHandle::spawn(),
        HashMap::new(),
        Box::new(recording),
    );

    let entries = vec![LogEntry::normal(1, 1, b"x".to_vec())];
    node.handle_append_entries(2, 1, 2, 0, 0, entries, 1).await;
    node.advance_apply().await;

    assert_eq!(node.state.role, RaftRole::Follower);
    assert_eq!(node.state.last_applied, 1);
    assert_eq!(*seen.0.lock().unwrap(), vec![(1, b"x".to_vec())]);
}
