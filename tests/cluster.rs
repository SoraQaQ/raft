use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use raft::log::log_actor::LogHandle;
use raft::raft::command::InspectResult;
use raft::raft::message::PeerMessage;
use raft::raft::node::NodeHandle;
use raft::raft::role::RaftRole;
use raft::state_machine::KvStateMachine;
use tokio::sync::{RwLock, mpsc};

const ELECTION_DEADLINE: Duration = Duration::from_secs(10);
const COMMIT_DEADLINE: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(20);

/// A three (or N) node cluster wired over in-memory channels.
///
/// Each node has one inbox. Every sender stamps its own id on the message
/// (`PeerMessage`), so the inbox knows which peer sent it even though
/// responses carry no sender in their payload.
struct Cluster {
    handles: HashMap<u64, NodeHandle>,
    logs: HashMap<u64, LogHandle>,
    isolated: Arc<RwLock<HashSet<u64>>>,
}

impl Cluster {
    fn spawn(ids: &[u64]) -> Self {
        let mut senders: HashMap<u64, mpsc::Sender<PeerMessage>> = HashMap::new();
        let mut receivers: HashMap<u64, mpsc::Receiver<PeerMessage>> = HashMap::new();
        for &id in ids {
            let (tx, rx) = mpsc::channel(256);
            senders.insert(id, tx);
            receivers.insert(id, rx);
        }

        let mut handles = HashMap::new();
        let mut logs = HashMap::new();
        for &id in ids {
            let peers: Vec<u64> = ids.iter().copied().filter(|&p| p != id).collect();
            let peer_txs = peers.iter().map(|&p| (p, senders[&p].clone())).collect();
            let log = LogHandle::spawn();
            logs.insert(id, log.clone());
            handles.insert(
                id,
                NodeHandle::spawn(
                    id,
                    peers,
                    log,
                    peer_txs,
                    Box::new(KvStateMachine::default()),
                ),
            );
        }

        let isolated = Arc::new(RwLock::new(HashSet::new()));
        for (id, mut rx) in receivers {
            let handle = handles[&id].clone();
            let isolated = Arc::clone(&isolated);
            tokio::spawn(async move {
                while let Some((from, msg)) = rx.recv().await {
                    // Hold through enqueue so isolation waits for in-flight forwards.
                    let blocked = isolated.read().await;
                    if !blocked.contains(&from) && !blocked.contains(&id) {
                        handle.send_raft(from, msg).await;
                    }
                }
            });
        }

        Self {
            handles,
            logs,
            isolated,
        }
    }

    async fn isolate(&self, id: u64) {
        self.isolated.write().await.insert(id);
    }

    async fn reconnect(&self, id: u64) {
        self.isolated.write().await.remove(&id);
    }

    async fn inspect(&self, id: u64) -> InspectResult {
        self.handles[&id].inspect().await
    }

    /// Wait for one leader and an agreed term among the selected nodes.
    async fn wait_for_leader(&self, ids: &[u64]) -> (u64, u64) {
        let deadline = tokio::time::Instant::now() + ELECTION_DEADLINE;
        loop {
            let mut reports = Vec::new();
            for &id in ids {
                reports.push((id, self.inspect(id).await));
            }

            let leaders: Vec<_> = reports
                .iter()
                .filter(|(_, r)| r.role == RaftRole::Leader)
                .collect();
            if leaders.len() == 1 {
                let term = leaders[0].1.term;
                if reports.iter().all(|(_, r)| r.term == term) {
                    return (leaders[0].0, term);
                }
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "no single leader: {reports:?}"
            );
            tokio::time::sleep(POLL).await;
        }
    }

    async fn wait_until_committed(&self, index: u64) {
        let deadline = tokio::time::Instant::now() + COMMIT_DEADLINE;
        loop {
            let mut committed = 0;
            for &id in self.handles.keys() {
                if self.inspect(id).await.commit_index >= index {
                    committed += 1;
                }
            }
            if committed == self.handles.len() {
                return;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "index {index} not committed on every node: {committed}/{}",
                self.handles.len()
            );
            tokio::time::sleep(POLL).await;
        }
    }

    async fn wait_until_applied(&self, index: u64, ids: &[u64]) {
        let deadline = tokio::time::Instant::now() + COMMIT_DEADLINE;
        loop {
            let mut applied = 0;
            for &id in ids {
                if self.inspect(id).await.last_applied >= index {
                    applied += 1;
                }
            }
            if applied == ids.len() {
                return;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "index {index} not applied on nodes {ids:?}: {applied}/{}",
                ids.len()
            );
            tokio::time::sleep(POLL).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_a_leader_and_commits_its_noop() {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let (leader_id, _) = cluster.wait_for_leader(&[1, 2, 3]).await;

    // A new leader appends a no-op entry; with no client traffic it must still
    // get committed on every node, otherwise old entries would stay stuck.
    let leader = cluster.inspect(leader_id).await;
    assert!(
        leader.last_index >= 1,
        "leader did not append a no-op: {leader:?}"
    );
    cluster.wait_until_committed(leader.last_index).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_a_client_proposal() {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let (leader_id, _) = cluster.wait_for_leader(&[1, 2, 3]).await;

    let result = cluster.handles[&leader_id].propose(b"hello".to_vec()).await;
    assert!(result.accepted, "propose rejected: {result:?}");
    cluster.wait_until_committed(result.index.unwrap()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn applies_committed_entries_on_every_node() {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let (leader_id, _) = cluster.wait_for_leader(&[1, 2, 3]).await;

    let result = cluster.handles[&leader_id].propose(b"k=v".to_vec()).await;
    assert!(result.accepted, "propose rejected: {result:?}");
    cluster
        .wait_until_applied(result.index.unwrap(), &[1, 2, 3])
        .await;

    for (&id, node) in &cluster.handles {
        let value = node.query_local(b"k".to_vec()).await.unwrap();
        assert_eq!(value, b"v", "node {id} returned an unexpected value");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_after_leader_isolation() {
    let ids = [1, 2, 3];
    let cluster = Cluster::spawn(&ids);
    let (old_leader, old_term) = cluster.wait_for_leader(&ids).await;

    let initial = cluster.handles[&old_leader].propose(b"k=v1".to_vec()).await;
    assert!(initial.accepted, "initial proposal rejected: {initial:?}");
    let initial_index = initial.index.unwrap();
    cluster.wait_until_applied(initial_index, &ids).await;

    cluster.isolate(old_leader).await;
    let abandoned = cluster.handles[&old_leader]
        .propose(b"k=abandoned".to_vec())
        .await;
    assert!(
        abandoned.accepted,
        "isolated proposal rejected: {abandoned:?}"
    );
    let abandoned_index = abandoned.index.unwrap();

    let majority: Vec<_> = ids.into_iter().filter(|&id| id != old_leader).collect();
    let (new_leader, new_term) = cluster.wait_for_leader(&majority).await;
    assert!(new_term > old_term);

    let update = cluster.handles[&new_leader].propose(b"k=v2".to_vec()).await;
    assert!(update.accepted, "majority proposal rejected: {update:?}");
    let committed_index = update.index.unwrap();
    assert!(committed_index > abandoned_index);
    cluster.wait_until_applied(committed_index, &majority).await;

    // Majority progress gives the isolated leader time to process replies and ticks.
    let isolated = cluster.inspect(old_leader).await;
    assert_eq!(isolated.last_index, abandoned_index);
    assert_eq!(isolated.commit_index, initial_index, "{isolated:?}");
    assert_eq!(isolated.last_applied, initial_index, "{isolated:?}");
    assert_eq!(
        cluster.handles[&old_leader]
            .query_local(b"k".to_vec())
            .await
            .unwrap(),
        b"v1"
    );
    for &id in &majority {
        assert_eq!(
            cluster.handles[&id]
                .query_local(b"k".to_vec())
                .await
                .unwrap(),
            b"v2",
            "majority node {id} has not applied the update"
        );
    }

    cluster.reconnect(old_leader).await;
    cluster.wait_for_leader(&ids).await;
    cluster.wait_until_applied(committed_index, &ids).await;

    // Compare terms and payloads too: a later write could mask an unrepaired conflict.
    let expected_log = cluster.logs[&new_leader].slice(1, committed_index).await;
    for id in ids {
        assert_eq!(
            cluster.logs[&id].slice(1, committed_index).await,
            expected_log,
            "node {id} retained a conflicting log"
        );
        assert_eq!(
            cluster.handles[&id]
                .query_local(b"k".to_vec())
                .await
                .unwrap(),
            b"v2",
            "node {id} did not converge after reconnecting"
        );
    }
}
