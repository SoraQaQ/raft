use std::collections::HashMap;
use std::time::Duration;

use raft::log::log_actor::LogHandle;
use raft::raft::command::InspectResult;
use raft::raft::message::PeerMessage;
use raft::raft::node::NodeHandle;
use raft::raft::role::RaftRole;
use tokio::sync::mpsc;

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
        for &id in ids {
            let peers: Vec<u64> = ids.iter().copied().filter(|&p| p != id).collect();
            let peer_txs = peers.iter().map(|&p| (p, senders[&p].clone())).collect();
            handles.insert(
                id,
                NodeHandle::spawn(id, peers, LogHandle::spawn(), peer_txs),
            );
        }

        for (id, mut rx) in receivers {
            let handle = handles[&id].clone();
            tokio::spawn(async move {
                while let Some((from, msg)) = rx.recv().await {
                    handle.send_raft(from, msg).await;
                }
            });
        }

        Self { handles }
    }

    async fn inspect(&self, id: u64) -> InspectResult {
        self.handles[&id].inspect().await
    }

    /// Wait until exactly one node is leader and every node agrees on the term.
    async fn wait_for_leader(&self) -> (u64, u64) {
        let deadline = tokio::time::Instant::now() + ELECTION_DEADLINE;
        loop {
            let mut reports = Vec::new();
            for &id in self.handles.keys() {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_a_leader_and_commits_its_noop() {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let (leader_id, _) = cluster.wait_for_leader().await;

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
    let (leader_id, _) = cluster.wait_for_leader().await;

    let result = cluster.handles[&leader_id].propose(b"hello".to_vec()).await;
    assert!(result.accepted, "propose rejected: {result:?}");
    cluster.wait_until_committed(result.index.unwrap()).await;
}
