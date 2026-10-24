use tokio::sync::{mpsc, oneshot};

use crate::{
    error::append_error::AppendError,
    log::{
        command::LogCommand,
        raft_log::{Log, LogEntry, RaftLog},
    },
};

struct LogActor {
    log: RaftLog,
}

impl LogActor {
    fn new() -> Self {
        Self {
            log: RaftLog::new(),
        }
    }

    /// handle single command
    fn handle(&mut self, cmd: LogCommand) {
        match cmd {
            LogCommand::Append {
                prev_log_index,
                prev_log_term,
                entries,
                committed,
                reply,
            } => {
                let result = self
                    .log
                    .append(prev_log_index, prev_log_term, entries, committed);
                let _ = reply.send(result);
            }
            LogCommand::LastInfo { reply } => {
                let _ = reply.send((self.log.last_index(), self.log.last_term()));
            }
            LogCommand::Get { index, reply } => {
                let _ = reply.send(self.log.get(index).cloned());
            }
            LogCommand::Slice { from, to, reply } => {
                let _ = reply.send(self.log.slice(from, to));
            }
            LogCommand::Matches {
                prev_log_index,
                prev_log_term,
                reply,
            } => {
                let _ = reply.send(self.log.matches(prev_log_index, prev_log_term));
            }
            LogCommand::TermAt { index, reply } => {
                let _ = reply.send(self.log.term_at(index));
            }
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<LogCommand>) {
        while let Some(cmd) = rx.recv().await {
            self.handle(cmd);
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogHandle {
    tx: mpsc::Sender<LogCommand>,
}

impl LogHandle {
    pub fn spawn() -> Self {
        Self::spawn_with_capacity(1024)
    }

    pub fn spawn_with_capacity(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let actor = LogActor::new();
        tokio::spawn(actor.run(rx));
        Self { tx }
    }

    pub async fn append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        entires: Vec<LogEntry>,
        commit_index: u64,
    ) -> Result<(), AppendError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::Append {
                prev_log_index,
                prev_log_term,
                entries: entires,
                committed: commit_index,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn last_info(&self) -> (u64, u64) {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::LastInfo { reply: reply_tx })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn get(&self, index: u64) -> Option<LogEntry> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::Get {
                index,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn slice(&self, from: u64, to: u64) -> Vec<LogEntry> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::Slice {
                from,
                to,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn matches(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::Matches {
                prev_log_index,
                prev_log_term,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn term_at(&self, index: u64) -> Option<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::TermAt {
                index,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::append_error::AppendError,
        log::{log_actor::LogHandle, raft_log::LogEntry},
    };

    fn normal(i: u64, t: u64, s: &[u8]) -> LogEntry {
        LogEntry::normal(i, t, s.to_vec())
    }

    #[tokio::test]
    async fn spawn_add_append() {
        let log = LogHandle::spawn();
        assert_eq!(log.last_info().await, (0, 0));

        let result = log
            .append(0, 0, vec![normal(1, 1, b"a"), normal(2, 1, b"b")], 0)
            .await;
        assert!(result.is_ok());
        assert_eq!(log.last_info().await, (2, 1));
    }

    #[tokio::test]
    async fn append_conflict_overwrites() {
        let log = LogHandle::spawn();
        let _ = log
            .append(
                0,
                0,
                vec![normal(1, 1, b"a"), normal(2, 1, b"b"), normal(3, 1, b"c")],
                0,
            )
            .await;

        let result = log
            .append(1, 1, vec![normal(2, 2, b"B"), normal(3, 2, b"C")], 0)
            .await;
        assert!(result.is_ok());

        assert_eq!(log.get(2).await.unwrap().term, 2);
        assert_eq!(log.get(3).await.unwrap().term, 2);
        assert_eq!(log.last_info().await, (3, 2));
    }

    #[tokio::test]
    async fn append_preserves_concrete_errors_and_entries() {
        let log = LogHandle::spawn();
        let entries = vec![normal(1, 1, b"a"), normal(2, 1, b"b")];
        log.append(0, 0, entries.clone(), 0).await.unwrap();

        let cases = [
            (
                1,
                99,
                vec![normal(2, 2, b"B")],
                0,
                AppendError::LogMismatch {
                    prev_log_index: 1,
                    prev_log_term: 99,
                },
            ),
            (
                1,
                1,
                vec![normal(2, 2, b"B"), normal(4, 2, b"D")],
                0,
                AppendError::NonContiguous {
                    offset: 1,
                    expected: 3,
                    got: 4,
                },
            ),
            (
                1,
                1,
                vec![normal(2, 2, b"B")],
                2,
                AppendError::OverwriteCommitted {
                    index: 2,
                    committed: 2,
                },
            ),
        ];
        for (prev_index, prev_term, incoming, committed, expected) in cases {
            let error = log
                .append(prev_index, prev_term, incoming, committed)
                .await
                .unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(log.slice(1, 2).await, entries);
        }
    }

    #[tokio::test]
    async fn slice_actor() {
        let log = LogHandle::spawn();
        let _ = log
            .append(
                0,
                0,
                vec![normal(1, 1, b"a"), normal(2, 1, b"b"), normal(3, 1, b"c")],
                0,
            )
            .await;

        let s = log.slice(1, 2).await;
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].index, 1);
        assert_eq!(s[1].index, 2);
    }

    #[tokio::test]
    async fn matches_actor() {
        let log = LogHandle::spawn();
        let _ = log.append(0, 0, vec![normal(1, 1, b"a")], 0).await;

        assert!(log.matches(0, 0).await);
        assert!(log.matches(1, 1).await);
        assert!(!log.matches(1, 2).await);
    }

    #[tokio::test]
    async fn handle_is_clonable() {
        let log = LogHandle::spawn();
        let log2 = log.clone();

        let _ = log.append(0, 0, vec![normal(1, 1, b"a")], 0).await;
        assert_eq!(log2.last_info().await, (1, 1));
    }

    #[tokio::test]
    async fn actor_exits_when_all_handles_dropped() {
        let log = LogHandle::spawn();
        let log2 = log.clone();
        drop(log);
        drop(log2);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
