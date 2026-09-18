use tokio::sync::{mpsc, oneshot};

use crate::log::{
    command::LogCommand,
    logs::{LogEntry, RaftLog},
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
                reply,
            } => {
                let ok = self.log.append(prev_log_index, prev_log_term, entries);
                let _ = reply.send(ok);
            }
            LogCommand::TruncateFrom { from_index, reply } => {
                let removed = self.log.truncate_from(from_index);
                let _ = reply.send(removed);
            }
            LogCommand::TruncateSuffix { upto_index, reply } => {
                let removed = self.log.truncate_suffix(upto_index);
                let _ = reply.send(removed);
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
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<LogCommand>) {
        while let Some(cmd) = rx.recv().await {
            self.handle(cmd);
        }
    }
}

#[derive(Clone)]
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
    ) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::Append {
                prev_log_index,
                prev_log_term,
                entries: entires,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn truncate_from(&self, from_index: u64) -> usize {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::TruncateFrom {
                from_index,
                reply: reply_tx,
            })
            .await
            .expect("LogActor has shut down");
        reply_rx.await.expect("LogActor dropped reply")
    }

    pub async fn truncate_suffix(&self, upto_index: u64) -> usize {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::TruncateSuffix {
                upto_index,
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
}
