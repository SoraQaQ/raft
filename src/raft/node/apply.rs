//! Applying committed entries to the state machine.

use super::RaftNode;

impl RaftNode {
    /// Feed every committed but not yet applied entry to the state machine.
    ///
    /// Entries are taken in one batch and applied in index order; the node
    /// actor is single threaded, so reads never observe a partially applied
    /// prefix.
    pub(super) async fn advance_apply(&mut self) {
        if self.state.last_applied >= self.state.commit_index {
            return;
        }

        let entries = self
            .log
            .slice(self.state.last_applied + 1, self.state.commit_index)
            .await;

        for entry in entries {
            self.state.last_applied = entry.index;
            // The leader's no-op entry carries no application data, but it
            // still advances the applied index.
            if entry.is_empty() {
                continue;
            }
            self.state_machine.apply(entry.index, &entry);
        }
    }
}
