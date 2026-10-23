//! Election countdown and heartbeat cadence.

use rand::Rng;

use super::{ELECTION_TIMEOUT_MAX_TICKS, ELECTION_TIMEOUT_MIN_TICKS, RaftNode};

impl RaftNode {
    /// Restart the countdown and pick a fresh random election timeout.
    pub(super) fn reset_timer(&mut self) {
        self.timer_elapsed = 0;
        self.election_timeout =
            rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN_TICKS..=ELECTION_TIMEOUT_MAX_TICKS);
    }

    /// One clock tick: the leader sends heartbeats, everyone else runs
    /// the election countdown.
    pub(super) async fn handle_tick(&mut self) {
        if self.is_leader() {
            self.broadcast_heartbeat().await;
            return;
        }

        self.timer_elapsed += 1;
        if self.timer_elapsed >= self.election_timeout {
            self.become_candidate().await;
        }
    }
}
