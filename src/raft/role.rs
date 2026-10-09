#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Default)]
pub enum RaftRole {
    #[default]
    Follower,
    Candidate,
    Leader,
}

impl RaftRole {
    pub fn is_ledaer(self) -> bool {
        matches!(self, RaftRole::Leader)
    }

    pub fn is_follower(self) -> bool {
        matches!(self, RaftRole::Follower)
    }

    pub fn is_candidate(self) -> bool {
        matches!(self, RaftRole::Candidate)
    }
}
