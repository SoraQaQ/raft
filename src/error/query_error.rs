use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    NodeUnavailable,
    ResponseDropped,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::NodeUnavailable => write!(f, "raft node is unavailable"),
            QueryError::ResponseDropped => write!(f, "raft node dropped local query response"),
        }
    }
}

impl std::error::Error for QueryError {}
