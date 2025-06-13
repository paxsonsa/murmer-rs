//! Error types for node operations

use crate::net;

/// Errors that can occur during NodeActor operations
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("Failed while initiating connection: {0}")]
    InitiationFailed(String),
    #[error("Connection error: {0}")]
    ConnectionError(#[from] net::ConnectionError),
    #[error("Network error: {0}")]
    NetworkError(#[from] net::NetError),
}

/// Errors that can occur during Node operations
#[derive(thiserror::Error, Debug)]
pub enum NodeCreationError {
    #[error("Member not connected")]
    NotConnected,

    #[error("Node network error: {0}")]
    NodeNetworkError(#[from] net::NetError),
    
    #[error("Connection error: {0}")]
    ConnectionError(#[from] net::ConnectionError),
    
    #[error("NodeActor error: {0}")]
    NodeActorError(#[from] NodeError),
}