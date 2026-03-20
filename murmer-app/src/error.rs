//! Error types for the orchestrator layer.

/// Errors that can occur during orchestrator operations.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("no eligible nodes for placement: {reason}")]
    NoEligibleNodes { reason: String },

    #[error("spec already exists: {label}")]
    SpecAlreadyExists { label: String },

    #[error("spec not found: {label}")]
    SpecNotFound { label: String },

    #[error("spawn failed on node {node_id}: {error}")]
    SpawnFailed { node_id: String, error: String },

    #[error("not the current leader")]
    NotLeader,
}
