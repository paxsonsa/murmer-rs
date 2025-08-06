use uuid::Uuid;
use serde::{Serialize, Deserialize};

/// Unique identifier for a node in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    /// Generate a new random node ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    
    /// Create from existing UUID (for persistence/config)
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
    
    /// Get the inner UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}