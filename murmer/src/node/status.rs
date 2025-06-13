//! Node status types and state management

/// Represents the reachability status of a node in the cluster.
/// Indicates whether the node is reachable or unreachable based on heartbeat checks.
/// The reachability status is used to determine if the node is healthy enough to communicate with.
#[derive(Debug, Clone)]
pub enum ReachabilityStatus {
    /// The node's reachability is currently unknown or not yet determined.
    Pending,
    /// The node is reachable and has been successfully communicating.
    Reachable {
        /// The node is reachable and can be communicated with.
        last_seen: chrono::DateTime<chrono::Utc>,
        /// The number of missed heartbeats since the last successful communication.
        /// After a certain threshold, the node may be considered unreachable until it responds
        /// again.
        missed_heartbeats: u32,
    },
    /// The node is unreachable and cannot be communicated with.
    Unreachable {
        /// The node is unreachable and cannot be communicated with.
        last_seen: chrono::DateTime<chrono::Utc>,
        /// The number of successful heartbeats since the last missed heartbeat.
        /// After a certain threshold, the node may be considered reachable again.
        successful_heartbeat: u32,
    },
}

impl ReachabilityStatus {
    /// Get the last time the node was seen, if available
    pub fn last_seen_time(&self) -> chrono::DateTime<chrono::Utc> {
        match self {
            ReachabilityStatus::Reachable { last_seen, .. } => *last_seen,
            ReachabilityStatus::Unreachable { last_seen, .. } => *last_seen,
            ReachabilityStatus::Pending => chrono::Utc::now(), // Default to now for pending nodes
        }
    }
}

/// The lifecycle state of a NodeActor
#[derive(Debug)]
pub enum NodeState {
    /// Node is initiating an outbound connection
    Initiating,
    /// Node is accepting an inbound connection
    Accepting,
    /// Node is running and processing messages
    Running,
    /// Node has been stopped gracefully
    Stopped,
    /// Node has failed with an error
    Failed {
        /// The reason for the failure, if available.
        reason: String,
    },
}

/// Cluster membership status of a node
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipStatus {
    /// Node membership is pending determination
    Pending,
    /// Node is up and active in the cluster
    Up,
    /// Node is down or unavailable
    Down,
    /// Node is in the process of joining
    Joining,
    /// Node is in the process of leaving
    Leaving,
    /// Node has been removed from the cluster
    Removed,
    /// Node has failed
    Failed,
}