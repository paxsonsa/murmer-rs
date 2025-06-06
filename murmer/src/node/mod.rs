use crate::cluster::ClusterId;
use crate::net::{self, NetworkAddrRef};
use crate::prelude::*;
use chrono::{DateTime, Utc};
use std::sync::Arc;

mod actor;
use actor::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipStatus {
    Pending,
    Up,
    Down,
    Joining,
    Leaving,
    Removed,
    Failed,
}

#[derive(Debug, Clone)]
pub enum Reachability {
    Pending,
    Reachable {
        // The number of consecutive heartbeat misses.
        misses: u32,
        // The last time the node's heartbeat/message was received.
        last_seen: DateTime<Utc>,
    },
    Unreachable {
        pings: u32,
        last_seen: DateTime<Utc>,
    },
}

impl PartialEq for Reachability {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Pending, Self::Pending) => true,
            (Self::Reachable { misses: s_m, .. }, Self::Reachable { misses: o_m, .. }) => {
                s_m == o_m
            }
            (Self::Unreachable { pings: s_p, .. }, Self::Unreachable { pings: o_p, .. }) => {
                s_p == o_p
            }
            _ => false,
        }
    }
}

impl Eq for Reachability {}

impl Reachability {
    pub fn reachable_now() -> Self {
        Reachability::Reachable {
            misses: 0,
            last_seen: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct NodeInfo {
    /// The name of the node.
    pub display_name: String,
    /// The network address of the node.
    pub addr: NetworkAddrRef,
}

impl NodeInfo {
    pub fn new(addr: NetworkAddrRef) -> Self {
        NodeInfo {
            display_name: format!("{}", addr),
            addr,
        }
    }

    fn new_with_name(name: impl Into<String>, addr: NetworkAddrRef) -> Self {
        NodeInfo {
            display_name: name.into(),
            addr,
        }
    }
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "node(name={}, addr={})", self.display_name, self.addr)
    }
}

impl From<NetworkAddrRef> for NodeInfo {
    fn from(addr: NetworkAddrRef) -> Self {
        NodeInfo::new(addr)
    }
}

#[derive(Clone)]
pub struct Node {
    pub id: Id,
    pub endpoint: Endpoint<NodeActor>,
}

impl Node {
    pub fn spawn(
        system: System,
        cluster_id: Arc<ClusterId>,
        node_info: NodeInfo,
        driver: Box<dyn net::ConnectionDriver>,
    ) -> Option<Node> {
        todo!("Implement Node::spawn");
    }
}

impl Eq for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("Member not connected")]
    NotConnected,

    #[error("Node network error: {0}")]
    NodeNetworkError(#[from] net::NetError),
}
