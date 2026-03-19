//! OpLog — operation log for distributed receptionist replication.
//!
//! The OpLog is a CRDT-inspired mechanism for keeping actor registries
//! consistent across cluster nodes:
//!
//! - Each node appends [`Op`]s (Register/Remove) to its local log
//! - Nodes exchange ops they haven't seen via [`VersionVector`]-based diffing
//! - The receptionist's [`apply_ops`](crate::Receptionist::apply_ops) method
//!   ingests remote ops and updates the local registry
//!
//! # Blip avoidance
//!
//! Short-lived actors (registered then immediately deregistered) can cause
//! unnecessary replication traffic. When configured with a `blip_window`,
//! the receptionist delays oplog commits — if the actor deregisters within
//! the window, no Register op is ever committed.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// =============================================================================
// OPLOG
// =============================================================================

/// Operation types recorded in the receptionist's operation log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    Register {
        label: String,
        actor_type_name: String,
        key_ids: Vec<String>,
        /// "host:port" of the node where the actor lives.
        origin_addr: String,
    },
    Remove {
        label: String,
    },
}

/// A single operation in the log, tagged with sequence number and originating node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Op {
    pub seq: u64,
    pub node_id: String,
    pub op_type: OpType,
}

/// The operation log. Each receptionist maintains its own log of local operations.
/// Peers exchange ops to converge on the same view of the actor registry.
pub(crate) struct OpLog {
    pub(crate) ops: Vec<Op>,
    next_seq: u64,
    node_id: String,
}

impl OpLog {
    pub(crate) fn new(node_id: String) -> Self {
        Self {
            ops: Vec::new(),
            next_seq: 1,
            node_id,
        }
    }

    pub(crate) fn append(&mut self, op_type: OpType) -> Op {
        let op = Op {
            seq: self.next_seq,
            node_id: self.node_id.clone(),
            op_type,
        };
        self.next_seq += 1;
        self.ops.push(op.clone());
        op
    }

    pub(crate) fn ops_since(&self, seq: u64) -> &[Op] {
        let start = self.ops.partition_point(|op| op.seq <= seq);
        &self.ops[start..]
    }

    pub(crate) fn latest_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }
}

// =============================================================================
// VERSION VECTOR — tracks per-node replication progress
// =============================================================================

/// Tracks the latest sequence number seen from each node.
/// Used to deduplicate operations during replication.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VersionVector {
    versions: HashMap<String, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.versions.get(node_id).copied().unwrap_or(0)
    }

    pub fn update(&mut self, node_id: &str, seq: u64) {
        let entry = self.versions.entry(node_id.to_string()).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (node_id, &seq) in &other.versions {
            self.update(node_id, seq);
        }
    }
}
