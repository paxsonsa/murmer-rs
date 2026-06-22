//! Cluster singletons — exactly-one, anchor-pinned, fenced-handoff actors.
//!
//! A *cluster singleton* is an actor of which there must be **exactly one**
//! instance across the whole cluster (e.g. a catalog owner, a sequence minter).
//! It generalizes the Coordinator's own leader election + `is_leader` into a
//! reusable primitive: the singleton is pinned to an [`SingletonAnchor`]
//! (the leader, the oldest node of a class, or a specific node), and ownership
//! moves between nodes through a **fenced** stop-old → await-stopped → start-new
//! handoff so that two instances never run concurrently.
//!
//! # The fence: `(term, seq)` generations
//!
//! Every ownership grant carries a monotone [`SingletonGeneration`]. It is a
//! lexicographic `(term, seq)` pair (an FDB-style *recovery epoch*):
//!
//! - `term` bumps **once per ownership change** (node departure, anchor move,
//!   leader change). This is the recovery epoch.
//! - `seq` is a per-term monotone counter for repeated grants to the *same*
//!   owner (liveness renew / idempotent re-place).
//!
//! Because a successor always bumps `term` during handoff, a stale ex-owner
//! necessarily holds a strictly-lower `(term, seq)` than its successor. The
//! generation packs into a single `u64` ([`SingletonGeneration::packed`]) so a
//! downstream fence that compares a single integer (e.g. appdata's on-disk
//! `HEAD.writer_gen` advance-or-reject) fences the stale owner with **no change
//! to that comparison** — only *who mints* the generation changes.
//!
//! # Pluggable [`GenerationSource`]
//!
//! Minting is behind the [`GenerationSource`] trait so the authoritative store
//! is swappable: a single-owner external store (e.g. appdata's catalog) today,
//! a consensus-minted source later — without touching the downstream fence.
//! The default [`CoordinatorGenerationSource`] keeps generations in RAM and is
//! suitable only for single-node deployments and tests (it is **not** durable
//! across a Coordinator restart).
//!
//! Enable with `murmer = { features = ["app"] }`.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::app::election::{LeaderElection, OldestNode};
use crate::app::node_info::ClusterView;
use crate::cluster::config::NodeClass;

// =============================================================================
// SINGLETON ANCHOR
// =============================================================================

/// Where the unique instance is pinned. Generalizes the Coordinator's own
/// `is_leader` notion into per-singleton ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SingletonAnchor {
    /// Owner = whatever `election.elect(&cluster_view)` currently points at —
    /// the generalization of the Coordinator's own leader.
    Leader,
    /// Owner = oldest alive node of this class (reuses `OldestNode::with_class`).
    Class(NodeClass),
    /// Owner = exactly this node id (maps to `PlacementConstraints.required_node_id`).
    Node(String),
}

// =============================================================================
// SINGLETON GENERATION — the fence token
// =============================================================================

/// Monotone fence token minted per successful ownership grant.
///
/// Lexicographic `(term, seq)`: `term` bumps once per ownership change (recovery
/// epoch, FDB-style); `seq` is a per-term monotone counter for repeated claims
/// under one owner. A stale ex-owner necessarily holds a strictly-lower
/// `(term, seq)` than any successor.
///
/// The derived [`Ord`] compares `term` first, then `seq` (field declaration
/// order) — i.e. exactly lexicographic order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SingletonGeneration {
    /// Recovery epoch — bumps once per ownership change.
    pub term: u64,
    /// Per-term monotone re-grant counter.
    pub seq: u64,
}

impl SingletonGeneration {
    /// Pack into a single `u64` that a downstream fence can compare numerically:
    /// `term` in the high 32 bits, `seq` in the low 32 bits. While both fields
    /// stay within `u32` range, numeric comparison of the packed value equals
    /// lexicographic comparison of `(term, seq)`.
    pub fn packed(self) -> u64 {
        (self.term << 32) | (self.seq & 0xFFFF_FFFF)
    }

    /// Inverse of [`packed`](Self::packed).
    pub fn from_packed(packed: u64) -> Self {
        Self {
            term: packed >> 32,
            seq: packed & 0xFFFF_FFFF,
        }
    }
}

// =============================================================================
// SINGLETON SPEC + OWNERSHIP
// =============================================================================

/// Declarative request to run exactly one instance of an actor cluster-wide.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingletonSpec {
    /// Stable singleton label (e.g. `"catalog"`).
    pub label: String,
    /// `SpawnRegistry` key identifying the actor type to spawn.
    pub actor_type_name: String,
    /// Serialized initial state (`MigratableActor` boot bytes).
    pub initial_state: Vec<u8>,
    /// Where the unique instance is pinned.
    pub anchor: SingletonAnchor,
    /// How long to await the old instance's stopped-ack before force-proceeding
    /// with the new owner. `None` = wait indefinitely (safest for writers — a
    /// stuck old owner blocks failover but can never double-write).
    pub drain_timeout: Option<Duration>,
}

impl SingletonSpec {
    /// Create a spec with no drain timeout (wait indefinitely on handoff).
    pub fn new(
        label: impl Into<String>,
        actor_type_name: impl Into<String>,
        anchor: SingletonAnchor,
    ) -> Self {
        Self {
            label: label.into(),
            actor_type_name: actor_type_name.into(),
            initial_state: Vec::new(),
            anchor,
            drain_timeout: None,
        }
    }

    /// Set the serialized initial (boot) state.
    pub fn with_state(mut self, state: Vec<u8>) -> Self {
        self.initial_state = state;
        self
    }

    /// Set a bounded drain timeout for the handoff await-stopped barrier.
    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = Some(timeout);
        self
    }
}

/// Phase of a singleton's lifecycle during (re)placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SingletonPhase {
    /// The owner is running and serving.
    Active,
    /// Handoff in progress: the old owner has been asked to stop and we are
    /// awaiting its stopped-ack (or the drain timeout) before starting the new.
    Draining,
    /// The old owner has stopped; the new owner is being spawned.
    Starting,
}

/// Authoritative ownership record for a singleton.
///
/// The durable copy lives in the [`GenerationSource`]'s backing store (for the
/// default it is RAM; for appdata it is the catalog — one linearization point).
/// The Coordinator holds this as a soft, rebuildable cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingletonOwnership {
    /// The singleton label this record is for.
    pub label: String,
    /// The node currently granted ownership, or `None` while handing off.
    pub owner_node_id: Option<String>,
    /// The current granted generation.
    pub generation: SingletonGeneration,
    /// The current lifecycle phase.
    pub phase: SingletonPhase,
}

// =============================================================================
// OWNER RESOLUTION
// =============================================================================

/// Resolve the node that should own a singleton given its anchor and the
/// current cluster view. Returns `None` if no eligible node exists.
///
/// - [`SingletonAnchor::Leader`] reuses the supplied [`LeaderElection`].
/// - [`SingletonAnchor::Class`] reuses [`OldestNode`] ordering restricted to
///   that class (so it matches how the Coordinator itself would elect within a
///   class).
/// - [`SingletonAnchor::Node`] pins exactly to that node, but only while it is
///   alive in the view.
pub fn resolve_owner(
    anchor: &SingletonAnchor,
    view: &ClusterView,
    election: &dyn LeaderElection,
) -> Option<String> {
    match anchor {
        SingletonAnchor::Leader => election.elect(view),
        SingletonAnchor::Class(class) => OldestNode::with_class(class.clone()).elect(view),
        SingletonAnchor::Node(node_id) => view
            .alive_nodes()
            .find(|n| &n.node_id() == node_id)
            .map(|n| n.node_id()),
    }
}

// =============================================================================
// GENERATION SOURCE
// =============================================================================

/// A full singleton record in a [`GenerationSource`]: its spec plus the latest
/// ownership grant (if any). Returned by [`GenerationSource::list`] so a
/// newly-elected leader can rebuild the managed set from a shared/durable
/// backend (closing the "amnesia" gap where a new leader doesn't know which
/// singletons exist).
#[derive(Debug, Clone)]
pub struct SingletonRecord {
    pub spec: SingletonSpec,
    pub ownership: Option<SingletonOwnership>,
}

/// Pluggable authority that mints and persists [`SingletonGeneration`]s — the
/// **coordination backend**. It owns two durable facts per singleton: the
/// monotone fence generation (so two owners can never hold an equal token) and
/// the spec (so a new leader can rebuild the managed set).
///
/// Keeping this behind a trait lets the authoritative store be swapped — the
/// in-RAM default (single node / tests), a durable shared store both nodes reach
/// (e.g. a file-backed store, or appdata's catalog) for multi-node, or a
/// consensus-minted source (Raft, later) — **without** changing the downstream
/// fence that compares the packed generation. Correctness across multiple nodes
/// comes from the backend being one authority, so it works at 1, 2, or N nodes
/// with no quorum among the nodes themselves.
///
/// Implementations must guarantee that, for a given `label`, every successful
/// `claim_term` returns a strictly greater `term` than any previously returned
/// generation, and that `seq` is strictly increasing within a term.
#[async_trait::async_trait]
pub trait GenerationSource: Send + Sync {
    /// Begin a new ownership epoch for `label` granted to `owner_node_id`:
    /// bump `term`, reset `seq` to 0, and persist `(owner, term, 0)`. Returns
    /// the newly minted generation.
    async fn claim_term(
        &self,
        label: &str,
        owner_node_id: &str,
    ) -> Result<SingletonGeneration, String>;

    /// Re-grant to the current owner within the current term (liveness renew /
    /// idempotent re-place): bump `seq`. Returns the new generation.
    async fn claim_seq(&self, label: &str) -> Result<SingletonGeneration, String>;

    /// Read the current authoritative ownership for `label`. `None` if never
    /// claimed.
    async fn current(&self, label: &str) -> Result<Option<SingletonOwnership>, String>;

    /// Persist a singleton's spec, so a newly-elected leader can rebuild the full
    /// managed set (not just its ownership). Idempotent.
    ///
    /// Default: a no-op. A source that does not persist specs simply offers no
    /// leader-rebuild — a new leader stays amnesiac about singletons it did not
    /// place itself. The in-RAM and durable sources override this.
    async fn put_spec(&self, _label: &str, _spec: &SingletonSpec) -> Result<(), String> {
        Ok(())
    }

    /// List every known singleton (spec + latest ownership) — the leader-rebuild
    /// read. Default: empty (no rebuild). Sources that persist specs override it.
    async fn list(&self) -> Result<Vec<SingletonRecord>, String> {
        Ok(Vec::new())
    }
}

/// One stored singleton in the in-RAM source: its spec (set by `put_spec`) and
/// its latest ownership grant (set by `claim_term`/`claim_seq`). Either may be
/// absent depending on call order, though the Coordinator always claims a term
/// and then persists the spec when placing.
#[derive(Debug, Default, Clone)]
struct StoredSingleton {
    spec: Option<SingletonSpec>,
    ownership: Option<SingletonOwnership>,
}

/// Default in-RAM [`GenerationSource`].
///
/// As a **per-node** source (each Coordinator holds its own) it is correct only
/// for single-node deployments and tests: it is not durable across a Coordinator
/// restart, and two nodes' separate copies do not share monotonicity, so a
/// multi-node write deployment must inject a single shared/durable source (e.g. a
/// file-backed store, or appdata's catalog). Shared by reference (`Arc`) across
/// Coordinators in tests, this same type models that single durable store.
///
/// Uses a `BTreeMap` so `list` iterates in a deterministic (sorted-label) order.
#[derive(Debug, Default)]
pub struct CoordinatorGenerationSource {
    state: Mutex<BTreeMap<String, StoredSingleton>>,
}

impl CoordinatorGenerationSource {
    /// Create an empty in-RAM generation source.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl GenerationSource for CoordinatorGenerationSource {
    async fn claim_term(
        &self,
        label: &str,
        owner_node_id: &str,
    ) -> Result<SingletonGeneration, String> {
        let mut state = self.state.lock().expect("generation source mutex poisoned");
        // term 0 is reserved for "never claimed"; first grant is term 1.
        let term = state
            .get(label)
            .and_then(|s| s.ownership.as_ref())
            .map(|o| o.generation.term + 1)
            .unwrap_or(1);
        let generation = SingletonGeneration { term, seq: 0 };
        // Upsert the ownership, preserving any already-persisted spec.
        state.entry(label.to_string()).or_default().ownership = Some(SingletonOwnership {
            label: label.to_string(),
            owner_node_id: Some(owner_node_id.to_string()),
            generation,
            phase: SingletonPhase::Active,
        });
        Ok(generation)
    }

    async fn claim_seq(&self, label: &str) -> Result<SingletonGeneration, String> {
        let mut state = self.state.lock().expect("generation source mutex poisoned");
        match state.get_mut(label).and_then(|s| s.ownership.as_mut()) {
            Some(ownership) => {
                ownership.generation.seq += 1;
                Ok(ownership.generation)
            }
            None => Err(format!(
                "claim_seq before claim_term for singleton '{label}'"
            )),
        }
    }

    async fn current(&self, label: &str) -> Result<Option<SingletonOwnership>, String> {
        Ok(self
            .state
            .lock()
            .expect("generation source mutex poisoned")
            .get(label)
            .and_then(|s| s.ownership.clone()))
    }

    async fn put_spec(&self, label: &str, spec: &SingletonSpec) -> Result<(), String> {
        self.state
            .lock()
            .expect("generation source mutex poisoned")
            .entry(label.to_string())
            .or_default()
            .spec = Some(spec.clone());
        Ok(())
    }

    async fn list(&self) -> Result<Vec<SingletonRecord>, String> {
        Ok(self
            .state
            .lock()
            .expect("generation source mutex poisoned")
            .values()
            .filter_map(|s| {
                s.spec.clone().map(|spec| SingletonRecord {
                    spec,
                    ownership: s.ownership.clone(),
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::node_info::NodeInfo;
    use std::collections::HashMap;
    use crate::cluster::config::NodeIdentity;

    fn make_node(name: &str, incarnation: u64, class: NodeClass) -> NodeInfo {
        NodeInfo::new(
            NodeIdentity::for_test(name, incarnation),
            class,
            HashMap::new(),
        )
    }

    fn view_of(nodes: Vec<NodeInfo>) -> ClusterView {
        let mut view = ClusterView::new();
        for n in nodes {
            view.upsert_node(n);
        }
        view
    }

    #[test]
    fn test_packed_preserves_lexicographic_order_across_u32_boundary() {
        // The whole point of the fence: a higher term ALWAYS outranks any seq.
        let lo = SingletonGeneration {
            term: 0,
            seq: u32::MAX as u64,
        };
        let hi = SingletonGeneration { term: 1, seq: 0 };
        // Ord (lexicographic) and packed numeric compare must agree.
        assert!(hi > lo);
        assert!(hi.packed() > lo.packed());
        assert_eq!(hi.packed(), 1u64 << 32);
        assert_eq!(lo.packed(), u32::MAX as u64);
    }

    #[test]
    fn test_packed_roundtrip() {
        let g = SingletonGeneration { term: 7, seq: 42 };
        assert_eq!(SingletonGeneration::from_packed(g.packed()), g);
    }

    #[test]
    fn test_ord_is_term_major() {
        let a = SingletonGeneration { term: 2, seq: 0 };
        let b = SingletonGeneration {
            term: 2,
            seq: 1_000_000,
        };
        let c = SingletonGeneration { term: 3, seq: 0 };
        assert!(a < b); // same term, higher seq
        assert!(b < c); // higher term beats any seq
        assert!(a < c);
    }

    #[test]
    fn test_resolve_owner_leader_matches_election() {
        let view = view_of(vec![
            make_node("gamma", 300, NodeClass::Worker),
            make_node("alpha", 100, NodeClass::Worker),
            make_node("beta", 200, NodeClass::Worker),
        ]);
        let election = OldestNode::any();
        let owner = resolve_owner(&SingletonAnchor::Leader, &view, &election).unwrap();
        assert_eq!(owner, election.elect(&view).unwrap());
        assert!(
            owner.contains(
                &crate::cluster::config::NodeIdentity::test_endpoint_id("alpha").to_string()
            ),
            "oldest should win, got {owner}"
        );
    }

    #[test]
    fn test_resolve_owner_class_picks_oldest_of_class() {
        let view = view_of(vec![
            // alpha is oldest overall but a Worker
            make_node("alpha", 100, NodeClass::Worker),
            // beta is younger but the oldest Coordinator
            make_node("beta", 200, NodeClass::Coordinator),
            make_node("delta", 300, NodeClass::Coordinator),
        ]);
        let election = OldestNode::any();
        let owner = resolve_owner(
            &SingletonAnchor::Class(NodeClass::Coordinator),
            &view,
            &election,
        )
        .unwrap();
        assert!(
            owner.contains(
                &crate::cluster::config::NodeIdentity::test_endpoint_id("beta").to_string()
            ),
            "oldest Coordinator should win, got {owner}"
        );
    }

    #[test]
    fn test_resolve_owner_node_pins_exactly_and_requires_alive() {
        let target = make_node("beta", 200, NodeClass::Worker);
        let target_id = target.node_id();
        let view = view_of(vec![make_node("alpha", 100, NodeClass::Worker), target]);
        let election = OldestNode::any();

        // Pin to an alive node → exactly that node, even though alpha is older.
        let owner =
            resolve_owner(&SingletonAnchor::Node(target_id.clone()), &view, &election).unwrap();
        assert_eq!(owner, target_id);

        // Pin to a non-existent node → None (never silently relocates).
        assert!(
            resolve_owner(
                &SingletonAnchor::Node("ghost@127.0.0.1:9999#0".into()),
                &view,
                &election,
            )
            .is_none()
        );
    }

    #[test]
    fn test_resolve_owner_node_pin_rejects_dead_node() {
        let target = make_node("beta", 200, NodeClass::Worker);
        let target_id = target.node_id();
        let mut view = view_of(vec![make_node("alpha", 100, NodeClass::Worker), target]);
        view.mark_failed(&target_id);
        let election = OldestNode::any();
        assert!(resolve_owner(&SingletonAnchor::Node(target_id), &view, &election).is_none());
    }

    #[tokio::test]
    async fn test_ram_source_claim_term_increments_and_resets_seq() {
        let src = CoordinatorGenerationSource::new();

        let g1 = src.claim_term("catalog", "node-a").await.unwrap();
        assert_eq!(g1, SingletonGeneration { term: 1, seq: 0 });

        // Re-grant to same owner bumps seq within the term.
        let g1b = src.claim_seq("catalog").await.unwrap();
        assert_eq!(g1b, SingletonGeneration { term: 1, seq: 1 });

        // New ownership epoch bumps term and resets seq, strictly outranking g1b.
        let g2 = src.claim_term("catalog", "node-b").await.unwrap();
        assert_eq!(g2, SingletonGeneration { term: 2, seq: 0 });
        assert!(g2 > g1b);

        // current() reflects the latest grant.
        let cur = src.current("catalog").await.unwrap().unwrap();
        assert_eq!(cur.owner_node_id.as_deref(), Some("node-b"));
        assert_eq!(cur.generation, g2);
        assert_eq!(cur.phase, SingletonPhase::Active);
    }

    #[tokio::test]
    async fn test_ram_source_claim_seq_before_term_errors() {
        let src = CoordinatorGenerationSource::new();
        assert!(src.claim_seq("never-claimed").await.is_err());
    }

    #[tokio::test]
    async fn test_ram_source_current_unknown_is_none() {
        let src = CoordinatorGenerationSource::new();
        assert!(src.current("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_ram_source_put_spec_and_list_for_leader_rebuild() {
        let src = CoordinatorGenerationSource::new();

        // A claim with no persisted spec is invisible to `list` (rebuild only
        // surfaces singletons whose spec is known).
        let _ = src.claim_term("catalog", "node-a").await.unwrap();
        assert!(src.list().await.unwrap().is_empty());

        // Persist the spec → list returns it with the latest ownership.
        let spec = SingletonSpec::new("catalog", "T", SingletonAnchor::Leader);
        src.put_spec("catalog", &spec).await.unwrap();
        let records = src.list().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].spec.label, "catalog");
        assert_eq!(
            records[0]
                .ownership
                .as_ref()
                .and_then(|o| o.owner_node_id.as_deref()),
            Some("node-a")
        );

        // A later claim bumps the term but the spec is preserved — exactly what a
        // new leader needs to rebuild (spec) + fence (higher term).
        let g2 = src.claim_term("catalog", "node-b").await.unwrap();
        let records = src.list().await.unwrap();
        assert_eq!(records[0].ownership.as_ref().unwrap().generation, g2);
        assert_eq!(g2.term, 2);
        assert_eq!(records[0].spec.label, "catalog");
    }
}
