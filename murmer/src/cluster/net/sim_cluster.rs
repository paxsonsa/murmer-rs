//! Ergonomic multi-node simulation harness (`feature = "sim"`).
//!
//! [`SimCluster`] wraps the three moving parts every multi-node sim test wires by
//! hand — the [`SimWorld`](crate::sim::SimWorld) driver, the
//! [`SimFabric`](super::sim::SimFabric) in-memory wire, and N booted
//! [`ClusterSystem`]s on their own shutdown tokens — behind one builder and a
//! small verb vocabulary: `mesh`, `pump`, `advance`, `crash`, `partition`,
//! `rejoin`, `events`. The primitives underneath ([`SimFabric::bind`],
//! [`ClusterSystem::start_with_net`], [`SimNet::partition`]) are unchanged; this
//! is pure ceremony reduction so a fault scenario reads as the fault, not the
//! plumbing.
//!
//! # Example
//!
//! ```rust,ignore
//! let mut cluster = SimCluster::builder(1).node("node-a").node("node-b").node("node-c").build();
//! cluster.mesh();   // inject a full mesh of discovery edges
//! cluster.pump();   // converge (MemberUp is synchronous on apply_many)
//!
//! cluster.crash("node-a");                       // cancel A's shutdown token
//! cluster.advance(Duration::from_secs(30));      // past foca's detection budget
//! let ev = cluster.events("node-b");             // drained since last call
//! assert!(ev.failed.contains("node-a-id"));
//! ```
//!
//! # Determinism
//!
//! Nodes are stored in insertion order and every iteration (mesh, broadcast,
//! event drain) walks that order or a sorted key set — never a `HashMap` — so the
//! harness adds no nondeterminism of its own. All time is virtual; `advance`
//! fires timers on the one shared [`SimRuntime`](crate::sim::SimRuntime).
//!
//! # What it does *not* hide
//!
//! - **Per-node shutdown tokens, always.** `crash` needs to stop one node without
//!   touching the others, so every node gets its own token (strictly more general
//!   than a shared one).
//! - **Event subscription is eager + drain-since-last.** Each node's
//!   [`subscribe_events`](ClusterSystem::subscribe_events) receiver is taken at
//!   boot (before any peer is injected, so nothing is missed) and held. `events`
//!   drains whatever has accumulated since the previous call — so a test discards
//!   the convergence phase by draining once after `pump`, then reads the failure
//!   phase by draining again after the fault. This mirrors the hand-written tests'
//!   "subscribe after convergence" trick without the ordering hazard.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::cluster::ClusterSystem;
use crate::cluster::config::{ClusterConfig, ClusterConfigBuilder, Discovery, NodeClass, NodeIdentity};
use crate::cluster::membership::ClusterEvent;
use crate::cluster::sync::{SpawnRegistry, TypeRegistry};
use crate::runtime::Runtime;
use crate::sim::SimWorld;

use super::sim::{SimFabric, SimNet};
use super::{Net, NodeId, PeerAddr};

/// First port handed to node 0; each subsequent node gets the next integer.
/// Distinct ports give distinct `socket_addr`s, which foca's `MemberUp` guard
/// requires to emit `NodeJoined` (see `config.rs` and the sim gotchas doc).
const BASE_PORT: u16 = 7001;

/// Install the rustls ring provider (idempotent). Needed only because the sim
/// [`ClusterConfig`] carries a throwaway `iroh::SecretKey` whose construction
/// touches the crypto provider; nothing in the sim path uses the network. Mirrors
/// the `install_crypto` helper the hand-written tests called.
fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A [`ClusterConfig`] for a sim node: built through the normal builder, then its
/// iroh-derived identity is replaced with the synthetic sim `identity`. Discovery
/// is `None` (mDNS/seed are real-network paths; the harness drives topology via
/// [`inject_discovered`](ClusterSystem::inject_discovered)). The leftover
/// iroh-only fields (secret key, cookie, listen addr, allowlist) are never read by
/// [`start_with_net`](ClusterSystem::start_with_net).
fn sim_config(name: &str, identity: NodeIdentity) -> ClusterConfig {
    let mut config = ClusterConfigBuilder::new()
        .name(name)
        .secret_key(iroh::SecretKey::generate())
        .listen("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .cookie("sim")
        .discovery(Discovery::None)
        .build()
        .unwrap();
    config.identity = identity;
    config
}

/// The membership events a node's foca reported since the last drain, partitioned
/// into the three a fault test cares about. Joined carries full identities (so a
/// caller can read the incarnation, e.g. to assert a rejoin came back at a higher
/// one); failed/pruned are keyed by `endpoint_id` (the bare id string the
/// hand-written assertions used).
#[derive(Default, Debug, Clone)]
pub struct DrainedEvents {
    /// `NodeJoined` identities, in arrival order.
    pub joined: Vec<NodeIdentity>,
    /// Endpoint ids declared `NodeFailed`.
    pub failed: BTreeSet<String>,
    /// Endpoint ids `NodePruned` from the registry.
    pub pruned: BTreeSet<String>,
}

impl DrainedEvents {
    /// The set of endpoint ids that came up — the joined-identity analogue of
    /// [`failed`](Self::failed)/[`pruned`](Self::pruned), for set assertions.
    pub fn joined_ids(&self) -> BTreeSet<String> {
        self.joined.iter().map(|id| id.endpoint_id.0.clone()).collect()
    }

    /// True if any failure detector fired — the "did the cluster react" check.
    pub fn any_failed(&self) -> bool {
        !self.failed.is_empty()
    }
}

/// One simulated node: its booted system, the concrete [`SimNet`] (for partition
/// injection, a sim-only fault not on the [`Net`] trait), its own shutdown token,
/// the held event receiver, and the identity coordinates needed to rebuild it on
/// `rejoin`.
struct Node {
    /// Cluster display name (e.g. `"node-a"`) and `events`/lookup key.
    name: String,
    /// Synthetic endpoint id (e.g. `"node-a-id"`).
    id: String,
    /// The node's distinct port (drives foca's distinct-`socket_addr` guard).
    port: u16,
    /// Current incarnation; bumped on `rejoin` so a returning node outranks its
    /// own down instance (foca's `win_addr_conflict`).
    incarnation: u64,
    system: ClusterSystem,
    net: Arc<SimNet>,
    shutdown: CancellationToken,
    /// Taken at boot, drained by [`SimCluster::events`].
    events: broadcast::Receiver<ClusterEvent>,
    #[cfg(feature = "app")]
    coordinator: Option<crate::endpoint::Endpoint<crate::app::coordinator::Coordinator>>,
}

/// Builder for a [`SimCluster`]: name the nodes, optionally attach app-layer
/// Coordinators, then `build`.
pub struct SimClusterBuilder {
    seed: u64,
    /// Node names in insertion order; ids/ports are derived (`<name>-id`,
    /// `BASE_PORT + index`).
    names: Vec<String>,
    #[cfg(feature = "app")]
    coordinators: bool,
    #[cfg(feature = "app")]
    shared_source: Option<Arc<dyn crate::app::singleton::GenerationSource>>,
}

impl SimClusterBuilder {
    /// Add one node named `name`. Its endpoint id is `"<name>-id"` and its port is
    /// `BASE_PORT + (index)`, so `.node("node-a").node("node-b").node("node-c")`
    /// reproduces the canonical `node-a/b/c` @ `7001/2/3` spec.
    pub fn node(mut self, name: impl Into<String>) -> Self {
        self.names.push(name.into());
        self
    }

    /// Add `count` nodes named `node-a`, `node-b`, … (a-z; panics past 26, which no
    /// sim scenario approaches). Convenience over repeated [`node`](Self::node).
    pub fn nodes(mut self, count: usize) -> Self {
        assert!(count <= 26, "nodes(count): only a..z are auto-named");
        for i in 0..count {
            let letter = (b'a' + i as u8) as char;
            self.names.push(format!("node-{letter}"));
        }
        self
    }

    /// Attach an app-layer [`Coordinator`](crate::app::coordinator::Coordinator) to
    /// every node, each with its OWN in-RAM `GenerationSource` (the per-node ticket
    /// printer — single-node-correct, the split-brain-prone multi-node default).
    #[cfg(feature = "app")]
    pub fn with_coordinators(mut self) -> Self {
        self.coordinators = true;
        self
    }

    /// Attach a Coordinator to every node, all minting from ONE shared
    /// `GenerationSource` (the durable-store path: a single linearization point
    /// both sides can reach). Implies [`with_coordinators`](Self::with_coordinators).
    #[cfg(feature = "app")]
    pub fn shared_generation_source(
        mut self,
        source: Arc<dyn crate::app::singleton::GenerationSource>,
    ) -> Self {
        self.coordinators = true;
        self.shared_source = Some(source);
        self
    }

    /// Boot every node on one shared [`SimRuntime`], returning a driveable cluster.
    /// No discovery edges are injected yet — call [`mesh`](SimCluster::mesh) (or
    /// [`dial`](SimCluster::dial)) then [`pump`](SimCluster::pump) to converge.
    pub fn build(self) -> SimCluster {
        install_crypto();
        let world = SimWorld::new(self.seed);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);

        let mut cluster = SimCluster {
            world,
            fabric,
            rt,
            nodes: Vec::with_capacity(self.names.len()),
            #[cfg(feature = "app")]
            coordinators: self.coordinators,
            #[cfg(feature = "app")]
            shared_source: self.shared_source,
        };

        for (i, name) in self.names.iter().enumerate() {
            let id = format!("{name}-id");
            let port = BASE_PORT + i as u16;
            let node = cluster.boot_node(name.clone(), id, port, 1);
            cluster.nodes.push(node);
        }
        cluster
    }
}

/// A driveable multi-node simulation: N [`ClusterSystem`]s on one virtual clock,
/// wired over an in-memory [`SimFabric`], with fault injection (`crash`,
/// `partition`, `rejoin`) and event draining. See the module docs.
pub struct SimCluster {
    world: SimWorld,
    fabric: SimFabric,
    rt: Arc<dyn Runtime>,
    /// Insertion order — the deterministic iteration order for mesh/events.
    nodes: Vec<Node>,
    #[cfg(feature = "app")]
    coordinators: bool,
    #[cfg(feature = "app")]
    shared_source: Option<Arc<dyn crate::app::singleton::GenerationSource>>,
}

impl SimCluster {
    /// Start a builder seeded with `seed`. The same seed reproduces the same
    /// schedule, timer firings, and randomness across the whole cluster.
    pub fn builder(seed: u64) -> SimClusterBuilder {
        SimClusterBuilder {
            seed,
            names: Vec::new(),
            #[cfg(feature = "app")]
            coordinators: false,
            #[cfg(feature = "app")]
            shared_source: None,
        }
    }

    /// Boot one full `ClusterSystem` (and, if configured, its Coordinator) on the
    /// shared runtime over a freshly-bound `SimNet`. Subscribes the event receiver
    /// before returning so no event is missed.
    fn boot_node(&self, name: String, id: String, port: u16, incarnation: u64) -> Node {
        let identity =
            NodeIdentity::new_seeded(&name, NodeId(id.clone()), "127.0.0.1", port, incarnation);
        let shutdown = CancellationToken::new();
        let (sim_net, incoming_rx, conn_events_rx) = self.fabric.bind(
            identity.clone(),
            NodeClass::Worker,
            std::collections::HashMap::new(),
            shutdown.clone(),
        );
        let net: Arc<dyn Net> = sim_net.clone();
        let system = ClusterSystem::start_with_net(
            sim_config(&name, identity),
            TypeRegistry::new(),
            SpawnRegistry::new(),
            Arc::clone(&self.rt),
            net,
            incoming_rx,
            conn_events_rx,
            shutdown.clone(),
        );
        let events = system.subscribe_events();

        #[cfg(feature = "app")]
        let coordinator = if self.coordinators {
            Some(self.attach_coordinator(&system))
        } else {
            None
        };

        Node {
            name,
            id,
            port,
            incarnation,
            system,
            net: sim_net,
            shutdown,
            events,
            #[cfg(feature = "app")]
            coordinator,
        }
    }

    /// Start an app-layer Coordinator on `system` (bridge + election + placement),
    /// wiring the shared `GenerationSource` if one was configured, else the default
    /// per-node in-RAM source.
    #[cfg(feature = "app")]
    fn attach_coordinator(
        &self,
        system: &ClusterSystem,
    ) -> crate::endpoint::Endpoint<crate::app::coordinator::Coordinator> {
        use crate::app::bridge;
        use crate::app::coordinator::CoordinatorState;
        use crate::app::election::OldestNode;
        use crate::app::placement::LeastLoaded;

        let mut cstate = CoordinatorState::new(
            system.identity().node_id_string(),
            Box::new(LeastLoaded),
            Box::new(OldestNode::any()),
        );
        if let Some(src) = &self.shared_source {
            cstate = cstate.with_generation_source(Arc::clone(src));
        }
        bridge::start_coordinator(system, cstate)
    }

    /// Index of the node named `name`. Panics with a clear message on an unknown
    /// name (a test typo should fail loudly, not silently no-op).
    fn index(&self, name: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.name == name)
            .unwrap_or_else(|| {
                let known: Vec<_> = self.nodes.iter().map(|n| n.name.as_str()).collect();
                panic!("SimCluster: no node named {name:?} (have {known:?})")
            })
    }

    // ── topology ─────────────────────────────────────────────────────────────

    /// Inject a full mesh of discovery edges: one directed dial per unordered pair
    /// (`i → j` for every `i < j`). Each handshake makes *both* endpoints apply the
    /// other as alive, so one edge per pair suffices — no gossip round needed.
    /// Call [`pump`](Self::pump) afterward to converge.
    pub fn mesh(&self) {
        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                self.dial_idx(i, j);
            }
        }
    }

    /// Inject a single directed discovery edge `from → to` (e.g. to re-attach a
    /// rejoined node to the survivors). The reverse direction is learned via the
    /// handshake.
    pub fn dial(&self, from: &str, to: &str) {
        self.dial_idx(self.index(from), self.index(to));
    }

    fn dial_idx(&self, from: usize, to: usize) {
        self.nodes[from].system.inject_discovered(PeerAddr {
            id: self.nodes[to].system.identity().endpoint_id.clone(),
            hint: vec![],
        });
    }

    // ── driving the clock ────────────────────────────────────────────────────

    /// Run every ready task to quiescence without advancing virtual time. The way
    /// to converge membership after `mesh` (MemberUp is synchronous on foca's
    /// connect-time `apply_many`, so no clock advance is needed).
    pub fn pump(&mut self) {
        self.world.pump();
    }

    /// Advance virtual time by `by`, firing timers along the way. Used to cross
    /// foca's failure-detection budget after a `crash`/`partition`
    /// (probe 1.5s + suspect_to_down 3s ⇒ 30s is comfortable at any seed).
    pub fn advance(&mut self, by: Duration) {
        self.world.advance(by);
    }

    /// Drive the world until `fut` completes, advancing time as needed. The way to
    /// `send` into an actor/coordinator and get the reply (see
    /// [`coordinator`](Self::coordinator)).
    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: std::future::Future + 'static,
    {
        self.world.block_on(fut)
    }

    /// Current virtual time since the cluster started.
    pub fn now(&self) -> Duration {
        self.world.now()
    }

    /// Draw a deterministic `u64` from the cluster's seeded RNG (reproducible test
    /// choices).
    pub fn rng_u64(&self) -> u64 {
        self.world.rng_u64()
    }

    /// Derive an independent reproducible seed for `label` off the root seed.
    pub fn derive_seed(&self, label: &str) -> u64 {
        self.world.derive_seed(label)
    }

    // ── faults ───────────────────────────────────────────────────────────────

    /// Crash `name`: cancel its shutdown token. Its event loop, control
    /// readers/writers, accept loop, and foca timer manager all stop — the node
    /// goes silent without broadcasting a Departure (an abrupt crash, not a
    /// graceful leave). Survivors detect it via SWIM timeout once the clock is
    /// advanced past the detection budget. Use [`rejoin`](Self::rejoin) to bring it
    /// back.
    pub fn crash(&self, name: &str) {
        self.nodes[self.index(name)].shutdown.cancel();
    }

    /// Sever the link `a ↔ b` (byte-level partition). Cancels the connection's
    /// shared liveness token, so BOTH directions' streams fail while both nodes
    /// keep running. Returns `false` if there is no live connection from `a` to
    /// `b` (e.g. they never meshed). One call cuts both directions.
    pub fn partition(&self, a: &str, b: &str) -> bool {
        let ia = self.index(a);
        let ib = self.index(b);
        let peer_key = self.nodes[ib].system.identity().node_id_string();
        self.nodes[ia].net.partition(&peer_key)
    }

    /// Bring a crashed node back as the SAME endpoint id at a strictly-higher
    /// incarnation, re-bound on the fabric with a fresh shutdown token and event
    /// receiver. The old (cancelled) system is dropped. Does NOT re-establish
    /// links — call [`dial`](Self::dial)/[`mesh`](Self::mesh) afterward, then
    /// [`advance`](Self::advance) so foca's `win_addr_conflict` readmits it over
    /// its own down instance.
    pub fn rejoin(&mut self, name: &str) {
        let i = self.index(name);
        let (name, id, port, next_inc) = {
            let n = &self.nodes[i];
            (n.name.clone(), n.id.clone(), n.port, n.incarnation + 1)
        };
        self.nodes[i] = self.boot_node(name, id, port, next_inc);
    }

    // ── observation ──────────────────────────────────────────────────────────

    /// Drain `name`'s membership events accumulated since the last call,
    /// partitioned into joined/failed/pruned. Drain-since-last is the key to phase
    /// isolation: drain once after `pump` to discard convergence joins, then drain
    /// again after a fault to read just the failure phase.
    pub fn events(&mut self, name: &str) -> DrainedEvents {
        let i = self.index(name);
        let mut out = DrainedEvents::default();
        loop {
            match self.nodes[i].events.try_recv() {
                Ok(ClusterEvent::NodeJoined(id)) => out.joined.push(id),
                Ok(ClusterEvent::NodeFailed(id)) => {
                    out.failed.insert(id.endpoint_id.0);
                }
                Ok(ClusterEvent::NodePruned(id)) => {
                    out.pruned.insert(id.endpoint_id.0);
                }
                Ok(_) => {}
                // Empty / Closed / Lagged all mean "nothing more to read now".
                Err(_) => break,
            }
        }
        out
    }

    // ── accessors ────────────────────────────────────────────────────────────

    /// The booted system for `name` (start actors, look them up, inspect identity).
    pub fn system(&self, name: &str) -> &ClusterSystem {
        &self.nodes[self.index(name)].system
    }

    /// The concrete `SimNet` for `name` (rarely needed directly — `partition` is
    /// the usual entry point).
    pub fn net(&self, name: &str) -> &Arc<SimNet> {
        &self.nodes[self.index(name)].net
    }

    /// `name`'s current identity (endpoint id, incarnation, socket addr).
    pub fn identity(&self, name: &str) -> &NodeIdentity {
        self.nodes[self.index(name)].system.identity()
    }

    /// The bare endpoint id string for `name` (e.g. `"node-a-id"`) — the key
    /// `events` reports failures/prunes under.
    pub fn endpoint_id(&self, name: &str) -> String {
        self.nodes[self.index(name)].system.identity().endpoint_id.0.clone()
    }

    /// The app Coordinator endpoint for `name`. Panics if the cluster was built
    /// without coordinators (a configuration error, surfaced loudly).
    #[cfg(feature = "app")]
    pub fn coordinator(
        &self,
        name: &str,
    ) -> crate::endpoint::Endpoint<crate::app::coordinator::Coordinator> {
        self.nodes[self.index(name)]
            .coordinator
            .clone()
            .unwrap_or_else(|| panic!("SimCluster: node {name:?} has no Coordinator (build with .with_coordinators())"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Boot a converged full mesh of three nodes and return the cluster, ready for
    /// fault injection. Convergence is pump-only (MemberUp is synchronous on
    /// `apply_many`), so no clock advance is needed here.
    fn converged_trio(seed: u64) -> SimCluster {
        let mut c = SimCluster::builder(seed)
            .node("node-a")
            .node("node-b")
            .node("node-c")
            .build();
        c.mesh();
        c.pump();
        c
    }

    #[test]
    fn builder_assigns_distinct_ids_and_ports() {
        let c = SimCluster::builder(1).nodes(3).build();
        // nodes(3) auto-names node-a/b/c with derived ids and ascending ports.
        assert_eq!(c.endpoint_id("node-a"), "node-a-id");
        assert_eq!(c.identity("node-a").port, BASE_PORT);
        assert_eq!(c.identity("node-b").port, BASE_PORT + 1);
        assert_eq!(c.identity("node-c").port, BASE_PORT + 2);
        // Distinct socket addrs are what foca's MemberUp guard needs.
        assert_ne!(c.identity("node-a").socket_addr(), c.identity("node-b").socket_addr());
    }

    #[test]
    fn full_mesh_converges_deterministically() {
        // Every node sees the other two come up — the converged SET, asserted
        // identically across seeds (event ordering is unseeded; membership is not).
        let expect = |c: &mut SimCluster, me: &str, peers: [&str; 2]| {
            let joined = c.events(me).joined_ids();
            let want: BTreeSet<String> =
                peers.iter().map(|p| format!("{p}-id")).collect();
            assert_eq!(joined, want, "{me} converged set");
        };
        for seed in [1u64, 2, 0xC0FFEE] {
            let mut c = converged_trio(seed);
            expect(&mut c, "node-a", ["node-b", "node-c"]);
            expect(&mut c, "node-b", ["node-a", "node-c"]);
            expect(&mut c, "node-c", ["node-a", "node-b"]);
        }
    }

    #[test]
    fn crash_is_detected_failed_and_pruned_exactly() {
        let only_a = BTreeSet::from(["node-a-id".to_string()]);
        for seed in [1u64, 2, 0xC0FFEE] {
            let mut c = converged_trio(seed);
            // Discard the convergence joins so the next drain is the failure phase.
            let _ = (c.events("node-b"), c.events("node-c"));

            c.crash("node-a");
            c.advance(Duration::from_secs(30));

            for survivor in ["node-b", "node-c"] {
                let ev = c.events(survivor);
                assert_eq!(ev.failed, only_a, "{survivor} fails exactly A (seed {seed})");
                assert_eq!(ev.pruned, only_a, "{survivor} prunes exactly A (seed {seed})");
            }
        }
    }

    #[test]
    fn single_link_partition_is_masked_by_indirect_probing() {
        for seed in [1u64, 2, 0xC0FFEE] {
            let mut c = converged_trio(seed);
            for n in ["node-a", "node-b", "node-c"] {
                let _ = c.events(n); // drain convergence
            }
            assert!(c.partition("node-a", "node-b"), "A–B link is live");
            c.advance(Duration::from_secs(30));
            for n in ["node-a", "node-b", "node-c"] {
                assert!(
                    !c.events(n).any_failed(),
                    "{n} saw a failure — a single A–B cut must be masked by C (seed {seed})"
                );
            }
        }
    }

    #[test]
    fn full_isolation_is_detected_by_and_detects_all_peers() {
        let (a, b, cc) = ("node-a-id".to_string(), "node-b-id".to_string(), "node-c-id".to_string());
        for seed in [1u64, 2, 0xC0FFEE] {
            let mut c = converged_trio(seed);
            for n in ["node-a", "node-b", "node-c"] {
                let _ = c.events(n);
            }
            // Cut both of A's links; B–C stays healthy.
            assert!(c.partition("node-a", "node-b"));
            assert!(c.partition("node-a", "node-c"));
            c.advance(Duration::from_secs(30));

            assert_eq!(c.events("node-a").failed, BTreeSet::from([b.clone(), cc.clone()]));
            assert_eq!(c.events("node-b").failed, BTreeSet::from([a.clone()]));
            assert_eq!(c.events("node-c").failed, BTreeSet::from([a.clone()]));
        }
    }

    #[test]
    fn events_drain_is_phase_isolated() {
        let mut c = converged_trio(1);
        // First drain: the convergence phase (B saw A and C come up).
        let conv = c.events("node-b");
        assert_eq!(conv.joined_ids(), BTreeSet::from(["node-a-id".into(), "node-c-id".into()]));
        assert!(conv.failed.is_empty());

        // Second drain with no new events is empty (drain-since-last consumed them).
        assert!(c.events("node-b").joined.is_empty());

        // Fault phase: only the failure shows up now.
        c.crash("node-a");
        c.advance(Duration::from_secs(30));
        let fault = c.events("node-b");
        assert!(fault.joined.is_empty(), "no spurious joins in the fault phase");
        assert_eq!(fault.failed, BTreeSet::from(["node-a-id".to_string()]));
    }

    #[test]
    fn crashed_node_rejoins_at_higher_incarnation() {
        let mut c = converged_trio(1);
        for n in ["node-a", "node-b", "node-c"] {
            let _ = c.events(n);
        }
        // Crash A; B and C detect it failed.
        c.crash("node-a");
        c.advance(Duration::from_secs(30));
        let _ = (c.events("node-b"), c.events("node-c")); // clear the failure phase

        // A returns as itself at incarnation 2 and re-dials the survivors.
        c.rejoin("node-a");
        assert_eq!(c.identity("node-a").incarnation, 2, "rejoin bumps the incarnation");
        c.dial("node-a", "node-b");
        c.dial("node-a", "node-c");
        c.advance(Duration::from_secs(30));

        // B and C readmit the returned A at incarnation 2 (higher incarnation wins).
        let readmitted = |ev: &DrainedEvents| {
            ev.joined.iter().any(|id| id.endpoint_id.0 == "node-a-id" && id.incarnation == 2)
        };
        assert!(readmitted(&c.events("node-b")), "B readmits A@2");
        assert!(readmitted(&c.events("node-c")), "C readmits A@2");
    }
}
