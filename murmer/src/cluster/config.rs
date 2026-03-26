use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

// =============================================================================
// NODE IDENTITY — uniquely identifies a node in the cluster
// =============================================================================

/// Identifies a node in the cluster. The `incarnation` field starts as a
/// random value at startup and is incremented monotonically by SWIM on
/// each refutation, allowing the protocol to distinguish between a node
/// that restarted (new random value) and one resolving a suspicion (increment).
#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub struct NodeIdentity {
    /// User-provided or auto-generated name (e.g. "worker-1")
    pub name: String,
    /// IP or hostname
    pub host: String,
    /// QUIC listen port
    pub port: u16,
    /// Random incarnation discriminator — changes on restart
    pub incarnation: u64,
}

impl NodeIdentity {
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            port,
            incarnation: rand::random(),
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        let ip: std::net::IpAddr = self.host.parse().unwrap_or([127, 0, 0, 1].into());
        SocketAddr::new(ip, self.port)
    }

    /// String used as node_id in OpLog / VersionVector / Receptionist.
    /// Includes the incarnation counter so restarted nodes get distinct IDs.
    pub fn node_id_string(&self) -> String {
        format!(
            "{}@{}:{}#{}",
            self.name, self.host, self.port, self.incarnation
        )
    }

    /// Returns just the "name@host:port" portion without incarnation.
    /// Useful for human-readable logging.
    pub fn display_id(&self) -> String {
        format!("{}@{}:{}", self.name, self.host, self.port)
    }
}

impl fmt::Display for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}:{}#{}",
            self.name, self.host, self.port, self.incarnation
        )
    }
}

impl PartialEq for NodeIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.host == other.host
            && self.port == other.port
            && self.incarnation == other.incarnation
    }
}

impl std::hash::Hash for NodeIdentity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.host.hash(state);
        self.port.hash(state);
        self.incarnation.hash(state);
    }
}

/// foca requires Identity to support `Renew` (bump incarnation) and provide
/// an address for SWIM protocol communication.
impl foca::Identity for NodeIdentity {
    type Addr = SocketAddr;

    fn addr(&self) -> SocketAddr {
        self.socket_addr()
    }

    fn renew(&self) -> Option<Self> {
        Some(Self {
            name: self.name.clone(),
            host: self.host.clone(),
            port: self.port,
            incarnation: self.incarnation.wrapping_add(1),
        })
    }

    /// When two identities share the same address, the one with the higher
    /// incarnation wins (most recently started node takes precedence).
    fn win_addr_conflict(&self, other: &Self) -> bool {
        self.incarnation > other.incarnation
    }
}

// =============================================================================
// NODE CLASS — typed node classification for placement decisions
// =============================================================================

/// Classifies a node's role in the cluster. Inspired by FoundationDB's ProcessClass.
///
/// The orchestrator uses this to decide which nodes are eligible for which actors.
/// For example, a `PlacementStrategy` might require actors to run only on `Worker`
/// nodes, or ensure the Coordinator runs on a `Coordinator`-class node.
///
/// # Custom classes
///
/// Use `Custom(String)` for domain-specific roles not covered by the built-in
/// variants (e.g., `Custom("gpu")`, `Custom("ingest")`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeClass {
    /// General-purpose actor host.
    #[default]
    Worker,
    /// Eligible for leader election and coordination duties.
    Coordinator,
    /// Client-facing node with reduced capability (e.g., edge proxy).
    Edge,
    /// User-defined role.
    Custom(String),
}

impl std::fmt::Display for NodeClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Worker => write!(f, "worker"),
            Self::Coordinator => write!(f, "coordinator"),
            Self::Edge => write!(f, "edge"),
            Self::Custom(name) => write!(f, "custom({name})"),
        }
    }
}

// =============================================================================
// TRANSPORT TUNING — QUIC transport parameters for actor messaging
// =============================================================================

/// QUIC transport parameters tuned for actor messaging workloads.
///
/// Quinn's defaults assume a **100 Mbps link with 100ms RTT** — typical
/// internet conditions. For LAN clustering (the primary murmer-rs use case),
/// these are far too conservative. The defaults here target **low-latency LAN**
/// with sub-millisecond RTT and many small messages.
///
/// # Example
///
/// ```rust,ignore
/// // Use defaults (tuned for LAN)
/// let config = ClusterConfig::builder()
///     .listen("127.0.0.1:0".parse().unwrap())
///     .cookie("secret")
///     .build()?;
///
/// // Custom tuning for WAN or specific requirements
/// let config = ClusterConfig::builder()
///     .listen("0.0.0.0:9000".parse().unwrap())
///     .cookie("secret")
///     .transport(TransportTuning {
///         initial_rtt_ms: 50,
///         max_idle_timeout_secs: 60,
///         keep_alive_interval_secs: Some(15),
///         max_concurrent_bidi_streams: 512,
///         ..Default::default()
///     })
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct TransportTuning {
    /// Initial RTT estimate in milliseconds.
    ///
    /// Controls how quickly the first packet loss is detected. Lower = faster
    /// loss detection, but too low can cause spurious retransmissions.
    ///
    /// Default: **1ms** (LAN). Set to 50-100ms for WAN deployments.
    pub initial_rtt_ms: u64,

    /// Maximum time a connection can remain idle before being closed, in seconds.
    ///
    /// Default: **30s**. Pair with `keep_alive_interval_secs` to prevent
    /// idle connections from being torn down prematurely.
    pub max_idle_timeout_secs: u64,

    /// Interval between keep-alive packets, in seconds. `None` disables keep-alives.
    ///
    /// Keep-alives serve two purposes: (1) prevent NAT/firewall timeouts on idle
    /// connections, and (2) detect dead peers faster than waiting for the idle timeout.
    ///
    /// Should be less than `max_idle_timeout_secs` to prevent idle disconnects.
    ///
    /// Default: **5s**.
    pub keep_alive_interval_secs: Option<u64>,

    /// Maximum concurrent bidirectional streams per connection.
    ///
    /// Each remote actor gets its own QUIC stream, so this limits how many
    /// actors on a single remote node can be communicated with simultaneously.
    ///
    /// Default: **1024**.
    pub max_concurrent_bidi_streams: u32,

    /// Per-stream receive window in bytes.
    ///
    /// How much data can be in-flight per stream before flow control kicks in.
    /// On LAN with sub-ms RTT, small windows are fine. Larger windows help
    /// on high-latency links.
    ///
    /// Default: **256 KiB** (LAN). Quinn default is ~1.2 MiB (internet).
    pub stream_receive_window: u32,

    /// Connection-level receive window in bytes.
    ///
    /// Aggregate flow control across all streams on a connection.
    ///
    /// Default: **2 MiB**.
    pub receive_window: u32,

    /// Connection-level send window in bytes.
    ///
    /// Default: **2 MiB**.
    pub send_window: u64,

    /// Initial MTU for QUIC packets.
    ///
    /// LAN environments can safely start higher than the QUIC minimum (1200).
    /// MTU discovery will probe for larger sizes.
    ///
    /// Default: **1452** (typical LAN ethernet).
    pub initial_mtu: u16,
}

impl Default for TransportTuning {
    fn default() -> Self {
        Self {
            initial_rtt_ms: 1,
            max_idle_timeout_secs: 30,
            keep_alive_interval_secs: Some(5),
            max_concurrent_bidi_streams: 1024,
            stream_receive_window: 256 * 1024, // 256 KiB
            receive_window: 2 * 1024 * 1024,   // 2 MiB
            send_window: 2 * 1024 * 1024,      // 2 MiB
            initial_mtu: 1452,
        }
    }
}

// =============================================================================
// CLUSTER CONFIG — builder pattern for cluster setup
// =============================================================================

/// How the cluster discovers peers.
#[derive(Debug, Clone, Default)]
pub enum Discovery {
    /// Use mDNS for zero-config discovery on the local network.
    Mdns { service_name: String },
    /// Connect to known seed node addresses.
    SeedNodes(Vec<SocketAddr>),
    /// Use both mDNS and seed nodes.
    Both {
        service_name: String,
        seed_nodes: Vec<SocketAddr>,
    },
    /// No automatic discovery — only manual connections.
    #[default]
    None,
}

/// Configuration for starting a clustered actor system.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub identity: NodeIdentity,
    pub listen_addr: SocketAddr,
    pub cookie: String,
    pub discovery: Discovery,
    /// This node's class — used by the orchestrator for placement decisions.
    pub node_class: NodeClass,
    /// Arbitrary key-value metadata describing this node's capabilities.
    /// Examples: `"region" = "us-west"`, `"gpu" = "true"`, `"rack" = "A3"`.
    pub node_metadata: HashMap<String, String>,
    /// QUIC transport parameters. Defaults are tuned for LAN actor messaging.
    pub transport: TransportTuning,
}

/// Builder for `ClusterConfig`.
pub struct ClusterConfigBuilder {
    name: Option<String>,
    listen_addr: Option<SocketAddr>,
    advertise_addr: Option<SocketAddr>,
    cookie: Option<String>,
    discovery: Discovery,
    node_class: NodeClass,
    node_metadata: HashMap<String, String>,
    transport: TransportTuning,
}

impl ClusterConfigBuilder {
    /// Create a new builder with default settings.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let config = ClusterConfigBuilder::new()
    ///     .name("worker-1")
    ///     .listen("0.0.0.0:9001".parse()?)
    ///     .cookie("my-cluster-secret")
    ///     .seed_nodes(vec!["192.168.1.10:9001".parse()?])
    ///     .build()?;
    /// ```
    pub fn new() -> Self {
        Self {
            name: None,
            listen_addr: None,
            advertise_addr: None,
            cookie: None,
            discovery: Discovery::default(),
            node_class: NodeClass::default(),
            node_metadata: HashMap::new(),
            transport: TransportTuning::default(),
        }
    }

    /// Set an explicit advertise address for this node.
    ///
    /// Use this when binding on `0.0.0.0` or a wildcard — the advertise
    /// address is what other nodes will use to connect back to us.
    /// If not set, the listen address is used.
    pub fn advertise(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.advertise_addr = Some(addr.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn listen(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.listen_addr = Some(addr.into());
        self
    }

    pub fn cookie(mut self, cookie: impl Into<String>) -> Self {
        self.cookie = Some(cookie.into());
        self
    }

    pub fn seed_nodes(mut self, seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        let seeds: Vec<SocketAddr> = seeds.into_iter().collect();
        self.discovery = match self.discovery {
            Discovery::Mdns { service_name } => Discovery::Both {
                service_name,
                seed_nodes: seeds,
            },
            _ => Discovery::SeedNodes(seeds),
        };
        self
    }

    pub fn discovery(mut self, discovery: Discovery) -> Self {
        self.discovery = discovery;
        self
    }

    /// Set this node's class for orchestrator placement decisions.
    pub fn node_class(mut self, class: NodeClass) -> Self {
        self.node_class = class;
        self
    }

    /// Add a single metadata key-value pair.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.node_metadata.insert(key.into(), value.into());
        self
    }

    /// Set all metadata at once (replaces any previously set metadata).
    pub fn metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.node_metadata = metadata;
        self
    }

    /// Set QUIC transport tuning parameters.
    ///
    /// Defaults are tuned for LAN actor messaging (sub-ms RTT, many small messages).
    /// Override for WAN deployments or specific requirements.
    pub fn transport(mut self, tuning: TransportTuning) -> Self {
        self.transport = tuning;
        self
    }

    /// Build the cluster config. Requires `listen` and `cookie` to be set.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let config = ClusterConfig::builder()
    ///     .listen("127.0.0.1:0".parse()?)
    ///     .cookie("secret")
    ///     .build()?;
    /// ```
    pub fn build(self) -> Result<ClusterConfig, &'static str> {
        let listen_addr = self.listen_addr.ok_or("listen address is required")?;
        let cookie = self.cookie.ok_or("cookie is required")?;

        // Use advertise_addr for the identity if set, otherwise fall back to listen_addr.
        // This matters when binding on 0.0.0.0 — the advertise addr should be routable.
        let peer_addr = self.advertise_addr.unwrap_or(listen_addr);

        let name = self.name.unwrap_or_else(|| {
            let host = peer_addr.ip();
            let port = peer_addr.port();
            format!("{host}-{port}")
        });

        let identity = NodeIdentity::new(name, peer_addr.ip().to_string(), peer_addr.port());

        Ok(ClusterConfig {
            identity,
            listen_addr,
            cookie,
            discovery: self.discovery,
            node_class: self.node_class,
            node_metadata: self.node_metadata,
            transport: self.transport,
        })
    }
}

impl Default for ClusterConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterConfig {
    /// Create a builder for constructing cluster configuration.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let config = ClusterConfig::builder()
    ///     .listen("127.0.0.1:9001".parse()?)
    ///     .cookie("secret")
    ///     .seed_nodes(vec!["192.168.1.10:9001".parse()?])
    ///     .build()?;
    /// ```
    pub fn builder() -> ClusterConfigBuilder {
        ClusterConfigBuilder::new()
    }
}
