//! Edge client for connecting to a murmer cluster.
//!
//! [`MurmerClient`] connects to a cluster node as a lightweight Edge client
//! and provides access to public actors without running any actors itself.
//! Edge clients do not participate in cluster gossip (SWIM) and receive
//! only public actor registrations via pull-based sync.
//!
//! # When to use
//!
//! Use `MurmerClient` when you want to:
//! - Front actor services with a REST/gRPC API gateway
//! - Build CLI tools that interact with a running cluster
//! - Connect monitoring dashboards to live actor state
//! - Write integration tests that exercise actors from outside the cluster
//!
//! # Example
//!
//! ```rust,ignore
//! use murmer::client::{MurmerClient, ClientOptions};
//! use std::time::Duration;
//!
//! // Short-lived: connect, use, disconnect
//! let client = MurmerClient::connect("10.0.0.5:9000".parse()?, "cluster-cookie").await?;
//! let ep = client.lookup::<UserService>("api/users").unwrap();
//! let user = ep.send(GetUser { id: 42 }).await?;
//!
//! // Long-lived gateway: refresh every 30 seconds
//! let client = MurmerClient::connect_with_options(
//!     "10.0.0.5:9000".parse()?,
//!     "cluster-cookie".into(),
//!     ClientOptions {
//!         sync_interval: Some(Duration::from_secs(30)),
//!         ..Default::default()
//!     },
//! ).await?;
//!
//! // Wait for an actor to become available (e.g., registered after connect)
//! let ep = client
//!     .lookup_wait::<UserService>("api/users", Duration::from_secs(5))
//!     .await?;
//! ```
//!
//! # Visibility
//!
//! Edge clients only discover actors registered as [`crate::receptionist::Visibility`]`::Public`.
//! Use [`crate::System::start_public()`] on the cluster side to expose actors to Edge clients.
//!
//! # Scalability
//!
//! Edge clients use pull-based sync — the server only responds to `RegistrySyncRequest`
//! messages, never fans out to all connected clients. 1000 idle Edge clients add
//! near-zero server overhead. Version vectors ensure pull responses are deltas after
//! the first sync.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::actor::Actor;
use crate::cluster::config::{NodeClass, TransportTuning};
use crate::cluster::error::ClusterError;
use crate::cluster::framing::ControlMessage;
use crate::cluster::membership::ClusterEvent;
use crate::cluster::sync::{TypeRegistry, apply_remote_ops};
use crate::cluster::transport::{Transport, run_control_stream_reader};
use crate::endpoint::Endpoint;
use crate::receptionist::{ActorEvent, Receptionist, ReceptionistConfig};

// =============================================================================
// ClientOptions
// =============================================================================

/// Options for [`MurmerClient::connect_with_options`].
#[derive(Default)]
pub struct ClientOptions {
    /// Type registry for creating remote actor endpoints.
    ///
    /// Defaults to [`TypeRegistry::from_auto()`], which auto-discovers all
    /// actor types annotated with `#[handlers]` via the linkme slice.
    pub type_registry: Option<TypeRegistry>,

    /// QUIC transport tuning parameters.
    ///
    /// Defaults to LAN-optimized settings via [`TransportTuning::default()`].
    pub transport_tuning: TransportTuning,

    /// Periodic pull interval.
    ///
    /// - `None` — pull once on connect only (default; ideal for short-lived clients,
    ///   CLI tools, and tests)
    /// - `Some(d)` — re-pull every `d` to pick up new actor registrations (ideal for
    ///   long-lived gateways and dashboards)
    pub sync_interval: Option<Duration>,
}

// =============================================================================
// MurmerClient
// =============================================================================

/// A lightweight Edge client that connects to a murmer cluster and accesses public actors.
///
/// See the [module documentation](self) for usage examples and design rationale.
pub struct MurmerClient {
    receptionist: Receptionist,
    transport: Arc<Transport>,
    #[allow(dead_code)]
    type_registry: Arc<TypeRegistry>,
    server_node_id: String,
    shutdown: CancellationToken,
}

impl MurmerClient {
    /// Connect to a cluster node as an Edge client.
    ///
    /// Immediately sends a `RegistrySyncRequest` to pull the current set of
    /// public actors. Use [`lookup`](Self::lookup) after connecting, or
    /// [`lookup_wait`](Self::lookup_wait) if the actor may not be registered yet.
    pub async fn connect(
        addr: SocketAddr,
        cookie: impl Into<String>,
    ) -> Result<Self, ClusterError> {
        Self::connect_with_options(addr, cookie.into(), ClientOptions::default()).await
    }

    /// Connect with custom options.
    pub async fn connect_with_options(
        addr: SocketAddr,
        cookie: String,
        options: ClientOptions,
    ) -> Result<Self, ClusterError> {
        let shutdown = CancellationToken::new();
        let type_registry = Arc::new(
            options
                .type_registry
                .unwrap_or_else(TypeRegistry::from_auto),
        );

        let (transport, _conn_events) = Transport::connect_only(
            cookie,
            NodeClass::Edge,
            HashMap::new(),
            options.transport_tuning,
            shutdown.clone(),
        )
        .await?;

        let ic = transport.connect(addr).await?;
        let server_node_id = ic.remote_identity.node_id_string();

        let local_addr = transport.local_addr();
        let receptionist = Receptionist::with_config(ReceptionistConfig {
            node_id: local_addr.to_string(),
            origin_addr: local_addr.to_string(),
            ..Default::default()
        });

        // No-op broadcast — nobody subscribes to cluster events from this edge client,
        // but apply_remote_ops requires the sender to emit ActorRegistered events.
        let (event_tx, _) = broadcast::channel::<ClusterEvent>(16);

        spawn_edge_event_loop(
            ic.control_recv,
            Arc::clone(&transport),
            receptionist.clone(),
            Arc::clone(&type_registry),
            server_node_id.clone(),
            event_tx,
            options.sync_interval,
            shutdown.clone(),
        );

        // Trigger immediate pull to populate the local receptionist with public actors
        let vv = receptionist.version_vector();
        if let Err(e) = transport
            .send_control(&server_node_id, ControlMessage::RegistrySyncRequest(vv))
            .await
        {
            tracing::warn!("Failed to send initial sync request to {server_node_id}: {e}");
        }

        Ok(Self {
            receptionist,
            transport,
            type_registry,
            server_node_id,
            shutdown,
        })
    }

    /// Look up a public actor by label.
    ///
    /// Returns `None` if no public actor with this label has been synced yet.
    /// Use [`lookup_wait`](Self::lookup_wait) to block until the actor appears.
    pub fn lookup<A: Actor + 'static>(&self, label: &str) -> Option<Endpoint<A>> {
        self.receptionist.lookup(label)
    }

    /// Wait for a public actor to become available.
    ///
    /// Subscribes to receptionist events, triggers an immediate pull, then
    /// waits for an `ActorRegistered` event for the given label (or finds it
    /// already present after the pull). Re-polls the server every 500 ms so
    /// that actors registered *after* the initial pull are discovered even
    /// without a periodic `sync_interval` configured.
    ///
    /// **Fast path**: if the actor is already synced, returns after one pull
    /// round-trip (typically sub-millisecond on LAN).
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Timeout`] if the actor does not appear within
    /// the given duration.
    pub async fn lookup_wait<A: Actor + 'static>(
        &self,
        label: &str,
        timeout: Duration,
    ) -> Result<Endpoint<A>, ClusterError> {
        // Fast path: already synced
        if let Some(ep) = self.receptionist.lookup::<A>(label) {
            return Ok(ep);
        }

        // Subscribe before the pull to avoid missing events that arrive while we pull
        let mut events = self.receptionist.subscribe_events();

        let vv = self.receptionist.version_vector();
        if let Err(e) = self
            .transport
            .send_control(
                &self.server_node_id,
                ControlMessage::RegistrySyncRequest(vv),
            )
            .await
        {
            tracing::warn!("lookup_wait: failed to send sync request: {e}");
        }

        let deadline = tokio::time::Instant::now() + timeout;

        // Re-poll the server periodically in case the actor registers after
        // our initial pull (the server never pushes unsolicited updates).
        let mut poll_ticker = tokio::time::interval(Duration::from_millis(500));
        poll_ticker.tick().await; // skip immediate tick — initial pull already sent

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ClusterError::Timeout(format!(
                    "actor '{label}' not available after {timeout:?}"
                )));
            }

            tokio::select! {
                result = tokio::time::timeout(remaining, events.recv()) => {
                    match result {
                        Ok(Some(ActorEvent::Registered { label: ref l, .. })) if l == label => {
                            // This label was just registered — try the typed lookup now
                            if let Some(ep) = self.receptionist.lookup::<A>(label) {
                                return Ok(ep);
                            }
                            // Label registered but type doesn't match — keep waiting
                        }
                        Ok(Some(_)) => {
                            // Some other registration event — check our label anyway
                            if let Some(ep) = self.receptionist.lookup::<A>(label) {
                                return Ok(ep);
                            }
                        }
                        Ok(None) => {
                            return Err(ClusterError::Timeout(format!(
                                "event channel closed while waiting for '{label}'"
                            )));
                        }
                        Err(_elapsed) => {
                            return Err(ClusterError::Timeout(format!(
                                "actor '{label}' not available after {timeout:?}"
                            )));
                        }
                    }
                }
                _ = poll_ticker.tick() => {
                    // Re-pull in case the actor registered after our initial request
                    let vv = self.receptionist.version_vector();
                    if let Err(e) = self
                        .transport
                        .send_control(&self.server_node_id, ControlMessage::RegistrySyncRequest(vv))
                        .await
                    {
                        tracing::debug!("lookup_wait: re-poll failed: {e}");
                    }
                }
            }
        }
    }

    /// Disconnect from the cluster gracefully.
    ///
    /// Closes the QUIC connection to the server node and cancels the
    /// background event loop.
    pub async fn disconnect(self) {
        self.transport.remove_connection(&self.server_node_id).await;
        self.shutdown.cancel();
    }

    /// Returns `true` if the client has not been disconnected.
    pub fn is_connected(&self) -> bool {
        !self.shutdown.is_cancelled()
    }
}

// =============================================================================
// EDGE EVENT LOOP
// =============================================================================

/// Spawns the Edge client's event loop.
///
/// The loop is intentionally minimal compared to the cluster event loop:
/// - Processes `RegistrySync` ops from the server (the only relevant message)
/// - Optionally re-pulls on a timer for long-lived clients
/// - Ignores all cluster-specific messages (SWIM, spawn, departure)
#[allow(clippy::too_many_arguments)]
fn spawn_edge_event_loop(
    control_recv: quinn::RecvStream,
    transport: Arc<Transport>,
    receptionist: Receptionist,
    type_registry: Arc<TypeRegistry>,
    server_node_id: String,
    event_tx: broadcast::Sender<ClusterEvent>,
    sync_interval: Option<Duration>,
    shutdown: CancellationToken,
) {
    let (control_in_tx, mut control_in_rx) = mpsc::unbounded_channel::<(String, ControlMessage)>();

    tokio::spawn(run_control_stream_reader(
        control_recv,
        control_in_tx,
        server_node_id.clone(),
        shutdown.clone(),
    ));

    // Optional periodic pull — runs in its own task so the main loop stays clean
    let (sync_tick_tx, mut sync_tick_rx) = mpsc::unbounded_channel::<()>();
    if let Some(interval) = sync_interval {
        let tx = sync_tick_tx;
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.tick().await; // skip the immediate first tick; initial pull already sent
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        if tx.send(()).is_err() {
                            break;
                        }
                    }
                    _ = shutdown_clone.cancelled() => break,
                }
            }
        });
    }

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some((_node_id, msg)) = control_in_rx.recv() => {
                    if let ControlMessage::RegistrySync(ops) = msg {
                        apply_remote_ops(
                            ops,
                            &receptionist,
                            &type_registry,
                            &server_node_id,
                            &event_tx,
                            &transport,
                        );
                    }
                    // All other control messages (SWIM, SpawnActor, Departure, etc.)
                    // are cluster-internal and irrelevant to Edge clients — ignore them.
                }
                Some(()) = sync_tick_rx.recv() => {
                    let vv = receptionist.version_vector();
                    if let Err(e) = transport
                        .send_control(&server_node_id, ControlMessage::RegistrySyncRequest(vv))
                        .await
                    {
                        tracing::debug!("Edge periodic sync failed: {e}");
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    });
}
