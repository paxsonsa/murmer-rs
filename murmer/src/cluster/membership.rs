use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use foca::{BincodeCodec, Foca, Notification, Runtime, Timer};
use rand::SeedableRng;
use tokio::sync::{broadcast, mpsc};

use super::config::NodeIdentity;

// =============================================================================
// CLUSTER EVENTS — observable state changes in the cluster
// =============================================================================

/// Events about the cluster that other subsystems can subscribe to.
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    NodeJoined(NodeIdentity),
    NodeLeft(NodeIdentity),
    NodeFailed(NodeIdentity),
    ActorRegistered {
        label: String,
        node_id: String,
        actor_type: String,
    },
    ActorDeregistered {
        label: String,
        node_id: String,
    },
}

// =============================================================================
// TIMER EVENTS — bridge foca timers into tokio
// =============================================================================

#[derive(Debug)]
pub struct TimerEvent {
    pub timer: Timer<NodeIdentity>,
    pub timer_id: u64,
}

// =============================================================================
// CLUSTER MEMBERSHIP — SWIM protocol via foca
// =============================================================================

/// Wraps foca to provide SWIM-based cluster membership.
pub struct ClusterMembership {
    pub foca: Foca<
        NodeIdentity,
        BincodeCodec<bincode::config::Configuration>,
        rand::rngs::StdRng,
        foca::NoCustomBroadcast,
    >,
    pub event_tx: broadcast::Sender<ClusterEvent>,
}

impl ClusterMembership {
    pub fn new(identity: NodeIdentity, event_tx: broadcast::Sender<ClusterEvent>) -> Self {
        let config = foca::Config::simple();
        let rng = rand::rngs::StdRng::from_os_rng();
        let foca = Foca::new(
            identity,
            config,
            rng,
            BincodeCodec(bincode::config::standard()),
        );

        Self { foca, event_tx }
    }
}

// =============================================================================
// FOCA RUNTIME — implements foca::Runtime for tokio integration
// =============================================================================

/// Bridges foca's synchronous `Runtime` trait into async tokio land.
///
/// - `notify()`: converts foca Notifications into ClusterEvents
/// - `send_to()`: queues SWIM bytes to be sent over control streams
/// - `submit_after()`: schedules timers via tokio tasks
pub struct FocaRuntime {
    event_tx: broadcast::Sender<ClusterEvent>,
    local_identity: NodeIdentity,
    /// Outbound SWIM data: (target_addr, data)
    swim_tx: mpsc::UnboundedSender<(NodeIdentity, Vec<u8>)>,
    /// Timer events sent back to the main loop
    timer_tx: mpsc::UnboundedSender<TimerEvent>,
    active_timers: HashMap<u64, tokio::task::JoinHandle<()>>,
    timer_id_counter: AtomicU64,
}

impl FocaRuntime {
    pub fn new(
        local_identity: NodeIdentity,
        event_tx: broadcast::Sender<ClusterEvent>,
        swim_tx: mpsc::UnboundedSender<(NodeIdentity, Vec<u8>)>,
        timer_tx: mpsc::UnboundedSender<TimerEvent>,
    ) -> Self {
        Self {
            event_tx,
            local_identity,
            swim_tx,
            timer_tx,
            active_timers: HashMap::new(),
            timer_id_counter: AtomicU64::new(0),
        }
    }

    pub fn cancel_all_timers(&mut self) {
        for (_, handle) in self.active_timers.drain() {
            handle.abort();
        }
    }
}

impl Runtime<NodeIdentity> for FocaRuntime {
    fn notify(&mut self, notification: Notification<'_, NodeIdentity>) {
        match notification {
            Notification::MemberUp(identity) => {
                if identity.socket_addr() != self.local_identity.socket_addr() {
                    tracing::info!("SWIM: node joined: {identity}");
                    let _ = self
                        .event_tx
                        .send(ClusterEvent::NodeJoined(identity.clone()));
                }
            }
            Notification::MemberDown(identity) => {
                tracing::info!("SWIM: node left/failed: {identity}");
                let _ = self.event_tx.send(ClusterEvent::NodeLeft(identity.clone()));
            }
            Notification::Defunct => {
                tracing::warn!("SWIM: local node declared defunct");
            }
            Notification::Active => {
                tracing::info!("SWIM: local node is active");
            }
            Notification::Idle => {
                tracing::trace!("SWIM: cluster idle");
            }
            _ => {
                tracing::trace!("SWIM: unhandled notification: {notification:?}");
            }
        }
    }

    fn send_to(&mut self, to: NodeIdentity, data: &[u8]) {
        tracing::trace!("SWIM: sending {} bytes to {to}", data.len());
        let _ = self.swim_tx.send((to, data.to_vec()));
    }

    fn submit_after(&mut self, event: Timer<NodeIdentity>, after: Duration) {
        let timer_id = self.timer_id_counter.fetch_add(1, Ordering::SeqCst);
        let timer_tx = self.timer_tx.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let _ = timer_tx.send(TimerEvent {
                timer: event,
                timer_id,
            });
        });

        self.active_timers.insert(timer_id, handle);
    }
}

impl Drop for FocaRuntime {
    fn drop(&mut self) {
        self.cancel_all_timers();
    }
}
