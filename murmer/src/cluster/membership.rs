//! SWIM-based cluster membership via the `foca` crate.
//!
//! [`ClusterMembership`] wraps a `Foca` instance and translates its
//! protocol notifications into [`ClusterEvent`]s that drive the rest
//! of the cluster system (connection management, receptionist sync, etc.).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use foca::{BincodeCodec, Foca, Notification, Runtime, Timer};
use futures_util::StreamExt;
use rand::SeedableRng;
use tokio::sync::{broadcast, mpsc};
use tokio_util::time::DelayQueue;

use crate::instrument;

use super::config::NodeIdentity;

// =============================================================================
// CLUSTER EVENTS — observable state changes in the cluster
// =============================================================================

/// Events about the cluster that other subsystems can subscribe to.
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// A new node has joined the cluster (discovered via SWIM).
    NodeJoined(NodeIdentity),
    /// A node left gracefully (sent a Departure control message).
    NodeLeft(NodeIdentity),
    /// A node was detected as failed by the SWIM failure detector (no goodbye).
    NodeFailed(NodeIdentity),
    /// All actors from a departed/failed node have been pruned from the registry.
    NodePruned(NodeIdentity),
    ActorRegistered {
        label: String,
        node_id: String,
        actor_type: String,
    },
    ActorDeregistered {
        label: String,
        node_id: String,
    },
    /// A remote spawn succeeded (forwarded from SpawnAckOk control message).
    SpawnAckOk {
        request_id: u64,
        label: String,
    },
    /// A remote spawn failed (forwarded from SpawnAckErr control message).
    SpawnAckErr {
        request_id: u64,
        error: String,
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
// TIMER COMMANDS — sent from FocaRuntime to the centralized timer manager
// =============================================================================

/// Commands sent from FocaRuntime to the centralized timer manager task.
pub(crate) enum TimerCommand {
    /// Schedule a timer to fire after the given duration.
    Insert {
        timer: Timer<NodeIdentity>,
        timer_id: u64,
        delay: Duration,
    },
    /// Cancel all pending timers (used during shutdown).
    CancelAll,
}

/// Spawns a task that owns a single `DelayQueue` and processes timer commands.
///
/// Instead of spawning one tokio task per foca timer, all timers are managed
/// by this single task through a `DelayQueue`. Expired timers are forwarded
/// to `timer_tx` for the main event loop to process.
pub(crate) fn spawn_timer_manager(
    timer_tx: mpsc::UnboundedSender<TimerEvent>,
) -> mpsc::UnboundedSender<TimerCommand> {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<TimerCommand>();

    tokio::spawn(async move {
        let mut queue = DelayQueue::new();

        loop {
            tokio::select! {
                // Process commands from FocaRuntime
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(TimerCommand::Insert { timer, timer_id, delay }) => {
                            queue.insert(TimerEvent { timer, timer_id }, delay);
                        }
                        Some(TimerCommand::CancelAll) => {
                            queue.clear();
                        }
                        None => break, // command channel closed, exit
                    }
                }
                // Fire expired timers
                Some(expired) = queue.next(), if !queue.is_empty() => {
                    let timer_event = expired.into_inner();
                    let _ = timer_tx.send(timer_event);
                }
            }
        }
    });

    cmd_tx
}

// =============================================================================
// FOCA RUNTIME — implements foca::Runtime for tokio integration
// =============================================================================

/// Bridges foca's synchronous `Runtime` trait into async tokio land.
///
/// - `notify()`: converts foca Notifications into ClusterEvents
/// - `send_to()`: queues SWIM bytes to be sent over control streams
/// - `submit_after()`: schedules timers via a centralized `DelayQueue` manager
pub struct FocaRuntime {
    event_tx: broadcast::Sender<ClusterEvent>,
    local_identity: NodeIdentity,
    /// Outbound SWIM data: (target_addr, data)
    swim_tx: mpsc::UnboundedSender<(NodeIdentity, Vec<u8>)>,
    /// Command channel to the centralized timer manager task
    timer_cmd_tx: mpsc::UnboundedSender<TimerCommand>,
    timer_id_counter: AtomicU64,
}

impl FocaRuntime {
    pub(crate) fn new(
        local_identity: NodeIdentity,
        event_tx: broadcast::Sender<ClusterEvent>,
        swim_tx: mpsc::UnboundedSender<(NodeIdentity, Vec<u8>)>,
        timer_cmd_tx: mpsc::UnboundedSender<TimerCommand>,
    ) -> Self {
        Self {
            event_tx,
            local_identity,
            swim_tx,
            timer_cmd_tx,
            timer_id_counter: AtomicU64::new(0),
        }
    }

    pub fn cancel_all_timers(&mut self) {
        let _ = self.timer_cmd_tx.send(TimerCommand::CancelAll);
    }
}

impl Runtime<NodeIdentity> for FocaRuntime {
    fn notify(&mut self, notification: Notification<'_, NodeIdentity>) {
        match notification {
            Notification::MemberUp(identity) => {
                if identity.socket_addr() != self.local_identity.socket_addr() {
                    tracing::info!("SWIM: node joined: {identity}");
                    instrument::cluster_node_joined();
                    let _ = self
                        .event_tx
                        .send(ClusterEvent::NodeJoined(identity.clone()));
                }
            }
            Notification::MemberDown(identity) => {
                tracing::info!("SWIM: node failed (detected by failure detector): {identity}");
                instrument::cluster_node_failed();
                let _ = self
                    .event_tx
                    .send(ClusterEvent::NodeFailed(identity.clone()));
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
        let _ = self.timer_cmd_tx.send(TimerCommand::Insert {
            timer: event,
            timer_id,
            delay: after,
        });
    }
}

impl Drop for FocaRuntime {
    fn drop(&mut self) {
        self.cancel_all_timers();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timer_manager_fires_timer() {
        let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<TimerEvent>();
        let cmd_tx = spawn_timer_manager(timer_tx);

        // Insert a timer with a short delay
        cmd_tx
            .send(TimerCommand::Insert {
                timer: Timer::ProbeRandomMember(0),
                timer_id: 1,
                delay: Duration::from_millis(10),
            })
            .unwrap();

        // Wait for the timer to fire
        let event = tokio::time::timeout(Duration::from_secs(1), timer_rx.recv())
            .await
            .expect("timer should fire within 1s")
            .expect("channel should not close");

        assert_eq!(event.timer_id, 1);
    }

    #[tokio::test]
    async fn test_timer_manager_cancel_all() {
        let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<TimerEvent>();
        let cmd_tx = spawn_timer_manager(timer_tx);

        // Insert a timer with a longer delay
        cmd_tx
            .send(TimerCommand::Insert {
                timer: Timer::ProbeRandomMember(0),
                timer_id: 1,
                delay: Duration::from_millis(200),
            })
            .unwrap();

        // Cancel before it fires
        cmd_tx.send(TimerCommand::CancelAll).unwrap();

        // Verify it doesn't fire
        let result = tokio::time::timeout(Duration::from_millis(400), timer_rx.recv()).await;
        assert!(result.is_err(), "timer should NOT fire after CancelAll");
    }
}
