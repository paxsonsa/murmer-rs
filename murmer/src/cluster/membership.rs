//! SWIM-based cluster membership via the `foca` crate.
//!
//! [`ClusterMembership`] wraps a `Foca` instance and translates its
//! protocol notifications into [`ClusterEvent`]s that drive the rest
//! of the cluster system (connection management, receptionist sync, etc.).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use foca::{BincodeCodec, Foca, Notification, Runtime, Timer};
use rand::SeedableRng;
use tokio::sync::{broadcast, mpsc};

use crate::instrument;
// The Runtime *seam* (Tokio in prod, SimRuntime in sim). Aliased so it doesn't
// collide with foca's own `Runtime` trait, which `FocaRuntime` implements.
use crate::runtime::Runtime as SeamRuntime;

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
    /// A cluster singleton's owner confirmed it has stopped (forwarded from the
    /// SingletonStoppedAck control message). `stopped_generation` is the packed
    /// `(term, seq)` the owner was running, so the Coordinator can ignore a late
    /// ack from a superseded owner. This is the cross-node await-stopped barrier.
    SingletonStopped {
        label: String,
        stopped_generation: u64,
    },
}

// =============================================================================
// TIMER EVENTS — bridge foca timers onto the runtime seam
// =============================================================================

#[derive(Debug)]
pub struct TimerEvent {
    pub timer: Timer<NodeIdentity>,
    pub timer_id: u64,
}

/// Commands sent from `FocaRuntime` to the centralized timer manager task.
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

/// A timer scheduled at an absolute deadline (on the runtime clock). Ordered by
/// `(deadline, seq)` so the manager's min-heap fires due timers in a single
/// deterministic order — `seq` (monotonic insertion index) breaks deadline ties
/// reproducibly, which `tokio_util`'s `DelayQueue` did not guarantee.
struct Scheduled {
    deadline: Duration,
    seq: u64,
    event: TimerEvent,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.deadline, self.seq) == (other.deadline, other.seq)
    }
}
impl Eq for Scheduled {}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.deadline, self.seq).cmp(&(other.deadline, other.seq))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A single task that owns the timer heap and fires due foca timers, on the
/// runtime seam. Replaces the old `tokio_util::DelayQueue` manager: under
/// `SimRuntime` the `runtime.sleep` to the next deadline registers on the
/// virtual clock, so `advance()` fires foca's timers deterministically; under
/// Tokio it behaves like the queue did. One task, drained when `FocaRuntime`
/// drops its `cmd_tx` — so teardown stays clean (no per-timer task leak).
pub(crate) fn spawn_timer_manager(
    runtime: Arc<dyn SeamRuntime>,
    timer_tx: mpsc::UnboundedSender<TimerEvent>,
) -> mpsc::UnboundedSender<TimerCommand> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<TimerCommand>();
    let sleep_rt = Arc::clone(&runtime);

    runtime.spawn(Box::pin(async move {
        let mut heap: BinaryHeap<Reverse<Scheduled>> = BinaryHeap::new();
        let mut seq: u64 = 0;

        loop {
            let now = sleep_rt.now();
            let next_deadline = heap.peek().map(|Reverse(s)| s.deadline);

            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(TimerCommand::Insert { timer, timer_id, delay }) => {
                            heap.push(Reverse(Scheduled {
                                deadline: now + delay,
                                seq,
                                event: TimerEvent { timer, timer_id },
                            }));
                            seq += 1;
                        }
                        Some(TimerCommand::CancelAll) => heap.clear(),
                        None => break, // FocaRuntime dropped its cmd_tx → exit
                    }
                }
                // Sleep until the earliest deadline, then drain everything due.
                _ = sleep_rt.sleep(next_deadline.unwrap_or(Duration::ZERO).saturating_sub(now)),
                    if next_deadline.is_some() =>
                {
                    let now = sleep_rt.now();
                    while let Some(Reverse(s)) = heap.peek() {
                        if s.deadline <= now {
                            let Reverse(s) = heap.pop().unwrap();
                            let _ = timer_tx.send(s.event);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }));

    cmd_tx
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
    /// `foca_seed` seeds foca's PRNG (probe-target selection, gossip fanout,
    /// suspicion jitter). Callers pass `runtime.derive_seed("foca-prng")`: fresh
    /// entropy under `TokioRuntime` (production stays randomized), a reproducible
    /// value under `SimRuntime` (deterministic membership).
    pub fn new(
        identity: NodeIdentity,
        event_tx: broadcast::Sender<ClusterEvent>,
        foca_seed: u64,
    ) -> Self {
        let config = foca::Config::simple();
        let rng = rand::rngs::StdRng::seed_from_u64(foca_seed);
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
// FOCA RUNTIME — implements foca::Runtime over the Runtime seam
// =============================================================================

/// Bridges foca's synchronous `Runtime` trait onto the async [`SeamRuntime`].
///
/// - `notify()`: converts foca Notifications into ClusterEvents
/// - `send_to()`: queues SWIM bytes to be sent over control streams
/// - `submit_after()`: schedules a timer as a `runtime.sleep` task (so foca
///   timers fire on the virtual clock under `SimRuntime`, and on real time under
///   Tokio) via the centralized [`spawn_timer_manager`] task.
pub struct FocaRuntime {
    event_tx: broadcast::Sender<ClusterEvent>,
    local_identity: NodeIdentity,
    /// Outbound SWIM data: (target_addr, data)
    swim_tx: mpsc::UnboundedSender<(NodeIdentity, Vec<u8>)>,
    /// Command channel to the centralized timer manager task.
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
    use crate::runtime::TokioRuntime;

    #[tokio::test]
    async fn timer_manager_fires_timer() {
        let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<TimerEvent>();
        let cmd_tx = spawn_timer_manager(Arc::new(TokioRuntime), timer_tx);

        cmd_tx
            .send(TimerCommand::Insert {
                timer: Timer::ProbeRandomMember(0),
                timer_id: 1,
                delay: Duration::from_millis(10),
            })
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(1), timer_rx.recv())
            .await
            .expect("timer should fire within 1s")
            .expect("channel should not close");
        assert_eq!(event.timer_id, 1);
    }

    #[tokio::test]
    async fn timer_manager_cancel_all_prevents_firing() {
        let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<TimerEvent>();
        let cmd_tx = spawn_timer_manager(Arc::new(TokioRuntime), timer_tx);

        cmd_tx
            .send(TimerCommand::Insert {
                timer: Timer::ProbeRandomMember(0),
                timer_id: 1,
                delay: Duration::from_millis(200),
            })
            .unwrap();
        cmd_tx.send(TimerCommand::CancelAll).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(400), timer_rx.recv()).await;
        assert!(result.is_err(), "timer must NOT fire after CancelAll");
    }

    #[tokio::test]
    async fn timer_manager_fires_in_deadline_order() {
        // Insert out of order; the heap must deliver by (deadline, seq).
        let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<TimerEvent>();
        let cmd_tx = spawn_timer_manager(Arc::new(TokioRuntime), timer_tx);

        for (timer_id, ms) in [(1u64, 60u64), (2, 20), (3, 40)] {
            cmd_tx
                .send(TimerCommand::Insert {
                    timer: Timer::ProbeRandomMember(0),
                    timer_id,
                    delay: Duration::from_millis(ms),
                })
                .unwrap();
        }

        let mut order = Vec::new();
        for _ in 0..3 {
            let ev = tokio::time::timeout(Duration::from_secs(1), timer_rx.recv())
                .await
                .expect("timer fires")
                .expect("channel open");
            order.push(ev.timer_id);
        }
        assert_eq!(order, vec![2, 3, 1], "earliest deadline first");
    }

    /// The linchpin of the pump-only multi-node convergence proof: foca emits
    /// `MemberUp` **synchronously** during `apply_many` (it takes `&mut runtime`
    /// precisely so it can `notify` inline). There is no `announce` call anywhere
    /// in the cluster — production learns a peer exactly this way, on each
    /// handshake — so a node's `NodeJoined` needs no timer or gossip round to
    /// fire. This is why the 3-node `MemberUp` simulation converges on `pump()`
    /// alone, without advancing the virtual clock (which would instead start the
    /// failure detector — Phase 3 territory).
    #[cfg(feature = "sim")]
    #[test]
    fn apply_many_emits_member_up_synchronously_under_sim() {
        use crate::cluster::net::NodeId;
        use crate::sim::SimRuntime;

        let rt: Arc<dyn SeamRuntime> = Arc::new(SimRuntime::new(1));
        let (event_tx, mut event_rx) = broadcast::channel(16);

        let local =
            NodeIdentity::new_seeded("local", NodeId("local-id".into()), "127.0.0.1", 7001, 1);
        let mut membership =
            ClusterMembership::new(local.clone(), event_tx.clone(), rt.derive_seed("foca-prng"));

        let (swim_tx, _swim_rx) = mpsc::unbounded_channel();
        let (timer_tx, _timer_rx) = mpsc::unbounded_channel();
        let timer_cmd_tx = spawn_timer_manager(Arc::clone(&rt), timer_tx);
        let mut foca_rt = FocaRuntime::new(local, event_tx, swim_tx, timer_cmd_tx);

        let other =
            NodeIdentity::new_seeded("other", NodeId("other-id".into()), "127.0.0.1", 7002, 1);
        membership
            .foca
            .apply_many(
                std::iter::once(foca::Member::alive(other.clone())),
                false,
                &mut foca_rt,
            )
            .expect("apply_many succeeds");

        // No pump, no advance: the notification is already on the channel.
        match event_rx.try_recv() {
            Ok(ClusterEvent::NodeJoined(id)) => {
                assert_eq!(id.endpoint_id, other.endpoint_id, "joined peer is `other`");
            }
            got => panic!("expected NodeJoined synchronously, got {got:?}"),
        }
    }
}
