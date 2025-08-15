use crate::cluster::{ClusterEvent, ClusterTransport, NodeIdentity};
use foca::{Notification, Runtime, Timer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Custom tokio-integrated runtime for Foca SWIM protocol
/// Replaces AccumulatingRuntime with direct async integration
pub struct ClusterRuntime {
    transport: ClusterTransport,
    local_identity: NodeIdentity,
    // Event broadcasting
    event_tx: tokio::sync::broadcast::Sender<ClusterEvent>,
    // Timer management - use timer ID instead of Timer directly since Timer doesn't implement Hash
    timer_tx: mpsc::UnboundedSender<TimerEvent>,
    timer_rx: Option<mpsc::UnboundedReceiver<TimerEvent>>,
    active_timers: HashMap<u64, tokio::task::JoinHandle<()>>,
    timer_id_counter: AtomicU64,
}

#[derive(Debug)]
pub struct TimerEvent {
    pub timer: Timer<NodeIdentity>,
    pub timer_id: u64,
}

impl ClusterRuntime {
    /// Create a new tokio-integrated Foca runtime
    pub fn new(
        transport: ClusterTransport,
        local_identity: NodeIdentity,
        event_tx: tokio::sync::broadcast::Sender<ClusterEvent>,
    ) -> Self {
        let (timer_tx, timer_rx) = mpsc::unbounded_channel();

        Self {
            transport,
            local_identity,
            event_tx,
            timer_tx,
            timer_rx: Some(timer_rx),
            active_timers: HashMap::new(),
            timer_id_counter: AtomicU64::new(0),
        }
    }

    /// Take the timer receiver for the main loop
    pub fn take_timer_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<TimerEvent>> {
        self.timer_rx.take()
    }

    /// Handle a timer firing (call this from main loop when timer expires)
    pub fn handle_timer_expired(&mut self, timer_id: u64) {
        // Remove from active timers
        if let Some(handle) = self.active_timers.remove(&timer_id) {
            handle.abort();
        }
        tracing::trace!("Timer {} expired", timer_id);
    }

    /// Cancel all active timers (for cleanup)
    pub fn cancel_all_timers(&mut self) {
        for (_, handle) in self.active_timers.drain() {
            handle.abort();
        }
    }
}

impl Runtime<NodeIdentity> for ClusterRuntime {
    fn notify(&mut self, notification: Notification<'_, NodeIdentity>) {
        tracing::trace!("Foca notification: {:?}", notification);

        // Convert foca notifications to our cluster events and send immediately
        match notification {
            Notification::MemberUp(identity) => {
                // Skip our own identity
                if identity.socket_addr() != self.local_identity.socket_addr() {
                    tracing::info!("Node joined cluster: {}", identity);
                    let _ = self
                        .event_tx
                        .send(ClusterEvent::NodeJoined(identity.clone()));
                }
            }
            Notification::MemberDown(identity) => {
                tracing::info!("Node left cluster: {}", identity);
                let _ = self.event_tx.send(ClusterEvent::NodeLeft(identity.clone()));
            }
            Notification::Defunct => {
                tracing::warn!("Local node declared defunct by cluster");
            }
            Notification::Idle => {
                tracing::trace!("Cluster idle");
            }
            Notification::Active => {
                tracing::info!("Node is now active in cluster");
            }
            _ => {
                tracing::trace!("Unhandled notification: {:?}", notification);
            }
        }
    }

    fn send_to(&mut self, to: NodeIdentity, data: &[u8]) {
        tracing::trace!("Sending {} bytes to {} via QUIC", data.len(), to);

        // Send immediately via QUIC transport
        let transport = self.transport.clone();
        let target = to.socket_addr();
        let payload = data.to_vec();

        // Spawn async task to send the message
        tokio::spawn(async move {
            if let Err(e) = transport.send_to(&payload, target).await {
                tracing::warn!("Failed to send SWIM message to {}: {}", target, e);
            }
        });
    }

    fn submit_after(&mut self, event: Timer<NodeIdentity>, after: Duration) {
        // Generate unique timer ID
        let timer_id = self.timer_id_counter.fetch_add(1, Ordering::SeqCst);

        tracing::debug!(
            "Scheduling timer {} ({:?}) to fire in {:?}",
            timer_id,
            event,
            after
        );

        // Create new timer task
        let timer_tx = self.timer_tx.clone();
        let timer_event = TimerEvent {
            timer: event,
            timer_id,
        };

        let handle = tokio::spawn(async move {
            tokio::time::sleep(after).await;

            // Notify main loop that timer expired
            if let Err(e) = timer_tx.send(timer_event) {
                tracing::debug!("Failed to send timer event: {}", e);
            }
        });

        // Store the handle for potential cancellation
        self.active_timers.insert(timer_id, handle);
    }
}

impl Drop for ClusterRuntime {
    fn drop(&mut self) {
        self.cancel_all_timers();
    }
}
