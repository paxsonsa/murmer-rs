//! Node receiver — routes incoming remote invocations to local actors.
//!
//! [`run_node_receiver`] bridges the network layer to the receptionist.
//! It reads [`RemoteInvocation`] messages from a
//! channel, looks up the target actor's dispatch channel via the receptionist,
//! and forwards the request. Responses are sent back through the response channel.
//!
//! In the real cluster implementation, this role is handled by
//! [`cluster::remote::handle_actor_stream`](crate::cluster::remote::handle_actor_stream)
//! which operates directly on QUIC streams. This function is primarily used
//! for testing and simulated multi-node scenarios.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::receptionist::Receptionist;
use crate::wire::{DispatchRequest, RemoteInvocation, RemoteResponse};

/// Simulates the receiving side of a node. Routes incoming RemoteInvocations
/// to the correct actor via the receptionist's dispatch channel.
pub async fn run_node_receiver(
    receptionist: Arc<Receptionist>,
    mut wire_rx: mpsc::UnboundedReceiver<RemoteInvocation>,
    response_tx: mpsc::UnboundedSender<RemoteResponse>,
) {
    while let Some(invocation) = wire_rx.recv().await {
        let dispatch_tx = receptionist.get_dispatch_sender(&invocation.actor_label);

        match dispatch_tx {
            Some(tx) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let request = DispatchRequest {
                    invocation,
                    respond_to: resp_tx,
                };
                if tx.send(request).is_ok()
                    && let Ok(response) = resp_rx.await
                {
                    let _ = response_tx.send(response);
                }
            }
            None => {
                let response = RemoteResponse {
                    call_id: invocation.call_id,
                    result: Err(format!("actor not found: {}", invocation.actor_label)),
                };
                let _ = response_tx.send(response);
            }
        }
    }
}
