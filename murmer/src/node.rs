//! Simulated node server — processes incoming remote invocations.

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
