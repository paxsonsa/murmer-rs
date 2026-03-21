use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{DispatchRequest, Receptionist, RemoteInvocation, RemoteResponse, ResponseRegistry};

use super::framing::{self, FrameCodec, StreamInit};
use super::transport::Transport;

// =============================================================================
// OUTBOUND — sends RemoteInvocations over a QUIC stream to a remote actor
// =============================================================================

/// Bridges a local `Endpoint::Remote`'s wire channel to a QUIC stream.
///
/// 1. Opens a bidirectional QUIC stream to the remote node
/// 2. Sends `StreamInit { actor_label }` as the first frame
/// 3. Reads RemoteInvocations from `wire_rx`, serializes, writes to stream
/// 4. Reads RemoteResponses from the stream, routes to ResponseRegistry
///
/// **Fail-fast:** If any I/O operation fails the task exits immediately,
/// failing all pending responses so callers see the error right away
/// instead of hanging.  The dropped `invocation_rx` causes future
/// `wire_tx.send()` calls to return an error, which propagates back to
/// the `Endpoint` as a `SendError`.
pub async fn run_actor_stream_writer(
    transport: Arc<Transport>,
    remote_node_id: String,
    actor_label: String,
    mut invocation_rx: mpsc::UnboundedReceiver<RemoteInvocation>,
    response_registry: ResponseRegistry,
) {
    // Open a stream to the remote node
    let (mut send, recv) = match transport.open_actor_stream(&remote_node_id).await {
        Ok(streams) => streams,
        Err(e) => {
            tracing::error!(
                "Failed to open actor stream to {remote_node_id} for {actor_label}: {e}"
            );
            response_registry.fail_all(&format!("connection to {remote_node_id} unavailable: {e}"));
            return;
        }
    };

    // Send StreamInit
    let init = StreamInit {
        actor_label: actor_label.clone(),
    };
    let frame = match framing::encode_message(&init) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to encode StreamInit: {e}");
            response_registry.fail_all(&format!("StreamInit encode failed: {e}"));
            return;
        }
    };
    if let Err(e) = send.write_all(&frame).await {
        tracing::error!("Failed to send StreamInit for {actor_label}: {e}");
        response_registry.fail_all(&format!("StreamInit send failed: {e}"));
        return;
    }

    // Spawn the response reader
    let registry_clone = response_registry.clone();
    let label_clone = actor_label.clone();
    let reader_handle = tokio::spawn(async move {
        read_responses(recv, registry_clone, label_clone).await;
    });

    // Write invocations to the stream using the lean wire format.
    // The payload bytes are written directly — no double-serialization.
    while let Some(invocation) = invocation_rx.recv().await {
        let frame = framing::encode_invocation_frame(
            invocation.call_id,
            &invocation.message_type,
            &invocation.payload,
        );
        if let Err(e) = send.write_all(&frame).await {
            tracing::error!("Actor stream write failed for {actor_label}: {e} — closing stream");
            break;
        }
    }

    // Clean up: finish the send side, abort the response reader, and
    // fail any in-flight responses so callers don't hang.
    let _ = send.finish();
    reader_handle.abort();
    response_registry.fail_all(&format!(
        "actor stream to {remote_node_id} for {actor_label} closed"
    ));
    tracing::debug!("Actor stream writer for {actor_label} on {remote_node_id} closed");
}

/// Read RemoteResponses from the QUIC stream and route them via ResponseRegistry.
///
/// When the stream closes or errors, any remaining pending responses are
/// failed so callers don't hang.
async fn read_responses(
    mut recv: quinn::RecvStream,
    response_registry: ResponseRegistry,
    actor_label: String,
) {
    let mut codec = FrameCodec::new();
    let mut buf = vec![0u8; 8192];

    let close_reason = loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                codec.push_data(&buf[..n]);
                while let Ok(Some(frame)) = codec.next_frame() {
                    match framing::decode_response_frame(&frame) {
                        Ok(decoded) => {
                            let response = RemoteResponse {
                                call_id: decoded.call_id,
                                result: decoded
                                    .result
                                    .map(|bytes| bytes.to_vec()),
                            };
                            response_registry.complete(response);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to decode response for {actor_label}: {e}");
                        }
                    }
                }
            }
            Ok(None) => {
                tracing::debug!("Response stream for {actor_label} closed");
                break "response stream closed by peer".to_string();
            }
            Err(e) => {
                tracing::error!("Response stream read error for {actor_label}: {e}");
                break format!("response stream error: {e}");
            }
        }
    };

    // Fail any pending responses that haven't been completed yet
    response_registry.fail_all(&close_reason);
}

// =============================================================================
// INBOUND — receives RemoteInvocations from a QUIC stream, dispatches locally
// =============================================================================

/// Handles an incoming actor stream from a remote node.
///
/// 1. Reads `StreamInit` to determine which local actor is targeted
/// 2. Gets the dispatch channel from the receptionist
/// 3. Reads RemoteInvocations, dispatches to the actor, sends responses back
pub async fn handle_actor_stream(
    receptionist: Receptionist,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) {
    let mut codec = FrameCodec::new();
    let mut buf = vec![0u8; 8192];

    // Step 1: Read StreamInit
    let actor_label = loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                codec.push_data(&buf[..n]);
                if let Ok(Some(frame)) = codec.next_frame() {
                    match framing::decode_message::<StreamInit>(&frame) {
                        Ok(init) => break init.actor_label,
                        Err(e) => {
                            tracing::warn!("Invalid StreamInit: {e}");
                            return;
                        }
                    }
                }
            }
            Ok(None) | Err(_) => {
                tracing::debug!("Actor stream closed before StreamInit");
                return;
            }
        }
    };

    // Step 2: Get dispatch channel
    let dispatch_tx = match receptionist.get_dispatch_sender(&actor_label) {
        Some(tx) => tx,
        None => {
            tracing::warn!("Actor stream for unknown actor: {actor_label}");
            let frame = framing::encode_response_frame(
                0,
                &Err(format!("actor not found: {actor_label}")),
            );
            let _ = send.write_all(&frame).await;
            return;
        }
    };

    tracing::debug!("Handling actor stream for {actor_label}");

    // Step 3: Read invocations and dispatch (lean wire format)
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                codec.push_data(&buf[..n]);
                while let Ok(Some(frame)) = codec.next_frame() {
                    match framing::decode_invocation_frame(&frame) {
                        Ok(decoded) => {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let request = DispatchRequest {
                                invocation: RemoteInvocation {
                                    call_id: decoded.call_id,
                                    actor_label: actor_label.clone(),
                                    message_type: decoded.message_type.to_string(),
                                    payload: decoded.payload.to_vec(),
                                },
                                respond_to: resp_tx,
                            };

                            if dispatch_tx.send(request).is_err() {
                                tracing::warn!(
                                    "Dispatch channel closed for {actor_label}"
                                );
                                return;
                            }

                            if let Ok(response) = resp_rx.await {
                                let frame = framing::encode_response_frame(
                                    response.call_id,
                                    &response.result,
                                );
                                if let Err(e) = send.write_all(&frame).await {
                                    tracing::warn!(
                                        "Failed to send response for {actor_label}: {e}"
                                    );
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to decode invocation for {actor_label}: {e}"
                            );
                        }
                    }
                }
            }
            Ok(None) => {
                tracing::debug!("Actor stream for {actor_label} closed by peer");
                break;
            }
            Err(e) => {
                tracing::warn!("Actor stream read error for {actor_label}: {e}");
                break;
            }
        }
    }
}
