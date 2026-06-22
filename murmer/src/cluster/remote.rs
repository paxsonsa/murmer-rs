//! Remote actor streams — multiplexed byte streams for actor messaging.
//!
//! Each remote actor gets a dedicated stream over the [`Net`] seam. This module
//! provides the outbound side: [`run_actor_stream_writer`] serializes messages,
//! writes them to a stream, and reads responses back. The inbound side
//! (`handle_incoming_stream` / `handle_actor_stream_after_init`) lives in the
//! cluster event loop (`super`).

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::instrument;
use crate::{RemoteInvocation, RemoteResponse, ResponseRegistry};

use super::framing::{self, FrameCodec, StreamInit};
use super::net::{Net, RecvHalf};

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
    net: Arc<dyn Net>,
    remote_node_id: String,
    actor_label: String,
    mut invocation_rx: mpsc::UnboundedReceiver<RemoteInvocation>,
    response_registry: ResponseRegistry,
) {
    // Open a stream to the remote node
    let (mut send, recv) = match net.open_actor_stream(&remote_node_id).await {
        Ok(streams) => {
            instrument::stream_opened();
            streams
        }
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
        instrument::network_bytes_sent(frame.len() as u64);
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
    instrument::stream_closed();
    tracing::debug!("Actor stream writer for {actor_label} on {remote_node_id} closed");
}

/// Read RemoteResponses from the QUIC stream and route them via ResponseRegistry.
///
/// When the stream closes or errors, any remaining pending responses are
/// failed so callers don't hang.
async fn read_responses(
    mut recv: Box<dyn RecvHalf>,
    response_registry: ResponseRegistry,
    actor_label: String,
) {
    let mut codec = FrameCodec::new();
    let mut buf = vec![0u8; 8192];

    let close_reason = loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                instrument::network_bytes_received(n as u64);
                codec.push_data(&buf[..n]);
                while let Ok(Some(frame)) = codec.next_frame() {
                    match framing::decode_response_frame(&frame) {
                        Ok(decoded) => {
                            let response = RemoteResponse {
                                call_id: decoded.call_id,
                                result: decoded.result.map(|bytes| bytes.to_vec()),
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
