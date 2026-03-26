use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use super::config::{NodeClass, NodeIdentity};
use crate::{Op, VersionVector};

/// Maximum frame size (4 MB).
const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

// =============================================================================
// CONTROL MESSAGES — sent on stream 0 (long-lived, bidirectional)
// =============================================================================

/// Messages exchanged on the control stream (stream 0) of each node connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    /// First message on a new connection — mutual authentication.
    Handshake(HandshakePayload),
    /// Opaque SWIM protocol bytes (foca).
    Swim(Vec<u8>),
    /// OpLog delta: "here are ops you haven't seen".
    RegistrySync(Vec<Op>),
    /// Request peer's delta: "send me ops I haven't seen".
    RegistrySyncRequest(VersionVector),
    /// Keepalive ping.
    Ping,
    /// Keepalive pong.
    Pong,
    /// Graceful departure — "I'm leaving cleanly, don't wait for SWIM timeout".
    Departure(NodeIdentity),
    /// Coordinator instructs a node to spawn an actor locally.
    SpawnActor(SpawnRequest),
    /// Node confirms successful actor spawn.
    SpawnAckOk { request_id: u64, label: String },
    /// Node reports actor spawn failure.
    SpawnAckErr { request_id: u64, error: String },
}

/// A request to spawn an actor on a remote node.
///
/// Sent by the Coordinator over the control stream. The receiving node
/// looks up the `actor_type_name` in its `SpawnRegistry`, deserializes
/// the state bytes, and calls `receptionist.start()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnRequest {
    /// Unique identifier for correlating ack responses.
    pub request_id: u64,
    /// Label to register the actor under.
    pub label: String,
    /// Key into the SpawnRegistry — identifies the actor type to instantiate.
    pub actor_type_name: String,
    /// Serialized initial state (via MigratableActor).
    pub initial_state: Vec<u8>,
}

/// The handshake payload exchanged when two nodes first connect.
///
/// In addition to authentication (cookie) and capability negotiation
/// (type_manifest, protocol_version), this carries the node's class and
/// metadata so the orchestrator can make placement decisions immediately
/// after a node joins the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakePayload {
    pub identity: NodeIdentity,
    pub cookie: String,
    pub type_manifest: Vec<String>,
    pub protocol_version: u32,
    /// This node's role in the cluster (Worker, Coordinator, Edge, etc.).
    pub node_class: NodeClass,
    /// Arbitrary key-value metadata describing node capabilities.
    /// Examples: `"region" = "us-west"`, `"gpu" = "true"`, `"rack" = "A3"`.
    pub node_metadata: HashMap<String, String>,
}

/// Current protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

// =============================================================================
// ACTOR STREAM MESSAGES — sent on per-actor streams (lazy, bidirectional)
// =============================================================================

/// First message on a new actor stream — tells the receiver which actor
/// this stream is for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInit {
    pub actor_label: String,
}

// =============================================================================
// FRAME CODEC — stateful length-prefix codec for reading from QUIC streams
// =============================================================================

/// A stateful length-prefix codec. Accumulates bytes from a QUIC stream and
/// yields complete frames.
///
/// Wire format: `[u32 LE length][payload bytes]`
///
/// The length prefix indicates the size of the payload (not including the
/// 4-byte length field itself).
pub struct FrameCodec {
    buffer: BytesMut,
    expected_length: Option<usize>,
}

impl FrameCodec {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
            expected_length: None,
        }
    }

    /// Push raw bytes into the codec's internal buffer.
    pub fn push_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to extract the next complete frame. Returns `None` if more data
    /// is needed, or an error if the frame exceeds the size limit.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, std::io::Error> {
        // Read length prefix if we don't have it yet
        if self.expected_length.is_none() {
            if self.buffer.len() < 4 {
                return Ok(None);
            }
            let length = u32::from_le_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
            ]) as usize;

            if length > MAX_FRAME_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("frame too large: {length} bytes (max {MAX_FRAME_SIZE})"),
                ));
            }
            self.expected_length = Some(length);
        }

        let expected = self.expected_length.unwrap();

        // Wait until we have the full payload
        if self.buffer.len() < 4 + expected {
            return Ok(None);
        }

        // Consume the length prefix + payload
        self.buffer.advance(4);
        let payload = self.buffer.split_to(expected).to_vec();
        self.expected_length = None;

        Ok(Some(payload))
    }

    /// Encode a payload into a length-prefixed frame.
    pub fn encode_frame(data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32_le(data.len() as u32);
        buf.put_slice(data);
        buf.to_vec()
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// SERDE HELPERS — encode/decode typed messages to/from frames
// =============================================================================

/// Serialize a value to a length-prefixed frame using bincode.
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, String> {
    let payload = bincode::serde::encode_to_vec(msg, bincode::config::standard())
        .map_err(|e| e.to_string())?;
    Ok(FrameCodec::encode_frame(&payload))
}

/// Deserialize a value from a raw payload (no length prefix — already stripped).
pub fn decode_message<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T, String> {
    let (val, _): (T, _) = bincode::serde::decode_from_slice(data, bincode::config::standard())
        .map_err(|e| e.to_string())?;
    Ok(val)
}

// =============================================================================
// LEAN WIRE FORMAT — actor stream invocations and responses
// =============================================================================
//
// These functions encode/decode actor stream messages using a custom binary
// format that writes the payload bytes directly into the frame, avoiding the
// double-serialization that occurs when using `encode_message(&RemoteInvocation)`.
//
// With bincode, encoding a `RemoteInvocation { payload: Vec<u8> }` re-encodes
// the already-serialized payload bytes (length prefix + full copy). The lean
// format writes the raw payload bytes directly after the header fields.
//
// Additionally, the lean format omits `actor_label` from every frame — the
// StreamInit message already establishes which actor the stream targets, so
// repeating it per-message is redundant.
//
// Wire format for invocations:
//   [u32 LE frame_length]            — handled by FrameCodec
//   [u64 LE call_id]                 — response correlation ID
//   [u16 LE message_type_len]        — length of the message type string
//   [message_type bytes]             — UTF-8 TYPE_ID (e.g., "counter::Increment")
//   [payload bytes until end]        — raw serialized message (no length prefix)
//
// Wire format for responses:
//   [u32 LE frame_length]            — handled by FrameCodec
//   [u64 LE call_id]                 — matches the invocation's call_id
//   [u8 status]                      — 1 = Ok, 0 = Err
//   [body bytes until end]           — raw result payload (Ok) or UTF-8 error string (Err)

/// Encode an actor stream invocation to a length-prefixed frame.
///
/// Writes the payload bytes directly — no double-serialization.
pub fn encode_invocation_frame(call_id: u64, message_type: &str, payload: &[u8]) -> Vec<u8> {
    let type_bytes = message_type.as_bytes();
    let body_len = 8 + 2 + type_bytes.len() + payload.len();
    let mut buf = Vec::with_capacity(4 + body_len);
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());
    buf.extend_from_slice(&call_id.to_le_bytes());
    buf.extend_from_slice(&(type_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(type_bytes);
    buf.extend_from_slice(payload);
    buf
}

/// Decoded invocation from a frame payload.
pub struct DecodedInvocation<'a> {
    pub call_id: u64,
    pub message_type: &'a str,
    pub payload: &'a [u8],
}

/// Decode an actor stream invocation from a frame payload (length prefix already stripped).
///
/// Returns borrowed slices into the frame data — zero-copy for the payload.
pub fn decode_invocation_frame(data: &[u8]) -> Result<DecodedInvocation<'_>, String> {
    if data.len() < 10 {
        return Err("invocation frame too short".into());
    }
    let call_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let type_len = u16::from_le_bytes(data[8..10].try_into().unwrap()) as usize;
    if data.len() < 10 + type_len {
        return Err(format!(
            "invocation frame too short for message type (need {}, have {})",
            10 + type_len,
            data.len()
        ));
    }
    let message_type = std::str::from_utf8(&data[10..10 + type_len])
        .map_err(|e| format!("invalid message type UTF-8: {e}"))?;
    let payload = &data[10 + type_len..];
    Ok(DecodedInvocation {
        call_id,
        message_type,
        payload,
    })
}

/// Encode an actor stream response to a length-prefixed frame.
///
/// Writes the result bytes directly — no double-serialization.
pub fn encode_response_frame(call_id: u64, result: &Result<Vec<u8>, String>) -> Vec<u8> {
    let (status, body): (u8, &[u8]) = match result {
        Ok(payload) => (1, payload.as_slice()),
        Err(error) => (0, error.as_bytes()),
    };
    let body_len = 8 + 1 + body.len();
    let mut buf = Vec::with_capacity(4 + body_len);
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());
    buf.extend_from_slice(&call_id.to_le_bytes());
    buf.push(status);
    buf.extend_from_slice(body);
    buf
}

/// Decoded response from a frame payload.
pub struct DecodedResponse<'a> {
    pub call_id: u64,
    pub result: Result<&'a [u8], String>,
}

/// Decode an actor stream response from a frame payload (length prefix already stripped).
///
/// The Ok payload is a borrowed slice — zero-copy.
pub fn decode_response_frame(data: &[u8]) -> Result<DecodedResponse<'_>, String> {
    if data.len() < 9 {
        return Err("response frame too short".into());
    }
    let call_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let status = data[8];
    let body = &data[9..];
    let result = if status == 1 {
        Ok(body)
    } else {
        Err(String::from_utf8_lossy(body).into_owned())
    };
    Ok(DecodedResponse { call_id, result })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_codec_roundtrip() {
        let original = b"hello, cluster!";
        let frame = FrameCodec::encode_frame(original);

        let mut codec = FrameCodec::new();
        codec.push_data(&frame);

        let decoded = codec.next_frame().unwrap().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_frame_codec_incremental() {
        let original = b"incremental data";
        let frame = FrameCodec::encode_frame(original);

        let mut codec = FrameCodec::new();

        // Feed byte by byte
        for (i, byte) in frame.iter().enumerate() {
            codec.push_data(std::slice::from_ref(byte));
            let result = codec.next_frame().unwrap();
            if i < frame.len() - 1 {
                assert!(result.is_none(), "should not yield frame at byte {i}");
            } else {
                assert_eq!(result.unwrap(), original);
            }
        }
    }

    #[test]
    fn test_frame_codec_multiple_frames() {
        let msg1 = b"first";
        let msg2 = b"second";

        let mut wire = FrameCodec::encode_frame(msg1);
        wire.extend_from_slice(&FrameCodec::encode_frame(msg2));

        let mut codec = FrameCodec::new();
        codec.push_data(&wire);

        assert_eq!(codec.next_frame().unwrap().unwrap(), msg1);
        assert_eq!(codec.next_frame().unwrap().unwrap(), msg2);
        assert!(codec.next_frame().unwrap().is_none());
    }

    #[test]
    fn test_control_message_serde() {
        let msg = ControlMessage::Ping;
        let frame = encode_message(&msg).unwrap();

        // Strip the 4-byte length prefix
        let payload = &frame[4..];
        let decoded: ControlMessage = decode_message(payload).unwrap();

        assert!(matches!(decoded, ControlMessage::Ping));
    }

    #[test]
    fn test_frame_too_large() {
        let mut codec = FrameCodec::new();
        // Fake a frame header claiming 5MB
        let len = (5 * 1024 * 1024u32).to_le_bytes();
        codec.push_data(&len);
        codec.push_data(&[0u8; 10]);

        let result = codec.next_frame();
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data_returns_none() {
        let mut codec = FrameCodec::new();
        // Push 0 bytes — nothing to decode
        codec.push_data(&[]);
        assert!(codec.next_frame().unwrap().is_none());
    }

    #[test]
    fn test_truncated_length_prefix() {
        let mut codec = FrameCodec::new();

        // Only 1 byte — not enough for a 4-byte length prefix
        codec.push_data(&[0x02]);
        assert!(codec.next_frame().unwrap().is_none());

        // 3 bytes total — still not enough
        codec.push_data(&[0x00, 0x00]);
        assert!(codec.next_frame().unwrap().is_none());

        // Complete the 4-byte prefix: 2u32 LE = [0x02, 0x00, 0x00, 0x00]
        // We already pushed [0x02, 0x00, 0x00], push the last prefix byte
        codec.push_data(&[0x00]);

        // Now we have a valid length prefix saying 2 bytes of payload, but no payload yet
        assert!(codec.next_frame().unwrap().is_none());

        // Push the 2-byte payload
        codec.push_data(b"hi");
        assert_eq!(codec.next_frame().unwrap().unwrap(), b"hi");
    }

    #[test]
    fn test_partial_payload_returns_none() {
        let frame = FrameCodec::encode_frame(b"hello"); // 4 prefix + 5 payload = 9 bytes

        let mut codec = FrameCodec::new();

        // Push only first 7 bytes (prefix + 2 payload bytes)
        codec.push_data(&frame[..7]);
        assert!(codec.next_frame().unwrap().is_none());

        // Push remaining 2 bytes
        codec.push_data(&frame[7..]);
        assert_eq!(codec.next_frame().unwrap().unwrap(), b"hello");
    }

    #[test]
    fn test_garbage_after_valid_frame() {
        let valid_frame = FrameCodec::encode_frame(b"good");
        let garbage: &[u8] = &[0xff, 0xfe, 0x01];

        let mut codec = FrameCodec::new();
        codec.push_data(&valid_frame);
        codec.push_data(garbage);

        // First frame decodes successfully
        assert_eq!(codec.next_frame().unwrap().unwrap(), b"good");

        // Garbage is only 3 bytes — not enough for a length prefix, returns None
        assert!(codec.next_frame().unwrap().is_none());
    }

    #[test]
    fn test_frame_exactly_at_max_size() {
        let mut codec = FrameCodec::new();

        // Header claiming exactly MAX_FRAME_SIZE bytes
        let len = (MAX_FRAME_SIZE as u32).to_le_bytes();
        codec.push_data(&len);

        // Valid size, just waiting for payload — should not error
        assert!(codec.next_frame().unwrap().is_none());
    }

    #[test]
    fn test_frame_one_over_max_size() {
        let mut codec = FrameCodec::new();

        // Header claiming MAX_FRAME_SIZE + 1 bytes
        let len = ((MAX_FRAME_SIZE + 1) as u32).to_le_bytes();
        codec.push_data(&len);

        // Should error — exceeds limit
        assert!(codec.next_frame().is_err());
    }

    #[test]
    fn test_zero_length_frame() {
        // encode_frame with empty data should produce [0, 0, 0, 0]
        let frame = FrameCodec::encode_frame(b"");
        assert_eq!(frame, vec![0, 0, 0, 0]);

        let mut codec = FrameCodec::new();
        codec.push_data(&frame);

        // Empty payload is valid
        let result = codec.next_frame().unwrap().unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_multiple_frames_with_partial_interleaved() {
        let frame1 = FrameCodec::encode_frame(b"alpha");
        let frame2 = FrameCodec::encode_frame(b"beta");

        let mut codec = FrameCodec::new();

        // Feed all of frame1 and first 3 bytes of frame2
        codec.push_data(&frame1);
        codec.push_data(&frame2[..3]);

        assert_eq!(codec.next_frame().unwrap().unwrap(), b"alpha");
        assert!(codec.next_frame().unwrap().is_none());

        // Feed remaining bytes of frame2
        codec.push_data(&frame2[3..]);
        assert_eq!(codec.next_frame().unwrap().unwrap(), b"beta");
    }

    #[test]
    fn test_encode_decode_control_message_departure() {
        let identity = NodeIdentity::new("test-node", "127.0.0.1", 9000);
        let msg = ControlMessage::Departure(identity.clone());

        // Round-trip through encode_message / decode_message
        let frame = encode_message(&msg).unwrap();

        // decode_message expects payload without the 4-byte length prefix
        let payload = &frame[4..];
        let decoded: ControlMessage = decode_message(payload).unwrap();

        match decoded {
            ControlMessage::Departure(decoded_identity) => {
                assert_eq!(decoded_identity.name, identity.name);
                assert_eq!(decoded_identity.host, identity.host);
                assert_eq!(decoded_identity.port, identity.port);
                assert_eq!(decoded_identity.incarnation, identity.incarnation);
            }
            other => panic!("expected Departure, got {other:?}"),
        }
    }

    // ── Lean wire format tests ─────────────────────────────────────

    #[test]
    fn test_invocation_frame_roundtrip() {
        let call_id = 42u64;
        let message_type = "counter::Increment";
        let payload = b"some-serialized-message-bytes";

        let frame = encode_invocation_frame(call_id, message_type, payload);

        // Strip the 4-byte length prefix (FrameCodec would do this)
        let body = &frame[4..];
        let decoded = decode_invocation_frame(body).unwrap();

        assert_eq!(decoded.call_id, call_id);
        assert_eq!(decoded.message_type, message_type);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_invocation_frame_empty_payload() {
        let frame = encode_invocation_frame(1, "test::Empty", &[]);
        let body = &frame[4..];
        let decoded = decode_invocation_frame(body).unwrap();

        assert_eq!(decoded.call_id, 1);
        assert_eq!(decoded.message_type, "test::Empty");
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn test_invocation_frame_through_codec() {
        let frame = encode_invocation_frame(99, "actor::Ping", b"hello");

        let mut codec = FrameCodec::new();
        codec.push_data(&frame);

        let body = codec.next_frame().unwrap().unwrap();
        let decoded = decode_invocation_frame(&body).unwrap();

        assert_eq!(decoded.call_id, 99);
        assert_eq!(decoded.message_type, "actor::Ping");
        assert_eq!(decoded.payload, b"hello");
    }

    #[test]
    fn test_invocation_frame_too_short() {
        assert!(decode_invocation_frame(&[0u8; 5]).is_err());
    }

    #[test]
    fn test_response_frame_roundtrip_ok() {
        let call_id = 42u64;
        let result_bytes = vec![1, 2, 3, 4, 5];

        let frame = encode_response_frame(call_id, &Ok(result_bytes.clone()));
        let body = &frame[4..];
        let decoded = decode_response_frame(body).unwrap();

        assert_eq!(decoded.call_id, call_id);
        assert_eq!(decoded.result.unwrap(), &result_bytes[..]);
    }

    #[test]
    fn test_response_frame_roundtrip_err() {
        let call_id = 7u64;
        let error = "actor not found".to_string();

        let frame = encode_response_frame(call_id, &Err(error.clone()));
        let body = &frame[4..];
        let decoded = decode_response_frame(body).unwrap();

        assert_eq!(decoded.call_id, call_id);
        assert_eq!(decoded.result.unwrap_err(), error);
    }

    #[test]
    fn test_response_frame_through_codec() {
        let frame = encode_response_frame(55, &Ok(vec![10, 20, 30]));

        let mut codec = FrameCodec::new();
        codec.push_data(&frame);

        let body = codec.next_frame().unwrap().unwrap();
        let decoded = decode_response_frame(&body).unwrap();

        assert_eq!(decoded.call_id, 55);
        assert_eq!(decoded.result.unwrap(), &[10, 20, 30]);
    }

    #[test]
    fn test_response_frame_too_short() {
        assert!(decode_response_frame(&[0u8; 5]).is_err());
    }

    #[test]
    fn test_multiple_invocation_frames_through_codec() {
        let frame1 = encode_invocation_frame(1, "a::B", b"first");
        let frame2 = encode_invocation_frame(2, "c::D", b"second");

        let mut codec = FrameCodec::new();
        codec.push_data(&frame1);
        codec.push_data(&frame2);

        let body1 = codec.next_frame().unwrap().unwrap();
        let d1 = decode_invocation_frame(&body1).unwrap();
        assert_eq!(d1.call_id, 1);
        assert_eq!(d1.message_type, "a::B");
        assert_eq!(d1.payload, b"first");

        let body2 = codec.next_frame().unwrap().unwrap();
        let d2 = decode_invocation_frame(&body2).unwrap();
        assert_eq!(d2.call_id, 2);
        assert_eq!(d2.message_type, "c::D");
        assert_eq!(d2.payload, b"second");
    }
}
