// Compatibility module - re-exports from net.rs
// This allows existing code to continue working while we transition
// TODO: Remove this file once all references are updated to use net.rs

pub use crate::net::*;
use bincode::{Options, config};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::id::Id;
use crate::path::ActorPath;

/// Protocol version for wire format
pub const CURRENT_PROTOCOL_VERSION: u16 = 1;

/// Error types for wire format operations
#[derive(thiserror::Error, Debug)]
pub enum WireError {
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Insufficient data: expected {expected} bytes, got {actual}")]
    InsufficientData { expected: usize, actual: usize },

    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    ProtocolVersionMismatch { expected: u16, actual: u16 },

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}

/// Node identifier type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Id);

impl NodeId {
    pub fn new() -> Self {
        NodeId(Id::new())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Frame header with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameHeader {
    /// Unique identifier for the sender node
    pub sender_id: NodeId,
    /// Optional target (might be broadcast)
    pub target_id: Option<NodeId>,
    /// Timestamp in UTC milliseconds
    pub timestamp: u64,
    /// Protocol version for future compatibility
    pub protocol_version: u16,
    /// Sequence number for ordering/deduplication
    pub sequence: u64,
    /// Optional correlation ID for request/response patterns
    pub correlation_id: Option<u64>,
}

impl FrameHeader {
    pub fn new(sender_id: NodeId, target_id: Option<NodeId>) -> Self {
        FrameHeader {
            sender_id,
            target_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            protocol_version: CURRENT_PROTOCOL_VERSION,
            sequence: 0, // Should be incremented by the sender
            correlation_id: None,
        }
    }

    pub fn with_correlation_id(mut self, id: u64) -> Self {
        self.correlation_id = Some(id);
        self
    }

    pub fn with_sequence(mut self, seq: u64) -> Self {
        self.sequence = seq;
        self
    }
}

/// System messages for node-to-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemMessage {
    Join {
        node_id: NodeId,
        name: String,
        capabilities: Vec<String>,
    },
    JoinAck {
        accepted: bool,
        reason: Option<String>,
    },
    Handshake {
        capabilities: Vec<String>,
    },
    HandshakeAck {
        capabilities: Vec<String>,
    },
    Heartbeat {
        timestamp: u64,
    },
    Disconnect {
        reason: String,
    },
}

/// Actor messages with addressing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorMessage {
    /// Source actor path
    pub source: ActorPath,
    /// Target actor path
    pub target: ActorPath,
    /// Message type identifier (for deserialization)
    pub message_type: String,
    /// Serialized message payload
    pub payload: Vec<u8>,
}

/// Member information for cluster state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: NodeId,
    pub name: String,
    pub status: MemberStatus,
    pub address: String,
    pub last_seen: u64,
}

/// Member status in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Joining,
    Up,
    Leaving,
    Down,
    Removed,
}

/// Cluster state representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub members: Vec<MemberInfo>,
    pub leader_id: Option<NodeId>,
    pub term: u64,
}

/// Cluster management messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    MembershipUpdate { members: Vec<MemberInfo> },
    StateSync { state: ClusterState },
    LeaderElection { candidate_id: NodeId, term: u64 },
}

/// Enum for different message categories
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    /// System-level messages (membership, heartbeats)
    System(SystemMessage),
    /// Actor-to-actor communication
    Actor(ActorMessage),
    /// Cluster state and management
    Cluster(ClusterMessage),
}

/// Top-level frame that gets encoded/decoded from the wire
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireFrame {
    /// Header with metadata
    pub header: FrameHeader,
    /// Typed payload
    pub payload: MessagePayload,
}

impl WireFrame {
    /// Create a new wire frame with system message
    pub fn system(sender_id: NodeId, target_id: Option<NodeId>, message: SystemMessage) -> Self {
        WireFrame {
            header: FrameHeader::new(sender_id, target_id),
            payload: MessagePayload::System(message),
        }
    }

    /// Create a new wire frame with actor message
    pub fn actor(sender_id: NodeId, target_id: Option<NodeId>, message: ActorMessage) -> Self {
        WireFrame {
            header: FrameHeader::new(sender_id, target_id),
            payload: MessagePayload::Actor(message),
        }
    }

    /// Create a new wire frame with cluster message
    pub fn cluster(sender_id: NodeId, target_id: Option<NodeId>, message: ClusterMessage) -> Self {
        WireFrame {
            header: FrameHeader::new(sender_id, target_id),
            payload: MessagePayload::Cluster(message),
        }
    }

    /// Encode the frame to bytes
    pub fn encode(&self) -> Result<Bytes, WireError> {
        // Use a consistent bincode configuration
        let config = config::standard();

        // Serialize the frame
        let serialized = config.serialize(self)?;

        // Create a buffer with enough space for length prefix + serialized data
        let mut buffer = BytesMut::with_capacity(8 + serialized.len());

        // Write the length as u64 in network byte order (big endian)
        buffer.put_u64(serialized.len() as u64);

        // Write the serialized data
        buffer.extend_from_slice(&serialized);

        Ok(buffer.freeze())
    }

    /// Decode a frame from bytes
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        // Use a consistent bincode configuration
        let config = config::standard();

        // Deserialize the frame
        let frame: WireFrame = config.deserialize(bytes)?;

        // Check protocol version
        if frame.header.protocol_version != CURRENT_PROTOCOL_VERSION {
            return Err(WireError::ProtocolVersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                actual: frame.header.protocol_version,
            });
        }

        Ok(frame)
    }
}

/// Frame reader for processing incoming data
pub struct FrameReader {
    state: ReaderState,
    buffer: BytesMut,
}

enum ReaderState {
    ReadingLength,
    ReadingData(usize),
}

impl FrameReader {
    /// Create a new frame reader
    pub fn new() -> Self {
        FrameReader {
            state: ReaderState::ReadingLength,
            buffer: BytesMut::new(),
        }
    }

    /// Add data to the reader buffer
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to parse a complete frame from the buffer
    pub fn parse(&mut self) -> Result<Option<WireFrame>, WireError> {
        loop {
            match &self.state {
                ReaderState::ReadingLength => {
                    // Need at least 8 bytes for the length
                    if self.buffer.len() < 8 {
                        return Ok(None);
                    }

                    // Read the length prefix
                    let length = (&self.buffer[0..8]).get_u64() as usize;

                    // Remove the length bytes from the buffer
                    self.buffer.advance(8);

                    // Switch to reading data state
                    self.state = ReaderState::ReadingData(length);
                }

                ReaderState::ReadingData(length) => {
                    // Check if we have enough data
                    if self.buffer.len() < *length {
                        return Ok(None);
                    }

                    // Extract the frame data
                    let frame_data = self.buffer.split_to(*length);

                    // Reset state to read the next frame length
                    self.state = ReaderState::ReadingLength;

                    // Decode the frame
                    let frame = WireFrame::decode(&frame_data)?;

                    return Ok(Some(frame));
                }
            }
        }
    }

    /// Try to parse multiple frames from the buffer
    pub fn parse_all(&mut self) -> Result<Vec<WireFrame>, WireError> {
        let mut frames = Vec::new();

        while let Some(frame) = self.parse()? {
            frames.push(frame);
        }

        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_frame_encode_decode() {
        // Create a system message
        let node_id = NodeId::new();
        let system_msg = SystemMessage::Join {
            node_id: node_id.clone(),
            name: "test-node".to_string(),
            capabilities: vec!["actor".to_string(), "cluster".to_string()],
        };

        // Create a wire frame
        let frame = WireFrame::system(node_id.clone(), None, system_msg);

        // Encode the frame
        let encoded = frame.encode().unwrap();

        // Skip the length prefix (first 8 bytes)
        let frame_data = &encoded[8..];

        // Decode the frame
        let decoded = WireFrame::decode(frame_data).unwrap();

        // Verify the decoded frame
        match &decoded.payload {
            MessagePayload::System(SystemMessage::Join {
                node_id: decoded_id,
                name,
                capabilities,
            }) => {
                assert_eq!(*decoded_id, node_id);
                assert_eq!(name, "test-node");
                assert_eq!(capabilities.len(), 2);
                assert_eq!(capabilities[0], "actor");
                assert_eq!(capabilities[1], "cluster");
            }
            _ => panic!("Decoded wrong message type"),
        }
    }

    #[test]
    fn test_frame_reader() {
        // Create a few frames
        let node_id = NodeId::new();

        let frame1 = WireFrame::system(
            node_id.clone(),
            None,
            SystemMessage::Heartbeat {
                timestamp: 123456789,
            },
        );

        let frame2 = WireFrame::system(
            node_id.clone(),
            None,
            SystemMessage::Join {
                node_id: node_id.clone(),
                name: "test-node".to_string(),
                capabilities: vec!["actor".to_string()],
            },
        );

        // Encode the frames
        let encoded1 = frame1.encode().unwrap();
        let encoded2 = frame2.encode().unwrap();

        // Create a reader
        let mut reader = FrameReader::new();

        // Add partial data and verify no complete frame yet
        reader.extend(&encoded1[0..4]);
        assert!(reader.parse().unwrap().is_none());

        // Add the rest of the first frame
        reader.extend(&encoded1[4..]);
        let parsed1 = reader.parse().unwrap().unwrap();

        // Verify no more frames
        assert!(reader.parse().unwrap().is_none());

        // Add the second frame
        reader.extend(&encoded2);
        let parsed2 = reader.parse().unwrap().unwrap();

        // Verify no more frames
        assert!(reader.parse().unwrap().is_none());

        // Verify the parsed frames
        match &parsed1.payload {
            MessagePayload::System(SystemMessage::Heartbeat { timestamp }) => {
                assert_eq!(*timestamp, 123456789);
            }
            _ => panic!("Parsed wrong message type for frame 1"),
        }

        match &parsed2.payload {
            MessagePayload::System(SystemMessage::Join {
                node_id: parsed_id,
                name,
                capabilities,
            }) => {
                assert_eq!(*parsed_id, node_id);
                assert_eq!(name, "test-node");
                assert_eq!(capabilities.len(), 1);
                assert_eq!(capabilities[0], "actor");
            }
            _ => panic!("Parsed wrong message type for frame 2"),
        }
    }
}
