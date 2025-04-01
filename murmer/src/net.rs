use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::id::Id;
use crate::path::ActorPath;

#[cfg(test)]
#[path = "./net.test.rs"]
mod tests;

/// Protocol version for wire format
pub const CURRENT_PROTOCOL_VERSION: u16 = 1;

/// Error types for wire format operations
#[derive(thiserror::Error, Debug)]
pub enum NetError {
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::error::EncodeError),

    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] bincode::error::DecodeError),

    #[error("Insufficient data: expected {expected} bytes, got {actual}")]
    InsufficientData { expected: usize, actual: usize },

    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    ProtocolVersionMismatch { expected: u16, actual: u16 },

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}

/// Frame header with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// Unique identifier for the sender node
    pub sender_id: Id,
    /// Optional target (might be broadcast)
    pub target_id: Option<Id>,
    /// Timestamp in UTC milliseconds
    pub timestamp: u64,
    /// Protocol version for future compatibility
    pub protocol_version: u16,
    /// Sequence number for ordering/deduplication
    pub sequence: u64,
    /// Optional correlation ID for request/response patterns
    pub correlation_id: Option<u64>,
}

impl Header {
    pub fn new(sender_id: Id, target_id: Option<Id>) -> Self {
        Header {
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
pub enum NodeMessage {
    Init {
        protocol_version: u16,
    },
    InitAck,
    Join {
        node_id: Id,
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
pub struct ActorMessage {}

/// Member information for cluster state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: Id,
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
    pub leader_id: Option<Id>,
    pub term: u64,
}

/// Cluster management messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    MembershipUpdate { members: Vec<MemberInfo> },
    StateSync { state: ClusterState },
    LeaderElection { candidate_id: Id, term: u64 },
}

/// Enum for different message categories
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    /// System-level messages (membership, heartbeats)
    Node(NodeMessage),
    /// Actor-to-actor communication
    Actor(ActorMessage),
    /// Cluster state and management
    Cluster(ClusterMessage),
}

/// Top-level frame that gets encoded/decoded from the wire
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    // TODO: add protocol versioning to decoding of the frame.
    /// Header with metadata
    pub header: Header,
    /// Typed payload
    pub payload: Payload,
}

impl Frame {
    /// Create a new wire frame with system message
    pub fn node(sender_id: Id, target_id: Option<Id>, message: NodeMessage) -> Self {
        Frame {
            header: Header::new(sender_id, target_id),
            payload: Payload::Node(message),
        }
    }

    /// Create a new wire frame with actor message
    pub fn actor(sender_id: Id, target_id: Option<Id>, message: ActorMessage) -> Self {
        Frame {
            header: Header::new(sender_id, target_id),
            payload: Payload::Actor(message),
        }
    }

    /// Create a new wire frame with cluster message
    pub fn cluster(sender_id: Id, target_id: Option<Id>, message: ClusterMessage) -> Self {
        Frame {
            header: Header::new(sender_id, target_id),
            payload: Payload::Cluster(message),
        }
    }

    /// Encode the frame to bytes
    pub fn encode(&self) -> Result<Bytes, NetError> {
        let config = bincode::config::standard();
        let mut buffer = BytesMut::new().writer();

        // Serialize the frame to bytes
        let _ = bincode::serde::encode_into_std_write(self, &mut buffer, config);

        // Calculate the length of the serialized data
        let buffer = buffer.into_inner();
        let length = buffer.len() as u64;

        let mut final_buffer = BytesMut::with_capacity(8);
        final_buffer.put_u64(length);
        final_buffer.put(buffer);

        Ok(final_buffer.freeze())
    }

    /// Decode a frame from bytes
    pub fn decode(bytes: &[u8]) -> Result<Self, NetError> {
        // Use a consistent bincode configuration
        let config = bincode::config::standard();

        // Deserialize the frame
        let frame: Frame = match bincode::serde::decode_from_slice(bytes, config) {
            Ok((frame, _)) => frame,
            Err(err) => return Err(NetError::DeserializationError(err)),
        };

        // Check protocol version
        if frame.header.protocol_version != CURRENT_PROTOCOL_VERSION {
            return Err(NetError::ProtocolVersionMismatch {
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
    pub fn parse(&mut self) -> Result<Option<Frame>, NetError> {
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
                    let frame = Frame::decode(&frame_data)?;

                    return Ok(Some(frame));
                }
            }
        }
    }

    /// Try to parse multiple frames from the buffer
    pub fn parse_all(&mut self) -> Result<Vec<Frame>, NetError> {
        let mut frames = Vec::new();

        while let Some(frame) = self.parse()? {
            frames.push(frame);
        }

        Ok(frames)
    }
}
