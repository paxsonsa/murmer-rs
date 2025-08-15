use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{Error as IoError, ErrorKind, Result as IoResult};

/// Maximum size of a single cluster message (1MB)
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Message type constants for cluster communication
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    /// Node announcement/discovery message
    Announce = 0x01,
    /// Heartbeat/ping message
    Heartbeat = 0x02,
    /// Request for member list
    MemberListRequest = 0x03,
    /// Response with member list
    MemberListResponse = 0x04,
    /// SWIM gossip protocol message
    Gossip = 0x05,
    /// NodeId exchange handshake message
    NodeIdExchange = 0x06,
}

impl TryFrom<u8> for MessageType {
    type Error = IoError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(MessageType::Announce),
            0x02 => Ok(MessageType::Heartbeat),
            0x03 => Ok(MessageType::MemberListRequest),
            0x04 => Ok(MessageType::MemberListResponse),
            0x05 => Ok(MessageType::Gossip),
            0x06 => Ok(MessageType::NodeIdExchange),
            _ => Err(IoError::new(ErrorKind::InvalidData, "Unknown message type")),
        }
    }
}

/// A framed cluster message with type and payload
#[derive(Debug, Clone)]
pub struct ClusterMessage {
    pub message_type: MessageType,
    pub payload: Bytes,
}

impl ClusterMessage {
    /// Create a new cluster message
    pub fn new(message_type: MessageType, payload: Bytes) -> Self {
        Self {
            message_type,
            payload,
        }
    }

    /// Serialize the message into bytes with framing
    /// Frame format: [length: u32] [type: u8] [payload: bytes]
    pub fn serialize(&self) -> Bytes {
        let total_len = 1 + self.payload.len(); // type byte + payload
        let mut buf = BytesMut::with_capacity(4 + total_len);

        // Write length (excluding the length field itself)
        buf.put_u32_le(total_len as u32);
        // Write message type
        buf.put_u8(self.message_type as u8);
        // Write payload
        buf.put_slice(&self.payload);

        buf.freeze()
    }

    /// Deserialize a message from bytes
    pub fn deserialize(mut data: Bytes) -> IoResult<Self> {
        if data.len() < 5 {
            // minimum: 4 bytes length + 1 byte type
            return Err(IoError::new(ErrorKind::UnexpectedEof, "Message too short"));
        }

        let length = data.get_u32_le() as usize;

        if length > MAX_MESSAGE_SIZE {
            return Err(IoError::new(ErrorKind::InvalidData, "Message too large"));
        }

        if data.remaining() < length {
            return Err(IoError::new(ErrorKind::UnexpectedEof, "Incomplete message"));
        }

        let message_type = MessageType::try_from(data.get_u8())?;
        let payload = data.split_to(length - 1); // -1 for the type byte we already read

        Ok(ClusterMessage {
            message_type,
            payload,
        })
    }
}

/// A message framer for reading from streams
pub struct MessageFramer {
    buffer: BytesMut,
    expected_length: Option<usize>,
}

impl MessageFramer {
    /// Create a new message framer
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
            expected_length: None,
        }
    }

    /// Add data to the framer and try to extract complete messages
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to extract the next complete message
    pub fn next_message(&mut self) -> IoResult<Option<ClusterMessage>> {
        // If we don't know the expected length, try to read it
        if self.expected_length.is_none() {
            if self.buffer.len() < 4 {
                // Need more data for length
                return Ok(None);
            }

            let length = u32::from_le_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
            ]) as usize;

            if length > MAX_MESSAGE_SIZE {
                return Err(IoError::new(ErrorKind::InvalidData, "Message too large"));
            }

            self.expected_length = Some(length);
        }

        let expected = self.expected_length.unwrap();

        // Check if we have the complete message (length field + message)
        if self.buffer.len() < 4 + expected {
            // Need more data
            return Ok(None);
        }

        // We have a complete message, extract it
        let _ = self.buffer.split_to(4); // Remove length field
        let message_data = self.buffer.split_to(expected).freeze();

        // Reset for next message
        self.expected_length = None;

        // Parse the message
        let message_type = MessageType::try_from(message_data[0])?;
        let payload = message_data.slice(1..);

        return Ok(Some(ClusterMessage {
            message_type,
            payload,
        }));
    }

    /// Check if the framer has buffered data
    pub fn has_data(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// Clear all buffered data
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.expected_length = None;
    }
}

impl Default for MessageFramer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialize_deserialize() {
        let payload = Bytes::from("Hello, cluster!");
        let msg = ClusterMessage::new(MessageType::Announce, payload.clone());

        let serialized = msg.serialize();
        let deserialized = ClusterMessage::deserialize(serialized).unwrap();

        assert_eq!(deserialized.message_type, MessageType::Announce);
        assert_eq!(deserialized.payload, payload);
    }

    #[test]
    fn test_message_framer() {
        let mut framer = MessageFramer::new();

        // Create a message
        let payload = Bytes::from("Test message");
        let msg = ClusterMessage::new(MessageType::Heartbeat, payload.clone());
        let serialized = msg.serialize();

        // Add data in chunks to test framing
        let chunks = serialized.chunks(3);

        for (i, chunk) in chunks.enumerate() {
            framer.add_data(chunk);

            // Should only get message on last chunk
            let result = framer.next_message().unwrap();
            if i == serialized.len() / 3 {
                // Last chunk (or close to it)
                if let Some(msg) = result {
                    assert_eq!(msg.message_type, MessageType::Heartbeat);
                    assert_eq!(msg.payload, payload);
                }
            }
        }
    }

    #[test]
    fn test_message_type_conversion() {
        assert_eq!(MessageType::try_from(0x01).unwrap(), MessageType::Announce);
        assert_eq!(MessageType::try_from(0x02).unwrap(), MessageType::Heartbeat);
        assert!(MessageType::try_from(0xFF).is_err());
    }

    #[test]
    fn test_invalid_message_size() {
        let mut data = BytesMut::new();
        data.put_u32_le(MAX_MESSAGE_SIZE as u32 + 1); // Too large
        data.put_u8(MessageType::Announce as u8);

        let result = ClusterMessage::deserialize(data.freeze());
        assert!(result.is_err());
    }
}
