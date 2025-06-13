//! Connection state management for NodeActor

use crate::net::{Connection, ConnectionError, FrameReader, FrameWriter};
use super::messages::Payload;

/// Represents the state of a network connection
pub(super) enum ConnectionState {
    /// Represents a connection that is currently being established.
    Pending {
        /// The handle to the connection driver used to establish the connection.
        handle: Option<Box<dyn Connection>>,

        /// The stream used for communication with the remote node, if available.
        read_stream: Option<FrameReader<Payload>>,

        /// The stream used for sending data to the remote node, if available.
        send_stream: Option<FrameWriter<Payload>>,
    },
    /// Represents an established connection ready for communication.
    Established {
        /// The connection handle
        handle: Option<Box<dyn Connection>>,
        /// The stream used for sending data to the remote node.
        send_stream: FrameWriter<Payload>,
    },
    /// Represents a closed or disconnected connection.
    Disconnected {
        /// The connection handle
        handle: Option<Box<dyn Connection>>,
        /// The reason for the disconnection, if available.
        reason: Option<ConnectionError>,
    },
}

impl ConnectionState {
    /// Checks if the connection is in a pending state.
    pub fn is_pending(&self) -> bool {
        matches!(self, ConnectionState::Pending { .. })
    }

    /// Checks if the connection is established.
    pub fn is_established(&self) -> bool {
        matches!(self, ConnectionState::Established { .. })
    }

    /// Checks if the connection is disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, ConnectionState::Disconnected { .. })
    }

    /// Transition from Pending to Established state and return the read stream.
    pub fn to_established(&mut self) -> Option<(ConnectionState, FrameReader<Payload>)> {
        if let ConnectionState::Pending {
            handle,
            read_stream,
            send_stream,
        } = self
        {
            let new_state = ConnectionState::Established {
                handle: Some(handle.take().expect("Connection handle must be available")),
                send_stream: send_stream.take().expect("Send stream must be available"),
            };
            let read_stream = read_stream.take().expect("Read stream must be available");
            Some((new_state, read_stream))
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Pending { .. } => write!(f, "ConnectionState::Pending"),
            ConnectionState::Established { .. } => write!(f, "ConnectionState::Established"),
            ConnectionState::Disconnected { reason, .. } => {
                write!(f, "ConnectionState::Disconnected({:?})", reason)
            }
        }
    }
}