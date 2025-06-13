use crate::net::{
    AcceptStream, Connection, ConnectionError, Stream,
};
use crate::test_utils::prelude::*;
use crate::prelude::{Actor, Id};

use super::{
    actor::NodeActor,
    messages::{Payload, RecvFrame, CheckHeartbeat},
    status::{NodeState, ReachabilityStatus, MembershipStatus},
    connection::ConnectionState,
};
use crate::net;

use assert_matches::assert_matches;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

// Helper function to create a mock receptionist for tests
fn create_mock_receptionist() -> crate::receptionist::Receptionist {
    let actor = crate::receptionist::ReceptionistActor::default();
    let supervisor = crate::system::Supervisor::construct(actor);
    crate::receptionist::Receptionist::new(supervisor.endpoint())
}
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Clone)]
struct MockStream {
    read_stream: Arc<Mutex<Vec<bytes::Bytes>>>,
    write_stream: Arc<Mutex<Vec<bytes::Bytes>>>,
}

impl MockStream {
    #[cfg(test)]
    pub fn with_recorder() -> (Self, Arc<Mutex<Vec<bytes::Bytes>>>) {
        let read_stream = Arc::new(Mutex::new(vec![]));
        let write_stream = Arc::new(Mutex::new(vec![]));

        let network = MockStream {
            read_stream,
            write_stream: write_stream.clone(),
        };

        (network, write_stream)
    }
    fn new() -> Self {
        MockStream {
            read_stream: Arc::new(Mutex::new(Vec::new())),
            write_stream: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // Add data to be read by the actor
    fn push(&self, data: bytes::Bytes) {
        let mut read_stream = self.read_stream.lock();
        read_stream.push(data);
    }

    // Push a frame to be read by the actor
    fn push_frame<T: Serialize + DeserializeOwned>(&self, frame: net::Frame<T>) {
        let encoded = frame.encode().unwrap();
        self.push(encoded);
    }

    // Get a frame that was written by the actor
    async fn expect_one_frame<T: Serialize + DeserializeOwned>(&self) -> Option<net::Frame<T>> {
        let mut write_stream = self.write_stream.lock();
        if write_stream.is_empty() {
            return None;
        }
        let data = write_stream.remove(0);
        let frame: net::Frame<T> = net::Frame::decode(data).expect("Failed to decode frame");
        Some(frame)
    }

    // This method has been replaced by process_until_frame in ActorTestHarnessExt

    // Check if there are any frames written by the actor
    fn has_frames(&self) -> bool {
        let write_stream = self.write_stream.lock();
        !write_stream.is_empty()
    }

    // Get all frames written by the actor
    fn get_all_frames(&self) -> Vec<bytes::Bytes> {
        let mut write_stream = self.write_stream.lock();
        std::mem::take(&mut *write_stream)
    }
}

struct MockConnection {
    stream: MockStream,
}

#[async_trait::async_trait]
impl Connection for MockConnection {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        // Mock connect - always succeeds
        Ok(())
    }

    async fn open_stream(&mut self) -> Result<Stream, ConnectionError> {
        // Create mock read/write streams
        let read_data = self.stream.read_stream.clone();
        let write_data = self.stream.write_stream.clone();

        let mock_read = MockStreamReader::new(read_data);
        let mock_write = MockStreamWriter::new(write_data);

        // Create raw stream
        Ok(Stream::new(Box::new(mock_read), Box::new(mock_write)))
    }

    async fn accept_stream(&mut self) -> Result<AcceptStream, ConnectionError> {
        let stream = self.stream.clone();

        // Create an AcceptStream that will create a new mock stream each time it's awaited
        Ok(AcceptStream::new(move || {
            let stream = stream.clone();
            async move {
                // Create mock read/write streams
                let read_data = stream.read_stream.clone();
                let write_data = stream.write_stream.clone();

                let mock_read = MockStreamReader::new(read_data);
                let mock_write = MockStreamWriter::new(write_data);

                // Create raw stream
                Ok(Stream::new(Box::new(mock_read), Box::new(mock_write)))
            }
        }))
    }
}


// Improved mock reader that reads from the TestingDriver's read_stream
struct MockStreamReader {
    read_data: Arc<Mutex<Vec<bytes::Bytes>>>,
    current_buffer: Vec<u8>,
    position: usize,
}

impl MockStreamReader {
    fn new(read_data: Arc<Mutex<Vec<bytes::Bytes>>>) -> Self {
        MockStreamReader {
            read_data,
            current_buffer: Vec::new(),
            position: 0,
        }
    }

    fn load_next_chunk(&mut self) -> bool {
        let mut read_data = self.read_data.lock();
        if read_data.is_empty() {
            return false;
        }

        let next_chunk = read_data.remove(0);
        self.current_buffer = next_chunk.to_vec();
        self.position = 0;
        true
    }
}

impl AsyncRead for MockStreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If we've read all data in the current buffer, try to load more
        if self.position >= self.current_buffer.len() && !self.load_next_chunk() {
            // No more data available, but we should wake the task later
            // to check again in case new data arrives
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // Copy data from current buffer to the output buffer
        let remaining = self.current_buffer.len() - self.position;
        let to_read = std::cmp::min(remaining, buf.remaining());

        if to_read > 0 {
            buf.put_slice(&self.current_buffer[self.position..self.position + to_read]);
            self.position += to_read;
            Poll::Ready(Ok(()))
        } else {
            // If there's no data to read, return Pending instead of Ready
            // This allows the runtime to schedule other tasks
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

// Improved mock writer that writes to the TestingDriver's write_stream
struct MockStreamWriter {
    write_data: Arc<Mutex<Vec<bytes::Bytes>>>,
    buffer: Vec<u8>,
}

impl MockStreamWriter {
    fn new(write_data: Arc<Mutex<Vec<bytes::Bytes>>>) -> Self {
        MockStreamWriter {
            write_data,
            buffer: Vec::new(),
        }
    }
}

impl AsyncWrite for MockStreamWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.buffer.extend_from_slice(buf);

        // Check if we have a complete frame
        // This is a simplification - in a real implementation, you'd need to
        // properly handle frame boundaries
        if self.buffer.len() >= 8 {
            let frame_size = u64::from_be_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
                self.buffer[4],
                self.buffer[5],
                self.buffer[6],
                self.buffer[7],
            ]) as usize
                + 8; // Include length prefix size.

            if self.buffer.len() >= frame_size {
                // Extract the complete frame data including the length prefix
                let frame_data = self.buffer[8..frame_size].to_vec();

                // Clone the write_data before borrowing self.buffer mutably
                let write_data = self.write_data.clone();
                let mut write_data_lock = write_data.lock();
                write_data_lock.push(bytes::Bytes::from(frame_data));

                // Remove the processed frame from the buffer
                self.buffer = self.buffer[frame_size..].to_vec();
            }
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// Extension trait for ActorTestHarness to add process_until_frame method
#[async_trait::async_trait]
trait HarnesssExt {
    /// Process actor messages until a frame is available or timeout occurs
    ///
    /// This method continuously processes messages from the actor's mailbox
    /// while waiting for a frame to become available from the network.
    ///
    /// # Arguments
    /// * `network` - The MockNetwork instance to check for frames
    /// * `timeout_ms` - Maximum time to wait in milliseconds
    ///
    /// # Returns
    /// * `Ok(Frame<T>)` - The frame if one becomes available before timeout
    /// * `Err(&'static str)` - Error message if timeout occurs
    async fn process_until_frame<T, F>(
        &mut self,
        network: &MockStream,
        timeout_ms: u64,
        match_fn: Option<F>,
    ) -> Result<net::Frame<T>, &'static str>
    where
        T: Serialize + DeserializeOwned + 'static,
        F: Fn(&net::Frame<T>) -> bool + Send + 'static;
}

#[async_trait::async_trait]
impl<A: Actor + Send + 'static> HarnesssExt for ActorTestHandle<A> {
    async fn process_until_frame<T, F>(
        &mut self,
        network: &MockStream,
        timeout_ms: u64,
        match_fn: Option<F>,
    ) -> Result<net::Frame<T>, &'static str>
    where
        T: Serialize + DeserializeOwned + 'static,
        F: Fn(&net::Frame<T>) -> bool + Send + 'static,
    {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

        loop {
            // First check if a frame is already available
            if let Some(frame) = network.expect_one_frame::<T>().await {
                // If a match function is provided, check if the frame matches
                if let Some(ref match_fn) = match_fn {
                    if !match_fn(&frame) {
                        // Frame does not match, continue processing
                        continue;
                    }
                }

                return Ok(frame);
            }

            // Process one message from the actor's mailbox
            if !self.process_one().await {
                // No more messages to process, wait a bit before checking again
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // Check if we've timed out
            if start.elapsed() >= timeout {
                return Err("Timed out waiting for frame");
            }
        }
    }
}

#[test_log::test(tokio::test)]
async fn test_node_initiation() {
    // Create a mock network
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let node = NodeActor::connect("127.0.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to connect");

    let harness = ActorTestHarness::new();
    let mut actor = harness.spawn(node);
    actor.start().await;

    // 1. Wait for the actor to send an initiation frame
    actor
        .process_until_frame::<Payload, _>(
            &stream,
            1000,
            Some(move |frame: &net::Frame<_>| {
                let body = &frame.body;
                matches!(body, net::FrameBody::Ok(Payload::Initialize))
            }),
        )
        .await
        .expect("Failed to receive initiation frame");

    let node = actor.actor_ref();
    assert_matches!(node.reachability, ReachabilityStatus::Pending);
    assert_matches!(node.state, NodeState::Initiating);
    assert_matches!(node.connection, ConnectionState::Established { .. });

    // 2. Respond to the initiation with a Join frame
    stream.push_frame(net::Frame::ok(remote_id, None, Payload::Join));
    actor
        .wait_for_state(100, |node| {
            matches!(node.membership_status, MembershipStatus::Joining)
        })
        .await
        .expect("Node did not transition to membership Joining state");

    actor
        .wait_for_state(100, |node| matches!(node.state, NodeState::Initiating))
        .await
        .expect("Node did not traingsition to Initiating state");
    actor
        .wait_for_state(
            100,
            |node| matches!(node.info.remote_id, Some(id) if id == remote_id),
        )
        .await
        .expect("Node did not set remote ID");
    // Send a join confirmation.
    stream.push_frame(net::Frame::ok(remote_id, None, Payload::JoinAck));
    actor
        .wait_for_state(100, |node| {
            matches!(node.membership_status, MembershipStatus::Up)
                && matches!(node.state, NodeState::Running)
        })
        .await
        .expect("Node did not transition to Up state");

    // Wait for a heartbeat to be sent
    actor
        .process_until_frame::<Payload, _>(
            &stream,
            1000,
            Some(move |frame: &net::Frame<Payload>| {
                let body = &frame.body;
                matches!(body, net::FrameBody::Ok(Payload::Heartbeat))
            }),
        )
        .await
        .expect("Failed to receive heatbeat frame, after joining");
}

/// Test that the node can handle reachability checks
#[test_log::test(tokio::test)]
async fn test_node_heartbeat() {
    // Create a mock network
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let harness = ActorTestHarness::new();
    let node = NodeActor::connect("127.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to connect");

    let mut handler = harness.spawn(node);
    handler.start().await;

    // Reachability should be pending initially until we enter a membership state
    assert_matches!(
        handler.actor_ref().reachability,
        ReachabilityStatus::Pending
    );

    // Update the node and memebership status to Running and Up respectively
    handler.actor_ref_mut().info.remote_id = Some(remote_id);
    handler.actor_ref_mut().membership_status = MembershipStatus::Up;
    handler.actor_ref_mut().state = NodeState::Running;
    handler.actor_ref_mut().reachability = ReachabilityStatus::Reachable {
        last_seen: chrono::Utc::now(),
        missed_heartbeats: 0,
    };

    // Send a heartbeat
    handler
        .send(RecvFrame {
            frame: net::Frame::ok(remote_id, None, Payload::Heartbeat),
        })
        .await
        .expect("Failed to send reachability check");
}

/// Test that the node can handle reachability checks
#[test_log::test(tokio::test)]
async fn test_node_reachability() {
    // Create a mock network
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let harness = ActorTestHarness::new();
    let node = NodeActor::connect("127.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to connect");

    let mut handler = harness.spawn(node);
    handler.start().await;

    // Create initial reference time and a last_seen time that's newer
    // (indicating the node has sent a heartbeat after the reference time)
    let reference_time = chrono::Utc::now();
    let last_seen_time = reference_time + chrono::Duration::seconds(1);

    // Update the node and membership status to Running and Up respectively
    handler.actor_ref_mut().info.remote_id = Some(remote_id);
    handler.actor_ref_mut().membership_status = MembershipStatus::Up;
    handler.actor_ref_mut().state = NodeState::Running;
    handler.actor_ref_mut().reachability = ReachabilityStatus::Reachable {
        last_seen: last_seen_time,
        missed_heartbeats: 0,
    };

    // Send a reachability check frame with the reference time
    handler
        .send(CheckHeartbeat {
            reference_time,
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
        .await
        .expect("Failed to send reachability check");

    // Since last_seen_time > reference_time, the node should still be reachable
    // and the last_seen time should be updated to the current time
    assert_matches!(
        handler.actor_ref().reachability,
        ReachabilityStatus::Reachable {
            last_seen: _,
            missed_heartbeats
        } if missed_heartbeats == 0
    );

    // Now test missed heartbeats
    // Set a reference time that's newer than the last_seen time
    let old_last_seen = handler.actor_ref().reachability.last_seen_time();
    let reference_time = old_last_seen + chrono::Duration::seconds(5);

    // Send heartbeat checks with the newer reference time
    // For our test, we need to send 4 heartbeats to reach the unreachable state
    // The first 3 will increment the missed_heartbeats counter, and the 4th will transition to Unreachable
    for i in 1..=4 {
        handler
            .send(CheckHeartbeat {
                reference_time,
                cancellation: tokio_util::sync::CancellationToken::new(),
            })
            .await
            .expect("Failed to send reachability check");

        // Check the state after each message
        if i < 4 {
            // The first 3 messages should increment missed_heartbeats but stay Reachable
            assert_matches!(
                handler.actor_ref().reachability,
                ReachabilityStatus::Reachable { last_seen, missed_heartbeats }
                if last_seen == old_last_seen && missed_heartbeats == i
            );
        } else {
            // The 4th message should transition to Unreachable
            assert_matches!(
                handler.actor_ref().reachability,
                ReachabilityStatus::Unreachable { last_seen, successful_heartbeat }
                if last_seen == old_last_seen && successful_heartbeat == 0
            );
        }
    }

    // Now test recovery from unreachable state
    // Set a newer last_seen time (simulating a heartbeat was received)
    let new_last_seen = reference_time + chrono::Duration::seconds(1);
    handler.actor_ref_mut().reachability = ReachabilityStatus::Unreachable {
        last_seen: new_last_seen,
        successful_heartbeat: 0,
    };

    // Send heartbeat checks with an older reference time (indicating heartbeats were received)
    // We need to send 4 heartbeats to reach the reachable state
    // The first 3 will increment the successful_heartbeat counter, and the 4th will transition to Reachable
    for i in 1..=4 {
        handler
            .send(CheckHeartbeat {
                reference_time,
                cancellation: tokio_util::sync::CancellationToken::new(),
            })
            .await
            .expect("Failed to send reachability check");

        // Check the state after each message
        if i < 4 {
            // The first 3 messages should increment successful_heartbeat but stay Unreachable
            assert_matches!(
                handler.actor_ref().reachability,
                ReachabilityStatus::Unreachable { last_seen, successful_heartbeat }
                if last_seen == new_last_seen && successful_heartbeat == i
            );
        } else {
            // The 4th message should transition to Reachable
            assert_matches!(
                handler.actor_ref().reachability,
                ReachabilityStatus::Reachable { last_seen: _, missed_heartbeats }
                if missed_heartbeats == 0
            );
            assert_eq!(handler.actor_ref().membership_status, MembershipStatus::Up);
        }
    }
}

/// Test that the node properly handles Leave messages
#[test_log::test(tokio::test)]
async fn test_node_leave_handling() {
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let harness = ActorTestHarness::new();
    let node = NodeActor::connect("127.0.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to connect");

    let mut handler = harness.spawn(node);
    handler.start().await;

    // Set up node as running
    handler.actor_ref_mut().info.remote_id = Some(remote_id);
    handler.actor_ref_mut().membership_status = MembershipStatus::Up;
    handler.actor_ref_mut().state = NodeState::Running;
    handler.actor_ref_mut().reachability = ReachabilityStatus::Reachable {
        last_seen: chrono::Utc::now(),
        missed_heartbeats: 0,
    };

    // Send Leave message
    handler
        .send(RecvFrame {
            frame: net::Frame::ok(remote_id, None, Payload::Leave),
        })
        .await
        .expect("Failed to send Leave message");

    // Verify state changes
    assert_matches!(handler.actor_ref().state, NodeState::Stopped);
    assert_matches!(handler.actor_ref().membership_status, MembershipStatus::Down);
    assert_matches!(
        handler.actor_ref().reachability,
        ReachabilityStatus::Unreachable { .. }
    );
}

/// Test that the node sends Leave message during shutdown
#[test_log::test(tokio::test)]
async fn test_node_leave_on_shutdown() {
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let harness = ActorTestHarness::new();
    let node = NodeActor::connect("127.0.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to connect");

    let mut handler = harness.spawn(node);
    handler.start().await;

    // Set up established connection
    handler.actor_ref_mut().info.remote_id = Some(remote_id);
    handler.actor_ref_mut().membership_status = MembershipStatus::Up;
    handler.actor_ref_mut().state = NodeState::Running;

    // Trigger shutdown
    handler.stop().await;

    // Verify Leave message was sent
    handler
        .process_until_frame::<Payload, _>(
            &stream,
            1000,
            Some(move |frame: &net::Frame<Payload>| {
                let body = &frame.body;
                matches!(body, net::FrameBody::Ok(Payload::Leave))
            }),
        )
        .await
        .expect("Failed to receive Leave frame during shutdown");
}

/// Test that the node properly handles Initialize messages (acceptor flow)
#[test_log::test(tokio::test)]
async fn test_node_initialize_handling() {
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let harness = ActorTestHarness::new();
    let node = NodeActor::accept("127.0.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to accept connection");

    let mut handler = harness.spawn(node);
    handler.start().await;

    // Verify initial state
    assert_matches!(handler.actor_ref().info.remote_id, None);
    assert_matches!(handler.actor_ref().state, NodeState::Accepting);

    // Send Initialize message from remote node
    handler
        .send(RecvFrame {
            frame: net::Frame::ok(remote_id, None, Payload::Initialize),
        })
        .await
        .expect("Failed to send Initialize message");

    // Verify that remote_id was set and state updated
    assert_matches!(
        handler.actor_ref().info.remote_id,
        Some(id) if id == remote_id
    );
    assert_matches!(handler.actor_ref().state, NodeState::Running);
    assert_matches!(handler.actor_ref().membership_status, MembershipStatus::Joining);

    // Verify Join response was sent
    handler
        .process_until_frame::<Payload, _>(
            &stream,
            1000,
            Some(move |frame: &net::Frame<Payload>| {
                let body = &frame.body;
                matches!(body, net::FrameBody::Ok(Payload::Join))
            }),
        )
        .await
        .expect("Failed to receive Join response frame");
}

/// Test that the connector flow properly starts heartbeats after sending JoinAck
#[test_log::test(tokio::test)]
async fn test_connector_heartbeat_setup() {
    let remote_id = Id::new();
    let (stream, _write_record) = MockStream::with_recorder();
    let connection: Box<dyn Connection> = Box::new(MockConnection {
        stream: stream.clone(),
    });

    let harness = ActorTestHarness::new();
    let node = NodeActor::connect("127.0.0.1:7788".into(), connection, create_mock_receptionist())
        .await
        .expect("Failed to connect");

    let mut handler = harness.spawn(node);
    handler.start().await;

    // Verify initial state
    assert_matches!(handler.actor_ref().info.remote_id, None);
    assert_matches!(handler.actor_ref().state, NodeState::Initiating);

    // Connector should have sent Initialize during construction, 
    // now we simulate receiving Join from acceptor
    handler
        .send(RecvFrame {
            frame: net::Frame::ok(remote_id, None, Payload::Join),
        })
        .await
        .expect("Failed to send Join message");

    // Verify that connector side is fully set up
    assert_matches!(
        handler.actor_ref().info.remote_id,
        Some(id) if id == remote_id
    );
    assert_matches!(handler.actor_ref().state, NodeState::Running);
    assert_matches!(handler.actor_ref().membership_status, MembershipStatus::Up);

    // Verify JoinAck response was sent
    handler
        .process_until_frame::<Payload, _>(
            &stream,
            1000,
            Some(move |frame: &net::Frame<Payload>| {
                let body = &frame.body;
                matches!(body, net::FrameBody::Ok(Payload::JoinAck))
            }),
        )
        .await
        .expect("Failed to receive JoinAck response frame");

    // Wait a bit and verify heartbeat was sent (indicating heartbeat tasks started)
    handler
        .process_until_frame::<Payload, _>(
            &stream,
            4000, // Give time for heartbeat interval (3s)
            Some(move |frame: &net::Frame<Payload>| {
                let body = &frame.body;
                matches!(body, net::FrameBody::Ok(Payload::Heartbeat))
            }),
        )
        .await
        .expect("Failed to receive heartbeat frame after connector handshake");
}
