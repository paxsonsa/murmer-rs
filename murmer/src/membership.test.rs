use crate::system::TestSupervisor;

use super::*;
use assert_matches::assert_matches;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Cursor};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Clone)]
struct TestingDriver {
    read_stream: Arc<Mutex<Vec<bytes::Bytes>>>,
    write_stream: Arc<Mutex<Vec<bytes::Bytes>>>,
}

impl TestingDriver {
    fn new() -> Self {
        TestingDriver {
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
    async fn expect_one_frame<T: Serialize + DeserializeOwned>(&self) -> net::Frame<T> {
        let mut write_stream = self.write_stream.lock();
        if write_stream.is_empty() {
            panic!("No data in write stream");
        }
        let data = write_stream.remove(0);
        let frame: net::Frame<T> = net::Frame::decode(data).expect("Failed to decode frame");
        frame
    }

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

struct MockConnectionDriver {
    test_driver: TestingDriver,
}

impl MockConnectionDriver {
    fn new(test_driver: TestingDriver) -> Self {
        MockConnectionDriver { test_driver }
    }
}

#[async_trait]
impl ConnectionDriver for MockConnectionDriver {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        Ok(())
    }

    async fn open_raw_stream(&mut self) -> Result<RawStream, ConnectionError> {
        // Create mock read/write streams
        let read_data = self.test_driver.read_stream.clone();
        let write_data = self.test_driver.write_stream.clone();

        let mock_read = MockStreamReader::new(read_data);
        let mock_write = MockStreamWriter::new(write_data);

        // Create raw stream
        Ok(RawStream::new(Box::new(mock_read), Box::new(mock_write)))
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
        _cx: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If we've read all data in the current buffer, try to load more
        if self.position >= self.current_buffer.len() {
            if !self.load_next_chunk() {
                // No more data available
                return Poll::Ready(Ok(()));
            }
        }

        // Copy data from current buffer to the output buffer
        let remaining = self.current_buffer.len() - self.position;
        let to_read = std::cmp::min(remaining, buf.remaining());

        if to_read > 0 {
            buf.put_slice(&self.current_buffer[self.position..self.position + to_read]);
            self.position += to_read;
        }

        Poll::Ready(Ok(()))
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
                // Extract the frame data by skipping the length prefix (8 bytes)
                // and taking the rest of the buffer.
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

#[tokio::test]
async fn test_member_actor_reachability_to_unreachable() {
    let test_driver = TestingDriver::new();
    let driver = MockConnectionDriver::new(test_driver);
    let actor = MemberActor {
        id: Id::new(),
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Pending,
        send_stream: None,
    };
    let system = System::local("test_system");
    let mut S = TestSupervisor::new(actor);

    let _timestamp = chrono::Utc::now();
    let msg = MemberActorHeartbeatCheck {
        timestamp: _timestamp.clone(),
    };

    let _ = S.send(&system, msg).await;
    /*   */
    assert_matches!(S.actor_ref().reachability, Reachability::Pending);

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: _timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: _timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let _check_timestamp = _timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: _check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 1,
            last_seen: _timestamp
        }
    );

    // 2) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = _check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 2,
            last_seen: _timestamp
        }
    );

    // 3) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 3,
            last_seen: _timestamp
        }
    );

    // 4) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    // SHOULD BE UNREACHABLE NOW.
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 0,
            last_seen: _timestamp
        }
    );
}

#[tokio::test]
async fn test_member_actor_reachability_reset() {
    let test_driver = TestingDriver::new();
    let driver = MockConnectionDriver::new(test_driver);
    let mut actor = MemberActor {
        id: Id::new(),
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Pending,
        send_stream: None,
    };
    let system = System::local("test_system");
    let mut S = TestSupervisor::new(actor);

    let timestamp = chrono::Utc::now();
    let msg = MemberActorHeartbeatCheck { timestamp };

    let _ = S.send(&system, msg).await;
    assert_matches!(S.actor_ref().reachability, Reachability::Pending);

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: _timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 1,
            last_seen: _timestamp
        }
    );

    // 2) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 2,
            last_seen: _timestamp
        }
    );

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    // Re-check to reset the misses.
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: _check_timestamp,
        }
    );

    // 3) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 1,
            last_seen: _timestamp
        }
    );
}

#[tokio::test]
async fn test_member_actor_unreachable_to_reachable() {
    let test_driver = TestingDriver::new();
    let driver = MockConnectionDriver::new(test_driver);
    let mut actor = MemberActor {
        id: Id::new(),
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Unreachable {
            pings: 0,
            last_seen: chrono::Utc::now(),
        },
        send_stream: None,
    };
    let system = System::local("test_system");
    let mut S = TestSupervisor::new(actor);

    let _timestamp = chrono::Utc::now();

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: _timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 1,
            last_seen: _timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = _timestamp + chrono::Duration::seconds(2);
    let msg = MemberActorHeartbeatUpdate {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;

    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 2,
            last_seen: _timestamp
        }
    );

    // 2) Simulate a heartbeat check
    let msg = MemberActorHeartbeatUpdate {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 3,
            last_seen: _check_timestamp
        }
    );

    // 4) Simulate a heartbeat check
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    // SHOULD BE REACHABLE NOW.
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: _timestamp
        }
    );
}

#[tokio::test]
async fn test_member_actor_unreachable_reset() {
    let test_driver = TestingDriver::new();
    let driver = MockConnectionDriver::new(test_driver);
    let mut actor = MemberActor {
        id: Id::new(),
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Unreachable {
            pings: 0,
            last_seen: chrono::Utc::now(),
        },
        send_stream: None,
    };
    let system = System::local("test_system");
    let mut S = TestSupervisor::new(actor);

    let _timestamp = chrono::Utc::now();

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: _timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 1,
            last_seen: _timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let _future_timestamp = _timestamp + chrono::Duration::seconds(2);
    let msg = MemberActorHeartbeatUpdate {
        timestamp: _future_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 2,
            last_seen: _future_timestamp,
        }
    );

    // 2) Simulate a heartbeat update in the far future
    let _future_timestamp = _timestamp + chrono::Duration::seconds(20);
    let msg = MemberActorHeartbeatUpdate {
        timestamp: _future_timestamp.clone(),
    };
    let _ = S.send(&system, msg).await;
    assert_matches!(
        S.actor_ref().reachability,
        Reachability::Unreachable {
            pings: 0,
            last_seen: _future_timestamp
        }
    );
}

#[tokio::test]
async fn test_membership_initiation() {
    let test_driver = TestingDriver::new();
    let driver = MockConnectionDriver::new(test_driver.clone());
    let node = Node {
        name: "test".to_string(),
        node_id: Id::new(),
        addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
    };

    let mut actor = MemberActor {
        id: Id::new(),
        node: node.clone(),
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Pending,
        send_stream: None,
    };

    let system = System::local("test_system");
    let mut S = TestSupervisor::new(actor);
    // 0) Connect to the Node.
    // 1) Send Init Message and Wait for Response. Pending
    // 2) Receive InitAccept Response and Send Join Message. Joining.
    // 2) Receive InitReject Response and update Membership. Down.
    // 3) Receive JoinAccept Response and update Membership. Up.
    // 4) Receive JoinReject

    S.started().await;

    assert_matches!(S.actor_ref().membership, Membership::Pending);
    assert_matches!(S.actor_ref().reachability, Reachability::Pending);

    // 1) Verify Init Message was sent
    let frame: net::Frame<net::NodeMessage> = test_driver.expect_one_frame().await;
    if let net::Payload::Ok(net::NodeMessage::Init {
        id,
        protocol_version,
    }) = frame.payload
    {
        assert_eq!(protocol_version, 1);
    } else {
        panic!("Expected Init message, got: {:?}", frame.payload);
    }

    // 2) Simulate receiving InitAck response
    let init_accept = net::Frame {
        header: net::Header::new(Id::new(), Some(node.node_id.clone())),
        payload: net::Payload::Ok(net::NodeMessage::InitAck { node_id: Id::new() }),
    };
    test_driver.push_frame(init_accept);

    // Wait a bit for the actor to process the response
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify that the membership state changed to Joining
    assert_matches!(S.actor_ref().membership, Membership::Joining);
    // TODO: We need to simulate the context for the actor to be able to send and receive messages.
}
