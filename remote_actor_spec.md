# Remote Actor Communication Specification

## Overview

This document specifies how the Murmer-rs actor system handles remote actor communication across nodes in a distributed environment. This includes both the client-side (accessing remote actors) and server-side (exposing local actors to remote nodes) aspects of the distributed actor system.

## Design Goals

1. **Strong Type Safety**: Maintain Rust's type safety across node boundaries
2. **Transparent Location**: Actor interactions should be similar regardless of actor location
3. **Resilient Communication**: Handle network failures and node disconnections gracefully
4. **Efficient Transport**: Optimize for minimal overhead in message passing

## Components

### 1. NodeActor

The NodeActor maintains a connection to a remote node and facilitates communication with actors on that node:

- Maintains bidirectional connection to remote node via QUIC
- Tracks available remote actors on the connected node
- Creates and manages proxy actors for remote actors
- Handles connection lifecycle and error conditions

### 2. RemoteActor Proxies

For each remote actor, a proxy is created that:

- Represents the remote actor in the local system
- Forwards messages to the remote actor
- Returns responses to the local sender
- Handles serialization/deserialization of messages

### 3. Actor Registration Protocol

A protocol for informing nodes about available actors:

- `ActorAdd`: Announce a new actor is available on a node
- `ActorRemove`: Announce an actor is no longer available
- These messages update the local node's knowledge of remote actors

### 4. Local Actor Proxy

For each local actor exposed to remote nodes:

- Receives serialized messages from remote nodes
- Deserializes messages and forwards to the actual local actor
- Serializes responses and sends them back to the remote node
- Handles lifecycle events like actor termination

## Message Flow

### Remote Actor Discovery

1. NodeActor establishes connection with remote node
2. Cluster actor on the remote node monitors receptionist registrations
3. For each local actor registered with the receptionist, the Cluster actor sends `ActorAdd` messages to all connected nodes
4. NodeActor creates proxies for each remote actor
5. Proxies are registered with local receptionist with remote ActorPath

### Local Actor Exposure

1. Local actor registers with receptionist
2. Cluster actor detects registration and notifies all connected NodeActors
3. Each NodeActor sends ActorAdd message to its remote node
4. Remote nodes create proxies for this actor

### Remote Actor Communication (Client Side)

1. Local code looks up an actor through receptionist (getting either local or remote actor)
2. Message is sent to actor endpoint
3. If endpoint is a RemoteActor proxy:
   a. Message is serialized into wire format
   b. Message is sent over dedicated QUIC stream for that actor
   c. Response is awaited on the same stream
   d. Response is deserialized and returned to caller

### Remote Actor Communication (Server Side)

1. NodeActor receives opening of new bidirectional QUIC stream
2. Initial message identifies the target local actor
3. NodeActor creates a LocalActorProxy for this stream
4. For each incoming message on the stream:
   a. Message is deserialized
   b. Message is forwarded to the local actor through its endpoint
   c. Response is received from local actor
   d. Response is serialized and sent back on the stream
5. If local actor is removed:
   a. All pending messages receive ActorMessage::Dead response
   b. Stream is closed

### Stream Management

1. For each RemoteActor, a dedicated bidirectional QUIC stream is established
2. Initial message identifies which actor this stream is for
3. Subsequent messages are actor-specific payloads
4. Messages/responses are processed strictly sequentially in the order received
5. Each stream maintains its own in-order processing guarantee
6. Although processing is sequential, correlation IDs are included for debugging and tracing

## Implementation Details

### NodeActor Extensions

```rust
// Messages handled by NodeActor for remote actor management
struct ActorAdd {
    actor_id: ActorId,
    actor_type_name: String,
    actor_type_version: String, // Version hash/string for type compatibility checking
    path: ActorPath,
}

struct ActorRemove {
    actor_id: ActorId,
}

// Inside NodeActor
struct NodeActor {
    // existing fields...
    
    // Track remote actors by ID (actors on the remote node)
    remote_actors: HashMap<ActorId, RemoteActorInfo>,
    
    // Track local proxies (local actors exposed to the remote node)
    local_proxies: HashMap<ActorId, LocalActorProxy>,
}

// Handles the receptionist subscription for the cluster actor
struct ReceptionistMonitor {
    subscription: ReceptionistSubscription,
    nodes: Vec<Endpoint<NodeActor>>,
}

impl ReceptionistMonitor {
    // Called when a new actor is registered with the receptionist
    async fn on_actor_registered(&mut self, key: Key, endpoint: Endpoint<dyn Actor>) {
        // Notify all connected nodes about this actor
        for node in &self.nodes {
            let add_msg = ActorAdd {
                actor_id: endpoint.id(),
                actor_type_name: key.type_name(),
                actor_type_version: key.type_version(),
                path: endpoint.path(),
            };
            
            node.send(add_msg).await;
        }
    }
    
    // Called when an actor is deregistered from the receptionist
    async fn on_actor_deregistered(&mut self, key: Key, endpoint: Endpoint<dyn Actor>) {
        // Notify all connected nodes to remove this actor
        for node in &self.nodes {
            let remove_msg = ActorRemove {
                actor_id: endpoint.id(),
            };
            
            node.send(remove_msg).await;
        }
    }
}
```

### RemoteActor Implementation (Client Side)

```rust
struct RemoteActor<A: Actor> {
    id: ActorId,
    path: ActorPath,
    stream: FrameStream<ActorMessage<A>>,
    _phantom: PhantomData<A>,
}

// Endpoint implementation for RemoteActor
impl<A: Actor> EndpointSender<A> for RemoteActorSender<A> {
    async fn send<M>(&self, msg: M) -> Result<M::Result, ActorError>
    where
        A: Handler<M>,
        M: Message + 'static,
    {
        // 1. Serialize message
        // 2. Send over QUIC stream
        // 3. Await response
        // 4. Deserialize and return
    }
}

/// LocalActorProxy Implementation (Server Side)
struct LocalActorProxy {
    id: ActorId,
    endpoint: Endpoint<dyn Actor>,
    stream: FrameStream<ActorMessage>,
    pending_messages: VecDeque<PendingMessage>,
}

struct PendingMessage {
    correlation_id: u64,
    type_name: String,
    payload: Vec<u8>,
}

impl LocalActorProxy {
    // Process incoming messages from the QUIC stream
    async fn process_messages(&mut self) {
        while let Some(msg) = self.stream.read_frame().await.ok().flatten() {
            // Deserialize the message
            let (correlation_id, type_name, payload) = deserialize_message(msg);
            
            // Add to pending queue
            self.pending_messages.push_back(PendingMessage {
                correlation_id,
                type_name,
                payload,
            });
            
            // Process the message
            self.forward_message_to_local_actor().await;
        }
    }
    
    // Forward a message to the local actor
    async fn forward_message_to_local_actor(&mut self) {
        if let Some(pending) = self.pending_messages.pop_front() {
            // Forward to the actual actor using the type_dispatcher
            let result = self.endpoint.dispatch_raw(
                pending.type_name.as_str(),
                pending.payload.clone()
            ).await;
            
            // Serialize the response
            let response = match result {
                Ok(response_bytes) => ActorMessage::Response { 
                    correlation_id: pending.correlation_id,
                    payload: response_bytes 
                },
                Err(error) => ActorMessage::Error { 
                    correlation_id: pending.correlation_id,
                    error: bincode::serialize(&error).unwrap_or_default()
                },
            };
            
            // Send the response
            if let Err(err) = self.stream.write_frame(&response).await {
                tracing::error!(error=%err, "Failed to send response to remote node");
            }
        }
    }
    
    // Handle actor removal
    async fn handle_actor_removed(&mut self) {
        // Send ActorMessage::Dead for all pending messages
        for pending in self.pending_messages.drain(..) {
            let dead_msg = ActorMessage::Dead { 
                correlation_id: pending.correlation_id,
                reason: "Actor has been removed".to_string(),
            };
            
            if let Err(err) = self.stream.write_frame(&dead_msg).await {
                tracing::error!(error=%err, "Failed to send actor dead message");
            }
        }
        
        // Close the stream
        self.stream.close().await;
    }
}
```

## Wire Protocol

### Actor Stream Initialization
```
+----------------+------------------+-------------------+
| ACTOR_STREAM   | ACTOR_ID_LENGTH  | ACTOR_ID          |
| (1 byte: 0x01) | (2 bytes)        | (variable length) |
+----------------+------------------+-------------------+
```

### Actor Message
```
+----------------+----------------+-------------------+---------------+------------------+
| MESSAGE        | MESSAGE_TYPE   | CORRELATION_ID    | PAYLOAD_SIZE  | PAYLOAD          |
| (1 byte: 0x02) | (16 bytes)     | (8 bytes)         | (4 bytes)     | (variable length)|
+----------------+----------------+-------------------+---------------+------------------+
```

### Actor Response
```
+----------------+-------------------+---------------+------------------+
| RESPONSE       | CORRELATION_ID    | PAYLOAD_SIZE  | PAYLOAD          |
| (1 byte: 0x03) | (8 bytes)         | (4 bytes)     | (variable length)|
+----------------+-------------------+---------------+------------------+
```

### Actor Error Response
```
+----------------+-------------------+---------------+------------------+
| ERROR          | CORRELATION_ID    | ERROR_SIZE    | ERROR_DATA       |
| (1 byte: 0x04) | (8 bytes)         | (4 bytes)     | (variable length)|
+----------------+-------------------+---------------+------------------+
```

### Actor Dead Response
```
+----------------+-------------------+---------------+------------------+
| DEAD           | CORRELATION_ID    | REASON_SIZE   | REASON_TEXT      |
| (1 byte: 0x05) | (8 bytes)         | (4 bytes)     | (variable length)|
+----------------+-------------------+---------------+------------------+
```

## Serialization

Message serialization will use bincode as the primary serialization format:
- Bincode is chosen for its simplicity, efficiency, and low overhead
- Actor type compatibility verified through version strings or type hashes
- All message payloads and responses will be bincode-serialized
- Error conditions are directly propagated from remote actors to callers as bincode-serialized structures
- Message types must implement Serialize/Deserialize traits from serde

## Error Handling

1. **Network Failures**: 
   - Detect disconnections through QUIC connection events
   - Mark remote actors as unreachable
   - Return errors for messages sent to unreachable actors

2. **Actor Failures**:
   - Remote actor failures transmitted as error responses
   - Local proxy directly propagates failures to callers without transformation
   - Error types must be Serialize/Deserialize compatible

3. **Reconnection**:
   - NodeActor attempts reconnection on failure
   - On reconnection, re-discover available actors

## Future Extensions

1. **Message Batching**: Optimize for multiple small messages
2. **Compression**: Add optional compression for large payloads
3. **Priorities**: Support priority levels for messages
4. **Backpressure**: Add backpressure mechanisms for high-volume scenarios