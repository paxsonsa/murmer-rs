# Receptionist Architecture: Distributed Actor Discovery & Communication

## Overview

The Receptionist system in murmer-rs provides **type-safe distributed service discovery** for actors across node boundaries. It serves as the unified registry that abstracts local vs. remote actor access, enabling seamless cross-node communication while maintaining compile-time type safety.

## Core Components

### 1. Receptionist (`receptionist.rs`)
The central service registry that maintains a type-centric actor directory.

**Key Features:**
- **Unified API**: Same interface for local and remote actors
- **Type Safety**: Compile-time guarantees through receptionist keys
- **Lazy Resolution**: Deferred endpoint creation for remote actors
- **Location Transparency**: Clients don't need to know actor location

### 2. NodeActor (`node/actor.rs`) 
Manages network connections and distributed communication between nodes.

**Key Features:**
- **Connection Management**: Handles node lifecycle and heartbeat monitoring
- **Proxy Management**: Maintains bidirectional proxy collections
- **Stream Multiplexing**: Multiple actor streams over single node connection
- **Cluster Messaging**: Processes ActorAdd/ActorRemove notifications

### 3. ActorProxy System
Transparent network proxies that handle marshaling and transport.

**Two Types:**
- **ActorProxy**: Outbound proxy for accessing remote actors
- **RemoteActorProxy**: Inbound proxy for serving local actors to remote clients

## Architecture Patterns

### Type Safety Strategy

The system ensures type safety through a multi-layered approach:

#### 1. Unique Receptionist Keys
```rust
impl Actor for MyActor {
    const ACTOR_TYPE_KEY: &'static str = "MyActor";
}
```
- Each actor type has a unique string identifier
- Macro validation ensures no collisions at compile time
- Keys are used for registration and lookup operations

#### 2. ActorPath Integration
```rust
struct ActorPath {
    node_id: String,     // Which node owns this actor
    type_id: String,     // The receptionist key
    // ... other fields
}
```
- Contains the receptionist key as `type_id`
- Used for routing and type verification
- Transformed when crossing node boundaries

#### 3. Typed Lookup API
```rust
// Type-safe lookup with compile-time guarantees
let endpoint: Endpoint<MyActor> = receptionist
    .lookup_one_typed("MyActor").await?;
```

### Lazy Endpoint Creation

The `LazyEndpoint` system solves the **distributed typing problem**:

```rust
struct LazyEndpoint {
    endpoint: Arc<Mutex<Option<AnyEndpoint>>>,
    endpoint_factory: Box<dyn EndpointFactory>,
}
```

**Problem**: NodeActor receives untyped actor registrations from network
**Solution**: Defer type resolution until access time

#### EndpointFactory Implementations
- **NoOpFactory**: For local actors (endpoint already exists)
- **RemoteEndpointFactory**: Creates ActorProxy for remote actors
- **PlaceholderFactory**: Backward compatibility (panics if used)

## Cluster-NodeActor-Receptionist Integration (Hybrid Approach)

### Ownership Hierarchy
The system follows a clear ownership chain for distributed coordination:
```
Cluster Actor (owns) → NodeActor Endpoints → ActorProxy Collections
```
The Cluster actor serves as the **distributed system coordinator**, managing the fleet of NodeActors that handle individual node connections.

### Hybrid Synchronization Strategy

The integration uses a **hybrid push-pull model** combining subscription-based local synchronization with direct remote registration:

#### Push: Local Actor Propagation (Subscription System)
NodeActors subscribe to local receptionist changes and broadcast them to the cluster:

```rust
impl NodeActor {
    async fn started(&mut self, ctx: &mut Context<Self>) {
        // 1. Establish network connection
        self.setup_connection().await;
        
        // 2. Get receptionist endpoint from cluster
        let receptionist = self.cluster_ref.get_receptionist().await;
        
        // 3. Subscribe to local actor changes with filtering
        let subscription = receptionist.subscribe(SubscriptionFilter {
            path_pattern: Some("local/**/*"),  // Only local actors
            exclude_system: true,              // Skip system actors
            public_only: true,                 // Only publicly accessible actors
        }).await?;
        
        // 4. Start registry sync task
        self.start_registry_sync_task(subscription, ctx).await;
    }
    
    async fn start_registry_sync_task(&mut self, subscription: Subscription, ctx: &mut Context<Self>) {
        let endpoint = ctx.endpoint();
        ctx.spawn(async move {
            while let Some(event) = subscription.next().await {
                match event {
                    RegistryEvent::Added(path, _endpoint) => {
                        if self.should_broadcast_actor(&path) {
                            self.broadcast_actor_add(path).await?;
                        }
                    }
                    RegistryEvent::Removed(path) => {
                        if self.should_broadcast_actor(&path) {
                            self.broadcast_actor_remove(path).await?;
                        }
                    }
                }
            }
        });
    }
}
```

**Benefits:**
- **Event-driven**: Only processes actual changes
- **Filtered**: Targets specific actor patterns (local, public actors only)
- **Real-time**: Immediate cluster propagation
- **Efficient**: No polling or periodic synchronization

#### Pull: Remote Actor Registration (Direct Integration)
NodeActors handle incoming `ActorAdd`/`ActorRemove` messages by directly registering with the local receptionist:

```rust
impl Handler<RecvFrame> for NodeActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: RecvFrame) {
        match msg.frame.body.payload {
            Payload::ActorAdd(remote_path) => {
                // Transform path to reflect remote origin
                let local_path = self.transform_remote_path(remote_path);
                
                // Create EndpointFactory for this remote actor
                // TODO: Needs design refinement - see EndpointFactory section
                let factory = RemoteEndpointFactory::new(ctx.weak_ref(), local_path.clone());
                
                // Register directly with receptionist
                self.receptionist.register_remote(local_path, factory).await?;
            }
            Payload::ActorRemove(remote_path) => {
                let local_path = self.transform_remote_path(remote_path);
                self.receptionist.deregister(local_path).await?;
            }
        }
    }
}
```

**Benefits:**
- **Centralized logic**: All remote actor handling in NodeActor
- **Lifecycle coupling**: Remote registrations tied to NodeActor health
- **Simplified debugging**: Clear ownership and responsibility chains

### Path Transformation and Filtering

#### Path Transformation Logic
```rust
impl NodeActor {
    fn transform_remote_path(&self, remote_path: ActorPath) -> ActorPath {
        ActorPath {
            node_id: self.remote_node_id.clone(),  // Actual remote node ID
            type_id: remote_path.type_id,          // Preserve receptionist key
            group_id: remote_path.group_id,        // Preserve hierarchy
            instance_id: remote_path.instance_id,  // Preserve instance
        }
    }
    
    fn should_broadcast_actor(&self, path: &ActorPath) -> bool {
        // Only broadcast locally-owned, public actors
        path.node_id == "local" && 
        !path.is_system_actor() &&
        path.is_public()  // Only public actors cross node boundaries
    }
}
```

#### Subscription Filtering
The subscription system allows fine-grained control over which registry changes trigger cluster synchronization:
- **Path patterns**: `"local/**/*"` for all local actors
- **Actor types**: Filter by specific `ACTOR_TYPE_KEY` values
- **Visibility**: Public vs. private actor distinction
- **System actors**: Exclude internal system actors from propagation

### Complete Integration Flow

#### Local Actor Registration Propagation
```
1. Local Actor → Receptionist.register_local()
2. Receptionist → RegistryEvent::Added → NodeActor Subscription
3. NodeActor → filter + broadcast ActorAdd to all remote nodes
4. Remote NodeActors → register with their local receptionists
5. Cluster-wide discovery enabled
```

#### Remote Actor Discovery Integration  
```
1. Remote NodeActor → ActorAdd message over network
2. Local NodeActor → transform path + create EndpointFactory
3. Local Receptionist → register_remote()
4. Local clients → transparent access via lookup_typed()
```

### EndpointFactory Design Solution: Phantom Type Endpoints

**The Core Challenge**: When a client performs a typed lookup like `receptionist.lookup_one_typed::<MyActor>("MyActor")`, they expect to receive an `Endpoint<MyActor>`. However, for remote actors, the actual implementation is a proxy that forwards messages over the network. The type system needs to bridge this gap between the client's expectation of `Endpoint<ActualActor>` and the reality of `Endpoint<ProxyActor>`.

#### The Phantom Type Approach

The solution uses **phantom type endpoints** where the type parameter represents the **interface contract** rather than the actual actor implementation:

```rust
pub struct Endpoint<I> // I = Interface type (phantom)
where
    I: Actor,
{
    sender: DynamicSender,
    path: Arc<ActorPath>,
    _phantom: PhantomData<I>,
}

trait DynamicEndpointSender: Send + Sync {
    async fn send_message<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        M: Message + 'static;
}

impl<I: Actor> Endpoint<I> {
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        I: Handler<M>,
        M: Message + 'static,
    {
        self.sender.send_message(msg).await
    }
}
```

#### Type Safety Through Receptionist Key System

The key insight is that **type safety is enforced by the receptionist key system** rather than direct trait bounds:

1. **Compile-time Registry**: A compile-time registry (via macros/inventory) maps actor keys to their supported message types
2. **Receptionist Key Binding**: Each actor type has a unique `ACTOR_TYPE_KEY` that serves as the lookup identifier
3. **Lookup Contract**: When a client looks up `"MyActor"` and casts to `Endpoint<MyActor>`, the registry guarantees that the remote actor can handle the same messages as `MyActor`

```rust
// Compile-time registry ensures this mapping is valid
inventory::submit! {
    ActorHandlerRegistration {
        actor_key: "MyActor",
        message_types: &["MyMessage", "AnotherMessage"],
        serialization_support: true,
    }
}

impl DynamicEndpointSender for RemoteEndpointSender {
    async fn send_message<M>(&self, msg: M) -> Result<M::Result, SendError> {
        // This is safe because:
        // 1. Client obtained this endpoint by looking up a specific actor key
        // 2. Compile-time registry guarantees the actor can handle message M
        // 3. Remote node has the same actor type with same capabilities
        
        debug_assert!(
            ActorRegistry::can_handle(&self.actor_key, std::any::type_name::<M>()),
            "Message type not registered for actor - compile-time registry bug"
        );
        
        self.send_marshaled(msg).await
    }
}
```

#### Safety Guarantees

This approach provides the same type safety as local endpoints, but with different enforcement:

- **Local Endpoints**: Safety enforced by direct trait bounds (`A: Handler<M>`)
- **Remote Endpoints**: Safety enforced by receptionist key system + compile-time registry

The safety guarantee chain:
1. **Compile-time**: `MyActor` implements `Handler<MyMessage>` (verified by compiler)
2. **Registration**: Actor registered under key "MyActor" (enforced by macro/registry)  
3. **Lookup**: Client requests "MyActor" and gets typed `Endpoint<MyActor>`
4. **Usage**: Client can only send messages that `MyActor` handles (phantom type constraint)
5. **Runtime**: Remote sender validates message type against registry (debug assertion)

#### Implementation Strategy

```rust
struct RemoteEndpointFactory {
    node_actor: WeakRef<NodeActor>,
    remote_path: ActorPath,
}

impl EndpointFactory for RemoteEndpointFactory {
    fn create(&self) -> Result<AnyEndpoint, Error> {
        // 1. Create dynamic sender that handles network marshaling
        let sender = RemoteEndpointSender::new(
            self.node_actor.clone(),
            self.remote_path.clone(),
        );
        
        // 2. Create phantom-typed endpoint
        // Note: The actual type parameter doesn't matter for AnyEndpoint
        // The type safety comes from the receptionist key system
        let endpoint = Endpoint {
            sender: DynamicSender::Remote(sender),
            path: Arc::new(self.remote_path.clone()),
            _phantom: PhantomData,
        };
        
        // 3. Type-erase for storage in receptionist
        Ok(AnyEndpoint::from_phantom_endpoint(endpoint, &self.remote_path.type_id))
    }
}
```

#### Benefits of This Approach

1. **No API Changes**: Clients continue using `Endpoint<MyActor>` exactly as before
2. **True Type Safety**: Phantom type constraints prevent invalid message sends at compile-time
3. **Unified Interface**: Local and remote endpoints have identical APIs
4. **Registry Validation**: Compile-time registry catches configuration errors early
5. **Network Transparency**: Marshaling and proxy management hidden from clients

#### Error Handling

Runtime errors can only occur from:
- **Registry Inconsistency**: Compile-time bug in actor registration (preventable)
- **Version Mismatches**: Operational issue between node versions
- **Serialization Failures**: Data corruption, not type mismatches
- **Network Failures**: Connection issues, not type safety violations

The phantom type approach **eliminates type mismatch errors** by ensuring they're caught at compile-time through the receptionist key system.

## Bidirectional Proxy Architecture

### Terminology
- **Remote Node/Actor**: Where the actor actually runs (the "server")
- **Local Proxy/Node/Endpoint**: Client-side entities making requests
- **RemoteActorProxy**: Server-side proxy handling incoming requests

### Outbound Flow (Local → Remote)

```
Local Client → Endpoint<A> → ActorProxy → Stream → Network → RemoteActorProxy → Local Actor
```

1. **Client Request**: Sends typed message to `Endpoint<A>`
2. **Marshaling**: Endpoint serializes message to wire format
3. **Network Transport**: ActorProxy sends over dedicated stream
4. **Remote Handling**: RemoteActorProxy unmarshals and forwards to local actor
5. **Response**: Response follows reverse path with marshaling

### Inbound Flow (Remote → Local)

```
Remote Client → Network → Stream → RemoteActorProxy → Endpoint<A> → Local Actor
```

1. **Stream Accept**: RemoteActorProxy accepts new stream from remote node
2. **Unmarshaling**: Converts wire format to typed messages
3. **Local Delivery**: Forwards to local actor via its `Endpoint<A>`
4. **Response Marshaling**: Serializes response back through stream

### NodeActor's Dual Proxy Management

Each NodeActor maintains **two proxy collections**:

#### Outbound Proxies (ActorProxy)
```rust
// Created when local clients access remote actors
struct ActorProxy {
    // Handles marshaling outbound requests
    // Opens dedicated stream to remote node
    // Manages request/response lifecycle
}
```

#### Inbound Proxies (RemoteActorProxy)  
```rust
// Created when remote nodes access local actors
struct RemoteActorProxy {
    // Accepts dedicated stream from remote node
    // Handles unmarshaling inbound requests
    // Forwards to local actor endpoints
}
```

## Remote Actor Registration Flow

### 1. ActorAdd Message Reception
When NodeActor receives `ActorAdd` from remote node:

```rust
// Incoming ActorPath from remote node
let incoming_path = ActorPath { 
    node_id: "local",           // Remote node's local reference
    type_id: "MyActor",         // Receptionist key
    // ...
};

// Transform to reflect actual remote location
let remote_path = ActorPath {
    node_id: remote_node_id,    // Actual remote node ID
    type_id: "MyActor",         // Preserved receptionist key
    // ...
};
```

### 2. EndpointFactory Creation
NodeActor creates factory for lazy proxy instantiation:

```rust
struct RemoteEndpointFactory {
    node_actor: WeakRef<NodeActor>,
    actor_path: ActorPath,
}

impl EndpointFactory for RemoteEndpointFactory {
    fn create(&self) -> Result<AnyEndpoint, Error> {
        // 1. Spawn ActorProxy owned by NodeActor
        // 2. Open dedicated stream for this proxy
        // 3. Return Endpoint<ActorProxy> with hidden marshaling
    }
}
```

### 3. Receptionist Registration
```rust
receptionist.register_remote(remote_path, factory).await?;
```

The remote actor is now discoverable locally but connection is deferred until first access.

## Stream Architecture

### Connection Multiplexing
- **Single Connection**: One NodeActor connection per remote node
- **Multiple Streams**: Each ActorProxy gets dedicated stream
- **Bidirectional**: Both inbound and outbound proxies use same connection

### Stream Lifecycle
1. **Proxy Creation**: New ActorProxy spawned when remote actor accessed
2. **Stream Opening**: Proxy opens dedicated stream to remote node
3. **Remote Accept**: Remote NodeActor accepts stream, creates RemoteActorProxy
4. **Communication**: Request/response over dedicated stream
5. **Cleanup**: Proxy death closes stream automatically

### Error Handling
- **Connection Failure**: All proxies for that node die together
- **Stream Failure**: Individual proxy dies, others continue
- **Proxy Lifecycle**: Coupled to NodeActor lifecycle

## Message Flow Examples

### Local Actor Access
```rust
// 1. Lookup (immediate)
let endpoint = receptionist.lookup_one_typed::<MyActor>("MyActor").await?;

// 2. Send message (direct in-memory)
let response = endpoint.send(MyMessage).await?;
```

### Remote Actor Access (First Time)
```rust
// 1. Lookup (triggers lazy creation)
let endpoint = receptionist.lookup_one_typed::<MyActor>("MyActor").await?;
// - EndpointFactory.create() called
// - ActorProxy spawned
// - Stream opened to remote node
// - RemoteActorProxy created on remote side

// 2. Send message (network transport)
let response = endpoint.send(MyMessage).await?;
// - Message marshaled to wire format
// - Sent over dedicated stream
// - RemoteActorProxy unmarshals and forwards
// - Response marshaled back
```

### Remote Actor Access (Subsequent)
```rust
// 1. Lookup (returns cached endpoint)
let endpoint = receptionist.lookup_one_typed::<MyActor>("MyActor").await?;

// 2. Send message (reuses existing stream)
let response = endpoint.send(MyMessage).await?;
```

## Integration with Actor System

### Registration Patterns

#### Local Actor Registration
```rust
// Direct registration with existing endpoint
receptionist.register_local(path, endpoint).await?;
```

#### Remote Actor Registration (via NodeActor)
```rust
// Automatic registration when NodeActor receives ActorAdd
impl Handler<ActorAdd> for NodeActor {
    async fn handle(&mut self, msg: ActorAdd) {
        let remote_path = transform_path(msg.path, self.remote_id);
        let factory = RemoteEndpointFactory::new(self.weak_ref(), remote_path);
        self.receptionist.register_remote(remote_path, factory).await;
    }
}
```

### Automatic Synchronization

When local actors are registered with receptionist:
1. **Local Registration**: Actor registered locally
2. **Cluster Notification**: NodeActor sends ActorAdd to all connected nodes
3. **Remote Registration**: Remote nodes register actor with their receptionists
4. **Bidirectional Discovery**: Actor now discoverable across entire cluster

## Benefits

### For Developers
- **Location Transparency**: Same API for local and remote actors
- **Type Safety**: Compile-time guarantees prevent runtime errors
- **Automatic Discovery**: No manual service configuration required
- **Fault Tolerance**: Failed nodes automatically clean up registrations

### For System
- **Network Efficiency**: Connections created only when needed
- **Resource Management**: Proxy lifecycle tied to connection health
- **Scalability**: Stream multiplexing reduces connection overhead
- **Consistency**: Eventual consistency across cluster for actor registry

## Future Enhancements

### Planned Features
- **Service Mesh Integration**: Load balancing across actor instances
- **Health Checks**: Periodic actor health verification
- **Versioning**: Support for actor interface evolution
- **Security**: Authentication and authorization for cross-node access

### Performance Optimizations
- **Connection Pooling**: Reuse streams across proxies
- **Batch Messaging**: Combine multiple messages in single network round-trip
- **Compression**: Wire format compression for large messages
- **Caching**: Smart caching of frequently accessed remote actors

This architecture provides a robust foundation for distributed actor communication while maintaining the simplicity and type safety that makes actor systems powerful for building concurrent, distributed applications.