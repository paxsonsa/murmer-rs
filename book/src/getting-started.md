# Getting Started

This chapter goes deeper into the components you saw in the [introduction](./introduction.md). If you haven't seen the 1-minute example yet, start there — this chapter assumes you've seen the basics and want to understand more.

## Dependencies

```toml
[dependencies]
murmer = "0.1"
murmer-macros = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

`murmer` is the core framework. `murmer-macros` provides the `#[handlers]`, `#[handler]`, and `#[derive(Message)]` proc macros. Both serde and tokio are required — serde for message serialization (even in local mode, the types need to be `Serialize + Deserialize` for remote readiness), and tokio as the async runtime.

## The Actor trait

Every actor implements the `Actor` trait, which has one required associated type: `State`.

```rust,ignore
use murmer::prelude::*;

#[derive(Debug)]
struct ChatRoom;

struct ChatRoomState {
    room_name: String,
    messages: Vec<ChatEntry>,
    max_messages: usize,
}

struct ChatEntry {
    from: String,
    text: String,
    timestamp: u64,
}

impl Actor for ChatRoom {
    type State = ChatRoomState;
}
```

### Why state lives separately

This is a deliberate design choice. In many actor frameworks, state lives directly on the actor struct. In murmer, the actor struct is typically empty (zero-sized) and all mutable state lives in the associated `State` type.

This gives you:
- **Explicit state threading** — every handler receives `&mut State`, making it clear what data is being read and modified.
- **Clean restarts** — when a supervisor restarts an actor, the factory creates a fresh `(Actor, State)` pair. No hidden state carried over from a crashed instance.
- **Separation of identity and data** — the actor struct can carry configuration or immutable context (like a database pool handle), while `State` holds the mutable per-instance data.

You *can* put fields on the actor struct — they just won't be part of the restart cycle:

```rust,ignore
struct ChatRoom {
    db: DatabasePool,  // immutable, shared across restarts
}

struct ChatRoomState {
    messages: Vec<ChatEntry>,  // mutable, reset on restart
}
```

## Defining handlers

Handlers are methods on the actor that process incoming messages. The `#[handlers]` macro on the `impl` block and `#[handler]` on individual methods does the heavy lifting:

```rust,ignore
#[handlers]
impl ChatRoom {
    #[handler]
    fn post_message(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        from: String,
        text: String,
    ) -> usize {
        state.messages.push(ChatEntry {
            from,
            text,
            timestamp: now(),
        });
        // Trim if over limit
        if state.messages.len() > state.max_messages {
            state.messages.remove(0);
        }
        state.messages.len()
    }

    #[handler]
    fn get_history(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
    ) -> Vec<String> {
        state.messages.iter()
            .map(|e| format!("[{}] {}: {}", e.timestamp, e.from, e.text))
            .collect()
    }

    #[handler]
    fn room_name(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
    ) -> String {
        state.room_name.clone()
    }
}
```

### Handler signature rules

Every handler method must follow this pattern:

```rust,ignore
fn method_name(
    &mut self,                          // always &mut self
    ctx: &ActorContext<Self>,           // always second — the actor's context
    state: &mut YourStateType,         // always third — mutable state access
    // ... additional parameters become message fields
) -> ReturnType {
    // ...
}
```

- **`&mut self`** — the actor instance.
- **`ctx: &ActorContext<Self>`** — provides access to the system, receptionist, the actor's own label, and methods like `ctx.watch()` for actor monitoring. Prefix with `_` if unused.
- **`state: &mut State`** — the actor's mutable state.
- **Additional parameters** — each one becomes a field on the generated message struct. `fn increment(... amount: i64)` generates `Increment { pub amount: i64 }`.
- **Return type** — becomes the message's `Result` type. Handlers must return a value (not `()`).

### What gets generated

For the `ChatRoom` example above, the macro generates:

```rust,ignore
// Message structs
struct PostMessage { pub from: String, pub text: String }
struct GetHistory;  // unit struct — no extra params
struct RoomName;

// Trait implementations
impl Handler<PostMessage> for ChatRoom { /* ... */ }
impl Handler<GetHistory> for ChatRoom { /* ... */ }
impl Handler<RoomName> for ChatRoom { /* ... */ }

// Remote dispatch table
impl RemoteDispatch for ChatRoom { /* ... */ }

// Extension trait for ergonomic sends
trait ChatRoomExt {
    fn post_message(&self, from: String, text: String) -> impl Future<Output = Result<usize>>;
    fn get_history(&self) -> impl Future<Output = Result<Vec<String>>>;
    fn room_name(&self) -> impl Future<Output = Result<String>>;
}

impl ChatRoomExt for Endpoint<ChatRoom> { /* ... */ }
```

This means you can call `endpoint.post_message("alice".into(), "hello".into())` directly on any `Endpoint<ChatRoom>`, without ever constructing a message struct yourself.

### Async handlers

For handlers that need to perform async work (I/O, database queries, HTTP calls), use `async fn`:

```rust,ignore
#[handlers]
impl ChatRoom {
    #[handler]
    async fn fetch_and_store(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        url: String,
    ) -> Result<usize, String> {
        // You can .await here — the supervisor handles scheduling
        let response = reqwest::get(&url).await
            .map_err(|e| e.to_string())?;
        let body = response.text().await
            .map_err(|e| e.to_string())?;

        state.messages.push(ChatEntry {
            from: "system".into(),
            text: body,
            timestamp: now(),
        });
        Ok(state.messages.len())
    }
}
```

Async handlers generate `AsyncHandler<FetchAndStore>` instead of `Handler<FetchAndStore>`. The supervisor processes async handlers cooperatively — while one handler is awaiting, no other messages are processed for that actor (preserving the single-writer invariant).

## Explicit message types

The auto-generated messages from `#[handlers]` cover most cases, but sometimes you want to define a message type explicitly — for example, when multiple actors handle the same message:

```rust,ignore
use murmer_macros::Message;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = String)]
struct Ping {
    payload: String,
}
```

To use an explicit message in a handler, name the parameter `msg`:

```rust,ignore
#[handlers]
impl ChatRoom {
    #[handler]
    fn ping(
        &mut self,
        _ctx: &ActorContext<Self>,
        _state: &mut ChatRoomState,
        msg: Ping,          // "msg" signals: use this type directly
    ) -> String {
        format!("pong: {}", msg.payload)
    }
}
```

The `msg` parameter name tells the macro to use `Ping` as-is instead of generating a new message struct.

For messages that need to cross the network, add `remote`:

```rust,ignore
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = String, remote = "my_app::Ping")]
struct Ping {
    payload: String,
}
```

The `remote` value is a unique string identifier used for wire-format routing. See the [Proc Macro Reference](./macros.md) for the full details.

## The System

The `System` is the entry point for everything — it runs the actor runtime, manages the receptionist, and optionally handles clustering.

```rust,ignore
// Local mode — in-memory, no networking
let system = System::local();

// Clustered mode — QUIC networking, SWIM membership
let system = System::clustered_auto(config).await?;
```

Both modes expose the same API. Your actor code doesn't change.

### Starting actors

```rust,ignore
// Returns Endpoint<ChatRoom>
let room = system.start(
    "room/general",                   // label — unique within the cluster
    ChatRoom,                         // actor instance
    ChatRoomState {                   // initial state
        room_name: "general".into(),
        messages: vec![],
        max_messages: 1000,
    },
);
```

The label `"room/general"` is a path-like string that uniquely identifies this actor in the system. Labels are how actors find each other through the receptionist.

### Looking up actors

```rust,ignore
// Type-safe lookup — returns Option<Endpoint<ChatRoom>>
let room = system.lookup::<ChatRoom>("room/general");

if let Some(ep) = room {
    let history = ep.get_history().await?;
}
```

Lookups are type-checked at compile time. If the label exists but the type doesn't match, `None` is returned.

## Endpoints in depth

`Endpoint<A>` is the central abstraction. It's:

- **Typed** — `Endpoint<ChatRoom>` can only send messages that `ChatRoom` handles
- **Cloneable** — lightweight handle, share freely across tasks
- **Location-transparent** — local endpoints dispatch through in-memory channels; remote endpoints serialize over QUIC

```rust,ignore
// All of these work identically
let room: Endpoint<ChatRoom> = system.start("room/1", ChatRoom, state);
let room: Endpoint<ChatRoom> = system.lookup::<ChatRoom>("room/1").unwrap();

// Clone and pass to another task
let room2 = room.clone();
tokio::spawn(async move {
    room2.post_message("bot".into(), "background task".into()).await.unwrap();
});

// Send via extension methods (ergonomic)
room.post_message("alice".into(), "hello".into()).await?;

// Or send a message struct directly
room.send(PostMessage { from: "alice".into(), text: "hello".into() }).await?;
```

Both `.post_message(...)` (extension method) and `.send(PostMessage { ... })` (direct) do the same thing. The extension methods are more ergonomic for the common case.

## Actor watches

Monitor other actors and get notified when they terminate — inspired by Erlang's `monitor/2`:

```rust,ignore
struct Watchdog;
struct WatchdogState { terminated: Vec<String> }

impl Actor for Watchdog {
    type State = WatchdogState;

    fn on_actor_terminated(
        &mut self,
        state: &mut WatchdogState,
        terminated: &ActorTerminated,
    ) {
        tracing::warn!(
            "Actor {} terminated: {:?}",
            terminated.label,
            terminated.reason
        );
        state.terminated.push(terminated.label.clone());
    }
}

#[handlers]
impl Watchdog {
    #[handler]
    fn watch(
        &mut self,
        ctx: &ActorContext<Self>,
        _state: &mut WatchdogState,
        label: String,
    ) -> bool {
        ctx.watch(&label);
        true
    }
}
```

The `on_actor_terminated` callback on the `Actor` trait fires when any watched actor stops, crashes, or is killed. The `ActorTerminated` struct tells you which actor and why.

## Going from local to clustered

The entire point of murmer's design is that this transition requires **zero changes to your actor code**. Only the system construction changes:

```rust,ignore
// Before: local
let system = System::local();

// After: clustered
let config = ClusterConfig::builder()
    .name("my-node")
    .listen("0.0.0.0:7100".parse()?)
    .cookie("my-cluster-secret")
    .seed_nodes(["192.168.1.1:7100".parse()?])
    .build()?;

let system = System::clustered_auto(config).await?;
```

Everything else — `system.start(...)`, `system.lookup(...)`, `endpoint.send(...)` — stays identical. Actors on remote nodes appear in your local receptionist automatically via registry replication.

See the [Clustering](./clustering.md) chapter for the full walkthrough, including Docker deployment and the interactive `cluster_chat` example.

## Build and test

```sh
cargo build
cargo nextest run
cargo clippy -- -D warnings
```

## Next steps

Now that you understand the components, dive into the specific areas:

- [Actors and Messages](./actors-and-messages.md) — the complete actor model: state, handlers, endpoints, location transparency
- [Discovery](./discovery.md) — labels, reception keys, listings, and routing
- [Supervision](./supervision.md) — restart policies, backoff, actor factories
- [Clustering](./clustering.md) — QUIC networking, SWIM membership, multi-node deployment
- [Proc Macro Reference](./macros.md) — everything `#[handlers]` and `#[derive(Message)]` generate
- [Application Orchestration](./murmer-app.md) — placement, leader election, crash recovery
