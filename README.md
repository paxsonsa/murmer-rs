# murmer

[![CI](https://github.com/paxsonsa/murmer-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/paxsonsa/murmer-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/murmer.svg)](https://crates.io/crates/murmer)
[![docs.rs](https://docs.rs/murmer/badge.svg)](https://docs.rs/murmer)
[![License](https://img.shields.io/crates/l/murmer.svg)](https://github.com/paxsonsa/murmer-rs#license)

A distributed actor framework for Rust, built on tokio and QUIC.

Murmer provides typed, location-transparent actors that communicate through message passing. Whether an actor lives in the same process or on a remote node across the network, you interact with it through the same `Endpoint<A>` API.

## Why I built this

I've spent years working with Elixir and the BEAM VM, and the actor model there is something I've grown deeply fond of — the simplicity of processes, message passing, and supervision just *works*. When I looked at bringing that experience to Rust, I studied existing implementations like Actix, Telepathy, and Akka (on the JVM side). They're impressive systems, but I kept running into the same friction: getting a basic actor up and running was complex, and adding remote communication across nodes was even more so.

Murmer is an experiment in answering a simple question: **can you build a robust distributed actor system in Rust that's actually simple to use?**

The answer, it seems, is yes.

The design draws heavy inspiration from Beam OTP's supervision and process model, Akka's clustering approach, and Apple's Swift Distributed Actors for the typed, location-transparent endpoint API. The goal is to combine these ideas with Rust's performance and safety guarantees — zero-cost local dispatch, compile-time message type checking, and automatic serialization over encrypted QUIC connections when actors span nodes.

## What it gives you

- **Send messages without caring where the actor lives.** Local and remote actors use the same `Endpoint<A>` API — your code doesn't change when an actor moves to another node.
- **Test distributed systems from a single process.** Spin up multiple nodes in-memory and verify clustering, replication, and failover without any network infrastructure.
- **Define actors with minimal boilerplate.** `#[handlers]` auto-generates message structs, dispatch tables, serialization, and ergonomic extension traits from plain method signatures — no manual message types needed.
- **Get networking and encryption handled for you.** QUIC transport with automatic TLS, SWIM-based cluster membership, and mDNS discovery — all configured, not hand-rolled.
- **Supervise actors like OTP.** Restart policies (Temporary, Transient, Permanent) with configurable limits and exponential backoff keep your system running through failures.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
murmer = { path = "murmer" }
murmer-macros = { path = "murmer-macros" }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

### Define an actor

```rust
use murmer::prelude::*;
use murmer_macros::handlers;

// 1. Actor struct + state
#[derive(Debug)]
struct Counter;

struct CounterState { count: i64 }

impl Actor for Counter {
    type State = CounterState;
}

// 2. Handlers — macro auto-generates message structs, Handler impls,
//    RemoteDispatch, and an extension trait on Endpoint<Counter>
#[handlers]
impl Counter {
    #[handler]
    fn increment(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState, amount: i64) -> i64 {
        state.count += amount;
        state.count
    }

    #[handler]
    fn get_count(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState) -> i64 {
        state.count
    }
}
```

The `#[handlers]` macro generates:
- **Message structs** — `Increment { pub amount: i64 }` and `GetCount` (from method names and params)
- **Handler trait impls** — `Handler<Increment>` and `Handler<GetCount>` for `Counter`
- **RemoteDispatch** — wire-format dispatch table for cross-node messaging
- **Extension trait** — `CounterExt` on `Endpoint<Counter>` for ergonomic sends

### Use it

```rust
#[tokio::main]
async fn main() {
    // System::local() for development, System::clustered() for production
    let system = System::local();

    // Start an actor — returns a typed endpoint
    let counter = system.start("counter/main", Counter, CounterState { count: 0 });

    // Send messages via the auto-generated extension trait
    let result = counter.increment(5).await.unwrap();
    assert_eq!(result, 5);

    // Lookup by label — works for local and remote actors
    let looked_up = system.lookup::<Counter>("counter/main").unwrap();
    let count = looked_up.get_count().await.unwrap();
    assert_eq!(count, 5);
}
```

## Architecture

```
┌─────────────────────────────────────────────┐
│    System  (unified entry point)            │
│    local() │ clustered(config)              │
├─────────────────────────────────────────────┤
│                  Receptionist               │
│  (type-erased registry, typed lookup)       │
├──────────┬──────────┬───────────────────────┤
│  Local   │  Remote  │  Listings / Keys      │
│  Actors  │  Actors  │  (subscription-based  │
│          │          │   discovery)           │
├──────────┴──────────┴───────────────────────┤
│              Supervisor Layer               │
│  (lifecycle, restart policies, crash        │
│   recovery, watch notifications)            │
├─────────────────────────────────────────────┤
│           Endpoint<A> (send API)            │
│  Local: envelope pattern (zero-cost)        │
│  Remote: bincode → QUIC stream → bincode    │
├─────────────────────────────────────────────┤
│              Cluster Layer                  │
│  SWIM membership │ mDNS discovery │ OpLog   │
│  QUIC transport   │ Per-actor streams       │
└─────────────────────────────────────────────┘
```

### Key design decisions

- **Endpoint<A> is the only API** — callers never know if an actor is local or remote.
- **Receptionist is non-generic** — stores type-erased entries internally, uses `TypeId` guards for safe downcasts at lookup time.
- **Supervisors are flat** — each actor has its own supervisor, no parent-child hierarchy.
- **Labels are paths** — `"cache/user"`, `"worker/0"`, `"thumbnail/processor/3"`. Hierarchical naming for organizational clarity.
- **Fail-fast networking** — if a QUIC stream fails, all pending responses error immediately instead of hanging.

## Proc Macros

### `#[handlers]` + `#[handler]`

The `#[handlers]` attribute on an `impl` block auto-generates everything from plain method signatures:

```rust
#[handlers]
impl MyActor {
    #[handler]
    fn do_thing(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, name: String, count: u32) -> String {
        format!("{name}: {count}")
    }

    #[handler]
    fn get_status(&mut self, _ctx: &ActorContext<Self>, state: &mut MyState) -> bool {
        state.is_active
    }

    #[handler]
    async fn fetch_data(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, url: String) -> Vec<u8> {
        some_async_call(&url).await
    }
}
```

This generates:
- **Message structs** — `DoThing { pub name: String, pub count: u32 }`, `GetStatus`, `FetchData { pub url: String }` (PascalCase from method name)
- **Handler/AsyncHandler impls** — dispatches the message fields as method arguments
- **RemoteDispatch** — routes serialized messages by type ID for cross-node delivery
- **Extension trait** — `MyActorExt` on `Endpoint<MyActor>`:

```rust
// Auto-generated — call handlers directly on the endpoint
let result = endpoint.do_thing("hello".into(), 42).await?;
let status = endpoint.get_status().await?;
let data = endpoint.fetch_data("https://...".into()).await?;
```

Each `#[handler]` method must start with `(&mut self, ctx: &ActorContext<Self>, state: &mut State, ...)`. Remaining parameters become message struct fields. Use `async fn` for async handlers.

### `#[derive(Message)]` (explicit messages)

For cases where you want to define message structs manually (e.g., shared across multiple actors), use `#[derive(Message)]`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64, remote = "counter::Increment")]
struct Increment { amount: i64 }
```

Then reference it in a handler with the `msg` parameter name:

```rust
#[handlers]
impl MyActor {
    #[handler]
    fn increment(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, msg: Increment) -> i64 {
        state.count += msg.amount;
        state.count
    }
}
```

The `remote = "..."` parameter is optional — omit it for local-only messages that don't need wire serialization.

## Supervision

Actors can be started with restart policies inspired by Erlang/OTP:

```rust
use murmer::{RestartPolicy, RestartConfig, BackoffConfig, ActorFactory};
use std::time::Duration;

struct MyFactory;
impl ActorFactory for MyFactory {
    type Actor = Counter;
    fn create(&mut self) -> (Counter, CounterState) {
        (Counter, CounterState { count: 0 })
    }
}

let endpoint = receptionist.start_with_config(
    "counter/resilient",
    MyFactory,
    RestartConfig {
        policy: RestartPolicy::Permanent,  // Always restart
        max_restarts: 5,                   // Max 5 restarts...
        window: Duration::from_secs(60),   // ...within 60 seconds
        backoff: BackoffConfig {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            multiplier: 2.0,
        },
    },
);
```

| Policy | Restart on panic? | Restart on clean stop? |
|--------|-------------------|------------------------|
| `Temporary` | No | No |
| `Transient` | Yes | No |
| `Permanent` | Yes | Yes |

## Actor Discovery

### Labels

Actors are identified by path-like labels: `"cache/user"`, `"worker/0"`, `"thumbnail/processor/3"`.

```rust
let ep = receptionist.start("service/auth", AuthActor, AuthState::new());
let ep = receptionist.lookup::<AuthActor>("service/auth").unwrap();
```

### Reception Keys & Listings

Group actors by type and subscribe to changes:

```rust
let worker_key = ReceptionKey::<Worker>::new("workers");

// Check actors into the group
receptionist.check_in("worker/0", worker_key.clone());
receptionist.check_in("worker/1", worker_key.clone());

// Subscribe — get existing actors immediately + live updates
let mut listing = receptionist.listing(worker_key);
while let Some(endpoint) = listing.next().await {
    endpoint.send(Work { task: "process".into() }).await?;
}
```

### Lifecycle Events

Subscribe to all actor registrations/deregistrations:

```rust
let mut events = receptionist.subscribe_events();
while let Some(event) = events.recv().await {
    match event {
        ActorEvent::Registered { label, actor_type } => { /* ... */ }
        ActorEvent::Deregistered { label, actor_type } => { /* ... */ }
    }
}
```

## Routing

Distribute messages across actor pools:

```rust
let router = Router::new(
    vec![ep1, ep2, ep3],
    RoutingStrategy::RoundRobin,
);

// Each send goes to the next endpoint in sequence
router.send(Increment { amount: 1 }).await?;

// Or broadcast to all
let results = router.broadcast(GetCount).await;
```

## From Local to Distributed

One of murmer's core design goals is that your actor code doesn't change when you go from a single process to a multi-node cluster. The same `Endpoint<A>` API works in both cases.

### Step 1: Run everything locally

Create a `System::local()` — no networking, no config. Your actors communicate through in-memory channels with zero serialization cost:

```rust
use murmer::prelude::*;

let system = System::local();

let room = system.start("room/general", ChatRoom, ChatRoomState {
    room_name: "general".into(),
    messages: vec![],
});

// Send messages via extension trait — works instantly
room.post_message("alice".into(), "Hello!".into()).await?;

// Look up actors by label
let ep = system.lookup::<ChatRoom>("room/general").unwrap();
let history = ep.get_history().await?;
```

### Step 2: Go distributed

When you're ready for real networking, swap `System::local()` for `System::clustered()`. **Your actor code stays identical** — only the system construction changes:

```rust
use murmer::prelude::*;
use murmer::cluster::config::ClusterConfig;

let config = ClusterConfig::builder()
    .name("alpha")
    .listen("0.0.0.0:7100".parse()?)
    .advertise("192.168.1.5:7100".parse()?)
    .cookie("my-cluster-secret")
    .seed_nodes(["192.168.1.1:7100".parse()?])
    .build()?;

// clustered_auto() discovers all #[handlers]-annotated actor types automatically
let system = System::clustered_auto(config).await?;

// Same API as local — start, lookup, send
let room = system.start("room/alpha", ChatRoom, state);
room.post_message("alice".into(), "Hello!".into()).await?;

// Actors on other nodes appear automatically via registry replication
let remote_room = system.lookup::<ChatRoom>("room/beta").unwrap();
remote_room.get_history().await?;  // transparently serialized over QUIC
```

Each node gets a single QUIC connection to every peer, multiplexed over per-actor streams. The OpLog replication protocol uses version vectors for efficient, idempotent sync.

### Step 3: Test it interactively

The [`cluster_chat`](examples/src/cluster_chat.rs) example lets you try both modes with an interactive CLI:

```sh
# Local mode — all actors in one process
cargo run -p murmer-examples --bin cluster_chat -- --local
```

```text
=== murmer cluster_chat (local mode) ===
  Started room: #general
  Started room: #random

> post general alice Hello everyone!
  [1 messages in #general]
> post general bob Hey alice!
  [2 messages in #general]
> history general
  --- #general ---
  alice: Hello everyone!
  bob: Hey alice!
> rooms
  Known rooms:
    #general — 2 messages
    #random — 0 messages
```

Same binary, same commands — just add cluster config:

```sh
# Terminal 1 — seed node
cargo run -p murmer-examples --bin cluster_chat -- --node alpha --port 7100

# Terminal 2 — joins via seed
cargo run -p murmer-examples --bin cluster_chat -- --node beta --port 7200 --seed 127.0.0.1:7100
```

### Step 4: Deploy with Docker

The [`docker-compose.yml`](docker-compose.yml) in this repo runs a 3-node cluster:

```sh
docker compose up --build
```

This starts three containers — `alpha`, `beta`, and `gamma` — each running the `cluster_chat` example. Beta and gamma seed from alpha and automatically mesh together:

```yaml
services:
  alpha:
    build: .
    command: ["--node", "alpha", "--port", "7100"]

  beta:
    build: .
    command: ["--node", "beta", "--port", "7100", "--seed", "alpha:7100"]

  gamma:
    build: .
    command: ["--node", "gamma", "--port", "7100", "--seed", "alpha:7100"]
```

Or run locally across terminals:

```sh
# Terminal 1 — seed node
cargo run -p murmer-examples --bin cluster_chat -- --node alpha --port 7100

# Terminal 2
cargo run -p murmer-examples --bin cluster_chat -- --node beta --port 7200 --seed 127.0.0.1:7100

# Terminal 3
cargo run -p murmer-examples --bin cluster_chat -- --node gamma --port 7300 --seed 127.0.0.1:7100
```

See [`examples/src/cluster_chat.rs`](examples/src/cluster_chat.rs) for the full runnable example.

## Actor Watches

Erlang-style actor monitoring — get notified when a watched actor terminates:

```rust
impl Actor for Monitor {
    type State = MonitorState;

    fn on_actor_terminated(&mut self, state: &mut MonitorState, terminated: &ActorTerminated) {
        match &terminated.reason {
            TerminationReason::Panicked(msg) => {
                tracing::error!("{} panicked: {}", terminated.label, msg);
            }
            _ => {}
        }
    }
}

#[handlers]
impl Monitor {
    #[handler]
    fn watch_actor(&mut self, ctx: &ActorContext<Self>, _state: &mut MonitorState, label: String) {
        ctx.watch(&label);  // Watch for termination
    }
}
```

## Build & Test

```sh
cargo build
cargo nextest run
cargo clippy -- -D warnings
```

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
