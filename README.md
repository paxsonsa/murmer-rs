# murmer

[![CI](https://github.com/paxsonsa/murmer-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/paxsonsa/murmer-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/murmer.svg)](https://crates.io/crates/murmer)
[![docs.rs](https://docs.rs/murmer/badge.svg)](https://docs.rs/murmer)
[![License](https://img.shields.io/crates/l/murmer.svg)](https://github.com/paxsonsa/murmer-rs#license)

A distributed actor framework for Rust, built on tokio and QUIC.

Murmer provides typed, location-transparent actors that communicate through message passing. Whether an actor lives in the same process or on a remote node across the network, you interact with it through the same `Endpoint<A>` API.

## Features

- **Location transparency** — `Endpoint<A>` abstracts local vs remote. Local actors use zero-cost in-memory dispatch; remote actors serialize over QUIC.
- **Typed message handling** — compile-time checked: if an actor doesn't handle a message type, it won't compile.
- **Proc macro ergonomics** — `#[derive(Message)]` eliminates message boilerplate. `#[handlers]` generates dispatch tables.
- **OTP-inspired supervision** — restart policies (Temporary, Transient, Permanent) with configurable limits and exponential backoff.
- **Subscription-based discovery** — `ReceptionKey<A>` groups actors by type, `Listing<A>` streams endpoints as actors come and go.
- **Cluster-ready** — SWIM protocol membership, mDNS discovery, OpLog-based registry replication with version vectors.

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
use murmer_macros::{Message, handlers};
use serde::{Serialize, Deserialize};

// 1. Actor struct + state
#[derive(Debug)]
struct Counter;

struct CounterState { count: i64 }

impl Actor for Counter {
    type State = CounterState;
}

// 2. Messages — derive does the boilerplate
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64, remote = "counter::Increment")]
struct Increment { amount: i64 }

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64, remote = "counter::GetCount")]
struct GetCount;

// 3. Handlers — macro generates Handler + RemoteDispatch impls
#[handlers]
impl Counter {
    #[handler]
    fn increment(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState, msg: Increment) -> i64 {
        state.count += msg.amount;
        state.count
    }

    #[handler]
    fn get_count(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState, _msg: GetCount) -> i64 {
        state.count
    }
}
```

### Use it

```rust
#[tokio::main]
async fn main() {
    let receptionist = Receptionist::new();

    // Start an actor — returns a typed endpoint
    let counter = receptionist.start("counter/main", Counter, CounterState { count: 0 });

    // Send messages
    let result = counter.send(Increment { amount: 5 }).await.unwrap();
    assert_eq!(result, 5);

    // Lookup by label — same API
    let looked_up = receptionist.lookup::<Counter>("counter/main").unwrap();
    let count = looked_up.send(GetCount).await.unwrap();
    assert_eq!(count, 5);
}
```

## Architecture

```
┌─────────────────────────────────────────────┐
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

### `#[derive(Message)]`

Eliminates the boilerplate of implementing `Message` and `RemoteMessage`:

```rust
// Before: 12 lines per message
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Increment { amount: i64 }
impl Message for Increment { type Result = i64; }
impl RemoteMessage for Increment { const TYPE_ID: &'static str = "counter::Increment"; }

// After: 3 lines per message
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64, remote = "counter::Increment")]
struct Increment { amount: i64 }
```

The `remote = "..."` parameter is optional — omit it for local-only messages that don't need wire serialization.

### `#[handlers]` + `#[handler]`

Generates `Handler<M>` trait implementations and the `RemoteDispatch` dispatch table from natural method signatures:

```rust
#[handlers]
impl MyActor {
    #[handler]
    fn do_thing(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, msg: DoThing) -> String {
        // Your handler logic here
    }
}
```

Each `#[handler]` method must follow the signature: `(&mut self, ctx: &ActorContext<Self>, state: &mut State, msg: MsgType) -> MsgType::Result`

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

## Clustering

Murmer supports multi-node actor systems over QUIC with automatic discovery:

```rust
use murmer::cluster::{ClusterSystem, ClusterConfigBuilder, Discovery};

let config = ClusterConfigBuilder::new("node-1", "0.0.0.0:9000")
    .discovery(Discovery::Mdns)           // Zero-config LAN discovery
    .gossip_interval(Duration::from_secs(1))
    .sync_interval(Duration::from_secs(5))
    .build()?;

let cluster = ClusterSystem::start(config, receptionist).await?;
```

Each node gets a single QUIC connection to every peer, multiplexed over per-actor streams. The OpLog replication protocol uses version vectors for efficient, idempotent sync.

## Actor Watches

Erlang-style actor monitoring — get notified when a watched actor terminates:

```rust
impl Actor for Supervisor {
    type State = SupervisorState;

    fn on_actor_terminated(&mut self, state: &mut SupervisorState, terminated: &ActorTerminated) {
        match &terminated.reason {
            TerminationReason::Panicked(msg) => {
                tracing::error!("{} panicked: {}", terminated.label, msg);
            }
            _ => {}
        }
    }
}

// Inside a handler:
fn handle_start(&mut self, ctx: &ActorContext<Self>, state: &mut SupervisorState, msg: StartWorker) {
    ctx.watch("worker/0");  // Watch for termination
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
