<p align="center">
  <img src="https://raw.githubusercontent.com/paxsonsa/murmer-rs/main/static/logo-lockup-dark.svg" alt="murmer — distributed actors for rust" width="560">
</p>

<p align="center">
  <a href="https://github.com/paxsonsa/murmer-rs/actions/workflows/ci.yml"><img src="https://github.com/paxsonsa/murmer-rs/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/murmer"><img src="https://img.shields.io/crates/v/murmer.svg" alt="crates.io"></a>
  <a href="https://docs.rs/murmer"><img src="https://docs.rs/murmer/badge.svg" alt="docs.rs"></a>
  <a href="#license"><img src="https://img.shields.io/crates/l/murmer.svg" alt="License"></a>
</p>

<p align="center">
  <a href="https://paxsonsa.github.io/murmer-rs/">Book</a> &middot;
  <a href="https://docs.rs/murmer">API Docs</a> &middot;
  <a href="https://github.com/paxsonsa/murmer-rs/tree/main/examples">Examples</a>
</p>

---

> **Built with AI.** Murmer is built and maintained with the help of AI coding tools, primarily Claude. I drive the design and review every change, but a lot of the implementation, tests, and docs are written in collaboration with AI. 

Typed, location-transparent actors that communicate through message passing. Whether an actor lives in the same process or on a remote node across the network, you interact with it through the same `Endpoint<A>` API.

```text
                          ┌─ local ──→ Actor (in-memory, zero-copy)
Your Code → Endpoint<A> ──┤
                          └─ remote ─→ QUIC stream (bincode, encrypted) ──→ Actor (remote node)
```

## Highlights

| | |
|---|---|
| **Location-transparent** | Same `Endpoint<A>` API whether the actor is local or on another node |
| **Minimal boilerplate** | `#[handlers]` generates message structs, dispatch tables, and extension traits from plain method signatures |
| **Networking included** | QUIC transport, TLS encryption, SWIM membership, mDNS discovery — configured, not hand-rolled |
| **OTP-style supervision** | Restart policies (Temporary, Transient, Permanent) with limits and exponential backoff |
| **Edge clients** | Lightweight `MurmerClient` connects to a cluster and calls public actors without joining it |
| **Test without infra** | Spin up multi-node clusters in-memory from a single process |

## Quick Start

```toml
[dependencies]
murmer = "0.3"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

**Define an actor:**

```rust
use murmer::prelude::*;

#[derive(Debug)]
struct Counter;
struct CounterState { count: i64 }

impl Actor for Counter {
    type State = CounterState;
}

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

**Use it:**

```rust
#[tokio::main]
async fn main() {
    let system = System::local();
    let counter = system.start("counter/main", Counter, CounterState { count: 0 });

    let result = counter.increment(5).await.unwrap();
    assert_eq!(result, 5);

    // Lookup by label — works for local and remote actors
    let looked_up = system.lookup::<Counter>("counter/main").unwrap();
    let count = looked_up.get_count().await.unwrap();
    assert_eq!(count, 5);
}
```

**Go distributed** — swap one line, your actor code stays the same:

```rust
// Before: let system = System::local();
let system = System::clustered(config, TypeRegistry::from_auto(), SpawnRegistry::new()).await?;

// Everything else is identical
let counter = system.start("counter/main", Counter, CounterState { count: 0 });
let remote = system.lookup::<Counter>("counter/on-other-node").unwrap();
remote.get_count().await?;  // transparently serialized over QUIC
```

## Edge Clients

Not everything that talks to your cluster needs to join it. Use `MurmerClient` to connect from a REST gateway, CLI tool, or test runner and call public actors directly:

```rust,ignore
use murmer::MurmerClient;

let client = MurmerClient::connect("10.0.0.5:9000".parse()?, "cluster-cookie").await?;
let ep = client.lookup::<UserService>("api/users").unwrap();
let user = ep.send(GetUser { id: 42 }).await?;
client.disconnect().await;
```

Mark actors as public on the server side with `system.start_public()`:

```rust,ignore
// Visible to Edge clients
let api = system.start_public("api/users", UserService, state);

// Internal cluster actors — not visible to Edge clients (default)
let router = system.start("routing/shard", ShardRouter, state);
```

`lookup_wait` blocks until the actor appears — useful when connecting before the cluster has finished placing actors:

```rust,ignore
let ep = client
    .lookup_wait::<UserService>("api/users", Duration::from_secs(5))
    .await?;
```

See the [Edge Clients](https://paxsonsa.github.io/murmer-rs/edge-clients.html) chapter in the book for the full API, visibility model, and scalability details.

## Documentation

| Resource | Description |
|----------|-------------|
| **[The Murmer Book](https://paxsonsa.github.io/murmer-rs/)** | User guide: actors, discovery, supervision, clustering, macros, orchestration |
| **[API Reference](https://docs.rs/murmer)** | Rustdoc on docs.rs |
| **[Examples](https://github.com/paxsonsa/murmer-rs/tree/main/examples)** | Runnable demos: counter, cluster chat, orchestrator, edge client |

## Build & Test

```sh
cargo build
cargo nextest run
cargo clippy -- -D warnings
```

## Why murmer?

I've spent years working with Elixir and the BEAM VM, and the actor model there is something I've grown deeply fond of — the simplicity of processes, message passing, and supervision just *works*. When I looked at bringing that experience to Rust, the existing frameworks were impressive but getting a basic actor up and running was complex, and adding remote communication was even more so.

Murmer is an experiment in answering: **can you build a robust distributed actor system in Rust that's actually simple to use?** The design draws from BEAM OTP, Akka's clustering, and Apple's Swift Distributed Actors — combined with Rust's performance and safety guarantees.

## Other libraries to consider

Murmer is still v0 and an experiment. For anything real, you should probably consider using one of these instead. They inspired Murmer and most of them are far more mature:

- **[Erlang/Elixir OTP](https://www.erlang.org/doc/system/design_principles.html)** — the actor model and supervision tree that started it all for me. If you can run on the BEAM, OTP is decades of battle-tested production experience.
- **[Apple Swift Distributed Actors](https://github.com/apple/swift-distributed-actors)** — the typed, location-transparent endpoint design that shaped Murmer's API. Worth reading even if you're not writing Swift.
- **[Actix](https://github.com/actix/actix)** — the most established actor framework in Rust. Mature, fast, and great for in-process actors.
- **[ractor](https://github.com/slawlor/ractor)** — a newer Rust actor library inspired by Erlang, with a clustering story of its own. Well worth a look if you're comparing options.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
