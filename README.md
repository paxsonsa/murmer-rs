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
| **Test without infra** | Spin up multi-node clusters in-memory from a single process |

## Quick Start

```toml
[dependencies]
murmer = "0.1"
murmer-macros = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

**Define an actor:**

```rust
use murmer::prelude::*;
use murmer_macros::handlers;

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

## Documentation

| Resource | Description |
|----------|-------------|
| **[The Murmer Book](https://paxsonsa.github.io/murmer-rs/)** | User guide: actors, discovery, supervision, clustering, macros, orchestration |
| **[API Reference](https://docs.rs/murmer)** | Rustdoc on docs.rs |
| **[Examples](https://github.com/paxsonsa/murmer-rs/tree/main/examples)** | Runnable demos: counter, cluster chat, orchestrator |

## Build & Test

```sh
cargo build
cargo nextest run
cargo clippy -- -D warnings
```

## Why murmer?

I've spent years working with Elixir and the BEAM VM, and the actor model there is something I've grown deeply fond of — the simplicity of processes, message passing, and supervision just *works*. When I looked at bringing that experience to Rust, the existing frameworks were impressive but getting a basic actor up and running was complex, and adding remote communication was even more so.

Murmer is an experiment in answering: **can you build a robust distributed actor system in Rust that's actually simple to use?** The design draws from BEAM OTP, Akka's clustering, and Apple's Swift Distributed Actors — combined with Rust's performance and safety guarantees.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
