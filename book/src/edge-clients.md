# Edge Clients

Not everything that talks to a murmer cluster needs to be a cluster member. A REST API gateway, a CLI tool, a monitoring dashboard, or an integration test runner just needs to *call* actors — it doesn't need to run any, participate in SWIM gossip, or store a registry.

**Edge clients** fill that role. A `MurmerClient` connects to any cluster node, pulls the set of public actors, and exposes the same `Endpoint<A>` API you already know — without any of the cluster machinery behind it.

Like every connection in murmer, an edge client dials the server **by key**: you give it the server node's iroh endpoint address (its endpoint id plus a host:port hint), not a bare socket address. Get the server's endpoint id by running `murmer id` on it.

```rust,ignore
use murmer::MurmerClient;
use std::time::Duration;

// The server's endpoint address = its endpoint id + a host:port hint.
let server = iroh::EndpointAddr::from_parts(
    server_endpoint_id,                       // from `murmer id` on the server
    [iroh::TransportAddr::Ip("10.0.0.5:9000".parse()?)],
);

let client = MurmerClient::connect(server, "cluster-cookie").await?;
let ep = client.lookup::<UserService>("api/users").unwrap();
let user = ep.send(GetUser { id: 42 }).await?;
client.disconnect().await;
```

> The edge client generates a fresh ephemeral key on each run. If the server runs
> with an [enforced allowlist](./administration.md#the-allowlist), that ephemeral
> key won't be admitted. Either run edge clients against `Open`-mode nodes, or
> give the client a persistent, allowlisted key.

## Visibility: controlling what Edge clients see

Every actor has a **visibility** that controls who can discover it. You set this at startup time on the server side:

```rust,ignore
// Public — visible to Edge clients and all cluster members
let api = system.start_public("api/users", UserService, state);

// Internal — visible to cluster members only (default)
let router = system.start("routing/shard-0", ShardRouter, state);

// Private — node-local only, never replicated
let metrics = system.start_private("node/metrics", MetricsCollector, state);
```

| Visibility | Edge clients | Cluster members | Replicated via OpLog |
|---|:---:|:---:|:---:|
| `Public` | ✓ | ✓ | ✓ |
| `Internal` (default) | ✗ | ✓ | ✓ |
| `Private` | ✗ | ✗ | ✗ |

`Private` is a zero-overhead choice: the actor is never written to the OpLog, never serialized, and never sent over the wire. Use it for utility actors that are purely node-local implementation details — connection managers, per-node caches, local metrics collectors.

## Connecting

Edge clients connect to any node in the cluster. The node you connect to acts as your sync source — it responds to pull requests with the current set of public actors.

```rust,ignore
// Short-lived: connect, call, disconnect. `server` is an iroh::EndpointAddr
// (endpoint id + host:port), built as shown above.
let client = MurmerClient::connect(server, "cluster-cookie").await?;
```

The cluster cookie must match the server's cookie or the handshake will be rejected.

### Requirements

The server must be started with `System::clustered()` — Edge clients connect via QUIC and need a listening endpoint. `System::local()` has no network layer and cannot accept Edge client connections.

## Looking up actors

### `lookup` — instant, returns None if not synced yet

```rust,ignore
if let Some(ep) = client.lookup::<UserService>("api/users") {
    let user = ep.send(GetUser { id: 42 }).await?;
}
```

Returns `None` if the actor hasn't been synced to the client yet. Use this after an initial sync has had time to complete.

### `lookup_wait` — blocks until the actor appears

```rust,ignore
let ep = client
    .lookup_wait::<UserService>("api/users", Duration::from_secs(5))
    .await?;
```

Triggers an immediate pull, then waits for the actor to appear — either from that pull's response or a subsequent one. Re-polls the server every 500 ms. Returns `ClusterError::Timeout` if the actor doesn't appear within the deadline.

**Fast path**: if the actor is already synced, `lookup_wait` returns after one pull round-trip (typically sub-millisecond on LAN).

## Usage patterns

### Pattern 1: Short-lived client (pull once)

Ideal for CLI tools, integration tests, and one-off queries. Pulls on connect, uses the snapshot, disconnects.

```rust,ignore
let client = MurmerClient::connect(addr, cookie).await?;

// Give the initial pull a moment to arrive
tokio::time::sleep(Duration::from_millis(50)).await;

let ep = client.lookup::<UserService>("api/users").unwrap();
let result = ep.send(GetUser { id: 1 }).await?;

client.disconnect().await;
```

### Pattern 2: Long-lived gateway (periodic pull)

Ideal for REST/gRPC API servers, dashboards, and proxies. Use `sync_interval` to re-pull periodically and pick up new actor registrations as the cluster evolves.

```rust,ignore
use murmer::ClientOptions;

let client = MurmerClient::connect_with_options(
    addr,
    cookie.into(),
    ClientOptions {
        sync_interval: Some(Duration::from_secs(30)),
        ..Default::default()
    },
).await?;

// client.lookup() stays fresh — re-pulled every 30 seconds
```

### Pattern 3: Wait for a specific actor

Useful when you connect before the target actor is registered — for example, a gateway that starts before the cluster has finished placing its actors.

```rust,ignore
let ep = client
    .lookup_wait::<PaymentService>("payments/processor", Duration::from_secs(10))
    .await?;
```

## How sync works

Edge clients use **pull-based sync** — the server never pushes unsolicited updates. The client sends a `RegistrySyncRequest` with its current version vector; the server responds with only the delta (new public actor registrations since that version).

```text
Edge client                     Cluster node
    │                                │
    │── RegistrySyncRequest(vv) ────▶│
    │◀── RegistrySync(delta ops) ────│
    │                                │
    │  ... time passes ...           │
    │                                │
    │── RegistrySyncRequest(vv') ───▶│  (periodic or lookup_wait re-poll)
    │◀── RegistrySync(new ops) ──────│
```

After the first sync, subsequent pulls return only the delta — O(new ops), not O(all ops). 1000 idle Edge clients add near-zero server overhead: no SWIM membership, no server-initiated fan-out, no per-client state.

## Scalability characteristics

| Property | Behavior |
|---|---|
| SWIM membership | Edge clients are **not** added to SWIM — no failure detection overhead |
| Server-initiated sync | Skipped for Edge clients — they pull on their own schedule |
| Disconnect | Silent — no cluster alarm, no actor pruning, no SWIM event |
| Server state per client | None — the server is stateless with respect to each Edge client |
| Wire overhead (idle) | Zero — the server never initiates contact |

## Full example

The [`edge_client`](https://github.com/paxsonsa/murmer-rs/blob/main/examples/src/edge_client.rs) example demonstrates all three patterns:

```sh
cargo test -p murmer-examples --bin edge_client
```

It covers:
1. Public actors visible to Edge clients, internal actors hidden
2. `lookup_wait` resolving when an actor registers after connect
3. Long-lived client with periodic `sync_interval`
