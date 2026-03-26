# Monitoring

Murmer provides production-grade metrics for your actor systems via the `monitor` feature. It follows a facade pattern — your actor code records metrics through thin instrumentation calls, and you choose the backend (Prometheus, StatsD, etc.) at startup.

## Enabling monitoring

Add the `monitor` feature to your `Cargo.toml`:

```toml
[dependencies]
murmer = { version = "0.1", features = ["monitor"] }
```

When the feature is **off**, all instrumentation compiles to nothing — zero overhead, zero dependencies.

## Quick start with Prometheus

One call installs the metrics recorder and starts an HTTP endpoint for scraping:

```rust,ignore
use murmer::monitor::start_prometheus;

#[tokio::main]
async fn main() {
    // Start serving metrics on :9000/metrics
    start_prometheus(9000).expect("failed to start prometheus exporter");

    // Now start your actor system as usual
    let system = System::local();
    let counter = system.start("counter/main", Counter, CounterState { count: 0 });

    // Every message send, handler invocation, and lifecycle event
    // is automatically recorded. Scrape with:
    //   curl http://localhost:9000/metrics
}
```

## What gets measured

Murmer instruments five categories of metrics automatically. You don't need to add any code — everything is recorded as actors run.

### Actor lifecycle

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `murmer_actors_active` | gauge | `actor_type` | Currently running actors |
| `murmer_actors_started_total` | counter | `actor_type` | Total actors started |
| `murmer_actors_stopped_total` | counter | `actor_type`, `reason` | Total actors stopped (reason: `stopped`, `panicked`, `restart_limit_exceeded`) |
| `murmer_actors_restarts_total` | counter | `actor_type` | Total actor restarts |
| `murmer_actors_restart_limit_exceeded_total` | counter | `actor_type` | Times restart limits were hit |

### Message processing

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `murmer_messages_processed_total` | counter | `actor_type` | Messages successfully handled |
| `murmer_messages_failed_total` | counter | `actor_type` | Messages that caused a panic |
| `murmer_message_processing_duration_seconds` | histogram | `actor_type` | Handler execution time |

### Endpoint sends

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `murmer_sends_total` | counter | `actor_type`, `locality` | Total sends (`local` or `remote`) |
| `murmer_send_errors_total` | counter | `actor_type`, `error_kind` | Send failures by error type |
| `murmer_network_roundtrip_duration_seconds` | histogram | `actor_type` | End-to-end remote call latency |

### Networking

| Metric | Type | Description |
|--------|------|-------------|
| `murmer_network_connections_active` | gauge | Active QUIC connections to peer nodes |
| `murmer_network_streams_active` | gauge | Active QUIC streams for actor messaging |
| `murmer_network_bytes_sent_total` | counter | Total bytes sent over actor streams |
| `murmer_network_bytes_received_total` | counter | Total bytes received over actor streams |
| `murmer_network_inflight_calls` | gauge | In-flight remote calls awaiting responses |
| `murmer_network_dead_letters_total` | counter | Failed in-flight calls (connection lost) |

### Cluster membership

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `murmer_cluster_nodes` | gauge | `status` | Number of nodes in the cluster |
| `murmer_cluster_membership_changes_total` | counter | `event_type` | Membership events (`joined`, `failed`, `left`) |

### Receptionist

| Metric | Type | Description |
|--------|------|-------------|
| `murmer_receptionist_lookups_total` | counter | Actor lookups |
| `murmer_receptionist_registrations_total` | counter | Actor registrations |
| `murmer_receptionist_deregistrations_total` | counter | Actor deregistrations |

## Label cardinality

All actor metrics use `actor_type` (the Rust type name, e.g. `my_app::ChatRoom`) rather than `actor_label` (e.g. `"room/general"`). This keeps cardinality bounded — you typically have fewer than 20 actor types, but could have thousands of labels.

## Architecture: how it works

The instrumentation uses a **facade pattern** inspired by how `tracing` works:

1. **`instrument.rs`** (always compiled) — Contains thin `pub(crate)` functions like `instrument::message_processed(actor_type)`. When `monitor` is on, these call `metrics::counter!(...)`. When off, they're empty `#[inline(always)]` functions that the compiler eliminates entirely.

2. **Call sites** (supervisor, receptionist, endpoint, etc.) — Call instrument functions unconditionally. No `#[cfg]` attributes scattered across the codebase.

3. **Your application** — Installs a metrics recorder at startup (e.g., `start_prometheus(9000)`). All recorded metrics flow to the backend you chose.

This means adding a new metric requires touching exactly two places: the instrument function and the call site. The facade keeps the `#[cfg]` logic in one file.

## ClusterMonitor actor

In addition to Prometheus metrics, murmer provides a `ClusterMonitor` actor that maintains a queryable in-memory view of cluster health:

```rust,ignore
use murmer::monitor::{ClusterMonitor, ClusterMonitorState, run_monitor_bridge, GetClusterHealth};

// Start the monitor actor
let monitor = system.start("murmer/monitor", ClusterMonitor, ClusterMonitorState::new());

// Bridge cluster events into the monitor
tokio::spawn(run_monitor_bridge(&cluster_system, monitor.clone()));

// Query health at any time
let health = monitor.send(GetClusterHealth).await?;
println!("Alive: {}, Joins: {}, Failures: {}",
    health.alive_nodes, health.total_joins, health.total_failures);
```

The `ClusterMonitor` tracks:
- Which nodes are alive and when they joined
- Cumulative counters for joins, failures, and departures
- Per-node uptime

## Grafana dashboard

A typical Prometheus + Grafana setup might query:

```promql
# Message throughput by actor type
rate(murmer_messages_processed_total[5m])

# 99th percentile handler latency
histogram_quantile(0.99, rate(murmer_message_processing_duration_seconds_bucket[5m]))

# Actor crash rate
rate(murmer_messages_failed_total[5m])

# Remote call latency
histogram_quantile(0.95, rate(murmer_network_roundtrip_duration_seconds_bucket[5m]))

# Cluster size over time
murmer_cluster_nodes{status="active"}
```
