# Supervision

Every actor in murmer runs inside a supervisor. The supervisor manages the actor's lifecycle — starting it, processing its mailbox, and restarting it when things go wrong. This model is directly inspired by Erlang/OTP's supervision trees, adapted for Rust's ownership and type system.

## How supervisors work

Each actor gets its own supervisor. The supervisor is responsible for:

- **Starting** the actor and registering it with the [receptionist](./discovery.md).
- **Mailbox processing** — ingesting messages and passing them to the actor's handlers in order of arrival.
- **Crash detection** — catching panics and deciding what to do next based on the restart policy.
- **Restarting** the actor using a factory if the policy allows it.
- **State notifications** — informing the receptionist of state changes (started, stopped, dead, etc.).
- **Context** — providing the actor with access to the system, receptionist, and other actors via `ActorContext`.

Supervisors are **flat** — there is no parent-child hierarchy between actors. Each actor is independent and can be stopped or restarted without affecting others.

## Actor lifecycle

The supervisor manages an actor through a well-defined set of states:

<p align="center">
  <img src="diagram-lifecycle.svg" alt="Actor lifecycle: Starting → Running → Crashed/Stopped → Restarting or Dead" style="width:100%">
</p>

## Restart policies

Actors can be started with restart policies that control behavior on failure:

| Policy | Restart on panic? | Restart on clean stop? |
|--------|-------------------|------------------------|
| `Temporary` | No | No |
| `Transient` | Yes | No |
| `Permanent` | Yes | Yes |

- **Temporary** — the actor runs once. If it panics or stops, it's gone. This is the default.
- **Transient** — the actor restarts if it panics, but a clean shutdown is respected. Use this for actors that should survive crashes but can be intentionally stopped.
- **Permanent** — the actor always restarts, whether it panicked or stopped cleanly. Use this for critical services that must always be running.

## Configuration

To use restart policies, you provide an `ActorFactory` (which knows how to create fresh instances) and a `RestartConfig`:

```rust,ignore
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

### Restart limits

The `max_restarts` and `window` fields prevent infinite restart loops. If the actor exceeds the restart limit within the time window, the supervisor gives up and the actor is permanently stopped. This prevents a persistent bug from consuming all your resources.

### Exponential backoff

The `BackoffConfig` controls the delay between restarts:

- `initial` — delay before the first restart attempt.
- `max` — maximum delay (the backoff caps here).
- `multiplier` — each subsequent restart delay is multiplied by this factor.

For example, with `initial: 100ms`, `max: 30s`, `multiplier: 2.0`, restarts happen at 100ms, 200ms, 400ms, 800ms, ... up to 30s.

## Actor factories

The `ActorFactory` trait gives the supervisor a way to create fresh actor instances for restarts:

```rust,ignore
trait ActorFactory {
    type Actor: Actor;
    fn create(&mut self) -> (Self::Actor, <Self::Actor as Actor>::State);
}
```

The factory is called each time the supervisor needs a new instance. It can carry its own state if needed — for example, incrementing a generation counter or loading configuration from disk.

## Interaction with the receptionist

When a supervised actor restarts:
1. The old actor instance is dropped.
2. The supervisor creates a new instance via the factory.
3. The new instance is registered with the receptionist under the same label.
4. Any actors watching the old instance receive a termination notification, but the label remains routable.

This means endpoints held by other actors remain valid through restarts — messages sent during the brief restart window are queued in the supervisor's mailbox and delivered to the new instance.

## Watching actors

Any actor can monitor another actor for termination using `ctx.watch()`. When the watched actor terminates (for any reason), the watcher receives an `ActorTerminated` notification via `on_actor_terminated`.

```rust,ignore
impl Actor for Supervisor {
    type State = SupervisorState;

    fn on_actor_terminated(&mut self, state: &mut SupervisorState, event: &ActorTerminated) {
        tracing::warn!("Actor {} terminated: {:?}", event.label, event.reason);
    }
}

#[handlers]
impl Supervisor {
    #[handler]
    fn start_worker(&mut self, ctx: &ActorContext<Self>, state: &mut SupervisorState) {
        ctx.receptionist().start("worker/0", MyWorker, WorkerState::default());
        ctx.watch("worker/0");
    }
}
```

Watches are **one-shot**: they fire once when the watched actor terminates and are not re-armed.

**Erlang semantics**: If `ctx.watch()` is called for an actor that doesn't exist, `on_actor_terminated` fires immediately. This avoids the race condition where the actor dies between the lookup and the watch.

### Tagged watches

When a supervisor manages multiple children with different roles, parsing label strings in `on_actor_terminated` is fragile. Use `ctx.watch_with_tag()` to attach a role tag that is delivered alongside the termination notification:

```rust,ignore
fn start_children(&mut self, ctx: &ActorContext<Self>, state: &mut SupervisorState) {
    let r = ctx.receptionist();

    state.writer = Some(r.start("writer/0", WriterActor, WriterState::default()));
    ctx.watch_with_tag("writer/0", "writer");

    state.reader = Some(r.start("reader/0", ReaderActor, ReaderState::default()));
    ctx.watch_with_tag("reader/0", "reader");
}

fn on_actor_terminated(&mut self, state: &mut SupervisorState, event: &ActorTerminated) {
    match event.tag.as_deref() {
        Some("writer") => {
            tracing::error!("writer died — restarting");
            state.writer = None;
        }
        Some("reader") => {
            tracing::warn!("reader died — clearing slot");
            state.reader = None;
        }
        _ => {}
    }
}
```

Plain `ctx.watch()` delivers `ActorTerminated` with `tag: None` — fully backward compatible.

## Scheduling

Actors can send delayed or periodic messages to themselves using the scheduling API. The returned `ScheduleHandle` auto-cancels when dropped.

### schedule_once

Send a message to the actor after a delay:

```rust,ignore
fn handle(&self, ctx: &ActorContext<Self>, state: &mut MyState, _msg: StartTimeout) -> () {
    state.timeout = Some(ctx.schedule_once(Duration::from_secs(30), TimeoutExpired));
    // Drop state.timeout to cancel before it fires.
}
```

### schedule_repeat

Send a message to the actor on a repeating interval:

```rust,ignore
fn on_start(&self, ctx: &ActorContext<Self>, state: &mut MyState) {
    state.heartbeat = Some(ctx.schedule_repeat(Duration::from_secs(60), Heartbeat));
}
```

The first tick fires after one full `interval` elapses (not immediately). Missed ticks are skipped — if the actor is slow, it receives one tick per interval boundary rather than a burst of catch-up ticks.

### ScheduleHandle

`ScheduleHandle` is returned by both scheduling methods. Hold it in actor state to keep the schedule alive. Drop it (or call `.cancel()`) to stop the timer:

```rust,ignore
struct MyState {
    maintenance: Option<ScheduleHandle>, // Some = running, None = cancelled
}

// Cancel from a handler:
fn handle(&self, _ctx: &ActorContext<Self>, state: &mut MyState, _msg: StopMaintenance) -> () {
    state.maintenance = None; // drop cancels the timer
}
```

When the actor stops, all `ScheduleHandle`s in its state are dropped and their timers are cancelled automatically.
