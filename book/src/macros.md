# Proc Macro Reference

Murmer provides two proc macros to reduce boilerplate when defining actors: `#[handlers]` + `#[handler]` for handler generation, and `#[derive(Message)]` for explicit message types.

## `#[handlers]` + `#[handler]`

Place `#[handlers]` on an `impl` block containing actor message handlers. Mark each handler method with `#[handler]`.

### Auto-generated messages (recommended)

```rust,ignore
#[handlers]
impl MyActor {
    #[handler]
    fn do_thing(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut MyState,
        name: String,
        count: u32,
    ) -> String {
        format!("{name}: {count}")
    }

    #[handler]
    fn get_status(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut MyState,
    ) -> bool {
        state.is_active
    }

    #[handler]
    async fn fetch_data(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut MyState,
        url: String,
    ) -> Vec<u8> {
        some_async_call(&url).await
    }
}
```

### What gets generated

From the above, the macro produces:

**Message structs** — method name converted to PascalCase, parameters after `ctx` and `state` become fields:

```rust,ignore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoThing { pub name: String, pub count: u32 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStatus;  // no extra params → unit struct

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchData { pub url: String }
```

**Message + RemoteMessage impls** — each struct implements `Message` (with the handler's return type as `Result`) and `RemoteMessage` (with a `TYPE_ID` of `"ActorName::method_name"`):

```rust,ignore
impl Message for DoThing { type Result = String; }
impl RemoteMessage for DoThing { const TYPE_ID: &'static str = "MyActor::do_thing"; }
```

**Handler / AsyncHandler impls** — dispatches message fields as method arguments:

```rust,ignore
impl Handler<DoThing> for MyActor {
    fn handle(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, message: DoThing) -> String {
        self.do_thing(ctx, state, message.name, message.count)
    }
}

// async fn → AsyncHandler
impl AsyncHandler<FetchData> for MyActor { /* ... */ }
```

**RemoteDispatch** — a wire-format dispatch table that routes serialized messages by their `TYPE_ID`. This enables cross-node delivery without the sender knowing the concrete handler:

```rust,ignore
impl RemoteDispatch for MyActor {
    async fn dispatch_remote(&mut self, ctx, state, message_type: &str, payload: &[u8])
        -> Result<Vec<u8>, DispatchError>
    {
        match message_type {
            "MyActor::do_thing" => { /* deserialize DoThing, call handler, serialize result */ }
            "MyActor::get_status" => { /* ... */ }
            "MyActor::fetch_data" => { /* ... */ }
            _ => Err(DispatchError::UnknownMessageType(..))
        }
    }
}
```

**Extension trait** — ergonomic methods directly on `Endpoint<MyActor>`:

```rust,ignore
pub trait MyActorExt {
    fn do_thing(&self, name: String, count: u32) -> impl Future<Output = Result<String, SendError>>;
    fn get_status(&self) -> impl Future<Output = Result<bool, SendError>>;
    fn fetch_data(&self, url: String) -> impl Future<Output = Result<Vec<u8>, SendError>>;
}

impl MyActorExt for Endpoint<MyActor> { /* ... */ }
```

This lets you call handlers directly:

```rust,ignore
let result = endpoint.do_thing("hello".into(), 42).await?;
let status = endpoint.get_status().await?;
let data = endpoint.fetch_data("https://...".into()).await?;
```

**Auto-registration** — a `linkme` distributed slice entry for the `TypeRegistry`. At cluster startup, `TypeRegistry::from_auto()` collects all `#[handlers]`-annotated actor types automatically, enabling the cluster to route messages to the correct deserializer without manual registration.

### Handler method signature

Each `#[handler]` method must follow this pattern:

```rust,ignore
fn method_name(&mut self, ctx: &ActorContext<Self>, state: &mut State, ...params) -> ReturnType
```

- `&mut self` — the actor instance.
- `ctx: &ActorContext<Self>` — provides access to the system, receptionist, and lifecycle operations like `watch()`.
- `state: &mut State` — the actor's mutable state.
- Remaining parameters become message struct fields.
- Use `async fn` for handlers that need to `.await`.

### Explicit messages (backward compatible)

For messages shared across multiple actors, name the last parameter `msg` (or `_msg`) and the macro will use the type directly instead of generating a struct:

```rust,ignore
#[handlers]
impl MyActor {
    #[handler]
    fn increment(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut MyState,
        msg: Increment,
    ) -> i64 {
        state.count += msg.amount;
        state.count
    }
}
```

Here `Increment` must already exist and implement `Message`. The extension trait method will take the message as a parameter: `endpoint.increment(msg)`.

## `#[derive(Message)]`

Derives `Message` (and optionally `RemoteMessage`) for a struct or enum.

### Basic usage (local only)

```rust,ignore
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64)]
struct Increment { amount: i64 }
```

This implements `Message` with `type Result = i64`.

### With remote support

Add `remote = "..."` to also implement `RemoteMessage` with a wire-stable type ID:

```rust,ignore
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64, remote = "counter::Increment")]
struct Increment { amount: i64 }
```

The `TYPE_ID` string is used for wire dispatch — it must be unique across all message types in the cluster and stable across code changes (don't use `std::any::type_name` which can change between compiler versions).

### Attributes

| Attribute | Required | Description |
|-----------|----------|-------------|
| `result = Type` | Yes | The response type for this message |
| `remote = "id"` | No | Wire-stable type ID for `RemoteMessage` |
