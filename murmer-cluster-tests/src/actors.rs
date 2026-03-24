//! Reusable test actor definitions for cluster integration tests.
//!
//! Each actor uses the `#[handlers]` macro which auto-generates `Handler` impls,
//! `RemoteDispatch`, and a linkme auto-registration entry for the `TypeRegistry`.

use murmer::prelude::*;
use serde::{Deserialize, Serialize};

// =============================================================================
// PingPong — simple request-response for proving cross-node messaging
// =============================================================================

#[derive(Debug)]
pub struct PingPong;

pub struct PingPongState {
    pub name: String,
}

impl PingPongState {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl Actor for PingPong {
    type State = PingPongState;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ping {
    pub from: String,
}

impl Message for Ping {
    type Result = String;
}

impl RemoteMessage for Ping {
    const TYPE_ID: &'static str = "cluster_test::Ping";
}

#[handlers]
impl PingPong {
    #[handler]
    fn ping(&mut self, _ctx: &ActorContext<Self>, state: &mut PingPongState, msg: Ping) -> String {
        format!("pong from {} (pinged by {})", state.name, msg.from)
    }
}

// =============================================================================
// Counter — stateful accumulation for proving state isolation
// =============================================================================

#[derive(Debug)]
pub struct Counter;

pub struct CounterState {
    pub count: i64,
}

impl CounterState {
    pub fn new(initial: i64) -> Self {
        Self { count: initial }
    }
}

impl Actor for Counter {
    type State = CounterState;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Increment {
    pub amount: i64,
}

impl Message for Increment {
    type Result = i64;
}

impl RemoteMessage for Increment {
    const TYPE_ID: &'static str = "cluster_test::Increment";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCount;

impl Message for GetCount {
    type Result = i64;
}

impl RemoteMessage for GetCount {
    const TYPE_ID: &'static str = "cluster_test::GetCount";
}

#[handlers]
impl Counter {
    #[handler]
    fn increment(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CounterState,
        msg: Increment,
    ) -> i64 {
        state.count += msg.amount;
        state.count
    }

    #[handler]
    fn get_count(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CounterState,
        _msg: GetCount,
    ) -> i64 {
        state.count
    }
}

// =============================================================================
// ChatRoom — multi-message workflow for realistic usage patterns
// =============================================================================

#[derive(Debug)]
pub struct ChatRoom;

pub struct ChatRoomState {
    pub room_name: String,
    pub messages: Vec<(String, String)>,
}

impl ChatRoomState {
    pub fn new(room_name: &str) -> Self {
        Self {
            room_name: room_name.to_string(),
            messages: Vec::new(),
        }
    }
}

impl Actor for ChatRoom {
    type State = ChatRoomState;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMessage {
    pub from: String,
    pub text: String,
}

impl Message for PostMessage {
    type Result = usize; // returns message count
}

impl RemoteMessage for PostMessage {
    const TYPE_ID: &'static str = "cluster_test::PostMessage";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetHistory;

impl Message for GetHistory {
    type Result = Vec<String>;
}

impl RemoteMessage for GetHistory {
    const TYPE_ID: &'static str = "cluster_test::GetHistory";
}

#[handlers]
impl ChatRoom {
    #[handler]
    fn post_message(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        msg: PostMessage,
    ) -> usize {
        state.messages.push((msg.from, msg.text));
        state.messages.len()
    }

    #[handler]
    fn get_history(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        _msg: GetHistory,
    ) -> Vec<String> {
        state
            .messages
            .iter()
            .map(|(from, text)| format!("[{}] {}: {}", state.room_name, from, text))
            .collect()
    }
}
