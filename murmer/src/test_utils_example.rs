//! Example of using the test harness for testing actors
//!
//! This module provides a complete, self-contained example of how to use the
//! test harness for testing actors with different scenarios.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::actor::*;
use crate::context::Context;
use crate::message::Message;
use crate::test_utils::prelude::*;

/// A simple counter actor for testing
struct CounterActor {
    /// Internal counter
    counter: usize,
    /// Optional callback to execute when a message is handled
    on_increment: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Actor for CounterActor {}

impl CounterActor {
    /// Create a new counter actor
    fn new() -> Self {
        Self {
            counter: 0,
            on_increment: None,
        }
    }

    /// Create a counter actor with a callback
    fn with_callback<F>(callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            counter: 0,
            on_increment: Some(Arc::new(callback)),
        }
    }
}

/// Message to increment the counter
#[derive(Debug, Clone)]
struct IncrementMessage;

impl Message for IncrementMessage {
    type Result = usize;
}

#[async_trait::async_trait]
impl Handler<IncrementMessage> for CounterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: IncrementMessage) -> usize {
        self.counter += 1;

        // Execute callback if present
        if let Some(callback) = &self.on_increment {
            callback();
        }

        self.counter
    }
}

/// Message to get the current counter value
#[derive(Debug)]
struct GetCounterMessage;

impl Message for GetCounterMessage {
    type Result = usize;
}

#[async_trait::async_trait]
impl Handler<GetCounterMessage> for CounterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: GetCounterMessage) -> usize {
        self.counter
    }
}

/// Message to reset the counter
#[derive(Debug)]
struct ResetCounterMessage;

impl Message for ResetCounterMessage {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<ResetCounterMessage> for CounterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: ResetCounterMessage) -> () {
        self.counter = 0;
    }
}

#[test_log::test(tokio::test)]
async fn test_actor_with_harness_basic() {
    // Create a test harness
    let test = ActorTestHarness::new();

    // Create and spawn the actor
    let mut actor = test.spawn(CounterActor::new());

    // Start the actor
    actor.start().await;

    // Send a message and verify the result
    let count = actor.send(IncrementMessage).await.unwrap();
    assert_eq!(count, 1);

    // Verify the internal state directly
    actor.assert_state(|state| {
        assert_eq!(state.counter, 1);
    });

    // Send another message
    let count = actor.send(IncrementMessage).await.unwrap();
    assert_eq!(count, 2);

    // Reset the counter
    actor.send(ResetCounterMessage).await.unwrap();

    // Verify counter was reset
    let count = actor.send(GetCounterMessage).await.unwrap();
    assert_eq!(count, 0);
}

#[test_log::test(tokio::test)]
async fn test_actor_with_external_state() {
    // Create a shared counter to verify callbacks
    let external_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = external_counter.clone();

    // Create a test harness
    let test = ActorTestHarness::new();

    // Create actor with callback
    let mut actor = test.spawn(CounterActor::with_callback(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }));

    // Start the actor
    actor.start().await;

    // Send increment messages
    for _ in 0..5 {
        actor.send(IncrementMessage).await.unwrap();
    }

    // Verify both internal and external state
    actor.assert_state(|state| {
        assert_eq!(state.counter, 5);
    });

    assert_eq!(external_counter.load(Ordering::SeqCst), 5);
}

#[test_log::test(tokio::test)]
async fn test_actor_error_handling() {
    // This example shows how to test error conditions

    // Create a test harness
    let test = ActorTestHarness::new();

    // Create actor
    let mut actor = test.spawn(CounterActor::new());

    // Start the actor
    actor.start().await;

    // Define a special message type that CounterActor doesn't handle
    #[derive(Debug)]
    struct UnhandledMessage;

    impl Message for UnhandledMessage {
        type Result = ();
    }

    // The following would cause a compile error because CounterActor
    // doesn't implement Handler<UnhandledMessage>
    //
    // let result = actor.send(UnhandledMessage).await;

    // Instead, we can check specific error cases by verifying
    // the actor state remains unchanged after expected failures

    // Make sure counter is still 0
    actor.assert_state(|state| {
        assert_eq!(state.counter, 0);
    });
}

#[test_log::test(tokio::test)]
async fn test_actor_with_state_waiting() {
    // This example shows how to use the wait_for_state method

    // Create a test harness
    let test = ActorTestHarness::new();

    // Create actor
    let mut actor = test.spawn(CounterActor::new());

    // Start the actor
    actor.start().await;

    // Initial state assertion
    actor.assert_state(|state| {
        assert_eq!(state.counter, 0);
    });

    // Send 5 increment messages first
    for _ in 0..5 {
        actor.send(IncrementMessage).await.unwrap();
    }

    // Wait for the counter to reach 5
    // This will process messages until the counter is 5 or until timeout
    let result = actor.wait_for_state(1000, |state| state.counter == 5).await;
    assert!(result.is_ok(), "Timed out waiting for counter to reach 5");

    // Verify final state
    actor.assert_state(|state| {
        assert_eq!(state.counter, 5);
    });
}
