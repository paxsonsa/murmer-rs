//! Simple test utilities for easier actor testing
//! 
//! This module provides a basic TestHarness for actor testing.
//! 
//! # Example
//! 
//! See `test_utils_example.rs` for a complete, self-contained example of how to use 
//! the test harness, including:
//! 
//! - Basic actor testing with assertions
//! - Testing actors with external state
//! - Error handling test patterns
//! 
//! # Quick Start
//! 
//! ```
//! use crate::test_utils::prelude::*;
//! 
//! #[test_log::test(tokio::test)]
//! async fn test_my_actor() {
//!     // Create test harness
//!     let test = ActorTestHarness::new();
//!     
//!     // Spawn actor
//!     let mut actor = test.spawn(MyActor::new());
//!     
//!     // Start the actor
//!     actor.start().await;
//!     
//!     // Send messages and check results
//!     let result = actor.send(MyMessage).await.unwrap();
//!     
//!     // Assert on internal state
//!     actor.assert_state(|state| {
//!         assert_eq!(state.some_value, expected_value);
//!     });
//! }
//! ```

use std::time::Duration;

use crate::actor::*;
use crate::message::Message;
use crate::system::{System, TestSupervisor};

/// Main test harness for testing actors
pub struct ActorTestHarness {
    /// System instance for the test
    system: System,
}

impl ActorTestHarness {
    /// Create a new test harness
    pub fn new() -> Self {
        let system = System::local("test_system");
        Self { system }
    }
    
    /// Create and initialize an actor using the test harness
    pub fn spawn<A>(&self, actor: A) -> ActorTestHandle<A>
    where
        A: Actor,
    {
        let supervisor = TestSupervisor::new(actor);
        
        ActorTestHandle {
            supervisor,
            system: self.system.clone(),
        }
    }
}

/// Handle for a specific actor under test
pub struct ActorTestHandle<A: Actor> {
    /// The supervisor for this actor
    pub supervisor: TestSupervisor<A>,
    /// The system for this actor
    pub system: System,
}

impl<A: Actor> ActorTestHandle<A> {
    /// Get a reference to the actor's internal state
    pub fn actor_ref(&self) -> &A {
        self.supervisor.actor_ref()
    }
    
    /// Start the actor
    pub async fn start(&mut self) {
        self.supervisor.started().await;
    }
    
    /// Send a message to the actor and get the result
    pub async fn send<M>(&mut self, message: M) -> Result<M::Result, ActorError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.supervisor.send(&self.system, message).await
    }
    
    /// Process one message in the actor's mailbox
    pub async fn process_one(&mut self) {
        self.supervisor.tick(self.system.clone(), Some(Duration::from_millis(100))).await;
    }
    
    /// Process multiple messages in the actor's mailbox
    pub async fn process_n(&mut self, n: usize) {
        for _ in 0..n {
            self.process_one().await;
        }
    }
    
    /// Assert a condition about the actor's state
    pub fn assert_state<F>(&self, check_fn: F) 
    where
        F: FnOnce(&A),
    {
        check_fn(self.actor_ref());
    }
    
    /// Wait for a specific condition to be true in the actor's state
    /// Will process messages until the condition is true or until the timeout is reached
    pub async fn wait_for_state<F>(&mut self, condition: F, timeout_ms: u64) -> Result<(), &'static str> 
    where
        F: Fn(&A) -> bool,
    {
        let start = std::time::Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        
        // Check initial state first
        if condition(self.actor_ref()) {
            return Ok(());
        }
        
        while start.elapsed() < timeout {
            // Process one message
            self.process_one().await;
            
            // Check if condition is now true
            if condition(self.actor_ref()) {
                return Ok(());
            }
            
            // Small sleep to avoid tight loop
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        Err("Timed out waiting for condition")
    }
}

// Re-export necessary types for easier imports
pub mod prelude {
    pub use super::{ActorTestHarness, ActorTestHandle};
}