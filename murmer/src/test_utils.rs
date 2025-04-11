//! Simple test utilities for easier actor testing
//! 
//! This module provides a basic TestHarness for actor testing.

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
    supervisor: TestSupervisor<A>,
    /// The system for this actor
    system: System,
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
    
    /// Assert a condition about the actor's state
    pub fn assert_state<F>(&self, check_fn: F) 
    where
        F: FnOnce(&A),
    {
        check_fn(self.actor_ref());
    }
}

// Re-export necessary types for easier imports
pub mod prelude {
    pub use super::{ActorTestHarness, ActorTestHandle};
}