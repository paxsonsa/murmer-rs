//! Router — distribute messages across a pool of actor endpoints.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::actor::{Actor, Handler, RemoteMessage};
use crate::endpoint::Endpoint;
use crate::wire::SendError;

/// Strategy for how a Router distributes messages across its endpoints.
#[derive(Debug, Clone, Default)]
pub enum RoutingStrategy {
    #[default]
    RoundRobin,
    Random,
    Broadcast,
}

/// A router distributes messages across a group of endpoints for the same actor type.
///
/// Useful for load balancing, fan-out, and work distribution patterns.
pub struct Router<A: Actor> {
    endpoints: Vec<Endpoint<A>>,
    strategy: RoutingStrategy,
    counter: AtomicUsize,
}

impl<A: Actor + 'static> Router<A> {
    pub fn new(endpoints: Vec<Endpoint<A>>, strategy: RoutingStrategy) -> Self {
        Self {
            endpoints,
            strategy,
            counter: AtomicUsize::new(0),
        }
    }

    /// Send a message to one endpoint based on the routing strategy.
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return Err(SendError::MailboxClosed);
        }

        match self.strategy {
            RoutingStrategy::RoundRobin => {
                let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
                self.endpoints[idx].send(msg).await
            }
            RoutingStrategy::Random => {
                use rand::Rng;
                let idx = rand::rng().random_range(0..self.endpoints.len());
                self.endpoints[idx].send(msg).await
            }
            RoutingStrategy::Broadcast => {
                // For broadcast via send(), just send to the first endpoint
                self.endpoints[0].send(msg).await
            }
        }
    }

    /// Send a message to ALL endpoints. Returns a Vec of results.
    pub async fn broadcast<M>(&self, msg: M) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let mut results = Vec::with_capacity(self.endpoints.len());
        for ep in &self.endpoints {
            results.push(ep.send(msg.clone()).await);
        }
        results
    }

    pub fn add(&mut self, endpoint: Endpoint<A>) {
        self.endpoints.push(endpoint);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.endpoints.len() {
            self.endpoints.remove(index);
        }
    }

    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }
}
