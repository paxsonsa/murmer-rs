//! Router — distribute messages across a pool of actor endpoints.
//!
//! A [`Router<A>`] holds multiple [`Endpoint<A>`] handles and distributes
//! messages according to a [`RoutingStrategy`]:
//!
//! - **RoundRobin** — cycles through endpoints sequentially (default)
//! - **Random** — picks a random endpoint per message
//! - **Broadcast** — via [`Router::broadcast`], sends to all endpoints
//!
//! # Example
//!
//! ```rust,ignore
//! let router = Router::new(
//!     vec![ep1, ep2, ep3],
//!     RoutingStrategy::RoundRobin,
//! );
//!
//! // Each send goes to the next endpoint in sequence
//! router.send(Increment { amount: 1 }).await?;
//!
//! // Or send to all at once
//! let results = router.broadcast(GetCount).await;
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::actor::{Actor, Handler, RemoteMessage};
use crate::endpoint::Endpoint;
use crate::listing::{ListingEvent, ReceptionKey, WatchedListing};
use crate::receptionist::Receptionist;
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

    /// Send a message to ALL endpoints concurrently and return when `k` respond
    /// successfully (or all have completed).
    ///
    /// This enables quorum patterns like "write to 3 replicas, succeed when 2
    /// acknowledge." Results are returned in completion order, not endpoint order.
    ///
    /// # Panics
    ///
    /// Does not panic if `k` exceeds the number of endpoints — returns all
    /// available results instead.
    pub async fn send_quorum<M>(&self, msg: M, k: usize) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return vec![Err(SendError::MailboxClosed)];
        }

        let mut join_set = tokio::task::JoinSet::new();
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            join_set.spawn(async move { ep.send(msg).await });
        }

        let target = k.min(self.endpoints.len());
        let mut results = Vec::with_capacity(target);
        let mut successes = 0;

        while let Some(join_result) = join_set.join_next().await {
            let result = join_result.unwrap_or(Err(SendError::ResponseDropped));
            if result.is_ok() {
                successes += 1;
            }
            results.push(result);

            if successes >= target {
                // Quorum reached — abort remaining tasks
                join_set.abort_all();
                break;
            }
        }

        results
    }

    /// Send a message to ALL endpoints concurrently and collect all results
    /// within a timeout. Returns partial results if the timeout expires.
    ///
    /// Unlike `broadcast()` which sends sequentially, this fires all sends
    /// concurrently and returns results in completion order.
    pub async fn scatter_gather<M>(
        &self,
        msg: M,
        timeout: std::time::Duration,
    ) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return vec![Err(SendError::MailboxClosed)];
        }

        let mut join_set = tokio::task::JoinSet::new();
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            join_set.spawn(async move { ep.send(msg).await });
        }

        let mut results = Vec::with_capacity(self.endpoints.len());
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            tokio::select! {
                Some(join_result) = join_set.join_next() => {
                    results.push(join_result.unwrap_or(Err(SendError::ResponseDropped)));
                    if results.len() == self.endpoints.len() {
                        break;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    join_set.abort_all();
                    break;
                }
            }
        }

        results
    }

    /// Send a message to ALL endpoints concurrently and return the first
    /// successful response. Remaining in-flight sends are cancelled.
    ///
    /// Returns `Err` only if ALL endpoints fail.
    pub async fn first_response<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return Err(SendError::MailboxClosed);
        }

        let mut join_set = tokio::task::JoinSet::new();
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            join_set.spawn(async move { ep.send(msg).await });
        }

        let mut last_error = SendError::MailboxClosed;
        while let Some(join_result) = join_set.join_next().await {
            match join_result.unwrap_or(Err(SendError::ResponseDropped)) {
                Ok(result) => {
                    join_set.abort_all();
                    return Ok(result);
                }
                Err(e) => last_error = e,
            }
        }

        Err(last_error)
    }

    /// Send a message to ALL endpoints concurrently without waiting for responses.
    ///
    /// Fire-and-forget pattern. The sends are spawned as background tasks.
    /// Errors are silently dropped.
    pub fn fan_out<M>(&self, msg: M)
    where
        A: Handler<M>,
        M: RemoteMessage + Clone + 'static,
        M::Result: Serialize + DeserializeOwned,
    {
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            tokio::spawn(async move {
                let _ = ep.send(msg).await;
            });
        }
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

// =============================================================================
// POOL ROUTER — reactive router that auto-tracks membership via WatchedListing
// =============================================================================

/// A reactive router that automatically maintains its endpoint pool from a
/// [`WatchedListing`]. As actors check in or deregister, the pool updates
/// in the background.
///
/// # Example
///
/// ```rust,ignore
/// let worker_key = ReceptionKey::<Worker>::new("workers");
///
/// // Pool auto-fills with existing workers and tracks changes
/// let pool = PoolRouter::new(&receptionist, worker_key, RoutingStrategy::RoundRobin);
///
/// // Send routes to a live worker — pool shrinks/grows automatically
/// let result = pool.send(DoWork { task: "hello".into() }).await?;
/// ```
/// Shared pool state: label → endpoint pairs, protected by RwLock.
type Pool<A> = Arc<RwLock<Vec<(String, Endpoint<A>)>>>;

pub struct PoolRouter<A: Actor> {
    pool: Pool<A>,
    strategy: RoutingStrategy,
    counter: AtomicUsize,
}

impl<A: Actor + 'static> PoolRouter<A> {
    /// Create a reactive pool router that tracks all actors checked in with the
    /// given key. Spawns a background task to consume the watched listing.
    pub fn new(
        receptionist: &Receptionist,
        key: ReceptionKey<A>,
        strategy: RoutingStrategy,
    ) -> Self {
        let watched = receptionist.watched_listing(key);
        Self::from_watched_listing(watched, strategy)
    }

    /// Create a pool router from an already-obtained watched listing.
    pub fn from_watched_listing(
        watched: WatchedListing<A>,
        strategy: RoutingStrategy,
    ) -> Self {
        let pool: Pool<A> = Arc::new(RwLock::new(Vec::new()));

        // Spawn background task to consume the watched listing
        let pool_clone = Arc::clone(&pool);
        tokio::spawn(Self::run_watcher(pool_clone, watched));

        Self {
            pool,
            strategy,
            counter: AtomicUsize::new(0),
        }
    }

    async fn run_watcher(pool: Pool<A>, mut watched: WatchedListing<A>) {
        while let Some(event) = watched.next().await {
            match event {
                ListingEvent::Added { label, endpoint } => {
                    let mut pool = pool.write().unwrap();
                    // Avoid duplicates (same label)
                    if !pool.iter().any(|(l, _)| l == &label) {
                        pool.push((label, endpoint));
                    }
                }
                ListingEvent::Removed { label } => {
                    let mut pool = pool.write().unwrap();
                    pool.retain(|(l, _)| l != &label);
                }
            }
        }
    }

    /// Send a message to one endpoint based on the routing strategy.
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let endpoint = {
            let pool = self.pool.read().unwrap();
            if pool.is_empty() {
                return Err(SendError::MailboxClosed);
            }

            let idx = match self.strategy {
                RoutingStrategy::RoundRobin => {
                    self.counter.fetch_add(1, Ordering::Relaxed) % pool.len()
                }
                RoutingStrategy::Random => {
                    use rand::Rng;
                    rand::rng().random_range(0..pool.len())
                }
                RoutingStrategy::Broadcast => 0,
            };

            pool[idx].1.clone()
        }; // lock released here

        endpoint.send(msg).await
    }

    /// Send a message to ALL endpoints concurrently and collect results.
    pub async fn broadcast<M>(&self, msg: M) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let endpoints: Vec<_> = {
            let pool = self.pool.read().unwrap();
            pool.iter().map(|(_, ep)| ep.clone()).collect()
        };

        let mut join_set = tokio::task::JoinSet::new();
        for ep in endpoints {
            let msg = msg.clone();
            join_set.spawn(async move { ep.send(msg).await });
        }

        let mut results = Vec::new();
        while let Some(join_result) = join_set.join_next().await {
            results.push(join_result.unwrap_or(Err(SendError::ResponseDropped)));
        }
        results
    }

    /// Number of endpoints currently in the pool.
    pub fn len(&self) -> usize {
        self.pool.read().unwrap().len()
    }

    /// Whether the pool is currently empty.
    pub fn is_empty(&self) -> bool {
        self.pool.read().unwrap().is_empty()
    }

    /// Get a snapshot of current labels in the pool.
    pub fn labels(&self) -> Vec<String> {
        self.pool
            .read()
            .unwrap()
            .iter()
            .map(|(l, _)| l.clone())
            .collect()
    }
}
