//! Reception keys and listings for subscription-based actor discovery.
//!
//! This module provides group-based actor discovery:
//!
//! - [`ReceptionKey<A>`] — a typed key for grouping actors (e.g., `"workers"`, `"caches"`)
//! - [`Listing<A>`] — an async stream of endpoints matching a key
//!
//! # How it works
//!
//! 1. Start actors via the receptionist
//! 2. Check them in with a key: `receptionist.check_in("counter/w1", worker_key)`
//! 3. Subscribe to the key: `let mut listing = receptionist.listing(worker_key)`
//! 4. The listing immediately yields all existing matches (backfill)
//! 5. New actors that check in with the same key appear as live updates
//!
//! Inspired by Swift's `DistributedReception.Key<Guest>` and `GuestListing<Guest>`.

use std::any::TypeId;
use std::fmt::Debug;
use std::marker::PhantomData;

use tokio::sync::mpsc;

use crate::actor::Actor;
use crate::endpoint::Endpoint;
use crate::receptionist::{ActorEntry, EntryLocation};

// =============================================================================
// RECEPTION KEY — typed group key for actor discovery
// =============================================================================

/// A typed key for grouping actors by type + group name.
/// Multiple actors can share the same key. Used with `listing()` for
/// subscription-based discovery (like Swift's `DistributedReception.Key<Guest>`).
pub struct ReceptionKey<A: Actor> {
    pub(crate) id: String,
    _marker: PhantomData<A>,
}

impl<A: Actor> ReceptionKey<A> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<A: Actor> Clone for ReceptionKey<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A: Actor> Debug for ReceptionKey<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReceptionKey<{}>({:?})",
            std::any::type_name::<A>(),
            self.id
        )
    }
}

/// Type-erased key for internal storage.
#[derive(Clone, Debug)]
pub(crate) struct ErasedReceptionKey {
    pub(crate) id: String,
    pub(crate) type_id: TypeId,
}

// =============================================================================
// LISTING — async stream of endpoints for a reception key
// =============================================================================

/// An async stream of endpoints matching a reception key.
/// Returns existing matches immediately (backfill), then streams new ones
/// as actors register with the matching key.
///
/// Analogous to Swift's `GuestListing<Guest>: AsyncSequence`.
pub struct Listing<A: Actor> {
    rx: mpsc::UnboundedReceiver<Endpoint<A>>,
}

impl<A: Actor> Listing<A> {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<Endpoint<A>>) -> Self {
        Self { rx }
    }

    /// Await the next endpoint. Returns None when the subscription is closed.
    pub async fn next(&mut self) -> Option<Endpoint<A>> {
        self.rx.recv().await
    }

    /// Try to receive without blocking. Returns None if no endpoint is ready.
    pub fn try_next(&mut self) -> Option<Endpoint<A>> {
        self.rx.try_recv().ok()
    }
}

/// Type-erased listing sender stored in the receptionist.
/// Created when listing() is called, notified when matching actors register.
pub(crate) trait ErasedListingSender: Send + Sync {
    fn key_id(&self) -> &str;
    fn actor_type_id(&self) -> TypeId;
    fn try_send_from_entry(&self, entry: &ActorEntry) -> bool;
    fn is_closed(&self) -> bool;
}

pub(crate) struct TypedListingSender<A: Actor + 'static> {
    pub(crate) key_id: String,
    pub(crate) tx: mpsc::UnboundedSender<Endpoint<A>>,
}

impl<A: Actor + 'static> ErasedListingSender for TypedListingSender<A> {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn actor_type_id(&self) -> TypeId {
        TypeId::of::<A>()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn try_send_from_entry(&self, entry: &ActorEntry) -> bool {
        if entry.type_id != TypeId::of::<A>() {
            return true; // wrong type, keep subscription alive
        }

        let endpoint = match &entry.location {
            EntryLocation::Local {
                endpoint_factory, ..
            } => {
                let any_ep = endpoint_factory.create_endpoint_any();
                match any_ep.downcast::<Endpoint<A>>() {
                    Ok(ep) => *ep,
                    Err(_) => return true,
                }
            }
            EntryLocation::Remote {
                wire_tx,
                response_registry,
            } => Endpoint::remote(
                entry.label.clone(),
                wire_tx.clone(),
                response_registry.clone(),
            ),
        };
        self.tx.send(endpoint).is_ok()
    }
}

// =============================================================================
// WATCHED LISTING — streams additions AND removals with labels
// =============================================================================

/// An event from a watched listing: an actor was added or removed from the group.
pub enum ListingEvent<A: Actor> {
    /// A new actor checked in with the matching key.
    Added {
        label: String,
        endpoint: Endpoint<A>,
    },
    /// An actor was deregistered (stopped, crashed, or departed).
    Removed { label: String },
}

/// A listing that streams both additions and removals, with labels.
///
/// Use this to build reactive pools that automatically track membership
/// changes. Created via [`Receptionist::watched_listing`].
pub struct WatchedListing<A: Actor> {
    rx: mpsc::UnboundedReceiver<ListingEvent<A>>,
}

impl<A: Actor> WatchedListing<A> {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<ListingEvent<A>>) -> Self {
        Self { rx }
    }

    /// Await the next event. Returns None when the subscription is closed.
    pub async fn next(&mut self) -> Option<ListingEvent<A>> {
        self.rx.recv().await
    }

    /// Try to receive without blocking.
    pub fn try_next(&mut self) -> Option<ListingEvent<A>> {
        self.rx.try_recv().ok()
    }
}

/// Type-erased watched listing sender. Supports both add and remove events.
pub(crate) trait ErasedWatchedListingSender: Send + Sync {
    fn key_id(&self) -> &str;
    fn actor_type_id(&self) -> TypeId;
    /// Send an Added event from a registry entry.
    fn try_send_added(&self, entry: &ActorEntry) -> bool;
    /// Send a Removed event for the given label.
    fn try_send_removed(&self, label: &str) -> bool;
    fn is_closed(&self) -> bool;
}

pub(crate) struct TypedWatchedListingSender<A: Actor + 'static> {
    pub(crate) key_id: String,
    pub(crate) tx: mpsc::UnboundedSender<ListingEvent<A>>,
}

impl<A: Actor + 'static> ErasedWatchedListingSender for TypedWatchedListingSender<A> {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn actor_type_id(&self) -> TypeId {
        TypeId::of::<A>()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn try_send_added(&self, entry: &ActorEntry) -> bool {
        if entry.type_id != TypeId::of::<A>() {
            return true; // wrong type, keep subscription alive
        }

        let endpoint = match &entry.location {
            EntryLocation::Local {
                endpoint_factory, ..
            } => {
                let any_ep = endpoint_factory.create_endpoint_any();
                match any_ep.downcast::<Endpoint<A>>() {
                    Ok(ep) => *ep,
                    Err(_) => return true,
                }
            }
            EntryLocation::Remote {
                wire_tx,
                response_registry,
            } => Endpoint::remote(
                entry.label.clone(),
                wire_tx.clone(),
                response_registry.clone(),
            ),
        };

        self.tx
            .send(ListingEvent::Added {
                label: entry.label.clone(),
                endpoint,
            })
            .is_ok()
    }

    fn try_send_removed(&self, label: &str) -> bool {
        self.tx
            .send(ListingEvent::Removed {
                label: label.to_string(),
            })
            .is_ok()
    }
}
