use serde::{Deserialize, Serialize};
use std::fmt::Display;

use uuid::Uuid;

pub struct MaybeId(pub Option<Id>);

impl MaybeId {
    pub fn new(id: Id) -> Self {
        Self(Some(id))
    }

    pub fn unset() -> Self {
        Self(None)
    }

    pub fn get(&self) -> Option<Id> {
        self.0
    }

    pub fn is_set(&self) -> bool {
        self.0.is_some()
    }
}

impl Display for MaybeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(ref id) => write!(f, "{}", id),
            None => write!(f, "unset"),
        }
    }
}

/// Unique identifier type within the system
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id(uuid::Uuid);

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl Id {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn from_u128(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }

    pub const fn nil() -> Self {
        Self(Uuid::nil())
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for Id {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}
