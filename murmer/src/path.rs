use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::id::Id;

/// Represents a unique path-like address for any actor in the system, whether local or remote.
/// Format: system://host:port/actor_type/instance_id
/// Examples:
/// - local://user/abc-123 (local actor)
/// - remote://127.0.0.1:4000/user/def-456 (remote actor)
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ActorPath {
    /// The scheme (local, tcp, etc)
    pub scheme: AddressScheme,
    /// The actor type path (e.g., "User", "Receptionist")
    pub type_id: Arc<str>,
    /// The unique instance identifier
    pub instance_id: Arc<Id>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum AddressScheme {
    Local,
    Remote { host: String, port: u16 },
}

impl AddressScheme {
    const LOCAL_SCHEME: &'static str = "local";
    const REMOTE_SCHEME: &'static str = "remote";
}

// TODO: Implement a group key that aligns with the receptionist key format of that is
// scheme://reception_id/group_id/instance_id
impl ActorPath {
    pub fn local(type_path: String, instance_id: Id) -> Self {
        Self {
            scheme: AddressScheme::Local,
            type_id: type_path.into(),
            instance_id: instance_id.into(),
        }
    }

    pub fn remote(host: String, port: u16, type_path: String, instance_id: Id) -> Self {
        Self {
            scheme: AddressScheme::Remote { host, port },
            type_id: type_path.into(),
            instance_id: instance_id.into(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self.scheme, AddressScheme::Local { .. })
    }

    pub fn is_remote(&self) -> bool {
        !self.is_local()
    }
}

impl serde::Serialize for ActorPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let path_str = self.to_string();
        serializer.serialize_str(&path_str)
    }
}

impl<'de> serde::Deserialize<'de> for ActorPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path_str = String::deserialize(deserializer)?;
        ActorPath::from_str(&path_str).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Change this to be 'murmur://<scheme>/root_key/group_key/instance_id'
        // So a remote actor is: murmur://127.0.0.1:4000/actorA/default/12345
        // and local actor is: murmur://local/actorA/default/12345
        match &self.scheme {
            AddressScheme::Local => {
                write!(
                    f,
                    "{}://{}/{}",
                    AddressScheme::LOCAL_SCHEME,
                    self.type_id,
                    self.instance_id
                )
            }
            AddressScheme::Remote { host, port } => {
                write!(
                    f,
                    "{}://{}:{}/{}/{}",
                    AddressScheme::REMOTE_SCHEME,
                    host,
                    port,
                    self.type_id,
                    self.instance_id
                )
            }
        }
    }
}

impl fmt::Debug for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ActorPathError {
    #[error("Invalid actor path format")]
    InvalidFormat,
    #[error("Invalid scheme")]
    InvalidScheme,
    #[error("Invalid UUID")]
    InvalidUuid,
    #[error("Missing required components")]
    MissingComponents,
}

impl FromStr for ActorPath {
    type Err = ActorPathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("://").collect();
        if parts.len() != 2 {
            return Err(ActorPathError::InvalidFormat);
        }

        let path_parts: Vec<&str> = parts[1].split('/').collect();

        match parts[0] {
            AddressScheme::LOCAL_SCHEME => {
                if path_parts.len() < 3 {
                    return Err(ActorPathError::MissingComponents);
                }

                if path_parts[0] != "local" {
                    return Err(ActorPathError::InvalidFormat);
                }

                let type_path = path_parts[1].to_string();
                let instance_id = Uuid::parse_str(path_parts[2])
                    .map_err(|_| ActorPathError::InvalidUuid)?
                    .into();

                Ok(ActorPath::local(type_path, instance_id))
            }
            AddressScheme::REMOTE_SCHEME => {
                if path_parts.len() < 3 {
                    return Err(ActorPathError::MissingComponents);
                }

                let addr_parts: Vec<&str> = path_parts[0].split(':').collect();
                if addr_parts.len() != 2 {
                    return Err(ActorPathError::InvalidFormat);
                }

                let host = addr_parts[0].to_string();
                let port = addr_parts[1]
                    .parse()
                    .map_err(|_| ActorPathError::InvalidFormat)?;

                let type_path = path_parts[1].to_string();
                let instance_id = Uuid::parse_str(path_parts[2])
                    .map_err(|_| ActorPathError::InvalidUuid)?
                    .into();

                Ok(ActorPath::remote(host, port, type_path, instance_id))
            }
            _ => Err(ActorPathError::InvalidScheme),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_actor_path() {
        let id = Id::new();
        let path = ActorPath::local("user".to_string(), id);
        let path_str = path.to_string();
        let parsed = ActorPath::from_str(&path_str).unwrap();
        assert_eq!(path, parsed);
        assert!(parsed.is_local());
    }

    #[test]
    fn test_remote_actor_path() {
        let id = Id::new();
        let path = ActorPath::remote("127.0.0.1".to_string(), 4000, "user".to_string(), id);
        let path_str = path.to_string();
        let parsed = ActorPath::from_str(&path_str).unwrap();
        assert_eq!(path, parsed);
        assert!(parsed.is_remote());
    }
}
