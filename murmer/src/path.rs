use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::id::Id;

/// Represents a unique path-like address for any actor in the system, whether local or remote.
/// Format: murmur://<scheme>/root_key/group_key/instance_id
/// Examples:
/// - murmur://local/user/default/abc-123 (local actor)
/// - murmur://127.0.0.1:4000/user/default/def-456 (remote actor)
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ActorPath {
    /// The scheme (local, remote, etc)
    pub scheme: AddressScheme,
    /// The actor type path (e.g., "User", "Receptionist")
    pub type_id: Arc<str>,
    /// The group identifier (typically "default" or a custom group)
    pub group_id: Arc<str>,
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
    /// Protocol prefix for all actor paths
    const MURMUR_PROTOCOL: &'static str = "murmur";
}

impl ActorPath {
    /// Creates a local actor path with the given type, group, and instance ID
    pub fn local(type_path: String, group_id: String, instance_id: Id) -> Self {
        Self {
            scheme: AddressScheme::Local,
            type_id: type_path.into(),
            group_id: group_id.into(),
            instance_id: instance_id.into(),
        }
    }

    /// Creates a local actor path with default group
    pub fn local_default(type_path: String, instance_id: Id) -> Self {
        Self::local(type_path, "default".to_string(), instance_id)
    }

    /// Creates a remote actor path with the given host, port, type, group, and instance ID
    pub fn remote(host: String, port: u16, type_path: String, group_id: String, instance_id: Id) -> Self {
        Self {
            scheme: AddressScheme::Remote { host, port },
            type_id: type_path.into(),
            group_id: group_id.into(),
            instance_id: instance_id.into(),
        }
    }
    
    /// Creates a remote actor path with default group
    pub fn remote_default(host: String, port: u16, type_path: String, instance_id: Id) -> Self {
        Self::remote(host, port, type_path, "default".to_string(), instance_id)
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
        match &self.scheme {
            AddressScheme::Local => {
                write!(
                    f,
                    "{}://{}/{}/{}/{}",
                    AddressScheme::MURMUR_PROTOCOL,
                    AddressScheme::LOCAL_SCHEME,
                    self.type_id,
                    self.group_id,
                    self.instance_id
                )
            }
            AddressScheme::Remote { host, port } => {
                write!(
                    f,
                    "{}://{}:{}/{}/{}/{}",
                    AddressScheme::MURMUR_PROTOCOL,
                    host,
                    port,
                    self.type_id,
                    self.group_id,
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

        // Check for murmur protocol
        if parts[0] != AddressScheme::MURMUR_PROTOCOL {
            return Err(ActorPathError::InvalidScheme);
        }

        // Split the address part (after "murmur://")
        let address_parts: Vec<&str> = parts[1].split('/').collect();
        
        // First part is either "local" or "host:port"
        let first_part = address_parts.get(0).ok_or(ActorPathError::MissingComponents)?;
        
        if *first_part == AddressScheme::LOCAL_SCHEME {
            // Local actor path: murmur://local/type/group/instance_id
            if address_parts.len() < 4 {
                return Err(ActorPathError::MissingComponents);
            }

            let type_path = address_parts.get(1)
                .ok_or(ActorPathError::MissingComponents)?
                .to_string();
                
            let group_id = address_parts.get(2)
                .ok_or(ActorPathError::MissingComponents)?
                .to_string();
                
            let instance_id = Uuid::parse_str(address_parts.get(3).ok_or(ActorPathError::MissingComponents)?)
                .map_err(|_| ActorPathError::InvalidUuid)?
                .into();

            Ok(ActorPath::local(type_path, group_id, instance_id))
        } else {
            // Remote actor path: murmur://host:port/type/group/instance_id
            if address_parts.len() < 4 {
                return Err(ActorPathError::MissingComponents);
            }

            // Parse host:port
            let addr_parts: Vec<&str> = first_part.split(':').collect();
            if addr_parts.len() != 2 {
                return Err(ActorPathError::InvalidFormat);
            }

            let host = addr_parts[0].to_string();
            let port = addr_parts[1]
                .parse()
                .map_err(|_| ActorPathError::InvalidFormat)?;

            let type_path = address_parts.get(1)
                .ok_or(ActorPathError::MissingComponents)?
                .to_string();
                
            let group_id = address_parts.get(2)
                .ok_or(ActorPathError::MissingComponents)?
                .to_string();
                
            let instance_id = Uuid::parse_str(address_parts.get(3).ok_or(ActorPathError::MissingComponents)?)
                .map_err(|_| ActorPathError::InvalidUuid)?
                .into();

            Ok(ActorPath::remote(host, port, type_path, group_id, instance_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_actor_path_default_group() {
        let id = Id::new();
        let path = ActorPath::local_default("user".to_string(), id);
        let path_str = path.to_string();
        let parsed = ActorPath::from_str(&path_str).unwrap();
        assert_eq!(path, parsed);
        assert!(parsed.is_local());
        assert_eq!(*parsed.group_id, "default");
    }

    #[test]
    fn test_local_actor_path_custom_group() {
        let id = Id::new();
        let path = ActorPath::local("user".to_string(), "custom".to_string(), id);
        let path_str = path.to_string();
        let parsed = ActorPath::from_str(&path_str).unwrap();
        assert_eq!(path, parsed);
        assert!(parsed.is_local());
        assert_eq!(*parsed.group_id, "custom");
    }

    #[test]
    fn test_remote_actor_path_default_group() {
        let id = Id::new();
        let path = ActorPath::remote_default("127.0.0.1".to_string(), 4000, "user".to_string(), id);
        let path_str = path.to_string();
        let parsed = ActorPath::from_str(&path_str).unwrap();
        assert_eq!(path, parsed);
        assert!(parsed.is_remote());
        assert_eq!(*parsed.group_id, "default");
    }

    #[test]
    fn test_remote_actor_path_custom_group() {
        let id = Id::new();
        let path = ActorPath::remote("127.0.0.1".to_string(), 4000, "user".to_string(), "region1".to_string(), id);
        let path_str = path.to_string();
        let parsed = ActorPath::from_str(&path_str).unwrap();
        assert_eq!(path, parsed);
        assert!(parsed.is_remote());
        assert_eq!(*parsed.group_id, "region1");
    }

    #[test]
    fn test_path_format() {
        let id = Id::new();
        let path = ActorPath::local("user".to_string(), "custom".to_string(), id);
        let path_str = path.to_string();
        assert!(path_str.starts_with("murmur://local/user/custom/"));
        
        let remote_path = ActorPath::remote("127.0.0.1".to_string(), 4000, "counter".to_string(), "region1".to_string(), id);
        let remote_str = remote_path.to_string();
        assert!(remote_str.starts_with("murmur://127.0.0.1:4000/counter/region1/"));
    }
}
