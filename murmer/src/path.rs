use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::id::Id;

/// Represents a unique path-like address for any actor in the system, whether local or remote.
/// Format: system://host:port/actor_type/instance_id
/// Examples:
/// - local://system1/user/abc-123 (local actor)
/// - tcp://127.0.0.1:4000/user/def-456 (remote actor)
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ActorPath {
    /// The scheme (local, tcp, etc)
    pub scheme: AddressScheme,
    /// The actor type path (e.g., "user", "system/receptionist")
    pub type_path: Arc<str>,
    /// The unique instance identifier
    pub instance_id: Arc<Id>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AddressScheme {
    Local,
    Tcp { host: String, port: u16 },
}

impl AddressScheme {
    const LOCAL_SCHEME: &'static str = "local";
    const TCP_SCHEME: &'static str = "tcp";
}

impl ActorPath {
    pub fn local(type_path: String, instance_id: Id) -> Self {
        Self {
            scheme: AddressScheme::Local,
            type_path: type_path.into(),
            instance_id: instance_id.into(),
        }
    }

    pub fn remote(host: String, port: u16, type_path: String, instance_id: Id) -> Self {
        Self {
            scheme: AddressScheme::Tcp { host, port },
            type_path: type_path.into(),
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

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.scheme {
            AddressScheme::Local => {
                write!(
                    f,
                    "{}://{}/{}",
                    AddressScheme::LOCAL_SCHEME,
                    self.type_path,
                    self.instance_id
                )
            }
            AddressScheme::Tcp { host, port } => {
                write!(
                    f,
                    "{}://{}:{}/{}/{}",
                    AddressScheme::TCP_SCHEME,
                    host,
                    port,
                    self.type_path,
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
            AddressScheme::TCP_SCHEME => {
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
