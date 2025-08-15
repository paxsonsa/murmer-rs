pub mod actor;
pub mod cluster;
pub mod endpoint;
pub mod message;
pub mod node;
pub mod receptionist;
pub mod supervisor;
pub mod system;

// Re-export the main types for convenience
pub use actor::{Actor, ActorError, Context, Handler};
pub use cluster::{ClusterError, ClusterEvent, ClusterManager, NodeIdentity};
pub use endpoint::{Endpoint, EndpointError};
pub use message::{Envelope, Message};
pub use node::{NodeAddress, NodeId, NodeServer, NodeServerConfig, NodeServerError};
pub use receptionist::{ActorKey, ActorLabel, AnyEndpoint, Receptionist};
pub use supervisor::{Initialized, State, Supervisor, Uninitialized};
pub use system::{System, SystemBuilder, SystemError};

#[cfg(test)]
#[path = "lib.test.rs"]
mod lib_test;
