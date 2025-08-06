pub mod actor;
pub mod message;
pub mod endpoint;
pub mod supervisor;
pub mod system;
pub mod receptionist;
pub mod node;

// Re-export the main types for convenience
pub use actor::{Actor, Context, Handler, ActorError};
pub use message::{Message, Envelope};
pub use endpoint::{Endpoint, EndpointError};
pub use supervisor::{Supervisor, State, Uninitialized, Initialized};
pub use system::{System, SystemBuilder, SystemError};
pub use receptionist::{Receptionist, ActorKey, ActorLabel, AnyEndpoint};
pub use node::{NodeId, NodeAddress, NodeServer, NodeServerConfig, NodeServerError};

#[cfg(test)]
#[path = "lib.test.rs"]
mod lib_test;
