//! High-level Node interface for cluster communication

use std::sync::Arc;
use crate::{
    cluster::ClusterId,
    net::{self, NetworkAddrRef},
    prelude::*,
};
use super::{
    actor::NodeActor,
    errors::NodeCreationError,
};

/// Represents a connection to a remote node in the distributed cluster
#[derive(Clone)]
pub struct Node {
    /// Unique identifier for this node
    pub id: Id,
    /// Local endpoint for communicating with the NodeActor
    pub endpoint: crate::system::LocalEndpoint<NodeActor>,
}

impl Node {
    /// Create a Node for outgoing connections (we initiate the connection)
    pub async fn connect(
        system: System,
        _cluster_id: Arc<ClusterId>,
        network_address: NetworkAddrRef,
        mut connection: Box<dyn net::Connection>,
    ) -> Result<Node, NodeCreationError> {
        // Ensure the connection is established
        connection.connect().await?;
        
        // Get system receptionist
        let receptionist = system.receptionist_ref().clone();
        
        // Create NodeActor for outgoing connection
        let node_actor = NodeActor::connect(
            network_address,
            connection,
            receptionist,
        ).await?;
        
        // Get the actor's ID before spawning
        let actor_id = Id::new(); // TODO: Should get this from NodeActor path
        
        // Spawn the NodeActor in the system
        let endpoint = system.spawn_with(node_actor)
            .map_err(|_| NodeCreationError::NotConnected)?;
        
        // Convert to LocalEndpoint
        let local_endpoint = crate::system::LocalEndpoint::from_endpoint(endpoint);
        
        Ok(Node {
            id: actor_id,
            endpoint: local_endpoint,
        })
    }

    /// Create a Node for incoming connections (they initiated the connection)
    pub async fn accept(
        system: System,
        _cluster_id: Arc<ClusterId>,
        network_address: NetworkAddrRef,
        connection: Box<dyn net::Connection>,
    ) -> Result<Node, NodeCreationError> {
        // Get system receptionist
        let receptionist = system.receptionist_ref().clone();
        
        // Create NodeActor for incoming connection
        let node_actor = NodeActor::accept(
            network_address,
            connection,
            receptionist,
        ).await?;
        
        // Get the actor's ID before spawning
        let actor_id = Id::new(); // TODO: Should get this from NodeActor path
        
        // Spawn the NodeActor in the system
        let endpoint = system.spawn_with(node_actor)
            .map_err(|_| NodeCreationError::NotConnected)?;
        
        // Convert to LocalEndpoint
        let local_endpoint = crate::system::LocalEndpoint::from_endpoint(endpoint);
        
        Ok(Node {
            id: actor_id,
            endpoint: local_endpoint,
        })
    }

    /// Create a remote proxy endpoint for a given actor path
    /// This method creates an endpoint that will communicate with a remote actor
    /// through this node's connection
    pub fn create_remote_proxy<A: crate::actor::Actor>(
        &self,
        path: crate::path::ActorPath,
    ) -> Result<crate::system::Endpoint<A>, NodeCreationError> {
        // Create a RemoteProxy for network communication
        let proxy = crate::system::RemoteProxy::new(self.clone(), path.clone());
        
        // Create an EndpointSender using the remote proxy
        let sender = crate::system::EndpointSender::from_remote_proxy(proxy);
        
        // Create and return the typed endpoint
        Ok(crate::system::Endpoint::new(sender, std::sync::Arc::new(path)))
    }
}

impl Eq for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}