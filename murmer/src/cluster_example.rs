use std::collections::HashSet;
use std::sync::Arc;

use super::connection_state::{Connecting, Connected, Disconnected};
use super::connection_typed::TypedConnection;
use super::member_typed::Member;
use super::{NetworkAddr, TlsConfig};

// Example of how to use the type-state pattern in your cluster code
pub struct ClusterExample {
    // We need separate collections for each member state
    connecting_members: HashSet<Member<Connecting>>,
    connected_members: HashSet<Member<Connected>>,
    disconnected_members: HashSet<Member<Disconnected>>,
}

impl ClusterExample {
    pub fn new() -> Self {
        ClusterExample {
            connecting_members: HashSet::new(),
            connected_members: HashSet::new(),
            disconnected_members: HashSet::new(),
        }
    }

    // Connect to a peer using the type-state pattern
    pub fn connect_to_peer(
        &mut self,
        peer: Arc<NetworkAddr>,
        socket: quinn::Endpoint,
        tls: TlsConfig,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        // Create a connection in the Connecting state
        match TypedConnection::connect(peer.clone(), socket, tls, cancellation) {
            Ok(connection) => {
                // Create a member with the Connecting connection
                let member = Member::new(connection, peer.clone());
                self.connecting_members.insert(member);
            }
            Err(e) => {
                tracing::error!(error=%e, "Failed to connect to peer");
            }
        }
    }

    // Process connections and move members between states
    pub async fn process_connections(&mut self) {
        // Take ownership of connecting members to process them
        let connecting_members = std::mem::take(&mut self.connecting_members);
        
        for member in connecting_members {
            match member.establish().await {
                Ok(connected_member) => {
                    // Successfully connected, move to connected members
                    self.connected_members.insert(connected_member);
                }
                Err(e) => {
                    // Connection failed, create a disconnected member
                    let disconnected_member = Member {
                        node: member.node,
                        connection: TypedConnection::disconnect(member.connection, Some(e)),
                        status: super::MembershipStatus::Failed,
                        reachability: super::Reachability::Unreachable,
                    };
                    self.disconnected_members.insert(disconnected_member);
                }
            }
        }
    }
}
