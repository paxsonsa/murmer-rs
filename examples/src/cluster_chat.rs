//! Cluster Chat — demonstrates running actors locally, then distributing them.
//!
//! The same actor code runs in both modes. Only the `System` construction
//! differs — your actor definitions, message types, and handlers are identical.
//!
//! # Local mode (single process, no networking)
//!
//! ```sh
//! cargo run -p murmer-examples --bin cluster_chat -- --local
//! ```
//!
//! # Cluster mode (one node per process)
//!
//! ```sh
//! # Terminal 1 — seed node
//! cargo run -p murmer-examples --bin cluster_chat -- --node alpha --port 7100
//!
//! # Terminal 2 — joins via seed
//! cargo run -p murmer-examples --bin cluster_chat -- --node beta --port 7200 --seed 127.0.0.1:7100
//!
//! # Terminal 3 — joins via seed
//! cargo run -p murmer-examples --bin cluster_chat -- --node gamma --port 7300 --seed 127.0.0.1:7100
//! ```
//!
//! # Interactive commands (both modes)
//!
//! ```text
//! > post general alice Hello everyone!
//! > history general
//! > status
//! > rooms
//! > help
//! > quit
//! ```

use murmer::prelude::*;
use serde::{Deserialize, Serialize};
use std::env;
use std::io::{self, BufRead, Write as _};
use std::net::SocketAddr;

// =============================================================================
// ACTOR DEFINITION — identical for local and clustered modes
// =============================================================================

#[derive(Debug)]
struct ChatRoom;

struct ChatRoomState {
    room_name: String,
    messages: Vec<ChatEntry>,
}

#[derive(Clone)]
struct ChatEntry {
    from: String,
    text: String,
}

impl Actor for ChatRoom {
    type State = ChatRoomState;
}

// =============================================================================
// RESPONSE TYPES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomStatus {
    room_name: String,
    message_count: usize,
    last_message: Option<String>,
}

// =============================================================================
// HANDLERS — auto-generates message structs + extension trait
// =============================================================================

#[handlers]
impl ChatRoom {
    #[handler]
    fn post_message(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        from: String,
        text: String,
    ) -> usize {
        state.messages.push(ChatEntry { from, text });
        state.messages.len()
    }

    #[handler]
    fn get_history(&mut self, _ctx: &ActorContext<Self>, state: &mut ChatRoomState) -> Vec<String> {
        state
            .messages
            .iter()
            .map(|e| format!("{}: {}", e.from, e.text))
            .collect()
    }

    #[handler]
    fn get_status(&mut self, _ctx: &ActorContext<Self>, state: &mut ChatRoomState) -> RoomStatus {
        RoomStatus {
            room_name: state.room_name.clone(),
            message_count: state.messages.len(),
            last_message: state
                .messages
                .last()
                .map(|e| format!("{}: {}", e.from, e.text)),
        }
    }
}

// =============================================================================
// INTERACTIVE COMMAND LOOP — works the same in local and clustered modes
// =============================================================================

/// Start one `ChatRoom` actor per name. Registering these actors is what gives
/// the cluster something to replicate, which in turn drives transitive discovery
/// (so every node connects to every other, not just to the seed).
fn start_default_rooms(system: &System, default_rooms: &[&str]) {
    for room_name in default_rooms {
        let label = format!("room/{room_name}");
        system.start(
            &label,
            ChatRoom,
            ChatRoomState {
                room_name: room_name.to_string(),
                messages: vec![],
            },
        );
        println!("  Started room: #{room_name}");
    }
}

/// Run the interactive command loop against a System.
///
/// This function doesn't know or care whether the system is local or
/// clustered. It uses the same `System` API either way.
async fn run_interactive(system: &System, default_rooms: &[&str]) {
    start_default_rooms(system, default_rooms);

    println!("\nType 'help' for available commands.\n");

    let stdin = io::stdin();
    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap() == 0 {
            break; // EOF
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        match parts[0] {
            "help" | "h" => {
                println!("Commands:");
                println!("  post <room> <user> <message>  — Post a message to a room");
                println!("  history <room>                — Show message history");
                println!("  status [room]                 — Show room status (or all rooms)");
                println!("  rooms                         — List known rooms");
                println!("  create <room>                 — Create a new room");
                println!("  quit | exit                   — Exit");
            }

            "post" | "p" => {
                if parts.len() < 4 {
                    println!("Usage: post <room> <user> <message>");
                    continue;
                }
                let label = format!("room/{}", parts[1]);
                match system.lookup::<ChatRoom>(&label) {
                    Some(ep) => {
                        let count = ep
                            .post_message(parts[2].into(), parts[3].into())
                            .await
                            .unwrap();
                        println!("  [{} messages in #{}]", count, parts[1]);
                    }
                    None => println!(
                        "  Room '{}' not found. Try 'rooms' to see available rooms.",
                        parts[1]
                    ),
                }
            }

            "history" | "hist" => {
                if parts.len() < 2 {
                    println!("Usage: history <room>");
                    continue;
                }
                let label = format!("room/{}", parts[1]);
                match system.lookup::<ChatRoom>(&label) {
                    Some(ep) => {
                        let history = ep.get_history().await.unwrap();
                        if history.is_empty() {
                            println!("  #{} has no messages yet.", parts[1]);
                        } else {
                            println!("  --- #{} ---", parts[1]);
                            for msg in &history {
                                println!("  {msg}");
                            }
                        }
                    }
                    None => println!("  Room '{}' not found.", parts[1]),
                }
            }

            "status" | "s" => {
                if parts.len() >= 2 {
                    let label = format!("room/{}", parts[1]);
                    match system.lookup::<ChatRoom>(&label) {
                        Some(ep) => {
                            let s = ep.get_status().await.unwrap();
                            println!(
                                "  #{}: {} messages (last: {})",
                                s.room_name,
                                s.message_count,
                                s.last_message.as_deref().unwrap_or("none")
                            );
                        }
                        None => println!("  Room '{}' not found.", parts[1]),
                    }
                } else {
                    println!("  Use 'rooms' to list rooms, then 'status <room>' for details.");
                }
            }

            "rooms" | "r" => {
                // Use the receptionist's event subscriber to list rooms.
                // In a real app you'd use ReceptionKey for this — but for
                // simplicity we'll try known labels.
                println!("  Known rooms:");
                for name in default_rooms {
                    let label = format!("room/{name}");
                    if let Some(ep) = system.lookup::<ChatRoom>(&label) {
                        let s = ep.get_status().await.unwrap();
                        println!("    #{name} — {} messages", s.message_count);
                    }
                }
            }

            "create" | "c" => {
                if parts.len() < 2 {
                    println!("Usage: create <room>");
                    continue;
                }
                let room_name = parts[1];
                let label = format!("room/{room_name}");
                if system.lookup::<ChatRoom>(&label).is_some() {
                    println!("  Room '#{room_name}' already exists.");
                } else {
                    system.start(
                        &label,
                        ChatRoom,
                        ChatRoomState {
                            room_name: room_name.into(),
                            messages: vec![],
                        },
                    );
                    println!("  Created room: #{room_name}");
                }
            }

            "quit" | "exit" | "q" => {
                println!("Shutting down...");
                system.shutdown().await;
                break;
            }

            other => {
                println!("  Unknown command: '{other}'. Type 'help' for usage.");
            }
        }
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() {
    // Show cluster activity (connections, membership). Tune with RUST_LOG,
    // e.g. RUST_LOG=murmer::cluster=debug. Defaults to murmer at info.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "murmer=info".into()),
        )
        .init();

    let args: Vec<String> = env::args().collect();

    if args.iter().any(|a| a == "--local") {
        // =====================================================================
        // LOCAL MODE — System::local(), no networking
        // =====================================================================
        println!("=== murmer cluster_chat (local mode) ===");
        println!("All actors run in-memory in this process.\n");

        let system = System::local();
        run_interactive(&system, &["general", "random"]).await;
    } else {
        // =====================================================================
        // CLUSTER MODE — System::clustered(), QUIC networking
        // =====================================================================
        use murmer::cluster::config::{ClusterConfig, Discovery};
        use murmer::cluster::sync::{SpawnRegistry, TypeRegistry};

        let node_name = args
            .iter()
            .position(|a| a == "--node")
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| "node-1".into());

        let port: u16 = args
            .iter()
            .position(|a| a == "--port")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(7100);

        // A seed is now an iroh endpoint: `<endpoint-id>@<host:port>`. iroh dials
        // by cryptographic key, so the bare address is no longer enough. Run a
        // seed node first and copy the `seed: <id>@<addr>` line it prints.
        let seed: Option<iroh::EndpointAddr> = args
            .iter()
            .position(|a| a == "--seed")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| parse_seed(s));

        let discovery = match &seed {
            Some(addr) => {
                println!("Joining cluster via seed: {}@{:?}", addr.id, addr);
                Discovery::SeedNodes(vec![addr.clone()])
            }
            None => {
                println!("Starting as seed node (no peers yet)");
                Discovery::None
            }
        };

        // Persist a distinct key per node so its endpoint id is stable across
        // restarts. `--key <path>` overrides the default `<node-name>.key` (used
        // by the Docker compose setup to mount per-node keys).
        let key_path = args
            .iter()
            .position(|a| a == "--key")
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| format!("{node_name}.key"));

        // The address other nodes use to reach this one. Defaults to loopback for
        // single-host demos; `--advertise <host:port>` sets a routable address
        // (the Docker entrypoint passes the container's IP so the nodes mesh).
        let advertise = args
            .iter()
            .position(|a| a == "--advertise")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| resolve_addr(s))
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], port)));

        let config = ClusterConfig::builder()
            .name(&node_name)
            .key_path(key_path)
            .listen(SocketAddr::from(([0, 0, 0, 0], port)))
            .advertise(advertise)
            .cookie("chat-example-secret")
            .discovery(discovery)
            .build()
            .expect("invalid cluster config");

        println!("=== murmer cluster_chat (cluster mode: {node_name} on :{port}) ===\n");

        let system = System::clustered(config, TypeRegistry::from_auto(), SpawnRegistry::new())
            .await
            .expect("failed to start cluster");

        // Print this node's seed line so other nodes can join via `--seed`.
        if let (Some(id), Some(addr)) = (system.endpoint_id(), system.local_addr()) {
            println!(
                "seed: {id}@127.0.0.1:{}  (pass to another node with --seed)",
                addr.port()
            );
        }

        // In cluster mode, each node hosts one room named after itself.
        // When attached to a terminal, run the interactive loop. Otherwise
        // (e.g. under Docker, where there is no stdin) stay up and serve until
        // the process is terminated, so the node keeps meshing.
        use std::io::IsTerminal as _;
        if std::io::stdin().is_terminal() {
            run_interactive(&system, &[&node_name]).await;
        } else {
            // Headless (e.g. Docker): still host this node's room so the cluster
            // has actors to replicate, then stay up until terminated.
            start_default_rooms(&system, &[&node_name]);
            println!("running headless ({node_name}); send SIGINT/SIGTERM to stop");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Parse a `--seed` argument of the form `<endpoint-id>@<host:port>` into an
/// iroh [`EndpointAddr`]. iroh dials by key, so both parts are required. The
/// host may be an IP or a DNS name (e.g. a Docker service name like `alpha`).
fn parse_seed(s: &str) -> Option<iroh::EndpointAddr> {
    let (id_str, addr_str) = s.split_once('@')?;
    let id: iroh::EndpointId = id_str.parse().ok()?;
    let addr = resolve_addr(addr_str)?;
    Some(iroh::EndpointAddr::from_parts(
        id,
        [iroh::TransportAddr::Ip(addr)],
    ))
}

/// Resolve a `host:port` string to a `SocketAddr`, accepting both literal IPs
/// and DNS names (via the system resolver).
fn resolve_addr(s: &str) -> Option<SocketAddr> {
    use std::net::ToSocketAddrs;
    s.to_socket_addrs().ok()?.next()
}

// =============================================================================
// TESTS — verify everything works through the System API
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// The simplest test: System::local() + start + send.
    #[tokio::test]
    async fn test_system_local_basics() {
        let system = System::local();

        let room = system.start(
            "room/test",
            ChatRoom,
            ChatRoomState {
                room_name: "test".into(),
                messages: vec![],
            },
        );

        let count = room
            .post_message("alice".into(), "hello".into())
            .await
            .unwrap();
        assert_eq!(count, 1);

        let status = room.get_status().await.unwrap();
        assert_eq!(status.room_name, "test");
        assert_eq!(status.message_count, 1);
    }

    /// Lookup works through System just like through Receptionist.
    #[tokio::test]
    async fn test_system_lookup() {
        let system = System::local();

        system.start(
            "room/lobby",
            ChatRoom,
            ChatRoomState {
                room_name: "lobby".into(),
                messages: vec![],
            },
        );

        // Lookup by label
        let ep = system.lookup::<ChatRoom>("room/lobby").unwrap();
        let status = ep.get_status().await.unwrap();
        assert_eq!(status.room_name, "lobby");

        // Missing label returns None
        assert!(system.lookup::<ChatRoom>("room/missing").is_none());
    }

    /// Multiple rooms on the same system are independent.
    #[tokio::test]
    async fn test_system_multiple_rooms() {
        let system = System::local();

        let a = system.start(
            "room/a",
            ChatRoom,
            ChatRoomState {
                room_name: "a".into(),
                messages: vec![],
            },
        );
        let b = system.start(
            "room/b",
            ChatRoom,
            ChatRoomState {
                room_name: "b".into(),
                messages: vec![],
            },
        );

        a.post_message("x".into(), "in a".into()).await.unwrap();
        b.post_message("y".into(), "in b".into()).await.unwrap();
        b.post_message("z".into(), "also b".into()).await.unwrap();

        assert_eq!(a.get_status().await.unwrap().message_count, 1);
        assert_eq!(b.get_status().await.unwrap().message_count, 2);
    }

    /// System::local() + simulated remote wire — proving the same actor code
    /// works for both local and remote dispatch without changes.
    #[tokio::test]
    async fn test_system_simulated_remote() {
        use murmer::{RemoteInvocation, RemoteResponse, ResponseRegistry, run_node_receiver};
        use std::sync::Arc;
        use tokio::sync::mpsc;

        // "Remote" system hosts the actor
        let remote_system = System::local();
        let _room = remote_system.start(
            "room/remote",
            ChatRoom,
            ChatRoomState {
                room_name: "remote".into(),
                messages: vec![],
            },
        );

        // Wire channels simulate the QUIC stream
        let remote_receptionist = Arc::new(Receptionist::new());
        remote_receptionist.start(
            "room/remote",
            ChatRoom,
            ChatRoomState {
                room_name: "remote".into(),
                messages: vec![],
            },
        );

        let (wire_tx, wire_rx) = mpsc::unbounded_channel::<RemoteInvocation>();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<RemoteResponse>();
        tokio::spawn(run_node_receiver(remote_receptionist, wire_rx, resp_tx));

        let response_registry = ResponseRegistry::new();
        let reg_clone = response_registry.clone();
        tokio::spawn(async move {
            while let Some(resp) = resp_rx.recv().await {
                reg_clone.complete(resp);
            }
        });

        // "Local" system registers the remote actor
        let local_system = System::local();
        local_system.receptionist().register_remote::<ChatRoom>(
            "room/remote",
            wire_tx,
            response_registry,
        );

        // Same API — caller doesn't know it's remote
        let ep = local_system.lookup::<ChatRoom>("room/remote").unwrap();
        let count = ep
            .post_message("user".into(), "hello over the wire!".into())
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    /// System can be shut down cleanly.
    #[tokio::test]
    async fn test_system_shutdown() {
        let system = System::local();
        system.start(
            "room/temp",
            ChatRoom,
            ChatRoomState {
                room_name: "temp".into(),
                messages: vec![],
            },
        );
        system.shutdown().await; // no-op for local, but should not panic
    }
}
