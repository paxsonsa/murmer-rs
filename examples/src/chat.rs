#![allow(dead_code)]
//! Chat room example — demonstrates murmer's distributed actor patterns:
//!
//! - Multiple actor types cooperating (ChatRoom + Participant)
//! - Actor watches for crash detection
//! - Reception keys for dynamic group discovery
//! - Router for broadcasting to participants
//! - Supervision with restart policies
//! - ActorRef for serializable actor identity

use murmer::prelude::*;
use murmer_macros::{Message, handlers};
use serde::{Deserialize, Serialize};

// =============================================================================
// CHAT ROOM ACTOR — manages a room with participants
// =============================================================================

#[derive(Debug)]
struct ChatRoom;

struct ChatRoomState {
    room_name: String,
    messages: Vec<ChatEntry>,
}

struct ChatEntry {
    from: String,
    text: String,
}

impl Actor for ChatRoom {
    type State = ChatRoomState;
}

/// Post a message to the room. Returns the total message count.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = usize, remote = "chat::PostMessage")]
struct PostMessage {
    from: String,
    text: String,
}

/// Get the message history.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Vec<String>, remote = "chat::GetHistory")]
struct GetHistory;

/// Get room info.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = RoomInfo, remote = "chat::GetRoomInfo")]
struct GetRoomInfo;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RoomInfo {
    name: String,
    message_count: usize,
}

#[handlers]
impl ChatRoom {
    #[handler]
    fn post_message(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        msg: PostMessage,
    ) -> usize {
        state.messages.push(ChatEntry {
            from: msg.from,
            text: msg.text,
        });
        state.messages.len()
    }

    #[handler]
    fn get_history(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        _msg: GetHistory,
    ) -> Vec<String> {
        state
            .messages
            .iter()
            .map(|e| format!("{}: {}", e.from, e.text))
            .collect()
    }

    #[handler]
    fn get_room_info(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ChatRoomState,
        _msg: GetRoomInfo,
    ) -> RoomInfo {
        RoomInfo {
            name: state.room_name.clone(),
            message_count: state.messages.len(),
        }
    }
}

// =============================================================================
// PARTICIPANT ACTOR — represents a user in the system
// =============================================================================

#[derive(Debug)]
struct Participant;

struct ParticipantState {
    username: String,
    inbox: Vec<String>,
}

impl Actor for Participant {
    type State = ParticipantState;

    fn on_actor_terminated(&mut self, state: &mut ParticipantState, terminated: &ActorTerminated) {
        state.inbox.push(format!(
            "[system] watched actor '{}' terminated: {:?}",
            terminated.label, terminated.reason
        ));
    }
}

/// Deliver a notification to the participant's inbox.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "chat::Notify")]
struct Notify {
    text: String,
}

/// Read the participant's inbox.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Vec<String>, remote = "chat::ReadInbox")]
struct ReadInbox;

/// Get the participant's username.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = String, remote = "chat::GetUsername")]
struct GetUsername;

/// Tell this participant to watch another actor.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "chat::WatchActor")]
struct WatchActor {
    label: String,
}

#[handlers]
impl Participant {
    #[handler]
    fn notify(&mut self, _ctx: &ActorContext<Self>, state: &mut ParticipantState, msg: Notify) {
        state.inbox.push(msg.text);
    }

    #[handler]
    fn read_inbox(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ParticipantState,
        _msg: ReadInbox,
    ) -> Vec<String> {
        state.inbox.clone()
    }

    #[handler]
    fn get_username(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ParticipantState,
        _msg: GetUsername,
    ) -> String {
        state.username.clone()
    }

    #[handler]
    fn watch_actor(
        &mut self,
        ctx: &ActorContext<Self>,
        _state: &mut ParticipantState,
        msg: WatchActor,
    ) {
        ctx.watch(&msg.label);
    }
}

// =============================================================================
// MAIN
// =============================================================================

fn main() {
    println!("Run tests with: cargo nextest run -p murmer-examples --bin chat");
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Test: basic chat room — post messages and retrieve history.
    #[tokio::test]
    async fn test_chat_room_basics() {
        let receptionist = Receptionist::new();

        let room = receptionist.start(
            "room/general",
            ChatRoom,
            ChatRoomState {
                room_name: "General".into(),
                messages: vec![],
            },
        );

        room.send(PostMessage {
            from: "alice".into(),
            text: "Hello!".into(),
        })
        .await
        .unwrap();

        room.send(PostMessage {
            from: "bob".into(),
            text: "Hi alice!".into(),
        })
        .await
        .unwrap();

        let history = room.send(GetHistory).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0], "alice: Hello!");
        assert_eq!(history[1], "bob: Hi alice!");

        let info = room.send(GetRoomInfo).await.unwrap();
        assert_eq!(
            info,
            RoomInfo {
                name: "General".into(),
                message_count: 2,
            }
        );
    }

    /// Test: multiple actor types interacting via the receptionist.
    #[tokio::test]
    async fn test_multi_actor_interaction() {
        let receptionist = Receptionist::new();

        // Start a chat room
        let room = receptionist.start(
            "room/dev",
            ChatRoom,
            ChatRoomState {
                room_name: "Dev".into(),
                messages: vec![],
            },
        );

        // Start participants
        let alice = receptionist.start(
            "user/alice",
            Participant,
            ParticipantState {
                username: "alice".into(),
                inbox: vec![],
            },
        );
        let bob = receptionist.start(
            "user/bob",
            Participant,
            ParticipantState {
                username: "bob".into(),
                inbox: vec![],
            },
        );

        // Alice posts to the room
        let count = room
            .send(PostMessage {
                from: "alice".into(),
                text: "Anyone here?".into(),
            })
            .await
            .unwrap();
        assert_eq!(count, 1);

        // Notify bob about the message
        bob.send(Notify {
            text: "New message in #dev from alice".into(),
        })
        .await
        .unwrap();

        // Check bob's inbox
        let inbox = bob.send(ReadInbox).await.unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0], "New message in #dev from alice");

        // Verify alice's inbox is empty
        let alice_inbox = alice.send(ReadInbox).await.unwrap();
        assert!(alice_inbox.is_empty());
    }

    /// Test: reception keys group participants for discovery.
    #[tokio::test]
    async fn test_participant_discovery_via_keys() {
        let receptionist = Receptionist::new();
        let online_key = ReceptionKey::<Participant>::new("online-users");

        // Start 3 participants and check them in
        for name in ["alice", "bob", "charlie"] {
            let label = format!("user/{name}");
            receptionist.start(
                &label,
                Participant,
                ParticipantState {
                    username: name.into(),
                    inbox: vec![],
                },
            );
            receptionist.check_in(&label, online_key.clone());
        }

        // Subscribe to listing — should get all 3 immediately (backfill)
        let mut listing = receptionist.listing(online_key.clone());
        let mut usernames = Vec::new();
        for _ in 0..3 {
            let ep = listing.next().await.unwrap();
            let name = ep.send(GetUsername).await.unwrap();
            usernames.push(name);
        }
        usernames.sort();
        assert_eq!(usernames, vec!["alice", "bob", "charlie"]);

        // No more in the listing right now
        assert!(listing.try_next().is_none());

        // A new user arrives — listing should get the update
        receptionist.start(
            "user/dave",
            Participant,
            ParticipantState {
                username: "dave".into(),
                inbox: vec![],
            },
        );
        receptionist.check_in("user/dave", online_key);

        let dave_ep = listing.next().await.unwrap();
        assert_eq!(dave_ep.send(GetUsername).await.unwrap(), "dave");
    }

    /// Test: router broadcasts notifications to all participants.
    #[tokio::test]
    async fn test_broadcast_via_router() {
        let receptionist = Receptionist::new();

        let alice = receptionist.start(
            "user/alice",
            Participant,
            ParticipantState {
                username: "alice".into(),
                inbox: vec![],
            },
        );
        let bob = receptionist.start(
            "user/bob",
            Participant,
            ParticipantState {
                username: "bob".into(),
                inbox: vec![],
            },
        );

        // Create a broadcast router for notifications
        let notifier = Router::new(vec![alice.clone(), bob.clone()], RoutingStrategy::Broadcast);

        // Broadcast a notification
        let results = notifier
            .broadcast(Notify {
                text: "Server maintenance in 5 minutes".into(),
            })
            .await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_ok()));

        // Both should have the message
        let alice_inbox = alice.send(ReadInbox).await.unwrap();
        let bob_inbox = bob.send(ReadInbox).await.unwrap();
        assert_eq!(alice_inbox.len(), 1);
        assert_eq!(bob_inbox.len(), 1);
        assert_eq!(alice_inbox[0], "Server maintenance in 5 minutes");
    }

    /// Test: actor watches detect termination.
    #[tokio::test]
    async fn test_actor_watch_notification() {
        let receptionist = Receptionist::new();

        // Start a participant that will watch the chat room
        let watcher = receptionist.start(
            "user/watcher",
            Participant,
            ParticipantState {
                username: "watcher".into(),
                inbox: vec![],
            },
        );

        // Start a chat room
        let _room = receptionist.start(
            "room/temp",
            ChatRoom,
            ChatRoomState {
                room_name: "Temporary".into(),
                messages: vec![],
            },
        );

        // Tell the watcher to watch the room
        watcher
            .send(WatchActor {
                label: "room/temp".into(),
            })
            .await
            .unwrap();

        // Stop the chat room
        receptionist.stop("room/temp");

        // Give the supervisor time to process shutdown
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The watcher should have received a termination notification
        let inbox = watcher.send(ReadInbox).await.unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(inbox[0].contains("room/temp"));
        assert!(inbox[0].contains("terminated"));
    }

    /// Test: lifecycle events track all actor registrations.
    #[tokio::test]
    async fn test_lifecycle_event_stream() {
        let receptionist = Receptionist::new();
        let mut events = receptionist.subscribe_events();

        // Start actors of different types
        receptionist.start(
            "room/main",
            ChatRoom,
            ChatRoomState {
                room_name: "Main".into(),
                messages: vec![],
            },
        );
        receptionist.start(
            "user/alice",
            Participant,
            ParticipantState {
                username: "alice".into(),
                inbox: vec![],
            },
        );

        // Should get 2 Registered events
        let ev1 = events.recv().await.unwrap();
        let ev2 = events.recv().await.unwrap();
        assert!(matches!(ev1, ActorEvent::Registered { .. }));
        assert!(matches!(ev2, ActorEvent::Registered { .. }));

        // Stop one
        receptionist.stop("room/main");
        let ev3 = events.recv().await.unwrap();
        assert!(matches!(ev3, ActorEvent::Deregistered { .. }));
    }

    /// Test: ActorRef can be created and resolved for different actor types.
    #[tokio::test]
    async fn test_actor_ref_across_types() {
        let receptionist = Receptionist::new();

        receptionist.start(
            "room/lobby",
            ChatRoom,
            ChatRoomState {
                room_name: "Lobby".into(),
                messages: vec![],
            },
        );
        receptionist.start(
            "user/admin",
            Participant,
            ParticipantState {
                username: "admin".into(),
                inbox: vec![],
            },
        );

        // Resolve refs of different types
        let room_ref = ActorRef::<ChatRoom>::new("room/lobby", "local");
        let user_ref = ActorRef::<Participant>::new("user/admin", "local");

        let room_ep = room_ref.resolve(&receptionist).unwrap();
        let user_ep = user_ref.resolve(&receptionist).unwrap();

        let info = room_ep.send(GetRoomInfo).await.unwrap();
        assert_eq!(info.name, "Lobby");

        let username = user_ep.send(GetUsername).await.unwrap();
        assert_eq!(username, "admin");

        // Wrong type resolution returns None
        let bad_ref = ActorRef::<ChatRoom>::new("user/admin", "local");
        assert!(bad_ref.resolve(&receptionist).is_none());
    }
}
