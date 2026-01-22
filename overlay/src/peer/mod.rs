//! Peer management module (Phase 3, 4, 6).
//!
//! Handles TCP connections, authentication, and peer lifecycle.

pub mod auth;
mod connection;
pub mod framing;
pub mod routing;
mod store;

pub use auth::{NoiseKeypair, NoiseSession, handshake_initiator, handshake_responder};
pub use connection::{PeerManager, PeerManagerHandle, PeerCommand, PeerEvent, PeerMessage, PeerId};
pub use framing::{read_message, write_message, MAX_MESSAGE_SIZE};
pub use routing::{MessageRouter, PeerMessageType, classify_message};
pub use store::{PeerStore, PeerInfo};

// TODO: Connection maintenance loop
