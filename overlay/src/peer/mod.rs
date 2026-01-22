//! Peer management module (Phase 3, 4, 6).
//!
//! Handles TCP connections, authentication, and peer lifecycle.

pub mod auth;
mod connection;
pub mod framing;
pub mod routing;
mod store;

pub use auth::NoiseKeypair;
pub use connection::{PeerManagerHandle, PeerId};

// TODO: Connection maintenance loop
