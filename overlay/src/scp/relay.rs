//! SCP message relay.
//!
//! Handles forwarding SCP envelopes between Core and network peers.
//! This is the latency-critical path - never blocks, never drops.

#![allow(dead_code)]

use blake2::{Blake2b, Digest};
use blake2::digest::consts::U32;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, trace};

use crate::ipc::{CoreSender, Message, MessageType};

/// Hash of an SCP envelope (32 bytes)
pub type ScpHash = [u8; 32];

/// Peer identifier
pub type PeerId = u64;

/// An SCP envelope received from a peer
#[derive(Debug, Clone)]
pub struct ScpEnvelope {
    /// Raw XDR bytes of the envelope
    pub data: Vec<u8>,
    /// Which peer sent this (None if from Core)
    pub from_peer: Option<PeerId>,
}

/// Message sent to the SCP relay task
#[derive(Debug)]
pub enum ScpCommand {
    /// Broadcast this envelope (from Core)
    Broadcast(Vec<u8>),
    /// Received this envelope from a peer
    Received { peer_id: PeerId, data: Vec<u8> },
}

/// SCP relay task state.
///
/// Tracks recently broadcast messages to avoid echoing them back.
pub struct ScpRelay {
    /// Messages we've recently broadcast (hash → timestamp)
    /// Used to avoid re-broadcasting messages we originated
    broadcast_history: HashMap<ScpHash, Instant>,
    
    /// How long to keep entries in broadcast history
    history_ttl: Duration,
    
    /// Channel to broadcast to all peers
    to_peers: broadcast::Sender<ScpEnvelope>,
    
    /// Channel to send to Core
    to_core: CoreSender,
    
    /// Receive commands
    commands: mpsc::UnboundedReceiver<ScpCommand>,
}

impl ScpRelay {
    /// Create a new SCP relay.
    pub fn new(
        to_core: CoreSender,
        commands: mpsc::UnboundedReceiver<ScpCommand>,
    ) -> (Self, broadcast::Receiver<ScpEnvelope>) {
        // Broadcast channel for peers - buffer of 256 should be plenty
        let (to_peers, peers_rx) = broadcast::channel(256);
        
        (
            Self {
                broadcast_history: HashMap::new(),
                history_ttl: Duration::from_secs(60),
                to_peers,
                to_core,
                commands,
            },
            peers_rx,
        )
    }
    
    /// Run the SCP relay task.
    pub async fn run(mut self) {
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(10));
        
        loop {
            tokio::select! {
                // Handle incoming commands
                Some(cmd) = self.commands.recv() => {
                    self.handle_command(cmd);
                }
                
                // Periodic cleanup
                _ = cleanup_interval.tick() => {
                    self.cleanup_history();
                }
            }
        }
    }
    
    /// Handle a command.
    fn handle_command(&mut self, cmd: ScpCommand) {
        match cmd {
            ScpCommand::Broadcast(data) => {
                self.handle_broadcast(data);
            }
            ScpCommand::Received { peer_id, data } => {
                self.handle_received(peer_id, data);
            }
        }
    }
    
    /// Core wants to broadcast an SCP envelope.
    fn handle_broadcast(&mut self, data: Vec<u8>) {
        let hash = self.hash_envelope(&data);
        
        // Record that we originated this
        self.broadcast_history.insert(hash, Instant::now());
        
        // Send to all peers (never blocks - broadcast channel)
        let envelope = ScpEnvelope { data, from_peer: None };
        if self.to_peers.send(envelope).is_err() {
            trace!("No peers subscribed to SCP broadcast");
        } else {
            debug!("SCP broadcast to peers");
        }
    }
    
    /// Received SCP envelope from a peer.
    fn handle_received(&mut self, peer_id: PeerId, data: Vec<u8>) {
        let hash = self.hash_envelope(&data);
        
        // Don't echo back messages we recently broadcast
        if self.broadcast_history.contains_key(&hash) {
            trace!("Skipping SCP message we originated");
            return;
        }
        
        // Forward to Core
        if let Err(e) = self.to_core.send(Message::new(MessageType::ScpReceived, data.clone())) {
            debug!("Failed to send SCP to Core: {}", e);
            return;
        }
        
        // Relay to other peers
        let envelope = ScpEnvelope { 
            data, 
            from_peer: Some(peer_id),
        };
        if self.to_peers.send(envelope).is_err() {
            trace!("No peers subscribed to SCP relay");
        } else {
            debug!("SCP relayed from peer {} to other peers", peer_id);
        }
    }
    
    /// Compute hash of an SCP envelope.
    fn hash_envelope(&self, data: &[u8]) -> ScpHash {
        type Blake2b256 = Blake2b<U32>;
        let mut hasher = Blake2b256::new();
        hasher.update(data);
        hasher.finalize().into()
    }
    
    /// Clean up old entries from broadcast history.
    fn cleanup_history(&mut self) {
        let cutoff = Instant::now() - self.history_ttl;
        self.broadcast_history.retain(|_, ts| *ts > cutoff);
        trace!("SCP broadcast history: {} entries", self.broadcast_history.len());
    }
}

/// Handle to send commands to the SCP relay task.
#[derive(Clone)]
pub struct ScpRelayHandle {
    tx: mpsc::UnboundedSender<ScpCommand>,
}

impl ScpRelayHandle {
    pub fn new(tx: mpsc::UnboundedSender<ScpCommand>) -> Self {
        Self { tx }
    }
    
    /// Send a broadcast command (from Core).
    pub fn broadcast(&self, data: Vec<u8>) -> Result<(), ()> {
        self.tx.send(ScpCommand::Broadcast(data)).map_err(|_| ())
    }
    
    /// Send a received notification (from peer).
    pub fn received(&self, peer_id: PeerId, data: Vec<u8>) -> Result<(), ()> {
        self.tx.send(ScpCommand::Received { peer_id, data }).map_err(|_| ())
    }
}

// Need to expose internal type for test
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use crate::ipc::CoreSender;
    
    #[tokio::test]
    async fn test_broadcast_dedup() {
        // Create mock Core sender
        let (core_tx, _core_rx) = mpsc::unbounded_channel::<Message>();
        let core_sender = CoreSender::new(core_tx);
        
        // Create relay
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (relay, mut peers_rx) = ScpRelay::new(core_sender, cmd_rx);
        
        // Spawn relay task
        let _handle = tokio::spawn(relay.run());
        
        // Broadcast a message
        let data = vec![1, 2, 3, 4];
        cmd_tx.send(ScpCommand::Broadcast(data.clone())).unwrap();
        
        // Should receive on peer channel
        let envelope = peers_rx.recv().await.unwrap();
        assert_eq!(envelope.data, data);
        assert!(envelope.from_peer.is_none());
    }
}
