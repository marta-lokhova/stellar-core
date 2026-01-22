//! Message routing between peers, SCP relay, and Core IPC.
//!
//! Routes messages based on type:
//! - SCP messages: peers → SCP relay → Core
//! - TX messages: peers → flood coordinator (Phase 5)
//! - Control messages: handled locally

use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::ipc::{Message, MessageType, CoreSender};
use crate::scp::ScpRelayHandle;
use super::connection::{PeerEvent, PeerMessage, PeerId};

/// Message types we expect from peers (simplified for now).
/// In reality, these would be parsed from XDR StellarMessage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerMessageType {
    /// SCP envelope
    Scp,
    /// Transaction
    Transaction,
    /// Transaction advert (hash only)
    TxAdvert,
    /// Transaction demand (request by hash)
    TxDemand,
    /// Peer list exchange
    Peers,
    /// Authentication/hello (Phase 4)
    Auth,
    /// Unknown/other
    Unknown,
}

/// Classify a message by looking at its XDR type tag.
/// 
/// StellarMessage XDR has a union discriminant at offset 0 (4 bytes, big-endian).
pub fn classify_message(data: &[u8]) -> PeerMessageType {
    if data.len() < 4 {
        return PeerMessageType::Unknown;
    }
    
    // Read XDR union discriminant (first 4 bytes, big-endian)
    let discriminant = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    
    // Based on StellarMessage XDR enum values (from stellar-core xdr)
    // See: src/xdr/Stellar-overlay.x
    match discriminant {
        0 => PeerMessageType::Auth,           // ERROR_MSG
        1 => PeerMessageType::Auth,           // AUTH  
        2 => PeerMessageType::Auth,           // DONT_HAVE
        3 => PeerMessageType::Peers,          // GET_PEERS
        4 => PeerMessageType::Peers,          // PEERS
        5 => PeerMessageType::Transaction,    // GET_TX_SET
        6 => PeerMessageType::Transaction,    // TX_SET
        7 => PeerMessageType::Transaction,    // TRANSACTION
        8 => PeerMessageType::Transaction,    // GET_SCP_QUORUM_SET
        9 => PeerMessageType::Scp,            // SCP_QUORUM_SET
        10 => PeerMessageType::Scp,           // SCP_MESSAGE
        11 => PeerMessageType::Transaction,   // GET_SCP_STATE
        12 => PeerMessageType::Auth,          // HELLO
        13 => PeerMessageType::Auth,          // SURVEY_REQUEST
        14 => PeerMessageType::Auth,          // SURVEY_RESPONSE
        15 => PeerMessageType::Auth,          // SEND_MORE
        16 => PeerMessageType::Auth,          // SEND_MORE_EXTENDED
        17 => PeerMessageType::TxAdvert,      // FLOOD_ADVERT
        18 => PeerMessageType::TxDemand,      // FLOOD_DEMAND
        19 => PeerMessageType::Transaction,   // GENERALIZED_TX_SET
        20 => PeerMessageType::Auth,          // TIME_SLICED_SURVEY_REQUEST
        21 => PeerMessageType::Auth,          // TIME_SLICED_SURVEY_RESPONSE  
        22 => PeerMessageType::Auth,          // TIME_SLICED_SURVEY_START_COLLECTING
        23 => PeerMessageType::Auth,          // TIME_SLICED_SURVEY_STOP_COLLECTING
        _ => PeerMessageType::Unknown,
    }
}

/// Routes messages from peers to appropriate handlers.
pub struct MessageRouter {
    /// Channel receiving peer events
    peer_events: mpsc::UnboundedReceiver<PeerEvent>,
    
    /// Handle to SCP relay for SCP messages
    scp_relay: ScpRelayHandle,
    
    /// Handle to Core IPC for forwarding
    #[allow(dead_code)]
    core_sender: CoreSender,
    
    // TODO: Phase 5 - FloodCoordinator handle for TX messages
}

impl MessageRouter {
    pub fn new(
        peer_events: mpsc::UnboundedReceiver<PeerEvent>,
        scp_relay: ScpRelayHandle,
        core_sender: CoreSender,
    ) -> Self {
        Self {
            peer_events,
            scp_relay,
            core_sender,
        }
    }
    
    /// Run the message routing loop.
    pub async fn run(mut self) {
        while let Some(event) = self.peer_events.recv().await {
            match event {
                PeerEvent::Connected { peer_id, addr } => {
                    debug!("Router: peer {} connected from {}", peer_id, addr);
                }
                PeerEvent::Disconnected { peer_id } => {
                    debug!("Router: peer {} disconnected", peer_id);
                }
                PeerEvent::Message(msg) => {
                    self.route_message(msg).await;
                }
            }
        }
    }
    
    /// Route a single message to appropriate handler.
    async fn route_message(&self, msg: PeerMessage) {
        let msg_type = classify_message(&msg.data);
        trace!("Routing {:?} from peer {}", msg_type, msg.peer_id);
        
        match msg_type {
            PeerMessageType::Scp => {
                // Forward to SCP relay
                if let Err(_) = self.scp_relay.received(msg.peer_id, msg.data) {
                    warn!("Failed to send SCP message to relay");
                }
            }
            PeerMessageType::Transaction | PeerMessageType::TxAdvert | PeerMessageType::TxDemand => {
                // TODO: Phase 5 - forward to flood coordinator
                trace!("TX message from peer {} (flood not implemented)", msg.peer_id);
            }
            PeerMessageType::Peers => {
                // TODO: Phase 6 - forward to peer manager
                trace!("PEERS message from peer {} (peer mgmt not implemented)", msg.peer_id);
            }
            PeerMessageType::Auth => {
                // TODO: Phase 4 - handle auth messages
                trace!("Auth message from peer {} (auth not implemented)", msg.peer_id);
            }
            PeerMessageType::Unknown => {
                debug!("Unknown message type from peer {}", msg.peer_id);
            }
        }
    }
}

/// Statistics for message routing (for monitoring)
#[derive(Default, Debug)]
pub struct RouterStats {
    pub scp_messages: u64,
    pub tx_messages: u64,
    pub advert_messages: u64,
    pub demand_messages: u64,
    pub peer_messages: u64,
    pub unknown_messages: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_classify_scp_message() {
        // SCP_MESSAGE has discriminant 10
        let mut data = vec![0u8; 100];
        data[0] = 0;
        data[1] = 0;
        data[2] = 0;
        data[3] = 10;
        
        assert_eq!(classify_message(&data), PeerMessageType::Scp);
    }
    
    #[test]
    fn test_classify_transaction() {
        // TRANSACTION has discriminant 7
        let mut data = vec![0u8; 100];
        data[0] = 0;
        data[1] = 0;
        data[2] = 0;
        data[3] = 7;
        
        assert_eq!(classify_message(&data), PeerMessageType::Transaction);
    }
    
    #[test]
    fn test_classify_flood_advert() {
        // FLOOD_ADVERT has discriminant 17
        let mut data = vec![0u8; 100];
        data[0] = 0;
        data[1] = 0;
        data[2] = 0;
        data[3] = 17;
        
        assert_eq!(classify_message(&data), PeerMessageType::TxAdvert);
    }
    
    #[test]
    fn test_classify_flood_demand() {
        // FLOOD_DEMAND has discriminant 18
        let mut data = vec![0u8; 100];
        data[0] = 0;
        data[1] = 0;
        data[2] = 0;
        data[3] = 18;
        
        assert_eq!(classify_message(&data), PeerMessageType::TxDemand);
    }
    
    #[test]
    fn test_classify_unknown() {
        // Unknown discriminant
        let mut data = vec![0u8; 100];
        data[0] = 0;
        data[1] = 0;
        data[2] = 0;
        data[3] = 255;
        
        assert_eq!(classify_message(&data), PeerMessageType::Unknown);
    }
    
    #[test]
    fn test_classify_short_message() {
        // Too short to have discriminant
        let data = vec![0u8; 2];
        assert_eq!(classify_message(&data), PeerMessageType::Unknown);
    }
    
    #[test]
    fn test_classify_peers_message() {
        // GET_PEERS has discriminant 3
        let mut data = vec![0u8; 100];
        data[0] = 0;
        data[1] = 0;
        data[2] = 0;
        data[3] = 3;
        
        assert_eq!(classify_message(&data), PeerMessageType::Peers);
        
        // PEERS has discriminant 4
        data[3] = 4;
        assert_eq!(classify_message(&data), PeerMessageType::Peers);
    }
    
    #[tokio::test]
    async fn test_router_scp_message_routing() {
        use crate::scp::{ScpRelay, ScpCommand};
        use tokio::sync::mpsc::unbounded_channel;
        
        // Set up SCP relay
        let (scp_tx, mut scp_rx) = unbounded_channel();
        let scp_handle = ScpRelayHandle::new(scp_tx);
        
        // Set up peer events
        let (peer_tx, peer_rx) = unbounded_channel();
        
        // Set up mock Core sender
        let (core_tx, _) = unbounded_channel();
        let core_sender = CoreSender::new(core_tx);
        
        // Create router
        let router = MessageRouter::new(peer_rx, scp_handle, core_sender);
        
        // Spawn router
        let router_handle = tokio::spawn(async move {
            router.run().await;
        });
        
        // Send an SCP message (discriminant 10)
        let mut scp_data = vec![0u8; 50];
        scp_data[3] = 10; // SCP_MESSAGE
        
        peer_tx.send(PeerEvent::Message(PeerMessage {
            peer_id: 42,
            data: scp_data.clone(),
        })).unwrap();
        
        // Should arrive at SCP relay
        let cmd = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            scp_rx.recv()
        ).await.unwrap().unwrap();
        
        match cmd {
            ScpCommand::Received { peer_id, data } => {
                assert_eq!(peer_id, 42);
                assert_eq!(data, scp_data);
            }
            _ => panic!("Expected Received command"),
        }
        
        // Clean up
        drop(peer_tx);
        router_handle.await.unwrap();
    }
}
