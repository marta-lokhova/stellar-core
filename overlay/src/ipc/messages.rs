//! IPC message types for communication between Core and Overlay.
//!
//! These types mirror the C++ definitions in src/overlay/IPC.h exactly.
//! Message format: [type:u32][length:u32][payload]

use std::io::{self, Read, Write};

/// IPC message types matching Core's IPCMessageType enum.
///
/// Value ranges:
/// - 1-99: Core → Overlay messages  
/// - 100-199: Overlay → Core messages
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    // ═══ Core → Overlay (Critical Path) ═══
    
    /// Broadcast this SCP envelope to all peers
    BroadcastScp = 1,
    
    /// Request a TX set hash for nomination
    RequestNominationHash = 2,
    
    /// Request current SCP state (peer asked via GET_SCP_STATE)
    RequestScpState = 3,
    
    // ═══ Core → Overlay (Non-Critical) ═══
    
    /// Ledger closed, here's the new state
    LedgerClosed = 4,
    
    /// We externalized this hash, drop related data
    TxSetExternalized = 5,
    
    /// Response: here's the SCP state you requested
    ScpStateResponse = 6,
    
    /// Shutdown the overlay process
    Shutdown = 7,
    
    /// Configure peer addresses to connect to
    /// Payload: JSON { "known_peers": [...], "preferred_peers": [...], "listen_port": u16 }
    SetPeerConfig = 8,
    
    /// Connect to a specific peer address
    /// Payload: "ip:port" string
    ConnectToPeer = 9,
    
    /// Submit a transaction for flooding
    /// Payload: [fee:i64][numOps:u32][txEnvelope XDR...]
    SubmitTx = 10,
    
    /// Request a TX set by hash
    /// Payload: [hash:32]
    RequestTxSet = 11,

    // ═══ Overlay → Core (Critical Path) ═══
    
    /// Received SCP envelope from network
    ScpReceived = 100,
    
    /// Response to nomination request
    NominationHash = 101,
    
    /// Peer requested SCP state
    PeerRequestsScpState = 102,
    
    // ═══ Overlay → Core (Non-Critical) ═══
    
    /// Here's a TX set you might need soon (optimistic push)
    TxSetAvailable = 103,
    
    /// Here's a quorum set referenced in SCP
    QuorumSetAvailable = 104,
    
    /// Peer connected
    PeerConnected = 105,
    
    /// Peer disconnected  
    PeerDisconnected = 106,
}

impl MessageType {
    /// Check if this is a Core → Overlay message
    pub fn is_core_to_overlay(&self) -> bool {
        (*self as u32) < 100
    }
    
    /// Check if this is an Overlay → Core message
    pub fn is_overlay_to_core(&self) -> bool {
        (*self as u32) >= 100
    }
}

impl TryFrom<u32> for MessageType {
    type Error = InvalidMessageType;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MessageType::BroadcastScp),
            2 => Ok(MessageType::RequestNominationHash),
            3 => Ok(MessageType::RequestScpState),
            4 => Ok(MessageType::LedgerClosed),
            5 => Ok(MessageType::TxSetExternalized),
            6 => Ok(MessageType::ScpStateResponse),
            7 => Ok(MessageType::Shutdown),
            8 => Ok(MessageType::SetPeerConfig),
            9 => Ok(MessageType::ConnectToPeer),
            10 => Ok(MessageType::SubmitTx),
            11 => Ok(MessageType::RequestTxSet),
            100 => Ok(MessageType::ScpReceived),
            101 => Ok(MessageType::NominationHash),
            102 => Ok(MessageType::PeerRequestsScpState),
            103 => Ok(MessageType::TxSetAvailable),
            104 => Ok(MessageType::QuorumSetAvailable),
            105 => Ok(MessageType::PeerConnected),
            106 => Ok(MessageType::PeerDisconnected),
            _ => Err(InvalidMessageType(value)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InvalidMessageType(pub u32);

impl std::fmt::Display for InvalidMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid IPC message type: {}", self.0)
    }
}

impl std::error::Error for InvalidMessageType {}

/// A single IPC message.
#[derive(Debug, Clone)]
pub struct Message {
    pub msg_type: MessageType,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(msg_type: MessageType, payload: Vec<u8>) -> Self {
        Self { msg_type, payload }
    }
    
    pub fn empty(msg_type: MessageType) -> Self {
        Self { msg_type, payload: Vec::new() }
    }
}

/// Maximum payload size (16 MB) - sanity check to prevent OOM
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// Header size: 4 bytes type + 4 bytes length
const HEADER_SIZE: usize = 8;

/// Synchronous message reader/writer for Unix sockets.
/// 
/// Note: We use blocking I/O wrapped in tokio::task::spawn_blocking
/// because Unix domain sockets with Tokio can be tricky on some platforms.
pub struct MessageCodec;

impl MessageCodec {
    /// Read a message from a stream (blocking).
    pub fn read<R: Read>(reader: &mut R) -> io::Result<Message> {
        // Read header
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header)?;
        
        let msg_type_raw = u32::from_ne_bytes(header[0..4].try_into().unwrap());
        let payload_len = u32::from_ne_bytes(header[4..8].try_into().unwrap()) as usize;
        
        // Sanity check
        if payload_len > MAX_PAYLOAD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("payload too large: {} bytes", payload_len),
            ));
        }
        
        // Parse message type
        let msg_type = MessageType::try_from(msg_type_raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        // Read payload
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            reader.read_exact(&mut payload)?;
        }
        
        Ok(Message { msg_type, payload })
    }
    
    /// Write a message to a stream (blocking).
    pub fn write<W: Write>(writer: &mut W, msg: &Message) -> io::Result<()> {
        // Build header
        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&(msg.msg_type as u32).to_ne_bytes());
        header[4..8].copy_from_slice(&(msg.payload.len() as u32).to_ne_bytes());
        
        // Write header + payload
        writer.write_all(&header)?;
        if !msg.payload.is_empty() {
            writer.write_all(&msg.payload)?;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_roundtrip() {
        let msg = Message::new(MessageType::BroadcastScp, vec![1, 2, 3, 4]);
        
        let mut buf = Vec::new();
        MessageCodec::write(&mut buf, &msg).unwrap();
        
        let mut cursor = Cursor::new(buf);
        let decoded = MessageCodec::read(&mut cursor).unwrap();
        
        assert_eq!(decoded.msg_type, MessageType::BroadcastScp);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4]);
    }
    
    #[test]
    fn test_empty_payload() {
        let msg = Message::empty(MessageType::Shutdown);
        
        let mut buf = Vec::new();
        MessageCodec::write(&mut buf, &msg).unwrap();
        
        let mut cursor = Cursor::new(buf);
        let decoded = MessageCodec::read(&mut cursor).unwrap();
        
        assert_eq!(decoded.msg_type, MessageType::Shutdown);
        assert!(decoded.payload.is_empty());
    }
    
    #[test]
    fn test_message_type_classification() {
        assert!(MessageType::BroadcastScp.is_core_to_overlay());
        assert!(!MessageType::BroadcastScp.is_overlay_to_core());
        
        assert!(MessageType::ScpReceived.is_overlay_to_core());
        assert!(!MessageType::ScpReceived.is_core_to_overlay());
    }
}
