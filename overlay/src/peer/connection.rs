//! Peer connection management.
//!
//! Handles TCP connections to peers, spawning read/write tasks for each.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, warn};

use super::framing::MAX_MESSAGE_SIZE;

/// Unique identifier for a peer connection
pub type PeerId = u64;

/// Message received from a peer
#[derive(Debug, Clone)]
pub struct PeerMessage {
    pub peer_id: PeerId,
    pub data: Vec<u8>,
}

/// Command to send to a peer
#[derive(Debug, Clone)]
pub enum PeerCommand {
    /// Send data to a specific peer
    Send { peer_id: PeerId, data: Vec<u8> },
    /// Broadcast data to all peers
    Broadcast { data: Vec<u8> },
    /// Disconnect a peer
    Disconnect { peer_id: PeerId },
    /// Connect to a peer address
    Connect { addr: SocketAddr },
}

/// Event from peer manager
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// New peer connected
    Connected { peer_id: PeerId, addr: SocketAddr },
    /// Peer disconnected
    Disconnected { peer_id: PeerId },
    /// Message received from peer
    Message(PeerMessage),
}

/// Handle to a peer connection (for sending)
struct PeerHandle {
    addr: SocketAddr,
    tx: mpsc::Sender<Vec<u8>>,
}

/// Manages all peer connections.
pub struct PeerManager {
    /// Next peer ID to assign
    next_peer_id: PeerId,
    
    /// Connected peers
    peers: Arc<RwLock<HashMap<PeerId, PeerHandle>>>,
    
    /// Channel to receive commands
    commands: mpsc::Receiver<PeerCommand>,
    
    /// Channel to send events
    events: mpsc::UnboundedSender<PeerEvent>,
    
    /// Broadcast channel for messages to all peers
    broadcast_tx: broadcast::Sender<Vec<u8>>,
}

impl PeerManager {
    /// Create a new peer manager.
    pub fn new(
        commands: mpsc::Receiver<PeerCommand>,
        events: mpsc::UnboundedSender<PeerEvent>,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        
        Self {
            next_peer_id: 1,
            peers: Arc::new(RwLock::new(HashMap::new())),
            commands,
            events,
            broadcast_tx,
        }
    }
    
    /// Start listening for connections and run the event loop.
    pub async fn run(mut self, listen_addr: SocketAddr) -> io::Result<()> {
        let listener = TcpListener::bind(listen_addr).await?;
        info!("Listening for peers on {}", listen_addr);
        
        loop {
            tokio::select! {
                // Accept new connections
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            self.handle_new_connection(stream, addr).await;
                        }
                        Err(e) => {
                            error!("Accept failed: {}", e);
                        }
                    }
                }
                
                // Handle commands
                Some(cmd) = self.commands.recv() => {
                    self.handle_command(cmd).await;
                }
            }
        }
    }
    
    /// Handle a new inbound connection.
    async fn handle_new_connection(&mut self, stream: TcpStream, addr: SocketAddr) {
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;
        
        info!("New peer connection: {} (id={})", addr, peer_id);
        
        // Create channel for sending to this peer
        let (tx, rx) = mpsc::channel(100);
        
        // Store peer handle
        {
            let mut peers = self.peers.write().await;
            peers.insert(peer_id, PeerHandle { addr, tx });
        }
        
        // Notify of connection
        let _ = self.events.send(PeerEvent::Connected { peer_id, addr });
        
        // Subscribe to broadcasts
        let broadcast_rx = self.broadcast_tx.subscribe();
        
        // Spawn peer tasks
        let peers = Arc::clone(&self.peers);
        let events = self.events.clone();
        
        tokio::spawn(async move {
            run_peer_connection(peer_id, stream, rx, broadcast_rx, events.clone()).await;
            
            // Clean up on disconnect
            {
                let mut peers = peers.write().await;
                peers.remove(&peer_id);
            }
            let _ = events.send(PeerEvent::Disconnected { peer_id });
            info!("Peer {} disconnected", peer_id);
        });
    }
    
    /// Handle a command.
    async fn handle_command(&mut self, cmd: PeerCommand) {
        match cmd {
            PeerCommand::Send { peer_id, data } => {
                let peers = self.peers.read().await;
                if let Some(peer) = peers.get(&peer_id) {
                    let _ = peer.tx.send(data).await;
                }
            }
            PeerCommand::Broadcast { data } => {
                let _ = self.broadcast_tx.send(data);
            }
            PeerCommand::Disconnect { peer_id } => {
                let mut peers = self.peers.write().await;
                peers.remove(&peer_id);
            }
            PeerCommand::Connect { addr } => {
                self.connect_to_peer(addr).await;
            }
        }
    }
    
    /// Connect to a peer at the given address.
    async fn connect_to_peer(&mut self, addr: SocketAddr) {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                info!("Connected to peer at {}", addr);
                self.handle_new_connection(stream, addr).await;
            }
            Err(e) => {
                warn!("Failed to connect to {}: {}", addr, e);
            }
        }
    }
}

/// Run read/write tasks for a single peer connection.
async fn run_peer_connection(
    peer_id: PeerId,
    stream: TcpStream,
    mut direct_rx: mpsc::Receiver<Vec<u8>>,
    mut broadcast_rx: broadcast::Receiver<Vec<u8>>,
    events: mpsc::UnboundedSender<PeerEvent>,
) {
    let (mut reader, mut writer) = stream.into_split();
    
    // Spawn writer task
    let write_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                // Direct message to this peer
                Some(data) = direct_rx.recv() => {
                    if let Err(e) = write_framed(&mut writer, &data).await {
                        debug!("Write error for peer {}: {}", peer_id, e);
                        break;
                    }
                }
                // Broadcast message
                Ok(data) = broadcast_rx.recv() => {
                    if let Err(e) = write_framed(&mut writer, &data).await {
                        debug!("Write error for peer {}: {}", peer_id, e);
                        break;
                    }
                }
                else => break,
            }
        }
    });
    
    // Reader loop (in current task)
    loop {
        match read_framed(&mut reader).await {
            Ok(data) => {
                let msg = PeerMessage { peer_id, data };
                if events.send(PeerEvent::Message(msg)).is_err() {
                    break;
                }
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    debug!("Read error for peer {}: {}", peer_id, e);
                }
                break;
            }
        }
    }
    
    // Stop writer task
    write_handle.abort();
}

/// Read a length-prefixed message from an async stream.
async fn read_framed<R: AsyncReadExt + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    // Read 4-byte header
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    
    // Parse length (big-endian, clear MSB)
    let length = u32::from_be_bytes(header) & 0x7FFF_FFFF;
    let length = length as usize;
    
    if length > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {}", length),
        ));
    }
    
    // Read payload
    let mut payload = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut payload).await?;
    }
    
    Ok(payload)
}

/// Write a length-prefixed message to an async stream.
async fn write_framed<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    // Write header (big-endian length with MSB set)
    let length = (data.len() as u32) | 0x8000_0000;
    writer.write_all(&length.to_be_bytes()).await?;
    
    // Write payload
    if !data.is_empty() {
        writer.write_all(data).await?;
    }
    
    writer.flush().await?;
    Ok(())
}

/// Handle for controlling the peer manager
pub struct PeerManagerHandle {
    commands: mpsc::Sender<PeerCommand>,
}

impl PeerManagerHandle {
    pub fn new(commands: mpsc::Sender<PeerCommand>) -> Self {
        Self { commands }
    }
    
    /// Send data to a specific peer
    pub async fn send(&self, peer_id: PeerId, data: Vec<u8>) -> Result<(), ()> {
        self.commands
            .send(PeerCommand::Send { peer_id, data })
            .await
            .map_err(|_| ())
    }
    
    /// Broadcast data to all peers
    pub async fn broadcast(&self, data: Vec<u8>) -> Result<(), ()> {
        self.commands
            .send(PeerCommand::Broadcast { data })
            .await
            .map_err(|_| ())
    }
    
    /// Disconnect a peer
    pub async fn disconnect(&self, peer_id: PeerId) -> Result<(), ()> {
        self.commands
            .send(PeerCommand::Disconnect { peer_id })
            .await
            .map_err(|_| ())
    }
    
    /// Connect to a peer at the given address
    pub async fn connect(&self, addr: SocketAddr) -> Result<(), ()> {
        self.commands
            .send(PeerCommand::Connect { addr })
            .await
            .map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;
    use tokio::time::{timeout, Duration};
    
    #[tokio::test]
    async fn test_peer_connect_disconnect() {
        // Start peer manager
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        
        let manager = PeerManager::new(cmd_rx, event_tx);
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        
        // Get actual bound address
        let listener = TcpListener::bind(listen_addr).await.unwrap();
        let actual_addr = listener.local_addr().unwrap();
        drop(listener);
        
        // Spawn manager
        tokio::spawn(async move {
            manager.run(actual_addr).await.unwrap();
        });
        
        // Give manager time to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Connect a client
        let _client = TcpStream::connect(actual_addr).await.unwrap();
        
        // Should receive Connected event
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        match event {
            PeerEvent::Connected { peer_id, addr } => {
                assert_eq!(peer_id, 1);
                assert_ne!(addr.port(), 0);
            }
            _ => panic!("Expected Connected event"),
        }
    }
    
    #[tokio::test]
    async fn test_peer_send_receive() {
        // Start peer manager
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        
        let manager = PeerManager::new(cmd_rx, event_tx);
        
        // Bind to random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();
        drop(listener);
        
        // Spawn manager
        tokio::spawn(async move {
            manager.run(actual_addr).await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Connect a client
        let mut client = TcpStream::connect(actual_addr).await.unwrap();
        
        // Wait for Connected event
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        let peer_id = match event {
            PeerEvent::Connected { peer_id, .. } => peer_id,
            _ => panic!("Expected Connected event"),
        };
        
        // Client sends a message
        let test_data = b"Hello, overlay!";
        write_framed(&mut client, test_data).await.unwrap();
        
        // Should receive Message event
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        match event {
            PeerEvent::Message(msg) => {
                assert_eq!(msg.peer_id, peer_id);
                assert_eq!(msg.data, test_data);
            }
            _ => panic!("Expected Message event"),
        }
    }
    
    #[tokio::test]
    async fn test_broadcast_to_peers() {
        // Start peer manager
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        
        let manager = PeerManager::new(cmd_rx, event_tx);
        let handle = PeerManagerHandle::new(cmd_tx);
        
        // Bind to random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();
        drop(listener);
        
        // Spawn manager
        tokio::spawn(async move {
            manager.run(actual_addr).await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Connect two clients
        let mut client1 = TcpStream::connect(actual_addr).await.unwrap();
        let mut client2 = TcpStream::connect(actual_addr).await.unwrap();
        
        // Wait for both Connected events
        for _ in 0..2 {
            let _ = timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        
        // Broadcast a message
        let test_data = b"Broadcast test";
        handle.broadcast(test_data.to_vec()).await.unwrap();
        
        // Both clients should receive it
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let msg1 = timeout(Duration::from_millis(500), read_framed(&mut client1))
            .await
            .unwrap()
            .unwrap();
        let msg2 = timeout(Duration::from_millis(500), read_framed(&mut client2))
            .await
            .unwrap()
            .unwrap();
        
        assert_eq!(msg1, test_data);
        assert_eq!(msg2, test_data);
    }
    
    #[tokio::test]
    async fn test_peer_disconnect_event() {
        // Start peer manager
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        
        let manager = PeerManager::new(cmd_rx, event_tx);
        
        // Bind to random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();
        drop(listener);
        
        // Spawn manager
        tokio::spawn(async move {
            manager.run(actual_addr).await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Connect and immediately disconnect
        {
            let _client = TcpStream::connect(actual_addr).await.unwrap();
            // Wait for Connected
            let _ = timeout(Duration::from_secs(1), event_rx.recv()).await;
        } // client dropped here
        
        // Should receive Disconnected event
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        match event {
            PeerEvent::Disconnected { peer_id } => {
                assert_eq!(peer_id, 1);
            }
            _ => panic!("Expected Disconnected event, got {:?}", event),
        }
    }
    
    #[tokio::test]
    async fn test_outbound_connection() {
        // Start a mock peer server
        let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        
        // Start peer manager
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        
        let manager = PeerManager::new(cmd_rx, event_tx);
        let handle = PeerManagerHandle::new(cmd_tx);
        
        // Bind manager to random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let manager_addr = listener.local_addr().unwrap();
        drop(listener);
        
        // Spawn manager
        tokio::spawn(async move {
            manager.run(manager_addr).await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Spawn mock peer acceptor
        let mock_handle = tokio::spawn(async move {
            let (mut stream, _) = mock_listener.accept().await.unwrap();
            
            // Send a message to verify connection works
            let test_msg = b"Hello from mock peer";
            write_framed(&mut stream, test_msg).await.unwrap();
            
            // Keep connection alive
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        
        // Command overlay to connect to mock peer
        handle.connect(mock_addr).await.unwrap();
        
        // Should receive Connected event
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        match event {
            PeerEvent::Connected { peer_id, addr } => {
                assert_eq!(peer_id, 1);
                assert_eq!(addr, mock_addr);
            }
            _ => panic!("Expected Connected event, got {:?}", event),
        }
        
        // Should receive the message from mock peer
        let event = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        match event {
            PeerEvent::Message(msg) => {
                assert_eq!(msg.peer_id, 1);
                assert_eq!(msg.data, b"Hello from mock peer");
            }
            _ => panic!("Expected Message event, got {:?}", event),
        }
        
        mock_handle.abort();
    }
}
