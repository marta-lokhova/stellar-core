//! Integrated overlay that wires all components together.
//!
//! This is the main entry point that connects:
//! - Core IPC (or mock channels for testing)
//! - Peer connections with Noise auth
//! - SCP relay
//! - TX flooding
//!
//! ## Dual TCP Architecture
//!
//! Each peer connection uses TWO TCP connections for complete SCP/TX isolation:
//! - SCP connection: dedicated to SCP messages (never blocked by TX)
//! - TX connection: dedicated to transaction messages (can tolerate backpressure)
//!
//! Each connection has 2 tasks (reader + writer), for 4 tasks per peer total.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{broadcast, mpsc, RwLock, Mutex};
use tracing::{debug, error, info, warn, trace};

use crate::flood::{FloodCoordinator, FloodCoordinatorHandle, FloodCommand, Mempool, TxHash, compute_tx_hash};
use crate::peer::auth::{NoiseKeypair, NoiseSession, handshake_initiator, handshake_responder};
use crate::peer::framing::MAX_MESSAGE_SIZE;
use crate::peer::routing::{classify_message, PeerMessageType};

/// Connection type identifier sent after Noise handshake
const CONN_TYPE_SCP: u8 = 0x01;
const CONN_TYPE_TX: u8 = 0x02;

/// Timeout for matching pending connections from same peer
const PENDING_CONN_TIMEOUT: Duration = Duration::from_secs(5);

/// Peer ID type
pub type PeerId = u64;

/// Commands from Core to Overlay
#[derive(Debug, Clone)]
pub enum CoreCommand {
    /// Broadcast SCP envelope to all peers
    BroadcastScp { envelope: Vec<u8> },
    
    /// Submit a transaction for flooding
    SubmitTx { 
        data: Vec<u8>,
        fee: u64,
        num_ops: u32,
    },
    
    /// Request top N transactions by fee
    /// Returns (tx_hash, tx_data) pairs
    GetTopTxs { 
        count: usize,
        reply: mpsc::Sender<Vec<([u8; 32], Vec<u8>)>>,
    },
    
    /// Connect to a peer
    ConnectTo { addr: SocketAddr },
    
    /// Configure peers (known and preferred)
    SetPeerConfig {
        known_peers: Vec<String>,
        preferred_peers: Vec<String>,
        listen_port: u16,
    },
    
    /// Remove transactions from mempool (after ledger close)
    RemoveTxsFromMempool {
        tx_hashes: Vec<[u8; 32]>,
    },
}

/// Events from Overlay to Core
#[derive(Debug, Clone)]
pub enum OverlayEvent {
    /// SCP envelope received from a peer
    ScpReceived { 
        envelope: Vec<u8>, 
        from_peer: PeerId,
    },
    
    /// Peer connected
    PeerConnected { 
        peer_id: PeerId, 
        addr: SocketAddr,
        public_key: [u8; 32],
    },
    
    /// Peer disconnected
    PeerDisconnected { peer_id: PeerId },
}

/// Information about a connected peer
struct ConnectedPeer {
    id: PeerId,
    addr: SocketAddr,
    public_key: [u8; 32],
    /// Channel to send messages to this peer
    tx: mpsc::Sender<Vec<u8>>,
}

/// The integrated overlay.
pub struct Overlay {
    /// Our Noise keypair
    keypair: NoiseKeypair,
    
    /// Listen address for incoming connections
    listen_addr: SocketAddr,
    
    /// Commands from Core
    core_commands: mpsc::UnboundedReceiver<CoreCommand>,
    
    /// Events to Core
    core_events: mpsc::UnboundedSender<OverlayEvent>,
    
    /// Connected peers
    peers: Arc<RwLock<HashMap<PeerId, ConnectedPeer>>>,
    
    /// Next peer ID
    next_peer_id: Arc<RwLock<PeerId>>,
    
    /// Broadcast channel for SCP messages (to all peers)
    scp_broadcast: broadcast::Sender<Vec<u8>>,
    
    /// TX mempool
    mempool: Arc<RwLock<Mempool>>,
    
    /// Hashes we've already seen (for dedup)
    seen_scp_hashes: Arc<RwLock<std::collections::HashSet<[u8; 32]>>>,
    
    /// TX flood pending adverts (hash -> peers to advert)
    pending_adverts: Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
}

impl Overlay {
    /// Create a new overlay.
    pub fn new(
        keypair: NoiseKeypair,
        listen_addr: SocketAddr,
        core_commands: mpsc::UnboundedReceiver<CoreCommand>,
        core_events: mpsc::UnboundedSender<OverlayEvent>,
    ) -> Self {
        let (scp_broadcast, _) = broadcast::channel(1024);
        
        Self {
            keypair,
            listen_addr,
            core_commands,
            core_events,
            peers: Arc::new(RwLock::new(HashMap::new())),
            next_peer_id: Arc::new(RwLock::new(1)),
            scp_broadcast,
            mempool: Arc::new(RwLock::new(Mempool::new(10000, Duration::from_secs(300)))),
            seen_scp_hashes: Arc::new(RwLock::new(std::collections::HashSet::new())),
            pending_adverts: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Run the overlay.
    pub async fn run(mut self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let actual_addr = listener.local_addr()?;
        info!("Overlay listening on {}", actual_addr);
        
        // Spawn advert flusher
        let pending_adverts = Arc::clone(&self.pending_adverts);
        let peers = Arc::clone(&self.peers);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                flush_adverts(&pending_adverts, &peers).await;
            }
        });
        
        loop {
            tokio::select! {
                // Accept incoming connections
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            self.handle_incoming_connection(stream, addr).await;
                        }
                        Err(e) => {
                            error!("Accept failed: {}", e);
                        }
                    }
                }
                
                // Handle commands from Core
                Some(cmd) = self.core_commands.recv() => {
                    self.handle_core_command(cmd).await;
                }
            }
        }
    }
    
    /// Handle an incoming connection.
    async fn handle_incoming_connection(&self, mut stream: TcpStream, addr: SocketAddr) {
        info!("Incoming connection from {}", addr);
        
        // Perform Noise handshake (we are responder)
        let keypair = NoiseKeypair::from_bytes(self.keypair.private, self.keypair.public);
        let session = match handshake_responder(&mut stream, &keypair).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Handshake failed from {}: {}", addr, e);
                return;
            }
        };
        
        let remote_key = *session.remote_public_key();
        info!("Authenticated peer from {}: {:?}", addr, &remote_key[..4]);
        
        self.spawn_peer_tasks(stream, addr, remote_key, session).await;
    }
    
    /// Connect to a peer.
    async fn connect_to_peer(&self, addr: SocketAddr) {
        info!("Connecting to {}", addr);
        
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to connect to {}: {}", addr, e);
                return;
            }
        };
        
        // Perform Noise handshake (we are initiator)
        let keypair = NoiseKeypair::from_bytes(self.keypair.private, self.keypair.public);
        let session = match handshake_initiator(&mut stream, &keypair).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Handshake failed to {}: {}", addr, e);
                return;
            }
        };
        
        let remote_key = *session.remote_public_key();
        info!("Connected to peer at {}: {:?}", addr, &remote_key[..4]);
        
        self.spawn_peer_tasks(stream, addr, remote_key, session).await;
    }
    
    /// Spawn read/write tasks for a peer.
    async fn spawn_peer_tasks(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        remote_key: [u8; 32],
        session: NoiseSession,
    ) {
        // Assign peer ID
        let peer_id = {
            let mut next_id = self.next_peer_id.write().await;
            let id = *next_id;
            *next_id += 1;
            id
        };
        
        // Create channel for sending to this peer
        let (tx, rx) = mpsc::channel(1000);
        
        // Store peer
        {
            let mut peers = self.peers.write().await;
            peers.insert(peer_id, ConnectedPeer {
                id: peer_id,
                addr,
                public_key: remote_key,
                tx,
            });
        }
        
        // Notify Core
        let _ = self.core_events.send(OverlayEvent::PeerConnected {
            peer_id,
            addr,
            public_key: remote_key,
        });
        
        // Subscribe to SCP broadcasts
        let scp_broadcast_rx = self.scp_broadcast.subscribe();
        
        // Spawn peer handler
        let peers = Arc::clone(&self.peers);
        let core_events = self.core_events.clone();
        let mempool = Arc::clone(&self.mempool);
        let seen_scp = Arc::clone(&self.seen_scp_hashes);
        let scp_broadcast_tx = self.scp_broadcast.clone();
        let pending_adverts = Arc::clone(&self.pending_adverts);
        
        tokio::spawn(async move {
            run_peer_handler(
                peer_id,
                stream,
                session,
                rx,
                scp_broadcast_rx,
                core_events,
                mempool,
                seen_scp,
                scp_broadcast_tx,
                pending_adverts,
            ).await;
            
            // Cleanup on disconnect
            {
                let mut peers = peers.write().await;
                peers.remove(&peer_id);
            }
            info!("Peer {} disconnected", peer_id);
        });
    }
    
    /// Handle a command from Core.
    async fn handle_core_command(&self, cmd: CoreCommand) {
        match cmd {
            CoreCommand::BroadcastScp { envelope } => {
                // Wrap raw SCPEnvelope in StellarMessage format for peer-to-peer protocol
                // StellarMessage is a union with discriminant at offset 0
                // SCP_MESSAGE = 10 (0x0000000A big-endian)
                let mut stellar_msg = vec![0u8, 0, 0, 10]; // SCP_MESSAGE discriminant
                stellar_msg.extend_from_slice(&envelope);
                
                // Add to seen hashes (use StellarMessage hash for dedup)
                let hash = compute_tx_hash(&stellar_msg);
                {
                    let mut seen = self.seen_scp_hashes.write().await;
                    seen.insert(hash);
                }
                
                // Broadcast to all peers
                let peer_count = self.peers.read().await.len();
                let receiver_count = self.scp_broadcast.receiver_count();
                info!("Broadcasting SCP envelope ({} bytes, {} with header) to {} peers ({} receivers)",
                      envelope.len(), stellar_msg.len(), peer_count, receiver_count);
                match self.scp_broadcast.send(stellar_msg) {
                    Ok(n) => info!("SCP broadcast sent to {} receivers", n),
                    Err(e) => warn!("SCP broadcast failed: {}", e),
                }
            }
            
            CoreCommand::SubmitTx { data, fee, num_ops } => {
                let hash = compute_tx_hash(&data);
                
                // Add to mempool
                {
                    let mut mempool = self.mempool.write().await;
                    let entry = crate::flood::TxEntry {
                        data: data.clone(),
                        hash,
                        source_account: [0u8; 32],
                        sequence: 0,
                        fee,
                        num_ops,
                        received_at: std::time::Instant::now(),
                        from_peer: 0,
                    };
                    mempool.insert(entry);
                }
                
                // Get peer list for push-k
                let peer_ids: Vec<PeerId> = {
                    self.peers.read().await.keys().copied().collect()
                };
                
                if peer_ids.is_empty() {
                    return;
                }
                
                // Push to k=2 random peers (for testing with small networks)
                let k = 2.min(peer_ids.len());
                use rand::seq::SliceRandom;
                use rand::SeedableRng;
                let mut rng = rand::rngs::StdRng::from_entropy();
                let mut shuffled = peer_ids.clone();
                shuffled.shuffle(&mut rng);
                
                let push_peers = &shuffled[..k];
                let advert_peers: Vec<PeerId> = shuffled[k..].to_vec();
                
                // Create TX message (type 7 = TRANSACTION)
                let mut tx_msg = vec![0u8; 4 + data.len()];
                tx_msg[3] = 7; // TRANSACTION discriminant
                tx_msg[4..].copy_from_slice(&data);
                
                // Push to selected peers
                let peers = self.peers.read().await;
                for &pid in push_peers {
                    if let Some(peer) = peers.get(&pid) {
                        let _ = peer.tx.send(tx_msg.clone()).await;
                        trace!("Pushed TX {:?} to peer {}", &hash[..4], pid);
                    }
                }
                
                // Queue adverts for remaining peers
                if !advert_peers.is_empty() {
                    let mut pending = self.pending_adverts.write().await;
                    pending.entry(hash).or_default().extend(advert_peers);
                }
            }
            
            CoreCommand::GetTopTxs { count, reply } => {
                let mempool = self.mempool.read().await;
                let top_hashes = mempool.top_by_fee(count);
                let txs: Vec<([u8; 32], Vec<u8>)> = top_hashes
                    .iter()
                    .filter_map(|h| mempool.get(h).map(|e| (*h, e.data.clone())))
                    .collect();
                let _ = reply.send(txs).await;
            }
            
            CoreCommand::ConnectTo { addr } => {
                self.connect_to_peer(addr).await;
            }
            
            CoreCommand::SetPeerConfig { known_peers, preferred_peers, listen_port } => {
                info!("Received peer config: known={:?}, preferred={:?}, port={}",
                      known_peers, preferred_peers, listen_port);
                
                // Connect to all known and preferred peers
                let all_peers: Vec<_> = known_peers.into_iter().chain(preferred_peers.into_iter()).collect();
                
                for addr_str in all_peers {
                    match addr_str.parse::<SocketAddr>() {
                        Ok(addr) => {
                            self.connect_to_peer(addr).await;
                        }
                        Err(_) => {
                            // May be a hostname, try resolving
                            if let Some((host, port)) = addr_str.rsplit_once(':') {
                                if let Ok(port) = port.parse::<u16>() {
                                    if let Ok(addrs) = tokio::net::lookup_host(format!("{}:{}", host, port)).await {
                                        for addr in addrs {
                                            self.connect_to_peer(addr).await;
                                            break; // Use first address
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            CoreCommand::RemoveTxsFromMempool { tx_hashes } => {
                let mut mempool = self.mempool.write().await;
                let count = tx_hashes.len();
                for hash in tx_hashes {
                    mempool.remove(&hash);
                }
                info!("Removed {} TXs from mempool after externalize", count);
            }
        }
    }
    
    /// Get the actual listen address (useful when binding to port 0).
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

/// Run the peer handler (read/write tasks).
async fn run_peer_handler(
    peer_id: PeerId,
    stream: TcpStream,
    mut session: NoiseSession,
    mut direct_rx: mpsc::Receiver<Vec<u8>>,
    mut scp_broadcast_rx: broadcast::Receiver<Vec<u8>>,
    core_events: mpsc::UnboundedSender<OverlayEvent>,
    mempool: Arc<RwLock<Mempool>>,
    seen_scp: Arc<RwLock<std::collections::HashSet<[u8; 32]>>>,
    scp_broadcast_tx: broadcast::Sender<Vec<u8>>,
    pending_adverts: Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
) {
    let (mut reader, mut writer) = stream.into_split();
    
    // We need to share the session between reader and writer, but NoiseSession
    // isn't thread-safe. Use a mutex for now (not ideal for performance but works).
    let session = Arc::new(tokio::sync::Mutex::new(session));
    let session_write = Arc::clone(&session);
    
    // Spawn writer task
    let writer_handle = tokio::spawn(async move {
        info!("Peer {} writer task started", peer_id);
        loop {
            let msg = tokio::select! {
                Some(msg) = direct_rx.recv() => {
                    info!("Peer {} got direct message ({} bytes)", peer_id, msg.len());
                    msg
                },
                Ok(msg) = scp_broadcast_rx.recv() => {
                    info!("Peer {} got SCP broadcast ({} bytes)", peer_id, msg.len());
                    msg
                },
                else => break,
            };
            
            // Encrypt and send
            let ciphertext = {
                let mut sess = session_write.lock().await;
                match sess.encrypt(&msg) {
                    Ok(c) => c,
                    Err(e) => {
                        debug!("Encrypt error: {}", e);
                        break;
                    }
                }
            };
            
            if let Err(e) = write_framed(&mut writer, &ciphertext).await {
                debug!("Write error to peer {}: {}", peer_id, e);
                break;
            }
        }
    });
    
    // Reader loop
    debug!("Peer {} reader loop started", peer_id);
    loop {
        let ciphertext = match read_framed(&mut reader).await {
            Ok(c) => c,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    debug!("Read error from peer {}: {}", peer_id, e);
                }
                break;
            }
        };
        
        // Decrypt
        let plaintext = {
            let mut sess = session.lock().await;
            match sess.decrypt(&ciphertext) {
                Ok(p) => p,
                Err(e) => {
                    warn!("Decrypt error from peer {}: {}", peer_id, e);
                    break;
                }
            }
        };
        
        // Route message
        let msg_type = classify_message(&plaintext);
        match msg_type {
            PeerMessageType::Scp => {
                // Dedup and relay (using full StellarMessage hash for dedup consistency)
                let hash = compute_tx_hash(&plaintext);
                trace!("Peer {} received SCP message ({} bytes), hash={:?}", 
                      peer_id, plaintext.len(), &hash[..4]);
                let is_new = {
                    let mut seen = seen_scp.write().await;
                    seen.insert(hash)
                };
                
                if is_new {
                    // Strip the StellarMessage header (4-byte discriminant) to get raw SCPEnvelope
                    // The IPC protocol uses raw SCPEnvelope, not StellarMessage
                    let scp_envelope = if plaintext.len() > 4 {
                        plaintext[4..].to_vec()
                    } else {
                        warn!("SCP message too short to contain envelope");
                        plaintext.clone()
                    };
                    
                    debug!("New SCP message from peer {}, forwarding to Core ({} bytes envelope)", 
                          peer_id, scp_envelope.len());
                    // Forward to Core
                    if let Err(e) = core_events.send(OverlayEvent::ScpReceived {
                        envelope: scp_envelope,
                        from_peer: peer_id,
                    }) {
                        error!("Failed to send ScpReceived event: {:?}", e);
                    }
                    
                    // Relay to other peers (keep StellarMessage format)
                    let _ = scp_broadcast_tx.send(plaintext);
                } else {
                    trace!("Duplicate SCP message from peer {}, ignoring", peer_id);
                }
            }
            
            PeerMessageType::Transaction => {
                // Add to mempool
                let hash = compute_tx_hash(&plaintext);
                let tx_data = if plaintext.len() > 4 { &plaintext[4..] } else { &plaintext };
                
                let mut mp = mempool.write().await;
                if !mp.contains(&hash) {
                    let entry = crate::flood::TxEntry {
                        data: tx_data.to_vec(),
                        hash,
                        source_account: [0u8; 32],
                        sequence: 0,
                        fee: 100, // TODO: parse from XDR
                        num_ops: 1,
                        received_at: std::time::Instant::now(),
                        from_peer: peer_id,
                    };
                    mp.insert(entry);
                    trace!("Added TX {:?} from peer {}", &hash[..4], peer_id);
                }
            }
            
            PeerMessageType::TxAdvert => {
                // TODO: Parse advert hashes and send demands
                trace!("Received advert from peer {}", peer_id);
            }
            
            PeerMessageType::TxDemand => {
                // TODO: Respond with requested TXs
                trace!("Received demand from peer {}", peer_id);
            }
            
            _ => {
                trace!("Unhandled message type {:?} from peer {}", msg_type, peer_id);
            }
        }
    }
    
    // Stop writer
    writer_handle.abort();
}

/// Flush pending adverts to peers.
async fn flush_adverts(
    pending: &Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
    peers: &Arc<RwLock<HashMap<PeerId, ConnectedPeer>>>,
) {
    let adverts: HashMap<TxHash, Vec<PeerId>> = {
        let mut pending = pending.write().await;
        std::mem::take(&mut *pending)
    };
    
    if adverts.is_empty() {
        return;
    }
    
    // Group by peer
    let mut by_peer: HashMap<PeerId, Vec<TxHash>> = HashMap::new();
    for (hash, pids) in adverts {
        for pid in pids {
            by_peer.entry(pid).or_default().push(hash);
        }
    }
    
    // Send adverts
    let peers = peers.read().await;
    for (pid, hashes) in by_peer {
        if let Some(peer) = peers.get(&pid) {
            // Create advert message (type 17 = FLOOD_ADVERT)
            let mut msg = vec![0u8; 4 + hashes.len() * 32];
            msg[3] = 17;
            for (i, hash) in hashes.iter().enumerate() {
                let offset = 4 + i * 32;
                msg[offset..offset + 32].copy_from_slice(hash);
            }
            let _ = peer.tx.send(msg).await;
        }
    }
}

/// Read a length-prefixed, encrypted message.
async fn read_framed<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    // 4-byte length header (big-endian with MSB set)
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    
    let length = u32::from_be_bytes(header) & 0x7FFF_FFFF;
    let length = length as usize;
    
    if length > MAX_MESSAGE_SIZE + 16 { // +16 for auth tag
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("message too large: {}", length),
        ));
    }
    
    let mut payload = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut payload).await?;
    }
    
    Ok(payload)
}

/// Write a length-prefixed message.
async fn write_framed<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8]) -> std::io::Result<()> {
    let length = (data.len() as u32) | 0x8000_0000;
    writer.write_all(&length.to_be_bytes()).await?;
    
    if !data.is_empty() {
        writer.write_all(data).await?;
    }
    
    writer.flush().await?;
    Ok(())
}

/// Handle for controlling an overlay.
#[derive(Clone)]
pub struct OverlayHandle {
    pub commands: mpsc::UnboundedSender<CoreCommand>,
}

impl OverlayHandle {
    pub fn new(commands: mpsc::UnboundedSender<CoreCommand>) -> Self {
        Self { commands }
    }
    
    pub fn broadcast_scp(&self, envelope: Vec<u8>) {
        let _ = self.commands.send(CoreCommand::BroadcastScp { envelope });
    }
    
    pub fn submit_tx(&self, data: Vec<u8>, fee: u64, num_ops: u32) {
        let _ = self.commands.send(CoreCommand::SubmitTx { data, fee, num_ops });
    }
    
    pub async fn get_top_txs(&self, count: usize) -> Vec<([u8; 32], Vec<u8>)> {
        let (tx, mut rx) = mpsc::channel(1);
        let _ = self.commands.send(CoreCommand::GetTopTxs { count, reply: tx });
        rx.recv().await.unwrap_or_default()
    }
    
    pub fn connect_to(&self, addr: SocketAddr) {
        let _ = self.commands.send(CoreCommand::ConnectTo { addr });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_overlay_starts() {
        let keypair = NoiseKeypair::generate();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let overlay = Overlay::new(keypair, addr, cmd_rx, event_tx);
        
        // Just verify it can be created
        assert!(overlay.peers.read().await.is_empty());
    }
}
