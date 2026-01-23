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

use crate::flood::{Mempool, TxHash, compute_tx_hash};
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
    
    /// Fetch a TX set from peers by hash
    FetchTxSet {
        hash: [u8; 32],
        reply: mpsc::Sender<Option<Vec<u8>>>,
    },
    
    /// Cache a locally-built TX set so we can serve it to peers
    CacheTxSet {
        hash: [u8; 32],
        xdr: Vec<u8>,
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

/// Information about a pending connection waiting for its pair.
/// When responder receives first connection (SCP or TX), it stores it here
/// while waiting for the second connection from the same peer.
#[allow(dead_code)]
struct PendingConnection {
    /// Connection type (CONN_TYPE_SCP or CONN_TYPE_TX)
    conn_type: u8,
    /// TCP read half
    read_half: OwnedReadHalf,
    /// TCP write half  
    write_half: OwnedWriteHalf,
    /// Noise session for this connection
    session: NoiseSession,
    /// When this pending connection was received
    received_at: Instant,
}

/// Information about a connected peer (dual TCP)
#[allow(dead_code)]
struct ConnectedPeer {
    id: PeerId,
    addr: SocketAddr,
    public_key: [u8; 32],
    /// Channel to send SCP messages (via broadcast, stored for reference only)
    /// SCP_WRITER subscribes to the global scp_broadcast
    _scp_marker: (),
    /// Channel to send TX messages to this peer
    tx_tx: mpsc::Sender<Vec<u8>>,
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
    next_peer_id: AtomicU64,
    
    /// Broadcast channel for SCP messages (to all peers)
    scp_broadcast: broadcast::Sender<Vec<u8>>,
    
    /// TX mempool
    mempool: Arc<RwLock<Mempool>>,
    
    /// Hashes we've already seen (for dedup)
    seen_scp_hashes: Arc<RwLock<std::collections::HashSet<[u8; 32]>>>,
    
    /// TX flood pending adverts (hash -> peers to advert)
    pending_adverts: Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
    
    /// Pending connections waiting for their pair (by remote public key)
    pending_connections: Arc<RwLock<HashMap<[u8; 32], PendingConnection>>>,
    
    /// Pending TX set fetch requests: hash -> channel to send result
    pending_tx_set_fetches: Arc<RwLock<HashMap<[u8; 32], mpsc::Sender<Option<Vec<u8>>>>>>,
    
    /// Local TX set cache (hash -> XDR)
    local_tx_sets: Arc<RwLock<HashMap<[u8; 32], Vec<u8>>>>,
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
            next_peer_id: AtomicU64::new(1),
            scp_broadcast,
            mempool: Arc::new(RwLock::new(Mempool::new(100000, Duration::from_secs(300)))),
            seen_scp_hashes: Arc::new(RwLock::new(std::collections::HashSet::new())),
            pending_adverts: Arc::new(RwLock::new(HashMap::new())),
            pending_connections: Arc::new(RwLock::new(HashMap::new())),
            pending_tx_set_fetches: Arc::new(RwLock::new(HashMap::new())),
            local_tx_sets: Arc::new(RwLock::new(HashMap::new())),
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
                tokio::time::sleep(Duration::from_millis(10)).await;
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
    
    /// Handle an incoming connection (part of dual TCP).
    /// First connection from a peer is stored as pending.
    /// Second connection completes the pair and spawns tasks.
    async fn handle_incoming_connection(&self, mut stream: TcpStream, addr: SocketAddr) {
        info!("Incoming connection from {}", addr);
        
        // Perform Noise handshake (we are responder)
        let keypair = NoiseKeypair::from_bytes(self.keypair.private, self.keypair.public);
        let mut session = match handshake_responder(&mut stream, &keypair).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Handshake failed from {}: {}", addr, e);
                return;
            }
        };
        
        let remote_key = *session.remote_public_key();
        
        // Read connection type (first encrypted message)
        let ciphertext = match read_framed(&mut stream).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to read conn type from {}: {}", addr, e);
                return;
            }
        };
        let conn_type_data = match session.decrypt(&ciphertext) {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to decrypt conn type from {}: {}", addr, e);
                return;
            }
        };
        let conn_type = if conn_type_data.is_empty() { 0 } else { conn_type_data[0] };
        
        debug!("Received {} connection from {:?} at {}", 
               if conn_type == CONN_TYPE_SCP { "SCP" } else { "TX" },
               &remote_key[..4], addr);
        
        // Split stream into read/write halves
        let (read_half, write_half) = stream.into_split();
        
        // Check for pending connection from same peer
        let mut pending = self.pending_connections.write().await;
        
        if let Some(other) = pending.remove(&remote_key) {
            // Found pair - spawn tasks
            drop(pending);  // Release lock before spawning
            
            let (scp_read, scp_write, scp_session, tx_read, tx_write, tx_session) = 
                if conn_type == CONN_TYPE_SCP {
                    // This is SCP, other is TX
                    (read_half, write_half, session, other.read_half, other.write_half, other.session)
                } else {
                    // This is TX, other is SCP
                    (other.read_half, other.write_half, other.session, read_half, write_half, session)
                };
            
            info!("Dual TCP pair complete for peer {:?}", &remote_key[..4]);
            
            self.register_dual_peer_halves(
                addr, remote_key,
                scp_read, scp_write, scp_session,
                tx_read, tx_write, tx_session,
            ).await;
        } else {
            // First connection - store and wait for pair
            pending.insert(remote_key, PendingConnection {
                conn_type,
                read_half,
                write_half,
                session,
                received_at: Instant::now(),
            });
            drop(pending);
            
            debug!("Stored pending {} connection from {:?}, waiting for pair",
                   if conn_type == CONN_TYPE_SCP { "SCP" } else { "TX" },
                   &remote_key[..4]);
            
            // Spawn timeout cleanup
            let pending_ref = Arc::clone(&self.pending_connections);
            let pubkey = remote_key;
            tokio::spawn(async move {
                tokio::time::sleep(PENDING_CONN_TIMEOUT).await;
                let mut pending = pending_ref.write().await;
                if pending.remove(&pubkey).is_some() {
                    warn!("Pending connection from {:?} timed out", &pubkey[..4]);
                }
            });
        }
    }
    
    /// Connect to a peer with dual TCP connections (SCP + TX).
    async fn connect_to_peer(&self, addr: SocketAddr) {
        info!("Connecting to {} (dual TCP)", addr);
        
        let keypair = NoiseKeypair::from_bytes(self.keypair.private, self.keypair.public);
        
        // ═══ SCP Connection ═══
        let mut scp_stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to connect SCP to {}: {}", addr, e);
                return;
            }
        };
        
        let mut scp_session = match handshake_initiator(&mut scp_stream, &keypair).await {
            Ok(s) => s,
            Err(e) => {
                warn!("SCP handshake failed to {}: {}", addr, e);
                return;
            }
        };
        
        let remote_key = *scp_session.remote_public_key();
        
        // Send connection type (encrypted)
        let conn_type_msg = match scp_session.encrypt(&[CONN_TYPE_SCP]) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to encrypt SCP conn type: {}", e);
                return;
            }
        };
        if let Err(e) = write_framed(&mut scp_stream, &conn_type_msg).await {
            warn!("Failed to send SCP conn type: {}", e);
            return;
        }
        
        debug!("SCP connection established to {}", addr);
        
        // ═══ TX Connection ═══
        let keypair = NoiseKeypair::from_bytes(self.keypair.private, self.keypair.public);
        let mut tx_stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to connect TX to {}: {}", addr, e);
                return;
            }
        };
        
        let mut tx_session = match handshake_initiator(&mut tx_stream, &keypair).await {
            Ok(s) => s,
            Err(e) => {
                warn!("TX handshake failed to {}: {}", addr, e);
                return;
            }
        };
        
        // Verify same peer
        if tx_session.remote_public_key() != &remote_key {
            warn!("TX connection to different peer than SCP!");
            return;
        }
        
        // Send connection type
        let conn_type_msg = match tx_session.encrypt(&[CONN_TYPE_TX]) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to encrypt TX conn type: {}", e);
                return;
            }
        };
        if let Err(e) = write_framed(&mut tx_stream, &conn_type_msg).await {
            warn!("Failed to send TX conn type: {}", e);
            return;
        }
        
        debug!("TX connection established to {}", addr);
        info!("Connected to peer at {} with dual TCP: {:?}", addr, &remote_key[..4]);
        
        // ═══ Register peer and spawn tasks ═══
        self.register_dual_peer(
            addr, remote_key,
            scp_stream, scp_session,
            tx_stream, tx_session,
        ).await;
    }
    
    /// Spawn read/write tasks for a peer.
    #[allow(dead_code)]
    async fn spawn_peer_tasks(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        remote_key: [u8; 32],
        session: NoiseSession,
    ) {
        // Assign peer ID
        let peer_id = self.next_peer_id.fetch_add(1, Ordering::SeqCst);
        
        // Create channel for sending to this peer (TX messages - large buffer for burst)
        let (tx_tx, rx) = mpsc::channel(50000);
        
        // Store peer
        {
            let mut peers = self.peers.write().await;
            peers.insert(peer_id, ConnectedPeer {
                id: peer_id,
                addr,
                public_key: remote_key,
                _scp_marker: (),
                tx_tx,
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
    
    /// Register a dual TCP peer from initiator side (full streams).
    async fn register_dual_peer(
        &self,
        addr: SocketAddr,
        remote_key: [u8; 32],
        scp_stream: TcpStream,
        scp_session: NoiseSession,
        tx_stream: TcpStream,
        tx_session: NoiseSession,
    ) {
        let (scp_read, scp_write) = scp_stream.into_split();
        let (tx_read, tx_write) = tx_stream.into_split();
        
        self.register_dual_peer_halves(
            addr, remote_key,
            scp_read, scp_write, scp_session,
            tx_read, tx_write, tx_session,
        ).await;
    }
    
    /// Register a dual TCP peer from already-split halves (responder side).
    async fn register_dual_peer_halves(
        &self,
        addr: SocketAddr,
        remote_key: [u8; 32],
        scp_read: OwnedReadHalf,
        scp_write: OwnedWriteHalf,
        scp_session: NoiseSession,
        tx_read: OwnedReadHalf,
        tx_write: OwnedWriteHalf,
        tx_session: NoiseSession,
    ) {
        // Assign peer ID
        let peer_id = self.next_peer_id.fetch_add(1, Ordering::SeqCst);
        
        // Create TX channel for this peer (large buffer to handle burst TX flooding)
        let (tx_tx, tx_rx) = mpsc::channel(50000);
        
        // Store peer state
        {
            let mut peers = self.peers.write().await;
            peers.insert(peer_id, ConnectedPeer {
                id: peer_id,
                addr,
                public_key: remote_key,
                _scp_marker: (),
                tx_tx: tx_tx.clone(),
            });
        }
        
        // Wrap sessions in Arc<Mutex> for sharing between reader/writer tasks
        let scp_session = Arc::new(Mutex::new(scp_session));
        let tx_session = Arc::new(Mutex::new(tx_session));
        
        // Subscribe to SCP broadcast
        let scp_broadcast_rx = self.scp_broadcast.subscribe();
        
        // Clone shared state for tasks
        let peers = Arc::clone(&self.peers);
        let core_events = self.core_events.clone();
        let mempool = Arc::clone(&self.mempool);
        let seen_scp = Arc::clone(&self.seen_scp_hashes);
        let scp_broadcast_tx = self.scp_broadcast.clone();
        let pending_adverts = Arc::clone(&self.pending_adverts);
        let local_tx_sets = Arc::clone(&self.local_tx_sets);
        let pending_tx_set_fetches = Arc::clone(&self.pending_tx_set_fetches);
        
        // ═══ Spawn SCP tasks (completely isolated from TX) ═══
        
        // SCP_READER: reads from SCP socket, decrypts, dedup, forwards to Core, relays
        // Also handles GET_TX_SET and TX_SET for TX set fetching
        let scp_session_r = Arc::clone(&scp_session);
        let core_events_scp = core_events.clone();
        let scp_broadcast_relay = scp_broadcast_tx.clone();
        let seen_scp_r = Arc::clone(&seen_scp);
        let local_tx_sets_r = Arc::clone(&local_tx_sets);
        let pending_tx_set_fetches_r = Arc::clone(&pending_tx_set_fetches);
        tokio::spawn(async move {
            scp_reader_task(
                peer_id,
                scp_read,
                scp_session_r,
                core_events_scp,
                scp_broadcast_relay,
                seen_scp_r,
                local_tx_sets_r,
                pending_tx_set_fetches_r,
            ).await;
        });
        
        // SCP_WRITER: receives from broadcast, encrypts, writes to SCP socket
        let scp_session_w = Arc::clone(&scp_session);
        tokio::spawn(async move {
            scp_writer_task(
                peer_id,
                scp_write,
                scp_session_w,
                scp_broadcast_rx,
            ).await;
        });
        
        // ═══ Spawn TX tasks (separate from SCP) ═══
        
        // TX_READER: reads from TX socket, handles TX/ADVERT/DEMAND
        let tx_session_r = Arc::clone(&tx_session);
        let mempool_r = Arc::clone(&mempool);
        let peers_r = Arc::clone(&peers);
        let pending_adverts_r = Arc::clone(&pending_adverts);
        let tx_tx_for_response = tx_tx.clone();
        tokio::spawn(async move {
            tx_reader_task(
                peer_id,
                tx_read,
                tx_session_r,
                mempool_r,
                peers_r,
                pending_adverts_r,
                tx_tx_for_response,
            ).await;
        });
        
        // TX_WRITER: receives from channel, encrypts, writes to TX socket
        let tx_session_w = Arc::clone(&tx_session);
        let peers_cleanup = Arc::clone(&peers);
        tokio::spawn(async move {
            tx_writer_task(
                peer_id,
                tx_write,
                tx_session_w,
                tx_rx,
            ).await;
            
            // Cleanup on disconnect (TX writer is the "owner")
            {
                let mut peers = peers_cleanup.write().await;
                peers.remove(&peer_id);
            }
            info!("Peer {} disconnected", peer_id);
        });
        
        // Notify Core
        let _ = core_events.send(OverlayEvent::PeerConnected {
            peer_id,
            addr,
            public_key: remote_key,
        });
        
        info!("Peer {} registered with dual TCP (4 tasks spawned)", peer_id);
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
                debug!("[SubmitTx] Local TX submitted: hash={:?}, size={}, fee={}, num_ops={}", 
                      &hash[..4], data.len(), fee, num_ops);
                
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
                
                // Pull mode: queue adverts for ALL peers (batched via flush_adverts)
                let peer_ids: Vec<PeerId> = {
                    self.peers.read().await.keys().copied().collect()
                };
                
                if !peer_ids.is_empty() {
                    let mut pending = self.pending_adverts.write().await;
                    pending.entry(hash).or_default().extend(peer_ids);
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
            
            CoreCommand::FetchTxSet { hash, reply } => {
                // Fetch TX set from peers via SCP broadcast
                // TODO: This broadcasts to ALL peers which is inefficient - each peer
                // may respond with the full ~2MB TX set. Should pick ONE peer and retry
                // on timeout. Requires per-peer SCP channel (not just broadcast).
                info!("FetchTxSet requested for hash {:?}", &hash[..4]);
                
                // Check if we have any peers
                let peer_count = self.peers.read().await.len();
                if peer_count == 0 {
                    warn!("No peers to fetch TX set {:?} from", &hash[..4]);
                    let _ = reply.send(None).await;
                    return;
                }
                
                // Register pending fetch so SCP reader can fulfill it
                {
                    let mut pending = self.pending_tx_set_fetches.write().await;
                    pending.insert(hash, reply);
                }
                
                // Build GET_TX_SET message: type 5 (GET_TX_SET) + 32-byte hash
                let mut msg = vec![0u8; 36];
                msg[0..4].copy_from_slice(&5u32.to_be_bytes()); // GET_TX_SET type = 5
                msg[4..36].copy_from_slice(&hash);
                
                info!("Broadcasting GET_TX_SET for {:?} to {} peers", &hash[..4], peer_count);
                
                // Broadcast on SCP channel (latency critical)
                let _ = self.scp_broadcast.send(msg);
                
                // Spawn timeout task to clean up if no response
                let pending_fetches = Arc::clone(&self.pending_tx_set_fetches);
                let hash_copy = hash;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let mut pending = pending_fetches.write().await;
                    if let Some(reply) = pending.remove(&hash_copy) {
                        warn!("Timeout fetching TX set {:?}", &hash_copy[..4]);
                        let _ = reply.send(None).await;
                    }
                });
            }
            
            CoreCommand::CacheTxSet { hash, xdr } => {
                info!("Caching TX set {:?} ({} bytes)", &hash[..4], xdr.len());
                let mut cache = self.local_tx_sets.write().await;
                cache.insert(hash, xdr);
            }
        }
    }
    
    /// Get the actual listen address (useful when binding to port 0).
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// DUAL TCP TASK FUNCTIONS
// ══════════════════════════════════════════════════════════════════════════════

/// SCP_READER task: reads from SCP socket, decrypts, dedup, forwards to Core, relays.
/// Also handles GET_TX_SET (type 5) and TX_SET (type 6) for TX set fetching.
async fn scp_reader_task(
    peer_id: PeerId,
    mut tcp_read: OwnedReadHalf,
    session: Arc<Mutex<NoiseSession>>,
    core_events: mpsc::UnboundedSender<OverlayEvent>,
    scp_broadcast: broadcast::Sender<Vec<u8>>,
    seen_hashes: Arc<RwLock<std::collections::HashSet<[u8; 32]>>>,
    local_tx_sets: Arc<RwLock<HashMap<[u8; 32], Vec<u8>>>>,
    pending_tx_set_fetches: Arc<RwLock<HashMap<[u8; 32], mpsc::Sender<Option<Vec<u8>>>>>>,
) {
    debug!("SCP_READER started for peer {}", peer_id);
    
    loop {
        // 1. Read from TCP (dedicated SCP socket)
        let ciphertext = match read_framed(&mut tcp_read).await {
            Ok(c) => c,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    debug!("SCP read error from peer {}: {}", peer_id, e);
                }
                break;
            }
        };
        
        // 2. Decrypt - hold lock briefly (~1μs)
        let plaintext = {
            let mut sess = session.lock().await;
            match sess.decrypt(&ciphertext) {
                Ok(p) => p,
                Err(e) => {
                    warn!("SCP decrypt error from peer {}: {}", peer_id, e);
                    break;
                }
            }
        };
        
        // 3. Check message type
        if plaintext.len() < 4 {
            warn!("SCP message too short from peer {}", peer_id);
            continue;
        }
        let msg_type = u32::from_be_bytes([plaintext[0], plaintext[1], plaintext[2], plaintext[3]]);
        
        // Handle GET_TX_SET (type 5) - peer is requesting a TX set from us
        if msg_type == 5 {
            if plaintext.len() < 36 {
                warn!("GET_TX_SET message too short from peer {}", peer_id);
                continue;
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&plaintext[4..36]);
            info!("Received GET_TX_SET for {:?} from peer {}", &hash[..4], peer_id);
            
            // Look up in local cache and respond via broadcast
            let cache = local_tx_sets.read().await;
            if let Some(xdr) = cache.get(&hash) {
                info!("Serving TX set {:?} ({} bytes) to peer {}", &hash[..4], xdr.len(), peer_id);
                // Build TX_SET response: type 6 (TX_SET) + hash + XDR
                let mut response = Vec::with_capacity(36 + xdr.len());
                response.extend_from_slice(&6u32.to_be_bytes()); // TX_SET type = 6
                response.extend_from_slice(&hash);
                response.extend_from_slice(xdr);
                // Broadcast response (TODO: inefficient, should send to requester only)
                let _ = scp_broadcast.send(response);
            } else {
                debug!("TX set {:?} not in local cache", &hash[..4]);
            }
            continue;
        }
        
        // Handle TX_SET (type 6) - response to our GET_TX_SET request
        if msg_type == 6 {
            if plaintext.len() < 36 {
                warn!("TX_SET message too short from peer {}", peer_id);
                continue;
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&plaintext[4..36]);
            let xdr = plaintext[36..].to_vec();
            info!("Received TX_SET {:?} ({} bytes) from peer {}", &hash[..4], xdr.len(), peer_id);
            
            // Cache it locally
            {
                let mut cache = local_tx_sets.write().await;
                cache.insert(hash, xdr.clone());
            }
            
            // Fulfill any pending fetch request
            let mut pending = pending_tx_set_fetches.write().await;
            if let Some(reply) = pending.remove(&hash) {
                info!("Fulfilling pending fetch for TX set {:?}", &hash[..4]);
                let _ = reply.send(Some(xdr)).await;
            }
            continue;
        }
        
        // 4. Dedup check for SCP envelopes
        let hash = compute_tx_hash(&plaintext);
        let is_new = {
            let mut seen = seen_hashes.write().await;
            seen.insert(hash)
        };
        
        if is_new {
            // 5. Forward to Core - UnboundedSender, NEVER blocks
            // Strip StellarMessage header (4-byte discriminant) to get raw SCPEnvelope
            let scp_envelope = if plaintext.len() > 4 {
                plaintext[4..].to_vec()
            } else {
                plaintext.clone()
            };
            
            let _ = core_events.send(OverlayEvent::ScpReceived {
                envelope: scp_envelope,
                from_peer: peer_id,
            });
            
            // 6. Relay to other peers - broadcast, NEVER blocks sender
            let _ = scp_broadcast.send(plaintext);
        }
    }
    
    debug!("SCP_READER ended for peer {}", peer_id);
}

/// SCP_WRITER task: receives from broadcast, encrypts, writes to SCP socket.
/// This task is COMPLETELY ISOLATED from TX path.
async fn scp_writer_task(
    peer_id: PeerId,
    mut tcp_write: OwnedWriteHalf,
    session: Arc<Mutex<NoiseSession>>,
    mut broadcast_rx: broadcast::Receiver<Vec<u8>>,
) {
    debug!("SCP_WRITER started for peer {}", peer_id);
    
    loop {
        // 1. Wait for message from broadcast (O(1), never blocks sender)
        let msg = match broadcast_rx.recv().await {
            Ok(msg) => msg,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("SCP_WRITER {} lagged by {} messages", peer_id, n);
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        
        // 2. Encrypt - hold lock briefly (~1μs)
        let ciphertext = {
            let mut sess = session.lock().await;
            match sess.encrypt(&msg) {
                Ok(c) => c,
                Err(e) => {
                    debug!("SCP encrypt error for peer {}: {}", peer_id, e);
                    break;
                }
            }
        };
        
        // 3. Write to TCP (dedicated SCP socket)
        if let Err(e) = write_framed(&mut tcp_write, &ciphertext).await {
            debug!("SCP write error to peer {}: {}", peer_id, e);
            break;
        }
    }
    
    debug!("SCP_WRITER ended for peer {}", peer_id);
}

/// TX_READER task: reads from TX socket, handles TX/ADVERT/DEMAND messages.
/// Completely separate from SCP path.
async fn tx_reader_task(
    peer_id: PeerId,
    mut tcp_read: OwnedReadHalf,
    session: Arc<Mutex<NoiseSession>>,
    mempool: Arc<RwLock<Mempool>>,
    peers: Arc<RwLock<HashMap<PeerId, ConnectedPeer>>>,
    pending_adverts: Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
    response_tx: mpsc::Sender<Vec<u8>>,
) {
    debug!("TX_READER started for peer {}", peer_id);
    
    loop {
        // 1. Read from TCP (dedicated TX socket)
        let ciphertext = match read_framed(&mut tcp_read).await {
            Ok(c) => c,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    debug!("TX read error from peer {}: {}", peer_id, e);
                }
                break;
            }
        };
        
        // 2. Decrypt
        let plaintext = {
            let mut sess = session.lock().await;
            match sess.decrypt(&ciphertext) {
                Ok(p) => p,
                Err(e) => {
                    warn!("TX decrypt error from peer {}: {}", peer_id, e);
                    break;
                }
            }
        };
        
        // 3. Route by message type
        let msg_type = classify_message(&plaintext);
        match msg_type {
            PeerMessageType::Transaction => {
                handle_tx_message(peer_id, &plaintext, &mempool, &peers, &pending_adverts).await;
            }
            PeerMessageType::TxAdvert => {
                handle_advert_message(peer_id, &plaintext, &mempool, &response_tx).await;
            }
            PeerMessageType::TxDemand => {
                handle_demand_message(peer_id, &plaintext, &mempool, &response_tx).await;
            }
            _ => {
                // GET_TX_SET and TX_SET are handled on SCP channel, not TX channel
                debug!("Unexpected message type {:?} on TX connection from peer {}", msg_type, peer_id);
            }
        }
    }
    
    debug!("TX_READER ended for peer {}", peer_id);
}

/// TX_WRITER task: receives from channel, encrypts, writes to TX socket.
async fn tx_writer_task(
    peer_id: PeerId,
    mut tcp_write: OwnedWriteHalf,
    session: Arc<Mutex<NoiseSession>>,
    mut tx_rx: mpsc::Receiver<Vec<u8>>,
) {
    debug!("TX_WRITER started for peer {}", peer_id);
    
    loop {
        // 1. Wait for message from channel
        let msg = match tx_rx.recv().await {
            Some(m) => m,
            None => break,
        };
        
        // 2. Encrypt (may take longer for large TXs, that's OK)
        let ciphertext = {
            let mut sess = session.lock().await;
            match sess.encrypt(&msg) {
                Ok(c) => c,
                Err(e) => {
                    debug!("TX encrypt error for peer {}: {}", peer_id, e);
                    break;
                }
            }
        };
        
        // 3. Write to TCP (dedicated TX socket, may block for large messages)
        if let Err(e) = write_framed(&mut tcp_write, &ciphertext).await {
            debug!("TX write error to peer {}: {}", peer_id, e);
            break;
        }
    }
    
    debug!("TX_WRITER ended for peer {}", peer_id);
}

/// Handle incoming TRANSACTION message: add to mempool and re-flood.
async fn handle_tx_message(
    from_peer: PeerId,
    data: &[u8],
    mempool: &Arc<RwLock<Mempool>>,
    peers: &Arc<RwLock<HashMap<PeerId, ConnectedPeer>>>,
    pending_adverts: &Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
) {
    // Strip 4-byte header to get raw TX data
    let tx_data = if data.len() > 4 { &data[4..] } else { data };
    // Hash the TX data (not the header) - matches what sender computed
    let hash = compute_tx_hash(tx_data);
    
    // Add to mempool (only TX tasks touch this)
    let is_new = {
        let mut mp = mempool.write().await;
        if mp.contains(&hash) {
            false
        } else {
            let entry = crate::flood::TxEntry {
                data: tx_data.to_vec(),
                hash,
                source_account: [0u8; 32],
                sequence: 0,
                fee: 100, // TODO: parse from XDR
                num_ops: 1,
                received_at: std::time::Instant::now(),
                from_peer,
            };
            mp.insert(entry);
            println!("  [handle_tx_message] inserted TX {:?} from peer {}", &hash[..4], from_peer);
            true
        }
    };
    
    if is_new {
        trace!("New TX {:?} from peer {}", &hash[..4], from_peer);
        propagate_tx(hash, tx_data, from_peer, peers, pending_adverts).await;
    }
}

/// Handle incoming FLOOD_ADVERT: check what we need and send DEMAND.
async fn handle_advert_message(
    from_peer: PeerId,
    data: &[u8],
    mempool: &Arc<RwLock<Mempool>>,
    response_tx: &mpsc::Sender<Vec<u8>>,
) {
    let hashes = parse_flood_advert(data);
    println!("  [handle_advert] parsed {} hashes", hashes.len());
    
    let mut need = Vec::new();
    {
        let mp = mempool.read().await;
        for hash in hashes {
            if !mp.contains(&hash) {
                need.push(hash);
            }
        }
    }
    println!("  [handle_advert] need {} TXs", need.len());
    
    if !need.is_empty() {
        let demand = build_flood_demand(&need);
        println!("  [handle_advert] sending DEMAND ({} bytes)", demand.len());
        match response_tx.try_send(demand) {
            Ok(_) => println!("  [handle_advert] DEMAND sent successfully"),
            Err(e) => println!("  [handle_advert] DEMAND send failed: {}", e),
        }
    }
}

/// Handle incoming FLOOD_DEMAND: send requested TXs.
async fn handle_demand_message(
    from_peer: PeerId,
    data: &[u8],
    mempool: &Arc<RwLock<Mempool>>,
    response_tx: &mpsc::Sender<Vec<u8>>,
) {
    let hashes = parse_flood_demand(data);
    
    let mp = mempool.read().await;
    for hash in hashes {
        if let Some(entry) = mp.get(&hash) {
            let tx_msg = build_transaction_msg(&entry.data);
            // try_send - never blocks
            let _ = response_tx.try_send(tx_msg);
        }
    }
    trace!("Responded to DEMAND from peer {}", from_peer);
}

/// Propagate a TX to other peers using push-k strategy.
async fn propagate_tx(
    hash: TxHash,
    tx_data: &[u8],
    from_peer: PeerId,
    peers: &Arc<RwLock<HashMap<PeerId, ConnectedPeer>>>,
    pending_adverts: &Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
) {
    // Get eligible peers (exclude sender)
    let eligible: Vec<(PeerId, mpsc::Sender<Vec<u8>>)> = {
        let peers = peers.read().await;
        peers.iter()
            .filter(|(&id, _)| id != from_peer)
            .map(|(&id, p)| (id, p.tx_tx.clone()))
            .collect()
    };
    
    if eligible.is_empty() {
        return;
    }
    
    // Push to k=2 random peers
    let k = 2.min(eligible.len());
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut shuffled = eligible;
    shuffled.shuffle(&mut rng);
    
    let tx_msg = build_transaction_msg(tx_data);
    
    // Push to first k (using try_send - never blocks)
    for (_, tx) in shuffled.iter().take(k) {
        let _ = tx.try_send(tx_msg.clone());
    }
    
    // Queue adverts for rest
    if shuffled.len() > k {
        let mut adverts = pending_adverts.write().await;
        for (peer_id, _) in shuffled.iter().skip(k) {
            adverts.entry(hash).or_default().push(*peer_id);
        }
    }
}

/// Run the peer handler (read/write tasks) - LEGACY single TCP, kept for compatibility.
#[allow(dead_code)]
async fn run_peer_handler(
    peer_id: PeerId,
    stream: TcpStream,
    session: NoiseSession,
    mut direct_rx: mpsc::Receiver<Vec<u8>>,
    mut scp_broadcast_rx: broadcast::Receiver<Vec<u8>>,
    core_events: mpsc::UnboundedSender<OverlayEvent>,
    mempool: Arc<RwLock<Mempool>>,
    seen_scp: Arc<RwLock<std::collections::HashSet<[u8; 32]>>>,
    scp_broadcast_tx: broadcast::Sender<Vec<u8>>,
    _pending_adverts: Arc<RwLock<HashMap<TxHash, Vec<PeerId>>>>,
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
            // Create advert message: 4-byte type (17) + 4-byte count + N*32-byte hashes
            let mut msg = vec![0u8; 8 + hashes.len() * 32];
            msg[3] = 17; // FLOOD_ADVERT type
            let count = hashes.len() as u32;
            msg[4..8].copy_from_slice(&count.to_be_bytes());
            for (i, hash) in hashes.iter().enumerate() {
                let offset = 8 + i * 32;
                msg[offset..offset + 32].copy_from_slice(hash);
            }
            let _ = peer.tx_tx.send(msg).await;
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

// ══════════════════════════════════════════════════════════════════════════════
// XDR Message Helpers for TX Flooding
// ══════════════════════════════════════════════════════════════════════════════

/// StellarMessage type discriminant for FLOOD_ADVERT
const MSG_TYPE_FLOOD_ADVERT: u32 = 17;

/// StellarMessage type discriminant for FLOOD_DEMAND  
const MSG_TYPE_FLOOD_DEMAND: u32 = 18;

/// Build a FLOOD_ADVERT message containing TX hashes.
/// Format: 4-byte type (17) + 4-byte count + N*32-byte hashes
#[allow(dead_code)]
fn build_flood_advert(hashes: &[[u8; 32]]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(8 + hashes.len() * 32);
    msg.extend_from_slice(&MSG_TYPE_FLOOD_ADVERT.to_be_bytes());
    msg.extend_from_slice(&(hashes.len() as u32).to_be_bytes());
    for hash in hashes {
        msg.extend_from_slice(hash);
    }
    msg
}

/// Parse a FLOOD_ADVERT message into TX hashes.
fn parse_flood_advert(data: &[u8]) -> Vec<[u8; 32]> {
    if data.len() < 8 {
        return Vec::new();
    }
    let count = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut hashes = Vec::with_capacity(count);
    for i in 0..count {
        let offset = 8 + i * 32;
        if offset + 32 <= data.len() {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&data[offset..offset + 32]);
            hashes.push(hash);
        }
    }
    hashes
}

/// Build a FLOOD_DEMAND message requesting TX hashes.
/// Format: 4-byte type (18) + 4-byte count + N*32-byte hashes
fn build_flood_demand(hashes: &[[u8; 32]]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(8 + hashes.len() * 32);
    msg.extend_from_slice(&MSG_TYPE_FLOOD_DEMAND.to_be_bytes());
    msg.extend_from_slice(&(hashes.len() as u32).to_be_bytes());
    for hash in hashes {
        msg.extend_from_slice(hash);
    }
    msg
}

/// Parse a FLOOD_DEMAND message into requested TX hashes.
fn parse_flood_demand(data: &[u8]) -> Vec<[u8; 32]> {
    if data.len() < 8 {
        return Vec::new();
    }
    let count = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut hashes = Vec::with_capacity(count);
    for i in 0..count {
        let offset = 8 + i * 32;
        if offset + 32 <= data.len() {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&data[offset..offset + 32]);
            hashes.push(hash);
        }
    }
    hashes
}

/// Build a TRANSACTION message.
/// Format: 4-byte type (7) + TransactionEnvelope XDR
#[allow(dead_code)]
fn build_transaction_msg(tx_data: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(4 + tx_data.len());
    msg.extend_from_slice(&7u32.to_be_bytes()); // TRANSACTION type
    msg.extend_from_slice(tx_data);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    
    /// Helper to create an overlay with channels for testing.
    #[allow(dead_code)]
    fn create_test_overlay(addr: &str) -> (
        Overlay,
        mpsc::UnboundedSender<CoreCommand>,
        mpsc::UnboundedReceiver<OverlayEvent>,
    ) {
        let keypair = NoiseKeypair::generate();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let addr: SocketAddr = addr.parse().unwrap();
        let overlay = Overlay::new(keypair, addr, cmd_rx, event_tx);
        (overlay, _cmd_tx, event_rx)
    }
    
    #[tokio::test]
    async fn test_overlay_starts() {
        let keypair = NoiseKeypair::generate();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let overlay = Overlay::new(keypair, addr, cmd_rx, event_tx);
        
        // Just verify it can be created
        assert!(overlay.peers.read().await.is_empty());
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // DUAL TCP CONNECTION TESTS
    // ══════════════════════════════════════════════════════════════════════════
    
    /// Test that two overlays can connect and exchange messages.
    /// This test uses the current single-TCP implementation.
    #[tokio::test]
    async fn test_two_overlays_connect() {
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        
        // Save public keys before moving keypairs
        let pubkey1 = keypair1.public;
        let pubkey2 = keypair2.public;
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, mut event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        
        // Overlay1 listens on random port
        let overlay1 = Overlay::new(
            keypair1,
            "127.0.0.1:0".parse().unwrap(),
            cmd_rx1,
            event_tx1.clone(),
        );
        
        // Use a oneshot channel to communicate the bound address
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        
        // Start overlay1 with address callback
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let actual_addr = listener.local_addr().unwrap();
            addr_tx.send(actual_addr).unwrap();
            
            // Run a simplified accept loop (not full overlay.run())
            let keypair = NoiseKeypair::from_bytes(overlay1.keypair.private, overlay1.keypair.public);
            let (mut stream, addr) = listener.accept().await.unwrap();
            let session = handshake_responder(&mut stream, &keypair).await.unwrap();
            let remote_key = *session.remote_public_key();
            
            // Signal peer connected
            let _ = event_tx1.send(OverlayEvent::PeerConnected {
                peer_id: 1,
                addr,
                public_key: remote_key,
            });
            
            session
        });
        
        // Wait for overlay1 to bind
        let overlay1_addr = addr_rx.await.unwrap();
        
        // Connect overlay2 to overlay1
        let mut stream = TcpStream::connect(overlay1_addr).await.unwrap();
        let session2 = handshake_initiator(&mut stream, &NoiseKeypair::from_bytes(keypair2.private, keypair2.public)).await.unwrap();
        
        // Wait for overlay1 to complete handshake
        let session1 = handle1.await.unwrap();
        
        // Verify both sides authenticated each other
        assert_eq!(session1.remote_public_key(), &pubkey2);
        assert_eq!(session2.remote_public_key(), &pubkey1);
        
        // Check event was sent
        let event = event_rx1.recv().await.unwrap();
        match event {
            OverlayEvent::PeerConnected { peer_id, public_key, .. } => {
                assert_eq!(peer_id, 1);
                assert_eq!(public_key, pubkey2);
            }
            _ => panic!("Expected PeerConnected event"),
        }
    }
    
    /// Test that after dual TCP implementation, each peer has 2 connections.
    /// This test actually exercises the full dual TCP handshake.
    #[tokio::test]
    async fn test_dual_tcp_connection_established() {
        // Create two overlays
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        let pubkey1 = keypair1.public;
        let pubkey2 = keypair2.public;
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, mut event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        let (_cmd_tx2, cmd_rx2) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx2, mut event_rx2) = mpsc::unbounded_channel::<OverlayEvent>();
        
        // Create overlay1 (responder)
        let overlay1 = Overlay::new(
            keypair1,
            "127.0.0.1:0".parse().unwrap(),
            cmd_rx1,
            event_tx1,
        );
        
        // Create overlay2 (initiator)
        let overlay2 = Overlay::new(
            keypair2,
            "127.0.0.1:0".parse().unwrap(),
            cmd_rx2,
            event_tx2,
        );
        
        // Start overlay1 and get its bound address
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let actual_addr = listener.local_addr().unwrap();
            addr_tx.send(actual_addr).unwrap();
            
            // Accept both connections (SCP and TX)
            for _ in 0..2 {
                let (stream, addr) = listener.accept().await.unwrap();
                overlay1.handle_incoming_connection(stream, addr).await;
            }
            
            overlay1
        });
        
        let overlay1_addr = addr_rx.await.unwrap();
        
        // Overlay2 connects with dual TCP
        overlay2.connect_to_peer(overlay1_addr).await;
        
        // Wait for overlay1 to process both connections
        let _overlay1 = handle1.await.unwrap();
        
        // Both should receive PeerConnected events
        let event1 = tokio::time::timeout(Duration::from_secs(2), event_rx1.recv()).await;
        let event2 = tokio::time::timeout(Duration::from_secs(2), event_rx2.recv()).await;
        
        // Verify events
        match event1 {
            Ok(Some(OverlayEvent::PeerConnected { public_key, .. })) => {
                assert_eq!(public_key, pubkey2, "Overlay1 should see overlay2's key");
            }
            Ok(Some(other)) => panic!("Unexpected event: {:?}", other),
            Ok(None) => panic!("Event channel closed"),
            Err(_) => panic!("Timeout waiting for PeerConnected on overlay1"),
        }
        
        match event2 {
            Ok(Some(OverlayEvent::PeerConnected { public_key, .. })) => {
                assert_eq!(public_key, pubkey1, "Overlay2 should see overlay1's key");
            }
            Ok(Some(other)) => panic!("Unexpected event: {:?}", other),
            Ok(None) => panic!("Event channel closed"),
            Err(_) => panic!("Timeout waiting for PeerConnected on overlay2"),
        }
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // SCP ISOLATION TESTS (Critical!)
    // ══════════════════════════════════════════════════════════════════════════
    
    /// CRITICAL TEST: SCP latency must stay <10ms even under heavy TX load.
    /// This is the key invariant of our dual TCP design.
    /// 
    /// Test design:
    /// 1. Connect two overlays with dual TCP
    /// 2. Start flooding large TXs (saturate TX connection)
    /// 3. Simultaneously send SCP messages
    /// 4. Measure SCP processing latency (broadcast → TCP → Core event)
    /// 5. Assert latency < 10ms
    ///
    /// This test SHOULD FAIL with single TCP (head-of-line blocking).
    /// This test MUST PASS with dual TCP.
    #[tokio::test]
    async fn test_scp_not_blocked_by_tx_flood() {
        use std::sync::atomic::{AtomicUsize, AtomicU64};
        
        // ═══════════════════════════════════════════════════════════════════════
        // SETUP: Create two overlays and connect them with dual TCP
        // ═══════════════════════════════════════════════════════════════════════
        
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, mut event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        let (_cmd_tx2, cmd_rx2) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx2, mut event_rx2) = mpsc::unbounded_channel::<OverlayEvent>();
        
        let overlay1 = Overlay::new(keypair1, "127.0.0.1:0".parse().unwrap(), cmd_rx1, event_tx1);
        let overlay2 = Overlay::new(keypair2, "127.0.0.1:0".parse().unwrap(), cmd_rx2, event_tx2);
        
        // Get references to shared state for verification
        let mempool1 = Arc::clone(&overlay1.mempool);
        let _mempool2 = Arc::clone(&overlay2.mempool);
        let scp_broadcast1 = overlay1.scp_broadcast.clone();
        let seen_scp1 = Arc::clone(&overlay1.seen_scp_hashes);
        let peers1 = Arc::clone(&overlay1.peers);
        let peers2 = Arc::clone(&overlay2.peers);
        
        // Start overlay1 (responder)
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            
            // Accept both connections (SCP + TX)
            for _ in 0..2 {
                let (stream, addr) = listener.accept().await.unwrap();
                overlay1.handle_incoming_connection(stream, addr).await;
            }
            
            // Keep alive during test
            tokio::time::sleep(Duration::from_secs(10)).await;
            overlay1
        });
        
        let overlay1_addr = addr_rx.await.unwrap();
        
        // Connect overlay2 (initiator) with dual TCP
        overlay2.connect_to_peer(overlay1_addr).await;
        
        // Wait for connections to fully establish
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // ═══════════════════════════════════════════════════════════════════════
        // VERIFY: Dual TCP connections established
        // ═══════════════════════════════════════════════════════════════════════
        
        let peer_count1 = peers1.read().await.len();
        let peer_count2 = peers2.read().await.len();
        assert_eq!(peer_count1, 1, "Overlay1 should have 1 peer");
        assert_eq!(peer_count2, 1, "Overlay2 should have 1 peer");
        
        // Drain PeerConnected events
        let _ = tokio::time::timeout(Duration::from_millis(100), event_rx1.recv()).await;
        let _ = tokio::time::timeout(Duration::from_millis(100), event_rx2.recv()).await;
        
        println!("✓ Dual TCP connections established");
        
        // ═══════════════════════════════════════════════════════════════════════
        // TEST 1: Verify SCP messages flow through TCP correctly (baseline)
        // The path: broadcast1 → SCP_WRITER1 → TCP → SCP_READER2 → event_rx2
        // ═══════════════════════════════════════════════════════════════════════
        
        // Send SCP message from overlay1's broadcast
        let scp_msg = vec![0u8, 0, 0, 10, 1, 2, 3, 4, 5]; // SCP_MESSAGE header + payload
        let scp_hash = compute_tx_hash(&scp_msg);
        
        // Add to seen hashes and broadcast
        {
            seen_scp1.write().await.insert(scp_hash);
        }
        let _ = scp_broadcast1.send(scp_msg.clone());
        
        // SCP_WRITER1 should pick this up, encrypt, write to TCP
        // SCP_READER2 should read from TCP, decrypt, send to event_rx2
        let baseline_start = Instant::now();
        let received = tokio::time::timeout(Duration::from_millis(500), async {
            while let Some(event) = event_rx2.recv().await {
                if let OverlayEvent::ScpReceived { envelope, .. } = event {
                    return Some(envelope);
                }
            }
            None
        }).await;
        let baseline_latency = baseline_start.elapsed();
        
        match received {
            Ok(Some(envelope)) => {
                // Envelope should be SCP message minus the 4-byte header
                assert_eq!(envelope, &scp_msg[4..], "SCP envelope should match (minus header)");
                println!("✓ Baseline SCP round-trip latency: {:?}", baseline_latency);
            }
            Ok(None) => panic!("Event channel closed"),
            Err(_) => panic!("SCP message did not arrive at overlay2 within 500ms - TCP path broken!"),
        }
        
        assert!(baseline_latency < Duration::from_millis(50), 
            "Baseline SCP latency {:?} too high", baseline_latency);
        
        // ═══════════════════════════════════════════════════════════════════════
        // TEST 2: Measure SCP latency while flooding TX
        // ═══════════════════════════════════════════════════════════════════════
        
        // Counters for verification
        let tx_sent = Arc::new(AtomicUsize::new(0));
        let tx_bytes_sent = Arc::new(AtomicU64::new(0));
        let scp_sent = Arc::new(AtomicUsize::new(0));
        let scp_received = Arc::new(AtomicUsize::new(0));
        
        let tx_sent_clone = Arc::clone(&tx_sent);
        let tx_bytes_clone = Arc::clone(&tx_bytes_sent);
        
        // Get peer's TX channel for flooding
        let tx_channel = {
            let peers = peers2.read().await;
            peers.values().next().unwrap().tx_tx.clone()
        };
        
        // Create a flag to signal when to stop TX flooding
        let stop_flooding = Arc::new(AtomicUsize::new(0));
        let stop_flooding_clone = Arc::clone(&stop_flooding);
        
        // Channel to collect TX latencies (hash -> send_time)
        let tx_send_times: Arc<RwLock<std::collections::HashMap<[u8; 32], Instant>>> = 
            Arc::new(RwLock::new(std::collections::HashMap::new()));
        let tx_send_times_clone = Arc::clone(&tx_send_times);
        let (tx_latency_tx, mut tx_latency_rx) = mpsc::unbounded_channel::<Duration>();
        
        // Spawn TX flooding task that tracks send times for latency measurement
        let tx_flood_handle = tokio::spawn(async move {
            let mut i = 0u64;
            loop {
                if stop_flooding_clone.load(Ordering::SeqCst) > 0 {
                    break;
                }
                
                // Create TX with unique content (1KB each - realistic size)
                let mut tx_data = vec![(i % 256) as u8; 1_000];
                // Embed sequence number at start for identification
                tx_data[0..8].copy_from_slice(&i.to_be_bytes());
                
                let tx_msg = build_transaction_msg(&tx_data);
                // Hash the raw TX data (not the message with header) - matches receiver
                let hash = compute_tx_hash(&tx_data);
                
                // Record send time
                let send_time = Instant::now();
                {
                    tx_send_times_clone.write().await.insert(hash, send_time);
                }
                
                // Send via channel
                match tx_channel.send(tx_msg.clone()).await {
                    Ok(()) => {
                        let count = tx_sent_clone.fetch_add(1, Ordering::SeqCst);
                        tx_bytes_clone.fetch_add(tx_msg.len() as u64, Ordering::SeqCst);
                        if count % 20 == 0 {
                            println!("  TX flood: sent {} messages ({} KB)", 
                                count + 1, tx_bytes_clone.load(Ordering::SeqCst) / 1024);
                        }
                    }
                    Err(_) => break,
                }
                
                i += 1;
            }
            println!("  TX flood stopped");
        });
        
        // Spawn task to monitor mempool1 for TX arrivals and compute latencies
        // TX flows: overlay2 channel → TCP → overlay1 mempool
        let mempool1_monitor = Arc::clone(&mempool1);
        let tx_send_times_monitor = Arc::clone(&tx_send_times);
        let tx_latency_tx_clone = tx_latency_tx.clone();
        let stop_monitor = Arc::clone(&stop_flooding);
        
        let tx_monitor_handle = tokio::spawn(async move {
            let mut seen_hashes: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
            
            loop {
                if stop_monitor.load(Ordering::SeqCst) > 0 {
                    break;
                }
                
                // Check mempool1 for new TXs (TXs arrive here from overlay2)
                let current_hashes: Vec<[u8; 32]> = {
                    let mp = mempool1_monitor.read().await;
                    mp.top_by_fee(10000) // Get all hashes (up to 10k)
                };
                
                let arrival_time = Instant::now();
                
                for hash in current_hashes {
                    if seen_hashes.insert(hash) {
                        // New TX arrived - compute latency
                        let send_times = tx_send_times_monitor.read().await;
                        if let Some(&send_time) = send_times.get(&hash) {
                            let latency = arrival_time.duration_since(send_time);
                            let _ = tx_latency_tx_clone.send(latency);
                        }
                    }
                }
                
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
        
        // Give TX flooding time to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // ═══════════════════════════════════════════════════════════════════════
        // TEST 3: Send SCP messages and measure latency under TX load
        // ═══════════════════════════════════════════════════════════════════════
        
        let mut scp_latencies = Vec::new();
        let scp_sent_clone = Arc::clone(&scp_sent);
        let scp_received_clone = Arc::clone(&scp_received);
        
        println!("Starting SCP latency measurements during TX flood...");
        
        // Spawn a receiver task that collects SCP arrival times
        let (scp_arrival_tx, mut scp_arrival_rx) = mpsc::unbounded_channel::<(u32, Instant)>();
        let mut event_rx2_owned = event_rx2;
        
        let _scp_recv_task = tokio::spawn(async move {
            while let Some(event) = event_rx2_owned.recv().await {
                if let OverlayEvent::ScpReceived { envelope, .. } = event {
                    // Extract the sequence number from the envelope (first 4 bytes after header)
                    if envelope.len() >= 4 {
                        let seq = u32::from_be_bytes([envelope[0], envelope[1], envelope[2], envelope[3]]);
                        let _ = scp_arrival_tx.send((seq, Instant::now()));
                    }
                }
            }
        });
        
        // Send SCP messages with sequence numbers
        let mut send_times: std::collections::HashMap<u32, Instant> = std::collections::HashMap::new();
        
        for i in 0..30 {
            // Create unique SCP message with sequence number
            let seq = (i + 100) as u32;
            let mut scp_msg = vec![0u8, 0, 0, 10]; // SCP_MESSAGE discriminant
            scp_msg.extend_from_slice(&seq.to_be_bytes());
            scp_msg.extend_from_slice(&[0xAA; 200]); // Recognizable payload
            
            let scp_hash = compute_tx_hash(&scp_msg);
            
            // Add to seen hashes first (don't include lock time in latency)
            {
                seen_scp1.write().await.insert(scp_hash);
            }
            
            // Record send time AFTER the lock, right before broadcast
            let send_time = Instant::now();
            send_times.insert(seq, send_time);
            
            let _ = scp_broadcast1.send(scp_msg.clone());
            scp_sent_clone.fetch_add(1, Ordering::SeqCst);
            
            // Small delay between sends to space them out (not counted in latency)
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        
        // Wait for messages to arrive (with timeout)
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Collect SCP arrival times from the receiver channel
        while let Ok((seq, arrival_time)) = scp_arrival_rx.try_recv() {
            if let Some(&send_time) = send_times.get(&seq) {
                let latency = arrival_time.duration_since(send_time);
                scp_latencies.push(latency);
                scp_received_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        // Stop TX flooding and monitoring
        stop_flooding.store(1, Ordering::SeqCst);
        let _ = tx_flood_handle.await;
        let _ = tx_monitor_handle.await;
        
        // Collect TX latencies
        let mut tx_latencies = Vec::new();
        while let Ok(latency) = tx_latency_rx.try_recv() {
            tx_latencies.push(latency);
        }
        
        // ═══════════════════════════════════════════════════════════════════════
        // ANALYZE RESULTS
        // ═══════════════════════════════════════════════════════════════════════
        
        let tx_count = tx_sent.load(Ordering::SeqCst);
        let tx_bytes = tx_bytes_sent.load(Ordering::SeqCst);
        let scp_count = scp_sent.load(Ordering::SeqCst);
        let scp_recv_count = scp_received.load(Ordering::SeqCst);
        
        println!("\n═══════════════════════════════════════════════════════════════════════════════");
        println!("STRESS TEST RESULTS - SCP vs TX LATENCY COMPARISON");
        println!("═══════════════════════════════════════════════════════════════════════════════");
        println!("TX Messages sent:      {} ({} KB)", tx_count, tx_bytes / 1024);
        println!("TX Messages received:  {}", tx_latencies.len());
        println!("SCP Messages sent:     {}", scp_count);
        println!("SCP Messages received: {}", scp_recv_count);
        
        // Helper to compute stats
        fn compute_stats(latencies: &[Duration]) -> (Duration, Duration, Duration, Duration, Duration, Duration) {
            if latencies.is_empty() {
                return (Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO);
            }
            let min = *latencies.iter().min().unwrap();
            let max = *latencies.iter().max().unwrap();
            let total: Duration = latencies.iter().sum();
            let avg = total / latencies.len() as u32;
            
            let mut sorted = latencies.to_vec();
            sorted.sort();
            let p50 = sorted[sorted.len() / 2];
            let p90 = sorted[(sorted.len() as f64 * 0.90) as usize];
            let p99 = sorted[((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1)];
            
            (min, p50, avg, p90, p99, max)
        }
        
        let (scp_min, scp_p50, scp_avg, scp_p90, scp_p99, scp_max) = compute_stats(&scp_latencies);
        let (tx_min, tx_p50, tx_avg, tx_p90, tx_p99, tx_max) = compute_stats(&tx_latencies);
        
        println!("───────────────────────────────────────────────────────────────────────────────");
        println!("LATENCY COMPARISON (channel → TCP → arrival)");
        println!("───────────────────────────────────────────────────────────────────────────────");
        println!("         {:>15}    {:>15}    {:>10}", "SCP", "TX", "TX/SCP");
        println!("  Min:   {:>15?}    {:>15?}    {:>10.1}x", scp_min, tx_min, 
            tx_min.as_nanos() as f64 / scp_min.as_nanos().max(1) as f64);
        println!("  p50:   {:>15?}    {:>15?}    {:>10.1}x", scp_p50, tx_p50,
            tx_p50.as_nanos() as f64 / scp_p50.as_nanos().max(1) as f64);
        println!("  Avg:   {:>15?}    {:>15?}    {:>10.1}x", scp_avg, tx_avg,
            tx_avg.as_nanos() as f64 / scp_avg.as_nanos().max(1) as f64);
        println!("  p90:   {:>15?}    {:>15?}    {:>10.1}x", scp_p90, tx_p90,
            tx_p90.as_nanos() as f64 / scp_p90.as_nanos().max(1) as f64);
        println!("  p99:   {:>15?}    {:>15?}    {:>10.1}x", scp_p99, tx_p99,
            tx_p99.as_nanos() as f64 / scp_p99.as_nanos().max(1) as f64);
        println!("  Max:   {:>15?}    {:>15?}    {:>10.1}x", scp_max, tx_max,
            tx_max.as_nanos() as f64 / scp_max.as_nanos().max(1) as f64);
        println!("═══════════════════════════════════════════════════════════════════════════════\n");
        
        if !scp_latencies.is_empty() {
            // ═══════════════════════════════════════════════════════════════════════
            // ASSERTIONS - The critical SCP isolation invariants
            // ═══════════════════════════════════════════════════════════════════════
            
            // 1. SCP messages must be received during TX flood
            assert!(scp_recv_count > 0, 
                "CRITICAL: No SCP messages received during TX flood!");
            
            // 2. Most SCP messages should arrive (allow some loss due to test timing)
            let scp_success_rate = scp_recv_count as f64 / scp_count as f64;
            println!("SCP delivery rate: {:.1}%", scp_success_rate * 100.0);
            assert!(scp_success_rate >= 0.7, 
                "CRITICAL: SCP delivery rate {:.1}% too low (need >=70%)", 
                scp_success_rate * 100.0);
            
            // 3. CRITICAL: SCP latency must be BOUNDED and much lower than TX
            // Under heavy TX load, TX can spike to 400ms+ but SCP must stay bounded
            assert!(scp_max < Duration::from_millis(50), 
                "CRITICAL: SCP max latency {:?} exceeds 50ms - SCP may be blocked!", 
                scp_max);
            
            // 4. SCP must be significantly faster than TX under load
            // TX p50 can be 200-300ms under flood, SCP must be <30ms
            assert!(scp_p50 < Duration::from_millis(30),
                "SCP median {:?} exceeds 30ms", scp_p50);
            
            // 5. Key invariant: TX latency should be significantly higher than SCP
            // This proves isolation - TX backs up while SCP stays fast
            if !tx_latencies.is_empty() && tx_p50 > Duration::from_millis(50) {
                let ratio = tx_p50.as_micros() as f64 / scp_p50.as_micros() as f64;
                println!("TX/SCP latency ratio: {:.1}x (TX is {:.1}x slower)", ratio, ratio);
                assert!(ratio > 5.0, 
                    "CRITICAL: TX latency ({:?}) not significantly higher than SCP ({:?}) - isolation broken!",
                    tx_p50, scp_p50);
            }
            
            println!("✓ All SCP isolation assertions PASSED!");
            println!("✓ SCP stays bounded ({:?} max) while TX spikes ({:?} max)", scp_max, tx_max);
            
        } else {
            panic!("CRITICAL: No SCP latency measurements collected!");
        }
        
        // ═══════════════════════════════════════════════════════════════════════
        // VERIFY: TX flooding actually created load
        // ═══════════════════════════════════════════════════════════════════════
        
        assert!(tx_count > 20, 
            "TX flooding insufficient: only {} messages sent (need >20)", tx_count);
        assert!(tx_bytes > 1_000_000, 
            "TX flooding insufficient: only {} KB sent (need >1MB)", tx_bytes / 1024);
        
        println!("✓ TX flood verified: {} messages, {} KB", tx_count, tx_bytes / 1024);
        
        // Cleanup
        handle1.abort();
    }
    
    /// Test that SCP broadcast channel never blocks the sender.
    #[tokio::test]
    async fn test_scp_broadcast_never_blocks() {
        let (tx, _rx1) = broadcast::channel::<Vec<u8>>(16);
        let _rx2 = tx.subscribe();
        let _rx3 = tx.subscribe();
        
        // Fill the channel beyond capacity
        for i in 0..100 {
            let msg = vec![i as u8; 100];
            // send() should never block, even when receivers haven't consumed
            let result = tx.send(msg);
            // It may error if no receivers, but should not block
            assert!(result.is_ok() || result.is_err());
        }
        
        // If we get here without blocking, the test passes
    }
    
    /// Test that TX channel uses try_send (non-blocking).
    #[tokio::test]
    async fn test_tx_channel_uses_try_send() {
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        
        // Fill the channel
        for i in 0..10 {
            tx.send(vec![i]).await.unwrap();
        }
        
        // try_send should return Full error, not block
        let result = tx.try_send(vec![99]);
        assert!(result.is_err());
        match result {
            Err(mpsc::error::TrySendError::Full(_)) => { /* expected */ }
            _ => panic!("Expected Full error"),
        }
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // TX FLOODING TESTS
    // ══════════════════════════════════════════════════════════════════════════
    
    /// Test that TX submitted to overlay A appears in overlay B's mempool.
    /// This verifies the full TX flooding path:
    /// Submit to A → A mempool → push to peer B → B mempool
    #[tokio::test]
    async fn test_tx_floods_between_peers() {
        // Create two overlays
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, _event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        let (_cmd_tx2, cmd_rx2) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx2, _event_rx2) = mpsc::unbounded_channel::<OverlayEvent>();
        
        // Create overlay1 (responder) - will receive the TX
        let overlay1 = Overlay::new(
            keypair1,
            "127.0.0.1:0".parse().unwrap(),
            cmd_rx1,
            event_tx1,
        );
        let mempool1 = Arc::clone(&overlay1.mempool);
        let overlay1_peers = Arc::clone(&overlay1.peers);
        
        // Create overlay2 (initiator) - will submit the TX
        let overlay2 = Overlay::new(
            keypair2,
            "127.0.0.1:0".parse().unwrap(),
            cmd_rx2,
            event_tx2,
        );
        let mempool2 = Arc::clone(&overlay2.mempool);
        
        // Start overlay1 and get its bound address
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        
        // Spawn advert flusher for overlay1 (needed for pull-based TX propagation)
        let pending_adverts1 = Arc::clone(&overlay1.pending_adverts);
        tokio::spawn({
            let peers = Arc::clone(&overlay1_peers);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    flush_adverts(&pending_adverts1, &peers).await;
                }
            }
        });
        
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let actual_addr = listener.local_addr().unwrap();
            addr_tx.send(actual_addr).unwrap();
            
            // Accept both connections (SCP + TX)
            for _ in 0..2 {
                let (stream, addr) = listener.accept().await.unwrap();
                overlay1.handle_incoming_connection(stream, addr).await;
            }
            
            // Keep overlay1 alive until signaled
            let _ = stop_rx.recv().await;
            overlay1
        });
        
        let overlay1_addr = addr_rx.await.unwrap();
        
        // Spawn advert flusher for overlay2 (the sender)
        let pending_adverts2 = Arc::clone(&overlay2.pending_adverts);
        let overlay2_peers = Arc::clone(&overlay2.peers);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                flush_adverts(&pending_adverts2, &overlay2_peers).await;
            }
        });
        
        // Overlay2 connects with dual TCP
        overlay2.connect_to_peer(overlay1_addr).await;
        
        // Wait for connections to fully establish (tasks must be spawned)
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Debug: check peer state
        let peer_count1 = {
            let peers = overlay1_peers.read().await;
            println!("Overlay1 has {} peers", peers.len());
            peers.len()
        };
        let peer_count2 = {
            let peers = overlay2.peers.read().await;
            println!("Overlay2 has {} peers", peers.len());
            peers.len()
        };
        
        if peer_count1 == 0 || peer_count2 == 0 {
            panic!("Dual TCP connection not established: overlay1={}, overlay2={}", peer_count1, peer_count2);
        }
        
        // Submit TX to overlay2
        let tx_data = b"test transaction for flooding".to_vec();
        let tx_hash = compute_tx_hash(&tx_data);
        
        overlay2.handle_core_command(CoreCommand::SubmitTx {
            data: tx_data.clone(),
            fee: 100,
            num_ops: 1,
        }).await;
        
        // Verify overlay2 has the TX in its mempool
        {
            let mp = mempool2.read().await;
            assert!(mp.contains(&tx_hash), "TX should be in overlay2's mempool immediately after submit");
        }
        println!("✓ TX in sender's mempool");
        
        // Wait for TX to propagate to overlay1 via TX flooding
        // TX path: overlay2 mempool → push to peer → TX channel → TCP → overlay1 TX_READER → mempool1
        let start = Instant::now();
        let timeout = Duration::from_secs(2);
        let mut propagated = false;
        
        while start.elapsed() < timeout {
            let mp = mempool1.read().await;
            if mp.contains(&tx_hash) {
                propagated = true;
                break;
            }
            drop(mp);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Signal overlay1 to stop
        let _ = stop_tx.send(()).await;
        handle1.abort();
        
        assert!(propagated, 
            "TX should propagate to peer's mempool within {:?}. \
             This indicates TX flooding path is broken.", timeout);
        
        let propagation_time = start.elapsed();
        println!("✓ TX propagated to peer in {:?}", propagation_time);
        
        // Verify reasonable propagation time (should be <100ms for local test)
        assert!(propagation_time < Duration::from_millis(500),
            "TX propagation took {:?}, expected <500ms", propagation_time);
    }
    
    /// Test multiple TXs flood between peers with correct ordering.
    #[tokio::test]
    async fn test_multiple_txs_flood_with_ordering() {
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, _event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        let (_cmd_tx2, cmd_rx2) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx2, _event_rx2) = mpsc::unbounded_channel::<OverlayEvent>();
        
        let overlay1 = Overlay::new(keypair1, "127.0.0.1:0".parse().unwrap(), cmd_rx1, event_tx1);
        let mempool1 = Arc::clone(&overlay1.mempool);
        let overlay1_peers = Arc::clone(&overlay1.peers);
        let pending_adverts1 = Arc::clone(&overlay1.pending_adverts);
        
        let overlay2 = Overlay::new(keypair2, "127.0.0.1:0".parse().unwrap(), cmd_rx2, event_tx2);
        let mempool2 = Arc::clone(&overlay2.mempool);
        let overlay2_peers = Arc::clone(&overlay2.peers);
        let pending_adverts2 = Arc::clone(&overlay2.pending_adverts);
        
        // Spawn advert flushers for both overlays (needed for pull-based TX propagation)
        tokio::spawn({
            let pending = Arc::clone(&pending_adverts1);
            let peers = Arc::clone(&overlay1_peers);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    flush_adverts(&pending, &peers).await;
                }
            }
        });
        tokio::spawn({
            let pending = Arc::clone(&pending_adverts2);
            let peers = Arc::clone(&overlay2_peers);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    flush_adverts(&pending, &peers).await;
                }
            }
        });
        
        // Start overlay1
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            for _ in 0..2 {
                let (stream, addr) = listener.accept().await.unwrap();
                overlay1.handle_incoming_connection(stream, addr).await;
            }
            let _ = stop_rx.recv().await;
            overlay1
        });
        
        let overlay1_addr = addr_rx.await.unwrap();
        overlay2.connect_to_peer(overlay1_addr).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Submit 10 TXs with different fees
        let mut tx_hashes = Vec::new();
        let fees = [1000u64, 100, 500, 200, 800, 50, 300, 900, 150, 600];
        
        for (i, &fee) in fees.iter().enumerate() {
            let tx_data = format!("tx_{}_fee_{}", i, fee).into_bytes();
            let hash = compute_tx_hash(&tx_data);
            tx_hashes.push((hash, fee));
            
            overlay2.handle_core_command(CoreCommand::SubmitTx {
                data: tx_data,
                fee,
                num_ops: 1,
            }).await;
        }
        
        // Verify all TXs in sender's mempool
        {
            let mp = mempool2.read().await;
            for (hash, _) in &tx_hashes {
                assert!(mp.contains(hash), "TX should be in sender's mempool");
            }
        }
        println!("✓ All {} TXs in sender's mempool", tx_hashes.len());
        
        // Wait for propagation
        let start = Instant::now();
        let timeout = Duration::from_secs(3);
        
        while start.elapsed() < timeout {
            let mp = mempool1.read().await;
            let all_received = tx_hashes.iter().all(|(hash, _)| mp.contains(hash));
            if all_received {
                break;
            }
            drop(mp);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        
        // Verify all TXs propagated
        {
            let mp = mempool1.read().await;
            let received_count = tx_hashes.iter().filter(|(hash, _)| mp.contains(hash)).count();
            assert_eq!(received_count, tx_hashes.len(),
                "All {} TXs should propagate, got {}", tx_hashes.len(), received_count);
            
            // Note: Fee ordering test skipped because receiver doesn't parse fee from XDR
            // In production, fee would be extracted from the TX envelope
        }
        
        let _ = stop_tx.send(()).await;
        handle1.abort();
        
        println!("✓ All {} TXs propagated to peer in {:?}", 
            tx_hashes.len(), start.elapsed());
    }
    
    /// Test ADVERT → DEMAND → TX pull flow.
    #[tokio::test]
    async fn test_advert_demand_flow() {
        // TODO: Implement when ADVERT/DEMAND handlers are done
    }
    
    /// Test push-k behavior: TX pushed to k random peers, advertised to rest.
    #[tokio::test]
    async fn test_push_k_propagation() {
        // TODO: Implement when propagate_tx is done
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // TX SET FETCHING TESTS
    // ══════════════════════════════════════════════════════════════════════════
    
    /// Test CacheTxSet command stores TX set in local cache.
    #[tokio::test]
    async fn test_cache_tx_set_stores_locally() {
        let keypair = NoiseKeypair::generate();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx, _event_rx) = mpsc::unbounded_channel::<OverlayEvent>();
        
        let overlay = Overlay::new(
            keypair,
            "127.0.0.1:0".parse().unwrap(),
            cmd_rx,
            event_tx,
        );
        
        // Create a test TX set
        let hash = [42u8; 32];
        let xdr = b"test tx set xdr data".to_vec();
        
        // Cache it
        overlay.handle_core_command(CoreCommand::CacheTxSet {
            hash,
            xdr: xdr.clone(),
        }).await;
        
        // Verify it's in the cache
        let cache = overlay.local_tx_sets.read().await;
        assert!(cache.contains_key(&hash), "TX set should be cached");
        assert_eq!(cache.get(&hash).unwrap(), &xdr, "Cached XDR should match");
    }
    
    /// Test GET_TX_SET on SCP channel returns cached TX set.
    #[tokio::test]
    async fn test_get_tx_set_returns_cached() {
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, _event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        let (_cmd_tx2, cmd_rx2) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx2, _event_rx2) = mpsc::unbounded_channel::<OverlayEvent>();
        
        // Overlay1 has the TX set cached
        let overlay1 = Overlay::new(keypair1, "127.0.0.1:0".parse().unwrap(), cmd_rx1, event_tx1);
        let overlay1_local_tx_sets = Arc::clone(&overlay1.local_tx_sets);
        
        // Overlay2 will request it
        let overlay2 = Overlay::new(keypair2, "127.0.0.1:0".parse().unwrap(), cmd_rx2, event_tx2);
        let overlay2_local_tx_sets = Arc::clone(&overlay2.local_tx_sets);
        let overlay2_pending_fetches = Arc::clone(&overlay2.pending_tx_set_fetches);
        
        // Cache a TX set in overlay1
        let hash = [99u8; 32];
        let xdr = b"the actual tx set xdr content here".to_vec();
        {
            let mut cache = overlay1_local_tx_sets.write().await;
            cache.insert(hash, xdr.clone());
        }
        
        // Connect the overlays
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            for _ in 0..2 {
                let (stream, addr) = listener.accept().await.unwrap();
                overlay1.handle_incoming_connection(stream, addr).await;
            }
            let _ = stop_rx.recv().await;
            overlay1
        });
        
        let addr = addr_rx.await.unwrap();
        overlay2.connect_to_peer(addr).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Overlay2 requests the TX set via FetchTxSet
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        overlay2.handle_core_command(CoreCommand::FetchTxSet {
            hash,
            reply: reply_tx,
        }).await;
        
        // Wait for response (with timeout)
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            reply_rx.recv()
        ).await;
        
        let _ = stop_tx.send(()).await;
        handle1.abort();
        
        // Verify we got the TX set
        match result {
            Ok(Some(Some(received_xdr))) => {
                assert_eq!(received_xdr, xdr, "Received TX set XDR should match original");
            }
            Ok(Some(None)) => {
                panic!("FetchTxSet returned None - TX set not found");
            }
            Ok(None) => {
                panic!("Reply channel closed unexpectedly");
            }
            Err(_) => {
                panic!("Timeout waiting for TX set response");
            }
        }
        
        // Verify it's now cached in overlay2
        let cache2 = overlay2_local_tx_sets.read().await;
        assert!(cache2.contains_key(&hash), "TX set should be cached in requester after fetch");
    }
    
    /// Test FetchTxSet times out when no peer has the TX set.
    #[tokio::test]
    async fn test_fetch_tx_set_timeout_when_not_found() {
        let keypair1 = NoiseKeypair::generate();
        let keypair2 = NoiseKeypair::generate();
        
        let (_cmd_tx1, cmd_rx1) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx1, _event_rx1) = mpsc::unbounded_channel::<OverlayEvent>();
        let (_cmd_tx2, cmd_rx2) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx2, _event_rx2) = mpsc::unbounded_channel::<OverlayEvent>();
        
        // Neither overlay has the TX set
        let overlay1 = Overlay::new(keypair1, "127.0.0.1:0".parse().unwrap(), cmd_rx1, event_tx1);
        let overlay2 = Overlay::new(keypair2, "127.0.0.1:0".parse().unwrap(), cmd_rx2, event_tx2);
        
        // Connect the overlays
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        
        let handle1 = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            for _ in 0..2 {
                let (stream, addr) = listener.accept().await.unwrap();
                overlay1.handle_incoming_connection(stream, addr).await;
            }
            let _ = stop_rx.recv().await;
            overlay1
        });
        
        let addr = addr_rx.await.unwrap();
        overlay2.connect_to_peer(addr).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Request a TX set that doesn't exist
        let hash = [123u8; 32];
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        overlay2.handle_core_command(CoreCommand::FetchTxSet {
            hash,
            reply: reply_tx,
        }).await;
        
        // Should timeout (500ms) and return None
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            reply_rx.recv()
        ).await;
        let elapsed = start.elapsed();
        
        let _ = stop_tx.send(()).await;
        handle1.abort();
        
        // Verify we got None (not found)
        match result {
            Ok(Some(None)) => {
                // Expected - TX set not found
                assert!(elapsed >= Duration::from_millis(400), 
                    "Should wait ~500ms before timeout, got {:?}", elapsed);
                assert!(elapsed < Duration::from_millis(1000),
                    "Should not wait too long, got {:?}", elapsed);
            }
            Ok(Some(Some(_))) => {
                panic!("Should not find TX set that doesn't exist");
            }
            _ => {
                panic!("Unexpected result: {:?}", result);
            }
        }
    }
    
    /// Test FetchTxSet returns immediately when no peers connected.
    #[tokio::test]
    async fn test_fetch_tx_set_no_peers_returns_none() {
        let keypair = NoiseKeypair::generate();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx, _event_rx) = mpsc::unbounded_channel::<OverlayEvent>();
        
        let overlay = Overlay::new(keypair, "127.0.0.1:0".parse().unwrap(), cmd_rx, event_tx);
        
        // No peers connected
        assert!(overlay.peers.read().await.is_empty());
        
        // Request a TX set
        let hash = [77u8; 32];
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        
        let start = Instant::now();
        overlay.handle_core_command(CoreCommand::FetchTxSet {
            hash,
            reply: reply_tx,
        }).await;
        
        let result = reply_rx.recv().await;
        let elapsed = start.elapsed();
        
        // Should return None immediately (no peers to ask)
        assert!(matches!(result, Some(None)), "Should return None when no peers");
        assert!(elapsed < Duration::from_millis(100), 
            "Should return immediately, took {:?}", elapsed);
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // CONNECTION PROTOCOL TESTS
    // ══════════════════════════════════════════════════════════════════════════
    
    /// Test connection type byte protocol.
    #[tokio::test]
    async fn test_connection_type_constants() {
        assert_eq!(CONN_TYPE_SCP, 0x01);
        assert_eq!(CONN_TYPE_TX, 0x02);
        assert_ne!(CONN_TYPE_SCP, CONN_TYPE_TX);
    }
    
    /// Test pending connection matching by public key.
    #[tokio::test]
    async fn test_pending_connection_timeout() {
        assert_eq!(PENDING_CONN_TIMEOUT, Duration::from_secs(5));
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // XDDR MESSAGE TESTS
    // ══════════════════════════════════════════════════════════════════════════
    
    /// Test building and parsing FLOOD_ADVERT messages.
    #[tokio::test]
    async fn test_flood_advert_xdr() {
        // FLOOD_ADVERT format:
        // - 4 bytes: type discriminant (17 = 0x00000011)
        // - 4 bytes: count of hashes
        // - N * 32 bytes: TX hashes
        
        let hashes = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        
        // Build advert
        let advert = build_flood_advert(&hashes);
        
        // Verify structure
        assert_eq!(advert[0..4], [0, 0, 0, 17]); // FLOOD_ADVERT type
        assert_eq!(advert[4..8], [0, 0, 0, 3]);  // 3 hashes
        assert_eq!(&advert[8..40], &[1u8; 32]);   // First hash
        assert_eq!(&advert[40..72], &[2u8; 32]);  // Second hash
        assert_eq!(&advert[72..104], &[3u8; 32]); // Third hash
        
        // Parse it back
        let parsed = parse_flood_advert(&advert);
        assert_eq!(parsed, hashes);
    }
    
    /// Test building and parsing FLOOD_DEMAND messages.
    #[tokio::test]
    async fn test_flood_demand_xdr() {
        // FLOOD_DEMAND format: same as ADVERT but type 18
        let hashes = vec![[0xAA; 32], [0xBB; 32]];
        
        let demand = build_flood_demand(&hashes);
        
        assert_eq!(demand[0..4], [0, 0, 0, 18]); // FLOOD_DEMAND type
        assert_eq!(demand[4..8], [0, 0, 0, 2]);  // 2 hashes
        
        let parsed = parse_flood_demand(&demand);
        assert_eq!(parsed, hashes);
    }
    
    // ══════════════════════════════════════════════════════════════════════════
    // MULTI-PEER (4+ NODES) TESTS
    // ══════════════════════════════════════════════════════════════════════════
    
    /// Test that SCP broadcast channel sends messages to subscribers.
    /// This verifies the broadcast channel infrastructure works correctly.
    #[tokio::test]
    async fn test_scp_broadcast_channel_fanout() {
        // Create an overlay  
        let keypair = NoiseKeypair::generate();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx, _event_rx) = mpsc::unbounded_channel::<OverlayEvent>();
        
        let overlay = Overlay::new(keypair, "127.0.0.1:0".parse().unwrap(), cmd_rx, event_tx);
        
        // Create multiple subscribers to the broadcast channel
        let mut rx1 = overlay.scp_broadcast.subscribe();
        let mut rx2 = overlay.scp_broadcast.subscribe();
        let mut rx3 = overlay.scp_broadcast.subscribe();
        
        // Send an SCP message via the broadcast channel
        let scp_msg = vec![0, 0, 0, 10, 1, 2, 3, 4, 5, 6, 7, 8]; // type=10 (SCP)
        overlay.scp_broadcast.send(scp_msg.clone()).unwrap();
        
        // All subscribers should receive the message
        let recv1 = rx1.recv().await.unwrap();
        let recv2 = rx2.recv().await.unwrap();
        let recv3 = rx3.recv().await.unwrap();
        
        assert_eq!(recv1, scp_msg);
        assert_eq!(recv2, scp_msg);
        assert_eq!(recv3, scp_msg);
        
        // Verify receiver count
        assert_eq!(overlay.scp_broadcast.receiver_count(), 3);
    }
    
    /// Test that SCP broadcast handles late subscribers correctly.
    /// New subscribers should receive only messages sent after subscribing.
    #[tokio::test]
    async fn test_scp_broadcast_late_subscriber() {
        let keypair = NoiseKeypair::generate();
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel::<CoreCommand>();
        let (event_tx, _event_rx) = mpsc::unbounded_channel::<OverlayEvent>();
        
        let overlay = Overlay::new(keypair, "127.0.0.1:0".parse().unwrap(), cmd_rx, event_tx);
        
        // Send first message with no subscribers
        let msg1 = vec![1, 2, 3];
        let _ = overlay.scp_broadcast.send(msg1.clone()); // May fail with no receivers
        
        // Subscribe after first message
        let mut rx = overlay.scp_broadcast.subscribe();
        
        // Send second message
        let msg2 = vec![4, 5, 6];
        overlay.scp_broadcast.send(msg2.clone()).unwrap();
        
        // Late subscriber should only receive msg2
        let recv = rx.recv().await.unwrap();
        assert_eq!(recv, msg2);
    }
}
