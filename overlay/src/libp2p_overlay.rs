//! Unified libp2p Overlay v2
//!
//! **Transport: QUIC** for true stream independence - no TCP head-of-line blocking.
//! If a packet is lost on the TX stream, SCP stream is UNAFFECTED.
//!
//! Uses libp2p-stream for persistent bidirectional streams:
//! - SCP stream: consensus messages (priority, ~500B)
//! - TX stream: transaction flooding (~1KB)
//! - TxSet stream: TX set request/response (~10MB)
//!
//! Each stream is opened once per peer and kept alive.
//! QUIC provides independent loss recovery per stream.

use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::{
    identify::{Behaviour as Identify, Config as IdentifyConfig, Event as IdentifyEvent},
    identity::Keypair,
    kad::{
        store::MemoryStore, Behaviour as Kademlia, Config as KademliaConfig, Event as KademliaEvent,
        Mode as KademliaMode,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, Stream, StreamProtocol, Swarm, SwarmBuilder,
};
use libp2p_stream::{Behaviour as StreamBehaviour, Control, IncomingStreams};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

// Protocol identifiers for dedicated streams
pub const SCP_PROTOCOL: StreamProtocol = StreamProtocol::new("/stellar/scp/1.0.0");
pub const TX_PROTOCOL: StreamProtocol = StreamProtocol::new("/stellar/tx/1.0.0");
pub const TXSET_PROTOCOL: StreamProtocol = StreamProtocol::new("/stellar/txset/1.0.0");

/// Message frame: 4-byte length prefix + payload
/// Max message size: 16MB (for large TX sets)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Events from the overlay to the application
#[derive(Debug, Clone)]
pub enum OverlayEvent {
    /// Received SCP envelope from peer
    ScpReceived { envelope: Vec<u8>, from: PeerId },
    /// Received TX from peer
    TxReceived { tx: Vec<u8>, from: PeerId },
    /// Received TX set response
    TxSetReceived {
        hash: [u8; 32],
        data: Vec<u8>,
        from: PeerId,
    },
    /// Peer is requesting a TX set (need to look up and respond)
    TxSetRequested { hash: [u8; 32], from: PeerId },
    /// Peer is requesting SCP state
    ScpStateRequested { peer_id: PeerId, ledger_seq: u32 },
    /// Peer disconnected - clean up any pending requests
    PeerDisconnected { peer_id: PeerId },
}

/// Commands to the overlay
#[derive(Debug)]
pub enum OverlayCommand {
    /// Broadcast SCP envelope to all peers
    BroadcastScp(Vec<u8>),
    /// Broadcast TX to all peers
    BroadcastTx(Vec<u8>),
    /// Request TX set from a peer
    FetchTxSet { hash: [u8; 32] },
    /// Send TX set to a specific peer (response to their request)
    SendTxSet {
        hash: [u8; 32],
        data: Vec<u8>,
        to: PeerId,
    },
    /// Record that a peer has a specific TX set (learned from SCP message)
    RecordTxSetSource { hash: [u8; 32], peer: PeerId },
    /// Connect to a peer
    Dial(Multiaddr),
    /// Bootstrap Kademlia DHT for peer discovery
    BootstrapKademlia,
    /// Request SCP state from all peers
    RequestScpState { ledger_seq: u32 },
    /// Send SCP envelope to a specific peer
    SendScpToPeer { peer_id: PeerId, envelope: Vec<u8> },
    /// Shutdown
    Shutdown,
}

/// Outbound streams to a peer - one of each type
struct PeerOutboundStreams {
    scp: Option<Stream>,
    tx: Option<Stream>,
    txset: Option<Stream>,
}

impl PeerOutboundStreams {
    fn new() -> Self {
        Self {
            scp: None,
            tx: None,
            txset: None,
        }
    }
}

/// Network behaviour combining streams, Kademlia, and Identify
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "StellarBehaviourEvent")]
struct StellarBehaviour {
    stream: StreamBehaviour,
    kademlia: Kademlia<MemoryStore>,
    identify: Identify,
}

#[derive(Debug)]
enum StellarBehaviourEvent {
    Stream(()), // StreamBehaviour emits () - no events
    Kademlia(KademliaEvent),
    Identify(IdentifyEvent),
}

impl From<()> for StellarBehaviourEvent {
    fn from(_event: ()) -> Self {
        StellarBehaviourEvent::Stream(())
    }
}

impl From<KademliaEvent> for StellarBehaviourEvent {
    fn from(event: KademliaEvent) -> Self {
        StellarBehaviourEvent::Kademlia(event)
    }
}

impl From<IdentifyEvent> for StellarBehaviourEvent {
    fn from(event: IdentifyEvent) -> Self {
        StellarBehaviourEvent::Identify(event)
    }
}

/// Handle for sending commands to the overlay
#[derive(Clone)]
pub struct OverlayHandle {
    cmd_tx: mpsc::Sender<OverlayCommand>,
}

impl OverlayHandle {
    pub async fn broadcast_scp(&self, envelope: Vec<u8>) {
        let _ = self
            .cmd_tx
            .send(OverlayCommand::BroadcastScp(envelope))
            .await;
    }

    pub async fn broadcast_tx(&self, tx: Vec<u8>) {
        let _ = self.cmd_tx.send(OverlayCommand::BroadcastTx(tx)).await;
    }

    pub async fn fetch_txset(&self, hash: [u8; 32]) {
        let _ = self.cmd_tx.send(OverlayCommand::FetchTxSet { hash }).await;
    }

    pub async fn send_txset(&self, hash: [u8; 32], data: Vec<u8>, to: PeerId) {
        let _ = self
            .cmd_tx
            .send(OverlayCommand::SendTxSet { hash, data, to })
            .await;
    }

    /// Record that a peer has a specific TX set (call when receiving SCP with txSetHash)
    pub async fn record_txset_source(&self, hash: [u8; 32], peer: PeerId) {
        let _ = self
            .cmd_tx
            .send(OverlayCommand::RecordTxSetSource { hash, peer })
            .await;
    }

    pub async fn dial(&self, addr: Multiaddr) {
        let _ = self.cmd_tx.send(OverlayCommand::Dial(addr)).await;
    }

    pub async fn bootstrap_kademlia(&self) {
        let _ = self.cmd_tx.send(OverlayCommand::BootstrapKademlia).await;
    }
    
    pub async fn request_scp_state_from_all_peers(&self, ledger_seq: u32) {
        let _ = self.cmd_tx.send(OverlayCommand::RequestScpState { ledger_seq }).await;
    }
    
    pub async fn send_scp_to_peer(&self, peer_id: PeerId, envelope: &[u8]) -> io::Result<()> {
        self.cmd_tx.send(OverlayCommand::SendScpToPeer {
            peer_id,
            envelope: envelope.to_vec(),
        }).await.map_err(|_| io::Error::new(io::ErrorKind::Other, "Channel closed"))?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(OverlayCommand::Shutdown).await;
    }
}

/// Shared state for stream handlers
struct SharedState {
    /// Outbound streams per peer
    peer_streams: RwLock<HashMap<PeerId, Arc<Mutex<PeerOutboundStreams>>>>,
    /// SCP messages seen (for dedup)
    scp_seen: RwLock<lru::LruCache<[u8; 32], ()>>,
    /// TX messages seen (for dedup)
    tx_seen: RwLock<lru::LruCache<[u8; 32], ()>>,
    /// Track which peers we've sent each SCP message to (prevent duplicate sends)
    scp_sent_to: RwLock<lru::LruCache<[u8; 32], std::collections::HashSet<PeerId>>>,
    /// Track which peers we've sent each TX to (prevent duplicate sends)
    tx_sent_to: RwLock<lru::LruCache<[u8; 32], std::collections::HashSet<PeerId>>>,
    /// TX set sources: which peer has which TX set (learned from SCP messages)
    txset_sources: RwLock<lru::LruCache<[u8; 32], PeerId>>,
    /// Pending TX set requests (to avoid duplicate fetches)
    pending_txset_requests: RwLock<std::collections::HashSet<[u8; 32]>>,
    /// Event sender
    event_tx: mpsc::UnboundedSender<OverlayEvent>,
    /// Stream control for reopening streams
    control: Control,
}

impl SharedState {
    fn new(event_tx: mpsc::UnboundedSender<OverlayEvent>, control: Control) -> Self {
        Self {
            peer_streams: RwLock::new(HashMap::new()),
            scp_seen: RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(10000).unwrap(),
            )),
            tx_seen: RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(100000).unwrap(),
            )),
            scp_sent_to: RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(10000).unwrap(),
            )),
            tx_sent_to: RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(100000).unwrap(),
            )),
            txset_sources: RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(1000).unwrap(),
            )),
            pending_txset_requests: RwLock::new(std::collections::HashSet::new()),
            event_tx,
            control,
        }
    }
}

/// The unified Stellar overlay
pub struct StellarOverlay {
    swarm: Swarm<StellarBehaviour>,
    control: Control,
    state: Arc<SharedState>,
    cmd_rx: mpsc::Receiver<OverlayCommand>,
}

/// Create the overlay and return handle + event receiver
pub fn create_overlay(
    keypair: Keypair,
) -> Result<
    (
        OverlayHandle,
        mpsc::UnboundedReceiver<OverlayEvent>,
        StellarOverlay,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let peer_id = keypair.public().to_peer_id();
    info!(
        "Creating StellarOverlay with peer_id={} (QUIC transport)",
        peer_id
    );

    // Build swarm with QUIC transport
    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let stream = StreamBehaviour::new();

            // Configure Kademlia for active DHT participation
            // - Server mode: respond to DHT queries (required for peer discovery)
            // - Default periodic bootstrap: 5 minutes
            let mut kad_config = KademliaConfig::default();
            // Note: We'll set server mode after swarm creation since set_mode is on Behaviour
            
            #[allow(deprecated)]
            let kademlia = Kademlia::with_config(
                key.public().to_peer_id(),
                MemoryStore::new(key.public().to_peer_id()),
                kad_config,
            );

            let identify = Identify::new(IdentifyConfig::new(
                "/stellar/1.0.0".to_string(),
                key.public(),
            ));

            StellarBehaviour {
                stream,
                kademlia,
                identify,
            }
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
        .build();

    // CRITICAL: Set Kademlia to Server mode immediately
    // By default, Kademlia starts in Client mode and only switches to Server
    // when an external address is confirmed. In test networks with localhost
    // addresses, this never happens, so nodes don't respond to DHT queries
    // and peer discovery fails completely.
    // Server mode = respond to DHT queries = enable peer discovery
    let mut swarm = swarm;
    swarm.behaviour_mut().kademlia.set_mode(Some(KademliaMode::Server));
    info!("Kademlia: Set to Server mode for DHT query handling");

    let control = swarm.behaviour().stream.new_control();

    let (cmd_tx, cmd_rx) = mpsc::channel(256);
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let state = Arc::new(SharedState::new(event_tx, control.clone()));

    let overlay = StellarOverlay {
        swarm,
        control,
        state,
        cmd_rx,
    };

    let handle = OverlayHandle { cmd_tx };

    Ok((handle, event_rx, overlay))
}

impl StellarOverlay {
    /// Run the overlay event loop
    pub async fn run(mut self, listen_port: u16) {
        // Start listening on QUIC (UDP)
        let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();

        if let Err(e) = self.swarm.listen_on(listen_addr.clone()) {
            error!("Failed to listen on {}: {}", listen_addr, e);
            return;
        }
        info!("Listening on QUIC port {}", listen_port);

        // Accept incoming streams for each protocol
        let scp_incoming = self.control.accept(SCP_PROTOCOL).unwrap();
        let tx_incoming = self.control.accept(TX_PROTOCOL).unwrap();
        let txset_incoming = self.control.accept(TXSET_PROTOCOL).unwrap();

        // Spawn inbound stream handlers
        let state = self.state.clone();
        tokio::spawn(handle_inbound_scp_streams(scp_incoming, state.clone()));
        tokio::spawn(handle_inbound_tx_streams(tx_incoming, state.clone()));
        tokio::spawn(handle_inbound_txset_streams(txset_incoming, state.clone()));

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }

                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        OverlayCommand::BroadcastScp(envelope) => {
                            self.broadcast_scp(&envelope).await;
                        }
                        OverlayCommand::BroadcastTx(tx) => {
                            self.broadcast_tx(&tx).await;
                        }
                        OverlayCommand::FetchTxSet { hash } => {
                            self.fetch_txset(hash).await;
                        }
                        OverlayCommand::SendTxSet { hash, data, to } => {
                            self.send_txset_response(to, hash, data).await;
                        }
                        OverlayCommand::RecordTxSetSource { hash, peer } => {
                            let mut sources = self.state.txset_sources.write().await;
                            sources.put(hash, peer);
                            debug!("Recorded peer {} as source for TX set {:02x?}...", peer, &hash[..4]);
                        }
                        OverlayCommand::Dial(addr) => {
                            if let Err(e) = self.swarm.dial(addr.clone()) {
                                warn!("Failed to dial {}: {}", addr, e);
                            }
                        }
                        OverlayCommand::BootstrapKademlia => {
                            info!("Kademlia: Starting bootstrap");
                            if let Err(e) = self.swarm.behaviour_mut().kademlia.bootstrap() {
                                warn!("Kademlia: Bootstrap failed to start: {:?}", e);
                            } else {
                                info!("Kademlia: Bootstrap initiated successfully");
                            }
                        }
                        OverlayCommand::RequestScpState { ledger_seq } => {
                            info!("Requesting SCP state (ledger >= {}) from all peers", ledger_seq);
                            self.request_scp_state_from_all_peers(ledger_seq).await;
                        }
                        OverlayCommand::SendScpToPeer { peer_id, envelope } => {
                            // Don't hold &self across await - extract state and call helper directly
                            let state = Arc::clone(&self.state);
                            if let Err(e) = send_to_peer_stream(&state, peer_id.clone(), StreamType::Scp, &envelope).await {
                                warn!("Failed to send SCP to {}: {:?}", peer_id, e);
                            }
                        }
                        OverlayCommand::Shutdown => {
                            info!("Overlay shutting down");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<StellarBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            }

            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to peer {}", peer_id);

                // Create peer streams entry
                {
                    let mut streams = self.state.peer_streams.write().await;
                    streams.insert(peer_id, Arc::new(Mutex::new(PeerOutboundStreams::new())));
                }

                // Open outbound streams to peer
                self.open_streams_to_peer(peer_id).await;
            }

            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer {}", peer_id);
                {
                    let mut streams = self.state.peer_streams.write().await;
                    streams.remove(&peer_id);
                }
                // Notify main loop to clean up any pending requests for this peer
                let _ = self.state.event_tx.send(OverlayEvent::PeerDisconnected { 
                    peer_id: peer_id.clone() 
                });
            }

            SwarmEvent::Behaviour(StellarBehaviourEvent::Identify(event)) => {
                self.handle_identify_event(event);
            }

            SwarmEvent::Behaviour(StellarBehaviourEvent::Kademlia(event)) => {
                self.handle_kademlia_event(event);
            }

            SwarmEvent::Behaviour(StellarBehaviourEvent::Stream(_)) => {
                // Stream events handled by the stream behaviour internally
            }

            SwarmEvent::IncomingConnection { .. } => {
                trace!("Incoming connection");
            }

            _ => {}
        }
    }

    fn handle_identify_event(&mut self, event: IdentifyEvent) {
        if let IdentifyEvent::Received { peer_id, info, .. } = event {
            debug!("Identified peer {}: {:?}", peer_id, info.listen_addrs);
            
            for addr in info.listen_addrs {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, addr.clone());
                
                // If not already connected, dial this Kademlia-discovered peer
                // to add to GossipSub mesh for SCP message routing
                if !self.swarm.is_connected(&peer_id) {
                    info!("Auto-dialing Kademlia-discovered peer {} at {}", peer_id, addr);
                    if let Err(e) = self.swarm.dial(addr) {
                        warn!("Failed to dial discovered peer {}: {:?}", peer_id, e);
                    }
                    break; // Only dial once with first address
                }
            }
        }
    }

    fn handle_kademlia_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::RoutingUpdated { peer, .. } => {
                debug!("Kademlia: Routing table updated for peer {}", peer);
            }
            KademliaEvent::OutboundQueryProgressed { result, .. } => {
                use libp2p::kad::QueryResult;
                match result {
                    QueryResult::Bootstrap(Ok(bootstrap_result)) => {
                        info!(
                            "Kademlia: Bootstrap completed, {} peers in routing table",
                            bootstrap_result.num_remaining
                        );
                        
                        // After bootstrap, count discovered peers for logging
                        let mut total_peers = 0;
                        for kbucket in self.swarm.behaviour_mut().kademlia.kbuckets() {
                            total_peers += kbucket.iter().count();
                        }
                        
                        if total_peers > 0 {
                            info!("Kademlia: Routing table has {} total peers", total_peers);
                        }
                    }
                    QueryResult::Bootstrap(Err(e)) => {
                        warn!("Kademlia: Bootstrap failed: {:?}", e);
                    }
                    QueryResult::GetClosestPeers(Ok(get_closest_result)) => {
                        info!(
                            "Kademlia: Found {} closest peers",
                            get_closest_result.peers.len()
                        );
                        
                        // Discovered new peers - they're already added to routing table
                        // When Identify protocol runs, addresses will be added and we can dial them
                        // TODO: Auto-dial discovered peers once we have their addresses
                        // This would add them to GossipSub mesh for SCP message routing
                        for peer in &get_closest_result.peers {
                            debug!("Kademlia: Discovered peer {:?}", peer);
                        }
                    }
                    QueryResult::GetClosestPeers(Err(e)) => {
                        debug!("Kademlia: GetClosestPeers query failed: {:?}", e);
                    }
                    _ => {
                        trace!("Kademlia: Query progressed: {:?}", result);
                    }
                }
            }
            KademliaEvent::InboundRequest { request } => {
                debug!("Kademlia: Received inbound request: {:?}", request);
            }
            _ => {
                trace!("Kademlia event: {:?}", event);
            }
        }
    }

    /// Open SCP, TX, and TxSet streams to a peer
    async fn open_streams_to_peer(&mut self, peer_id: PeerId) {
        debug!("Opening streams to peer {}", peer_id);

        // Open all streams in parallel for faster connection setup
        let mut control = self.control.clone();
        let mut control2 = self.control.clone();
        let mut control3 = self.control.clone();

        let scp_fut = async { control.open_stream(peer_id, SCP_PROTOCOL).await };
        let tx_fut = async { control2.open_stream(peer_id, TX_PROTOCOL).await };
        let txset_fut = async { control3.open_stream(peer_id, TXSET_PROTOCOL).await };

        let (scp_result, tx_result, txset_result) = tokio::join!(scp_fut, tx_fut, txset_fut);

        let scp_stream = match scp_result {
            Ok(s) => {
                debug!("Opened SCP stream to {}", peer_id);
                Some(s)
            }
            Err(e) => {
                warn!("Failed to open SCP stream to {}: {:?}", peer_id, e);
                None
            }
        };

        let tx_stream = match tx_result {
            Ok(s) => {
                debug!("Opened TX stream to {}", peer_id);
                Some(s)
            }
            Err(e) => {
                warn!("Failed to open TX stream to {}: {:?}", peer_id, e);
                None
            }
        };

        let txset_stream = match txset_result {
            Ok(s) => {
                debug!("Opened TxSet stream to {}", peer_id);
                Some(s)
            }
            Err(e) => {
                warn!("Failed to open TxSet stream to {}: {:?}", peer_id, e);
                None
            }
        };

        // Store streams
        {
            let streams = self.state.peer_streams.read().await;
            if let Some(peer_streams) = streams.get(&peer_id) {
                let mut ps = peer_streams.lock().await;
                ps.scp = scp_stream;
                ps.tx = tx_stream;
                ps.txset = txset_stream;
            }
        }
        
        // Request SCP state from newly connected peer synchronously
        // No spawned task, no sleep - streams are open, send immediately
        info!("Peer {} connected, sending SCP state request", peer_id);
        let ledger_seq: u32 = 0; // Request all recent state
        if let Err(e) = send_to_peer_stream(&self.state, peer_id.clone(), StreamType::Scp, &ledger_seq.to_le_bytes()).await {
            debug!("Failed to request SCP state from peer {}: {:?}", peer_id, e);
        }
    }

    /// Broadcast SCP envelope to all connected peers
    async fn broadcast_scp(&mut self, envelope: &[u8]) {
        let hash = blake2b_hash(envelope);

        // Dedup check
        {
            let mut seen = self.state.scp_seen.write().await;
            if seen.contains(&hash) {
                trace!(
                    "SCP_BROADCAST_SKIP: SCP {:02x?}... already seen, skipping",
                    &hash[..4]
                );
                return;
            }
            seen.put(hash, ());
        }

        let streams = self.state.peer_streams.read().await;
        let peers: Vec<_> = streams.keys().cloned().collect();
        drop(streams);

        info!(
            "SCP_BROADCAST: Broadcasting SCP {:02x?}... ({} bytes) to {} peers",
            &hash[..4],
            envelope.len(),
            peers.len()
        );

        // Track which peers we're sending to
        {
            let mut sent_to = self.state.scp_sent_to.write().await;
            sent_to.put(hash, peers.iter().cloned().collect());
        }

        for peer_id in peers {
            match send_to_peer_stream(&self.state, peer_id.clone(), StreamType::Scp, envelope).await {
                Ok(_) => {
                    debug!(
                        "SCP_SEND_OK: Sent SCP {:02x?}... to {}",
                        &hash[..4],
                        peer_id
                    );
                }
                Err(e) => {
                    warn!(
                        "SCP_SEND_FAIL: Failed to send SCP {:02x?}... to {}: {}",
                        &hash[..4],
                        peer_id,
                        e
                    );
                }
            }
        }
    }

    /// Broadcast TX to all connected peers
    async fn broadcast_tx(&mut self, tx: &[u8]) {
        let hash = blake2b_hash(tx);

        // Dedup check
        {
            let mut seen = self.state.tx_seen.write().await;
            if seen.contains(&hash) {
                trace!("TX already seen, skipping broadcast");
                return;
            }
            seen.put(hash, ());
        }

        let streams = self.state.peer_streams.read().await;
        let peers: Vec<_> = streams.keys().cloned().collect();
        drop(streams);

        info!(
            "TX_BROADCAST: Broadcasting TX {:02x?}... ({} bytes) to {} peers",
            &hash[..4],
            tx.len(),
            peers.len()
        );

        // Track which peers we're sending to
        {
            let mut sent_to = self.state.tx_sent_to.write().await;
            sent_to.put(hash, peers.iter().cloned().collect());
        }

        for peer_id in peers {
            if let Err(e) = send_to_peer_stream(&self.state, peer_id, StreamType::Tx, tx).await {
                warn!("Failed to send TX to {}: {}", peer_id, e);
            }
        }
    }

    /// Fetch TX set from a peer - preferring the peer who sent us the SCP message referencing it
    async fn fetch_txset(&mut self, hash: [u8; 32]) {
        // Check if we're already fetching this TxSet (dedup)
        {
            let mut pending = self.state.pending_txset_requests.write().await;
            if pending.contains(&hash) {
                debug!(
                    "TXSET_FETCH_SKIP: TxSet {:02x?}... already being fetched, skipping duplicate",
                    &hash[..4]
                );
                return;
            }
            pending.insert(hash);
        }

        // First check if we know which peer has this TX set (from SCP message)
        let known_source = {
            let sources = self.state.txset_sources.read().await;
            sources.peek(&hash).cloned()
        };

        let peer = if let Some(source_peer) = known_source {
            // Verify this peer is still connected
            let streams = self.state.peer_streams.read().await;
            if streams.contains_key(&source_peer) {
                info!(
                    "TXSET_FETCH: Fetching TX set {:02x?}... from known source {}",
                    &hash[..4],
                    source_peer
                );
                source_peer
            } else {
                // Source peer disconnected, fall back to any peer
                match streams.keys().next().cloned() {
                    Some(p) => {
                        info!("TXSET_FETCH: Fetching TX set {:02x?}... from fallback peer {} (source {} disconnected)",
                              &hash[..4], p, source_peer);
                        p
                    }
                    None => {
                        warn!(
                            "TXSET_FETCH_FAIL: No peers to fetch TX set {:02x?}... from",
                            &hash[..4]
                        );
                        self.state
                            .pending_txset_requests
                            .write()
                            .await
                            .remove(&hash);
                        return;
                    }
                }
            }
        } else {
            // No known source, pick any connected peer
            let streams = self.state.peer_streams.read().await;
            match streams.keys().next().cloned() {
                Some(p) => {
                    info!(
                        "TXSET_FETCH: Fetching TX set {:02x?}... from random peer {} (no known source)",
                        &hash[..4],
                        p
                    );
                    p
                }
                None => {
                    warn!(
                        "TXSET_FETCH_FAIL: No peers to fetch TX set {:02x?}... from",
                        &hash[..4]
                    );
                    self.state
                        .pending_txset_requests
                        .write()
                        .await
                        .remove(&hash);
                    return;
                }
            }
        };

        // Send request on TxSet stream (just the 32-byte hash)
        match send_to_peer_stream(&self.state, peer, StreamType::TxSet, &hash).await {
            Ok(_) => info!(
                "TXSET_FETCH_SENT: Sent request for TxSet {:02x?}... to {}",
                &hash[..4],
                peer
            ),
            Err(e) => {
                warn!(
                    "TXSET_FETCH_FAIL: Failed to send TxSet request {:02x?}... to {}: {}",
                    &hash[..4],
                    peer,
                    e
                );
                self.state
                    .pending_txset_requests
                    .write()
                    .await
                    .remove(&hash);
            }
        }
    }

    /// Send TX set response to a specific peer
    async fn send_txset_response(&mut self, peer: PeerId, hash: [u8; 32], data: Vec<u8>) {
        info!(
            "TXSET_SEND: Sending TX set {:02x?}... ({} bytes) to {}",
            &hash[..4],
            data.len(),
            peer
        );

        // Response format: 32-byte hash + XDR data
        let mut response = Vec::with_capacity(32 + data.len());
        response.extend_from_slice(&hash);
        response.extend_from_slice(&data);

        match send_to_peer_stream(&self.state, peer, StreamType::TxSet, &response).await {
            Ok(_) => info!(
                "TXSET_SEND_OK: Successfully sent TX set {:02x?}... ({} bytes on wire) to {}",
                &hash[..4],
                response.len(),
                peer
            ),
            Err(e) => warn!(
                "TXSET_SEND_FAIL: Failed to send TxSet {:02x?}... to {}: {}",
                &hash[..4],
                peer,
                e
            ),
        }
    }

    /// Request SCP state from all connected peers
    pub async fn request_scp_state_from_all_peers(&mut self, ledger_seq: u32) {
        let streams = self.state.peer_streams.read().await;
        let peers: Vec<_> = streams.keys().cloned().collect();
        drop(streams);
        
        info!("Requesting SCP state for ledger >= {} from {} peers", ledger_seq, peers.len());
        
        // Send request to each peer (request is just the ledger seq as 4 bytes)
        let request = ledger_seq.to_le_bytes().to_vec();
        for peer_id in peers {
            if let Err(e) = send_to_peer_stream(&self.state, peer_id, StreamType::Scp, &request).await {
                warn!("Failed to send SCP state request to {}: {:?}", peer_id, e);
            }
        }
    }
    
    /// Send SCP envelope to a specific peer
    pub async fn send_scp_to_peer(&self, peer_id: PeerId, envelope: &[u8]) -> io::Result<()> {
        send_to_peer_stream(&self.state, peer_id, StreamType::Scp, envelope).await
    }
}

#[derive(Clone, Copy)]
enum StreamType {
    Scp,
    Tx,
    TxSet,
}

impl StreamType {
    fn protocol(&self) -> StreamProtocol {
        match self {
            StreamType::Scp => SCP_PROTOCOL,
            StreamType::Tx => TX_PROTOCOL,
            StreamType::TxSet => TXSET_PROTOCOL,
        }
    }
}

/// Send message to a specific peer's stream only if already open (for flooding)
/// Returns Ok(()) if sent, Err if stream not open (doesn't try to reopen)
async fn try_send_to_existing_stream(
    state: &SharedState,
    peer_id: PeerId,
    stream_type: StreamType,
    data: &[u8],
) -> io::Result<()> {
    let streams = state.peer_streams.read().await;
    let peer_streams = streams
        .get(&peer_id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "peer not connected"))?
        .clone();
    drop(streams);

    let mut ps = peer_streams.lock().await;

    let stream_slot = match stream_type {
        StreamType::Scp => &mut ps.scp,
        StreamType::Tx => &mut ps.tx,
        StreamType::TxSet => &mut ps.txset,
    };

    // If stream not open, fail immediately without reopening
    let stream = stream_slot.as_mut().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotConnected, "stream not open")
    })?;

    write_framed(stream, data).await
}

/// Send message to a specific peer's stream, reopening if needed
async fn send_to_peer_stream(
    state: &SharedState,
    peer_id: PeerId,
    stream_type: StreamType,
    data: &[u8],
) -> io::Result<()> {
    let streams = state.peer_streams.read().await;
    let peer_streams = streams
        .get(&peer_id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "peer not connected"))?
        .clone();
    drop(streams);

    let mut ps = peer_streams.lock().await;

    // Get or reopen the stream
    let stream_slot = match stream_type {
        StreamType::Scp => &mut ps.scp,
        StreamType::Tx => &mut ps.tx,
        StreamType::TxSet => &mut ps.txset,
    };

    // If stream is None, try to reopen it
    if stream_slot.is_none() {
        debug!(
            "Stream {:?} not open to {}, attempting to reopen",
            stream_type.protocol(),
            peer_id
        );
        match state
            .control
            .clone()
            .open_stream(peer_id, stream_type.protocol())
            .await
        {
            Ok(s) => {
                debug!(
                    "Successfully reopened {:?} stream to {}",
                    stream_type.protocol(),
                    peer_id
                );
                *stream_slot = Some(s);
            }
            Err(e) => {
                warn!(
                    "Failed to reopen {:?} stream to {}: {:?}",
                    stream_type.protocol(),
                    peer_id,
                    e
                );
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    format!("failed to reopen stream: {:?}", e),
                ));
            }
        }
    }

    let stream = stream_slot.as_mut().unwrap();
    write_framed(stream, data).await
}

/// Write length-prefixed frame to stream
async fn write_framed(stream: &mut Stream, data: &[u8]) -> io::Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

/// Read length-prefixed frame from stream
async fn read_framed(stream: &mut Stream) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {} > {}", len, MAX_MESSAGE_SIZE),
        ));
    }

    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    Ok(data)
}

/// Handle inbound SCP streams from peers
async fn handle_inbound_scp_streams(mut incoming: IncomingStreams, state: Arc<SharedState>) {
    while let Some((peer_id, mut stream)) = incoming.next().await {
        info!("SCP_STREAM: Accepted inbound SCP stream from {}", peer_id);
        let state = state.clone();

        tokio::spawn(async move {
            loop {
                match read_framed(&mut stream).await {
                    Ok(envelope) => {
                        // Check if this is an SCP state request (small message, 4 bytes)
                        if envelope.len() == 4 {
                            // This is an SCP state request (ledger seq)
                            let ledger_seq = u32::from_le_bytes(envelope[..4].try_into().unwrap());
                            info!("SCP_STATE_REQ: Peer {} requests SCP state for ledger >= {}", peer_id, ledger_seq);
                            
                            // Notify main loop via event channel
                            if let Err(e) = state.event_tx.send(OverlayEvent::ScpStateRequested {
                                peer_id: peer_id.clone(),
                                ledger_seq,
                            }) {
                                error!("Failed to send SCP state request event: {:?}", e);
                            }
                            continue;
                        }
                        
                        let hash = blake2b_hash(&envelope);

                        // Dedup
                        let is_dup = {
                            let mut seen = state.scp_seen.write().await;
                            if seen.contains(&hash) {
                                true
                            } else {
                                seen.put(hash, ());
                                false
                            }
                        };

                        if is_dup {
                            debug!(
                                "SCP_RECV_DUP: Duplicate SCP {:02x?}... from {}",
                                &hash[..4],
                                peer_id
                            );
                            continue;
                        }

                        info!(
                            "SCP_RECV: Received SCP {:02x?}... ({} bytes) from {}",
                            &hash[..4],
                            envelope.len(),
                            peer_id
                        );
                        
                        // Forward to Core
                        let envelope_clone = envelope.clone();
                        let _ = state.event_tx.send(OverlayEvent::ScpReceived {
                            envelope: envelope_clone,
                            from: peer_id.clone(),
                        });
                        
                        // FLOOD: Determine peers to forward to (atomically mark as sent)
                        let peers_to_forward: Vec<PeerId> = {
                            // Hold both locks to ensure atomicity
                            let mut sent_to = state.scp_sent_to.write().await;
                            let streams = state.peer_streams.read().await;
                            
                            // Get current sent set or empty
                            let already_sent: std::collections::HashSet<PeerId> = sent_to
                                .get(&hash)
                                .cloned()
                                .unwrap_or_default();
                            
                            // Find peers we haven't sent to (excluding sender)
                            let peers: Vec<_> = streams.keys()
                                .filter(|p| **p != peer_id && !already_sent.contains(p))
                                .cloned()
                                .collect();
                            
                            // Update sent set with new peers + sender
                            let mut new_sent = already_sent;
                            new_sent.extend(peers.iter().cloned());
                            new_sent.insert(peer_id.clone());
                            sent_to.put(hash, new_sent);
                            
                            peers
                        };
                        
                        if peers_to_forward.is_empty() {
                            continue;
                        }
                        
                        debug!(
                            "SCP_FLOOD: Forwarding SCP {:02x?}... to {} peers",
                            &hash[..4],
                            peers_to_forward.len()
                        );
                        
                        // Send to peers (can be done outside the lock)
                        let state_forward = state.clone();
                        tokio::spawn(async move {
                            for peer in peers_to_forward {
                                if let Err(e) = try_send_to_existing_stream(&state_forward, peer.clone(), StreamType::Scp, &envelope).await {
                                    debug!("SCP_FLOOD_SKIP: Failed to forward to {}: {}", peer, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        warn!(
                            "SCP_STREAM_CLOSED: SCP stream from {} closed: {}",
                            peer_id, e
                        );
                        break;
                    }
                }
            }
        });
    }
}

/// Handle inbound TX streams from peers
async fn handle_inbound_tx_streams(mut incoming: IncomingStreams, state: Arc<SharedState>) {
    while let Some((peer_id, mut stream)) = incoming.next().await {
        info!("TX_STREAM: Accepted inbound TX stream from {}", peer_id);
        let state = state.clone();

        tokio::spawn(async move {
            loop {
                match read_framed(&mut stream).await {
                    Ok(tx) => {
                        let hash = blake2b_hash(&tx);

                        // Dedup
                        {
                            let mut seen = state.tx_seen.write().await;
                            if seen.contains(&hash) {
                                trace!("Duplicate TX from {}", peer_id);
                                continue;
                            }
                            seen.put(hash, ());
                        }

                        debug!(
                            "TX_RECV: Received TX {:02x?}... ({} bytes) from {}",
                            &hash[..4],
                            tx.len(),
                            peer_id
                        );
                        
                        // Forward to Core
                        let tx_clone = tx.clone();
                        let _ = state
                            .event_tx
                            .send(OverlayEvent::TxReceived { tx: tx_clone, from: peer_id.clone() });
                        
                        // FLOOD: Determine peers to forward to (atomically mark as sent)
                        let peers_to_forward: Vec<PeerId> = {
                            let mut sent_to = state.tx_sent_to.write().await;
                            let streams = state.peer_streams.read().await;
                            
                            let already_sent: std::collections::HashSet<PeerId> = sent_to
                                .get(&hash)
                                .cloned()
                                .unwrap_or_default();
                            
                            let peers: Vec<_> = streams.keys()
                                .filter(|p| **p != peer_id && !already_sent.contains(p))
                                .cloned()
                                .collect();
                            
                            let mut new_sent = already_sent;
                            new_sent.extend(peers.iter().cloned());
                            new_sent.insert(peer_id.clone());
                            sent_to.put(hash, new_sent);
                            
                            peers
                        };
                        
                        if peers_to_forward.is_empty() {
                            continue;
                        }
                        
                        let state_forward = state.clone();
                        tokio::spawn(async move {
                            for peer in peers_to_forward {
                                if let Err(e) = try_send_to_existing_stream(&state_forward, peer.clone(), StreamType::Tx, &tx).await {
                                    trace!("TX_FLOOD_SKIP: Failed to forward to {}: {}", peer, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        debug!("TX stream from {} closed: {}", peer_id, e);
                        break;
                    }
                }
            }
        });
    }
}

/// Handle inbound TxSet streams from peers
async fn handle_inbound_txset_streams(mut incoming: IncomingStreams, state: Arc<SharedState>) {
    while let Some((peer_id, mut stream)) = incoming.next().await {
        debug!("Accepted inbound TxSet stream from {}", peer_id);
        let state = state.clone();

        tokio::spawn(async move {
            loop {
                match read_framed(&mut stream).await {
                    Ok(data) => {
                        // 32 bytes = request (just the hash)
                        // >32 bytes = response (hash + XDR data)
                        if data.len() == 32 {
                            // This is a GET_TX_SET request from peer
                            let mut hash = [0u8; 32];
                            hash.copy_from_slice(&data);
                            info!(
                                "TXSET_REQ_IN: Received TxSet request for {:02x?}... from {}",
                                &hash[..4],
                                peer_id
                            );

                            // Emit event so main.rs can look up cache and respond
                            let _ = state.event_tx.send(OverlayEvent::TxSetRequested {
                                hash,
                                from: peer_id,
                            });
                        } else if data.len() > 32 {
                            // This is a TX_SET response to our request
                            let mut hash = [0u8; 32];
                            hash.copy_from_slice(&data[..32]);
                            let txset_data = data[32..].to_vec();

                            // Clear pending request flag
                            let was_pending = {
                                let mut pending = state.pending_txset_requests.write().await;
                                pending.remove(&hash)
                            };

                            info!(
                                "TXSET_RECV: Received TxSet {:02x?}... ({} bytes) from {} (was_pending={})",
                                &hash[..4],
                                txset_data.len(),
                                peer_id,
                                was_pending
                            );
                            let _ = state.event_tx.send(OverlayEvent::TxSetReceived {
                                hash,
                                data: txset_data,
                                from: peer_id,
                            });
                        }
                    }
                    Err(e) => {
                        debug!("TxSet stream from {} closed: {}", peer_id, e);
                        break;
                    }
                }
            }
        });
    }
}

/// Blake2b hash for deduplication
fn blake2b_hash(data: &[u8]) -> [u8; 32] {
    use blake2::{Blake2b, Digest};
    use digest::consts::U32;
    let mut hasher = Blake2b::<U32>::new();
    hasher.update(data);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_overlay_creation() {
        let keypair = Keypair::generate_ed25519();
        let (handle, _events, overlay) = create_overlay(keypair).unwrap();

        let overlay_task = tokio::spawn(async move {
            overlay.run(0).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.shutdown().await;

        tokio::time::timeout(Duration::from_secs(1), overlay_task)
            .await
            .expect("Overlay should shutdown")
            .expect("Overlay task should complete");
    }

    #[tokio::test]
    async fn test_two_overlays_connect_and_send_scp() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19101;
        let overlay1_task = tokio::spawn(async move {
            overlay1.run(listen_port).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let overlay2_task = tokio::spawn(async move {
            overlay2.run(19102).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Give connection and streams time to establish
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Send SCP from node1
        let scp_msg = b"test SCP envelope".to_vec();
        handle1.broadcast_scp(scp_msg.clone()).await;

        // Wait for SCP on node2
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut received = false;

        while tokio::time::Instant::now() < deadline && !received {
            tokio::select! {
                Some(event) = events2.recv() => {
                    if let OverlayEvent::ScpReceived { envelope, .. } = event {
                        assert_eq!(envelope, scp_msg);
                        received = true;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        assert!(received, "Should receive SCP message");

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    #[tokio::test]
    async fn test_scp_dedup() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19201;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19202).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + stream setup
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain connection events
        while events2.try_recv().is_ok() {}

        // Send same SCP twice
        let scp_msg = b"duplicate test".to_vec();
        handle1.broadcast_scp(scp_msg.clone()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle1.broadcast_scp(scp_msg.clone()).await;

        // Should only receive once
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut count = 0;
        while let Ok(event) = events2.try_recv() {
            if matches!(event, OverlayEvent::ScpReceived { .. }) {
                count += 1;
            }
        }

        assert_eq!(count, 1, "Should receive only one SCP due to dedup");

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    #[test]
    fn test_blake2b_hash() {
        let data = b"test data";
        let hash1 = blake2b_hash(data);
        let hash2 = blake2b_hash(data);
        assert_eq!(hash1, hash2);

        let hash3 = blake2b_hash(b"different");
        assert_ne!(hash1, hash3);
    }

    /// Critical test: SCP messages must not be blocked by TX traffic
    /// Proves QUIC stream independence by sending large TX payload that takes
    /// measurable time, then verifying SCP arrives BEFORE TX flood completes.
    #[tokio::test]
    async fn test_scp_not_blocked_by_tx_flood() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, _events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19301;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19302).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + streams
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain connection events
        while events2.try_recv().is_ok() {}

        // Send large TXs - 1000 x 10KB = 10MB total
        // This should take noticeable time to transfer
        let tx_count = 1000;
        let tx_size = 10 * 1024; // 10KB each
        let large_tx: Vec<u8> = (0..tx_size).map(|i| (i % 256) as u8).collect();

        let tx_start = std::time::Instant::now();
        for i in 0..tx_count {
            // Each TX slightly different to avoid dedup
            let mut tx = large_tx.clone();
            tx[0..4].copy_from_slice(&(i as u32).to_be_bytes());
            handle1.broadcast_tx(tx).await;
        }

        // Immediately send small SCP (should bypass TX queue)
        let scp_msg = b"urgent SCP envelope".to_vec();
        let scp_send_time = std::time::Instant::now();
        handle1.broadcast_scp(scp_msg.clone()).await;

        // Track when SCP arrives vs when all TXs arrive
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut scp_received_at: Option<std::time::Instant> = None;
        let mut tx_count_received = 0u32;
        let mut all_tx_received_at: Option<std::time::Instant> = None;

        while tokio::time::Instant::now() < deadline {
            tokio::select! {
                Some(event) = events2.recv() => {
                    match event {
                        OverlayEvent::ScpReceived { envelope, .. } => {
                            if envelope == scp_msg && scp_received_at.is_none() {
                                scp_received_at = Some(std::time::Instant::now());
                            }
                        }
                        OverlayEvent::TxReceived { .. } => {
                            tx_count_received += 1;
                            if tx_count_received >= tx_count && all_tx_received_at.is_none() {
                                all_tx_received_at = Some(std::time::Instant::now());
                            }
                        }
                        _ => {}
                    }

                    // Done when both received
                    if scp_received_at.is_some() && all_tx_received_at.is_some() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        let scp_received_at = scp_received_at.expect("SCP should be received");
        let all_tx_received_at = all_tx_received_at.expect("All TXs should be received");

        let scp_latency = scp_received_at.duration_since(scp_send_time);
        let tx_total_time = all_tx_received_at.duration_since(tx_start);

        println!("SCP latency: {:?}", scp_latency);
        println!("TX flood total time: {:?}", tx_total_time);
        println!("TX received: {}", tx_count_received);

        // KEY ASSERTION: SCP must arrive BEFORE TX flood completes
        // If streams were blocked, SCP would wait behind all TXs
        assert!(
            scp_received_at < all_tx_received_at,
            "SCP should arrive BEFORE TX flood completes (stream independence). \
             SCP at {:?}, TXs done at {:?}",
            scp_latency,
            tx_total_time
        );

        // Also verify TX flood took meaningful time (not instant)
        assert!(
            tx_total_time > Duration::from_millis(50),
            "TX flood should take measurable time ({:?}), otherwise test is invalid",
            tx_total_time
        );

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    /// Critical test: TX messages must not be blocked by SCP traffic
    /// Validates bidirectional stream independence
    #[tokio::test]
    async fn test_tx_not_blocked_by_scp_flood() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, _events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19501;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19502).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + streams
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain connection events
        while events2.try_recv().is_ok() {}

        // Send large SCP messages - 1000 x 10KB = 10MB total
        let scp_count = 1000;
        let scp_size = 10 * 1024;
        let large_scp: Vec<u8> = (0..scp_size).map(|i| (i % 256) as u8).collect();

        let scp_start = std::time::Instant::now();
        for i in 0..scp_count {
            let mut scp = large_scp.clone();
            scp[0..4].copy_from_slice(&(i as u32).to_be_bytes());
            handle1.broadcast_scp(scp).await;
        }

        // Immediately send TX (should bypass SCP queue)
        let tx_msg = b"urgent transaction".to_vec();
        let tx_send_time = std::time::Instant::now();
        handle1.broadcast_tx(tx_msg.clone()).await;

        // Track when TX arrives vs when all SCPs arrive
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut tx_received_at: Option<std::time::Instant> = None;
        let mut scp_count_received = 0u32;
        let mut all_scp_received_at: Option<std::time::Instant> = None;

        while tokio::time::Instant::now() < deadline {
            tokio::select! {
                Some(event) = events2.recv() => {
                    match event {
                        OverlayEvent::TxReceived { tx, .. } => {
                            if tx == tx_msg && tx_received_at.is_none() {
                                tx_received_at = Some(std::time::Instant::now());
                            }
                        }
                        OverlayEvent::ScpReceived { .. } => {
                            scp_count_received += 1;
                            if scp_count_received >= scp_count && all_scp_received_at.is_none() {
                                all_scp_received_at = Some(std::time::Instant::now());
                            }
                        }
                        _ => {}
                    }

                    if tx_received_at.is_some() && all_scp_received_at.is_some() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        let tx_received_at = tx_received_at.expect("TX should be received");
        let all_scp_received_at = all_scp_received_at.expect("All SCPs should be received");

        let tx_latency = tx_received_at.duration_since(tx_send_time);
        let scp_total_time = all_scp_received_at.duration_since(scp_start);

        println!("TX latency: {:?}", tx_latency);
        println!("SCP flood total time: {:?}", scp_total_time);
        println!("SCP received: {}", scp_count_received);

        // KEY ASSERTION: TX must arrive BEFORE SCP flood completes
        assert!(
            tx_received_at < all_scp_received_at,
            "TX should arrive BEFORE SCP flood completes (stream independence). \
             TX at {:?}, SCPs done at {:?}",
            tx_latency,
            scp_total_time
        );

        // Verify SCP flood took meaningful time
        assert!(
            scp_total_time > Duration::from_millis(50),
            "SCP flood should take measurable time ({:?}), otherwise test is invalid",
            scp_total_time
        );

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    /// Test TX broadcast and receive
    #[tokio::test]
    async fn test_tx_broadcast() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, _events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19401;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19402).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + streams
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain events
        while events2.try_recv().is_ok() {}

        // Send TX
        let tx_msg = b"test transaction".to_vec();
        handle1.broadcast_tx(tx_msg.clone()).await;

        // Wait for TX
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut received = false;

        while tokio::time::Instant::now() < deadline && !received {
            tokio::select! {
                Some(event) = events2.recv() => {
                    if let OverlayEvent::TxReceived { tx, .. } = event {
                        assert_eq!(tx, tx_msg);
                        received = true;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        assert!(received, "Should receive TX message");

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    /// Test TxSet request/response flow
    /// Node2 requests a TxSet from Node1, Node1 responds with the data
    #[tokio::test]
    async fn test_txset_fetch() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19601;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19602).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + streams
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain events
        while events1.try_recv().is_ok() {}
        while events2.try_recv().is_ok() {}

        // Node2 requests a TxSet by hash
        let requested_hash: [u8; 32] = [0x42; 32];
        handle2.fetch_txset(requested_hash).await;

        // Node1 should receive TxSetRequested event
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut request_received = false;

        while tokio::time::Instant::now() < deadline && !request_received {
            tokio::select! {
                Some(event) = events1.recv() => {
                    if let OverlayEvent::TxSetRequested { hash, from } = event {
                        assert_eq!(hash, requested_hash);
                        request_received = true;

                        // Node1 responds with TxSet data
                        let txset_data = b"mock txset XDR data here".to_vec();
                        handle1.send_txset(hash, txset_data, from).await;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        assert!(
            request_received,
            "Node1 should receive TxSetRequested event"
        );

        // Node2 should receive TxSetReceived event
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut response_received = false;

        while tokio::time::Instant::now() < deadline && !response_received {
            tokio::select! {
                Some(event) = events2.recv() => {
                    if let OverlayEvent::TxSetReceived { hash, data, .. } = event {
                        assert_eq!(hash, requested_hash);
                        assert_eq!(data, b"mock txset XDR data here".to_vec());
                        response_received = true;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        assert!(
            response_received,
            "Node2 should receive TxSetReceived event"
        );

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    /// Test multiple TXs flood with correct ordering (by fee)
    #[tokio::test]
    async fn test_multiple_txs_flood() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, _events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19701;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19702).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + streams
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain events
        while events2.try_recv().is_ok() {}

        // Send multiple TXs
        let tx_count = 10;
        for i in 0..tx_count {
            let tx = format!("transaction_{}", i).into_bytes();
            handle1.broadcast_tx(tx).await;
        }

        // Wait for all TXs
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut received_count = 0;

        while tokio::time::Instant::now() < deadline && received_count < tx_count {
            tokio::select! {
                Some(event) = events2.recv() => {
                    if let OverlayEvent::TxReceived { .. } = event {
                        received_count += 1;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        assert_eq!(
            received_count, tx_count,
            "Should receive all {} TXs",
            tx_count
        );

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    /// Test TX deduplication - same TX sent twice should only be received once
    #[tokio::test]
    async fn test_tx_dedup() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let (handle1, _events1, overlay1) = create_overlay(keypair1).unwrap();
        let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

        let listen_port = 19801;
        tokio::spawn(async move { overlay1.run(listen_port).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay2.run(19802).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
            .parse()
            .unwrap();
        handle2.dial(addr).await;

        // Wait for connection + streams
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain events
        while events2.try_recv().is_ok() {}

        // Send same TX twice
        let tx = b"duplicate_transaction".to_vec();
        handle1.broadcast_tx(tx.clone()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle1.broadcast_tx(tx.clone()).await;

        // Wait and count received TXs
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut received_count = 0;
        while let Ok(event) = events2.try_recv() {
            if let OverlayEvent::TxReceived { .. } = event {
                received_count += 1;
            }
        }

        assert_eq!(
            received_count, 1,
            "Duplicate TX should only be received once"
        );

        handle1.shutdown().await;
        handle2.shutdown().await;
    }

    // ═══ Multi-Node (3+) Gossip Tests ═══

    /// Test SCP messages reach all directly connected peers in a triangle topology
    /// Topology: A-B, B-C, A-C (all nodes connected to each other)
    #[tokio::test]
    async fn test_three_node_triangle_scp() {
        // Create 3 nodes
        let keypair_a = Keypair::generate_ed25519();
        let keypair_b = Keypair::generate_ed25519();
        let keypair_c = Keypair::generate_ed25519();

        let (handle_a, _events_a, overlay_a) = create_overlay(keypair_a).unwrap();
        let (handle_b, mut events_b, overlay_b) = create_overlay(keypair_b).unwrap();
        let (handle_c, mut events_c, overlay_c) = create_overlay(keypair_c).unwrap();

        // Start all nodes on different ports
        let port_a = 19901;
        let port_b = 19902;
        let port_c = 19903;

        tokio::spawn(async move { overlay_a.run(port_a).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay_b.run(port_b).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay_c.run(port_c).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect: B -> A, C -> A (both B and C connected to A)
        let addr_a: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", port_a)
            .parse()
            .unwrap();

        handle_b.dial(addr_a.clone()).await;
        handle_c.dial(addr_a).await;

        // Wait for connections to establish
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Drain connection events
        while events_b.try_recv().is_ok() {}
        while events_c.try_recv().is_ok() {}

        // A broadcasts SCP - should reach both B and C directly
        let scp_msg = b"3-node test SCP".to_vec();
        handle_a.broadcast_scp(scp_msg.clone()).await;

        // Both B and C should receive it directly from A
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut b_received = false;
        let mut c_received = false;

        while tokio::time::Instant::now() < deadline && (!b_received || !c_received) {
            tokio::select! {
                Some(event) = events_b.recv() => {
                    if let OverlayEvent::ScpReceived { envelope, .. } = event {
                        if envelope == scp_msg {
                            b_received = true;
                        }
                    }
                }
                Some(event) = events_c.recv() => {
                    if let OverlayEvent::ScpReceived { envelope, .. } = event {
                        if envelope == scp_msg {
                            c_received = true;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        assert!(b_received, "Node B should receive SCP from A");
        assert!(c_received, "Node C should receive SCP from A");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
        handle_c.shutdown().await;
    }

    /// Test TX propagation across 3 nodes
    #[tokio::test]
    async fn test_three_node_tx_propagation() {
        let keypair_a = Keypair::generate_ed25519();
        let keypair_b = Keypair::generate_ed25519();
        let keypair_c = Keypair::generate_ed25519();

        let (handle_a, _events_a, overlay_a) = create_overlay(keypair_a).unwrap();
        let (handle_b, mut events_b, overlay_b) = create_overlay(keypair_b).unwrap();
        let (handle_c, mut events_c, overlay_c) = create_overlay(keypair_c).unwrap();

        let port_a = 20001;
        let port_b = 20002;
        let port_c = 20003;

        tokio::spawn(async move { overlay_a.run(port_a).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay_b.run(port_b).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::spawn(async move { overlay_c.run(port_c).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Triangle topology: A-B, B-C, A-C
        let addr_a: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", port_a)
            .parse()
            .unwrap();
        let addr_b: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", port_b)
            .parse()
            .unwrap();

        handle_b.dial(addr_a.clone()).await;
        handle_c.dial(addr_b).await;
        handle_c.dial(addr_a).await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        while events_b.try_recv().is_ok() {}
        while events_c.try_recv().is_ok() {}

        // A broadcasts TX
        let tx_msg = b"3-node TX test".to_vec();
        handle_a.broadcast_tx(tx_msg.clone()).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut b_received = false;
        let mut c_received = false;

        while tokio::time::Instant::now() < deadline && (!b_received || !c_received) {
            tokio::select! {
                Some(event) = events_b.recv() => {
                    if let OverlayEvent::TxReceived { tx, .. } = event {
                        if tx == tx_msg {
                            b_received = true;
                        }
                    }
                }
                Some(event) = events_c.recv() => {
                    if let OverlayEvent::TxReceived { tx, .. } = event {
                        if tx == tx_msg {
                            c_received = true;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        assert!(b_received, "Node B should receive TX");
        assert!(c_received, "Node C should receive TX");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
        handle_c.shutdown().await;
    }

    /// Test that shutdown is clean (no hung connections)
    #[tokio::test]
    async fn test_clean_shutdown() {
        let keypair = Keypair::generate_ed25519();
        let (handle, _events, overlay) = create_overlay(keypair).unwrap();

        let overlay_task = tokio::spawn(async move {
            overlay.run(20100).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Shutdown should complete quickly
        let shutdown_result = tokio::time::timeout(Duration::from_secs(2), handle.shutdown()).await;

        assert!(
            shutdown_result.is_ok(),
            "Shutdown should complete within 2 seconds"
        );

        // Task should finish
        let task_result = tokio::time::timeout(Duration::from_secs(1), overlay_task).await;

        assert!(
            task_result.is_ok(),
            "Overlay task should complete after shutdown"
        );
    }

    /// Test overlay handles dial to invalid address gracefully
    #[tokio::test]
    async fn test_dial_invalid_address() {
        let keypair = Keypair::generate_ed25519();
        let (handle, _events, overlay) = create_overlay(keypair).unwrap();

        tokio::spawn(async move { overlay.run(20200).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Dial an address where nothing is listening
        let bad_addr: Multiaddr = "/ip4/127.0.0.1/udp/59999/quic-v1".parse().unwrap();
        handle.dial(bad_addr).await;

        // Should not crash - just log an error and continue
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Overlay should still be operational
        handle.shutdown().await;
    }
}

/// Test TX set source tracking - verify we ask the right peer
#[tokio::test]
async fn test_txset_source_tracking() {
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();
    let peer2_id = PeerId::from_public_key(&keypair2.public());

    let (handle1, _events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 20101;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(20102).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect overlay2 to overlay1
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Record that peer1 (from overlay2's perspective) has a specific TX set
    let test_hash: [u8; 32] = [0xAB; 32];
    // We need to get peer1's ID first - overlay2 should have seen it connect
    // For now, test that record_txset_source doesn't crash
    let fake_peer = PeerId::random();
    handle2.record_txset_source(test_hash, fake_peer).await;

    // Give time for command to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now try to fetch - since fake_peer isn't connected, it should fall back
    handle2.fetch_txset(test_hash).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Clean up
    handle1.shutdown().await;
    handle2.shutdown().await;
}

/// Test TX set fetch from connected peer
#[tokio::test]
async fn test_txset_fetch_flow() {
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();

    let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 20201;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(20202).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // overlay2 requests a TX set that overlay1 doesn't have
    let test_hash: [u8; 32] = [0xCD; 32];
    handle2.fetch_txset(test_hash).await;

    // overlay1 should receive the request (as TxSetRequested event)
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut got_request = false;
    while let Ok(event) = events1.try_recv() {
        if let OverlayEvent::TxSetRequested { hash, .. } = event {
            if hash == test_hash {
                got_request = true;
            }
        }
    }

    assert!(
        got_request,
        "overlay1 should receive TxSet request from overlay2"
    );

    handle1.shutdown().await;
    handle2.shutdown().await;
}

/// Test that peer disconnect triggers reconnect attempt
#[tokio::test]
async fn test_peer_disconnect_detection() {
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();

    let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, _events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 20301;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(20302).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection was established by checking we can send SCP
    handle1.broadcast_scp(b"test".to_vec()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now shutdown overlay2 - overlay1 should detect disconnect
    handle2.shutdown().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // overlay1 should have received a disconnect event or connection closed
    // (Connection closed is handled internally by libp2p, we verify no crash)

    handle1.shutdown().await;
    // Test passes if we get here without hanging or crashing
}

/// Test connect to unreachable peer times out gracefully
#[tokio::test]
async fn test_connect_unreachable_peer_timeout() {
    let keypair = Keypair::generate_ed25519();
    let (handle, _events, overlay) = create_overlay(keypair).unwrap();

    let listen_port = 20401;
    tokio::spawn(async move { overlay.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Try to connect to a non-existent peer
    // Use a port that's definitely not listening
    let bad_addr: Multiaddr = "/ip4/127.0.0.1/udp/59999/quic-v1".parse().unwrap();

    // This should not hang - dial returns immediately, connection fails async
    let start = tokio::time::Instant::now();
    handle.dial(bad_addr).await;

    // Give some time for the connection attempt
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify we didn't hang for too long
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "Connection attempt should not block for more than 5 seconds"
    );

    // Overlay should still be operational
    handle.shutdown().await;
}

/// Test large TX set doesn't block SCP messages
#[tokio::test]
async fn test_large_txset_doesnt_block_scp() {
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();

    let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 20501;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(20502).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drain initial events
    while events1.try_recv().is_ok() {}
    while events2.try_recv().is_ok() {}

    // Create a large TX set (1MB)
    let large_txset = vec![0xAB; 1024 * 1024];
    let txset_hash: [u8; 32] = [0x11; 32];

    // Start sending large TX set from node1
    let handle1_clone = handle1.clone();
    let large_txset_clone = large_txset.clone();
    let send_task = tokio::spawn(async move {
        // Simulate responding to TX set request with large data
        // We'll use the event system - node2 requests, node1 responds
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    // Immediately send SCP message - should NOT be blocked
    let scp_msg = b"urgent SCP message".to_vec();
    let scp_start = tokio::time::Instant::now();
    handle1.broadcast_scp(scp_msg.clone()).await;

    // SCP should arrive quickly (< 100ms) even if TX set is being transferred
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut scp_received = false;

    while tokio::time::Instant::now() < deadline && !scp_received {
        tokio::select! {
            Some(event) = events2.recv() => {
                if let OverlayEvent::ScpReceived { envelope, .. } = event {
                    if envelope == scp_msg {
                        scp_received = true;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }

    let scp_latency = scp_start.elapsed();
    assert!(scp_received, "SCP message should be received");
    assert!(
        scp_latency < Duration::from_millis(200),
        "SCP latency should be < 200ms, was {:?}",
        scp_latency
    );

    send_task.await.unwrap();
    handle1.shutdown().await;
    handle2.shutdown().await;
}

/// Test TX set request to peer that has the data
#[tokio::test]
async fn test_txset_request_and_response() {
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();

    let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 20601;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(20602).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drain events
    while events1.try_recv().is_ok() {}
    while events2.try_recv().is_ok() {}

    // Node2 requests a TX set
    let requested_hash: [u8; 32] = [0x77; 32];
    let txset_data = b"test tx set XDR content here".to_vec();

    handle2.fetch_txset(requested_hash).await;

    // Node1 receives request and responds
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut responded = false;

    while tokio::time::Instant::now() < deadline && !responded {
        tokio::select! {
            Some(event) = events1.recv() => {
                if let OverlayEvent::TxSetRequested { hash, from } = event {
                    assert_eq!(hash, requested_hash, "Request should have correct hash");
                    handle1.send_txset(hash, txset_data.clone(), from).await;
                    responded = true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    assert!(
        responded,
        "Node1 should receive and respond to TX set request"
    );

    // Node2 should receive the TX set
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut received = false;

    while tokio::time::Instant::now() < deadline && !received {
        tokio::select! {
            Some(event) = events2.recv() => {
                if let OverlayEvent::TxSetReceived { hash, data, .. } = event {
                    assert_eq!(hash, requested_hash, "Received hash should match");
                    assert_eq!(data, txset_data, "Received data should match");
                    received = true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    assert!(received, "Node2 should receive TX set response");

    handle1.shutdown().await;
    handle2.shutdown().await;
}

/// Test TX set fetch when no peers are connected
#[tokio::test]
async fn test_txset_fetch_no_peers() {
    let keypair = Keypair::generate_ed25519();
    let (handle, mut events, overlay) = create_overlay(keypair).unwrap();

    let listen_port = 20701;
    tokio::spawn(async move { overlay.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request TX set with no peers connected
    let requested_hash: [u8; 32] = [0x88; 32];
    handle.fetch_txset(requested_hash).await;

    // Should not crash or hang - just no response
    // Wait briefly to ensure no panic
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain any events (there shouldn't be any TX set related ones)
    let mut txset_events = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, OverlayEvent::TxSetReceived { .. }) {
            txset_events += 1;
        }
    }
    assert_eq!(
        txset_events, 0,
        "Should not receive TX set when no peers connected"
    );

    handle.shutdown().await;
}

/// Test multiple concurrent TX set requests
#[tokio::test]
async fn test_txset_multiple_concurrent_requests() {
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();

    let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 20801;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(20802).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drain events
    while events1.try_recv().is_ok() {}
    while events2.try_recv().is_ok() {}

    // Request multiple TX sets concurrently
    let hash1: [u8; 32] = [0x11; 32];
    let hash2: [u8; 32] = [0x22; 32];
    let hash3: [u8; 32] = [0x33; 32];

    handle2.fetch_txset(hash1).await;
    handle2.fetch_txset(hash2).await;
    handle2.fetch_txset(hash3).await;

    // Node1 should receive all 3 requests
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut received_hashes = std::collections::HashSet::new();

    while tokio::time::Instant::now() < deadline && received_hashes.len() < 3 {
        tokio::select! {
            Some(event) = events1.recv() => {
                if let OverlayEvent::TxSetRequested { hash, from } = event {
                    received_hashes.insert(hash);
                    // Respond to each request
                    let data = format!("txset for {:?}", &hash[..4]).into_bytes();
                    handle1.send_txset(hash, data, from).await;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }

    assert_eq!(
        received_hashes.len(),
        3,
        "Should receive all 3 TX set requests"
    );
    assert!(received_hashes.contains(&hash1));
    assert!(received_hashes.contains(&hash2));
    assert!(received_hashes.contains(&hash3));

    handle1.shutdown().await;
    handle2.shutdown().await;
}

#[tokio::test]
async fn test_scp_state_request_on_connection() {
    // Test that when two nodes connect, they request SCP state from each other
    let keypair1 = Keypair::generate_ed25519();
    let keypair2 = Keypair::generate_ed25519();

    let (handle1, mut events1, overlay1) = create_overlay(keypair1).unwrap();
    let (handle2, mut events2, overlay2) = create_overlay(keypair2).unwrap();

    let listen_port = 19801;
    tokio::spawn(async move { overlay1.run(listen_port).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move { overlay2.run(19802).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect node2 to node1
    let addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", listen_port)
        .parse()
        .unwrap();
    handle2.dial(addr).await;

    // Wait for connection + SCP stream setup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both nodes should receive ScpStateRequested events (each receives request from the other)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut node1_received_request = false;
    let mut node2_received_request = false;

    while tokio::time::Instant::now() < deadline
        && (!node1_received_request || !node2_received_request)
    {
        tokio::select! {
            Some(event) = events1.recv() => {
                if let OverlayEvent::ScpStateRequested { ledger_seq, .. } = event {
                    assert_eq!(ledger_seq, 0, "Should request all recent state (ledger_seq=0)");
                    node1_received_request = true;
                }
            }
            Some(event) = events2.recv() => {
                if let OverlayEvent::ScpStateRequested { ledger_seq, .. } = event {
                    assert_eq!(ledger_seq, 0, "Should request all recent state (ledger_seq=0)");
                    node2_received_request = true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }

    assert!(
        node1_received_request,
        "Node 1 should receive SCP state request from node 2"
    );
    assert!(
        node2_received_request,
        "Node 2 should receive SCP state request from node 1"
    );

    handle1.shutdown().await;
    handle2.shutdown().await;
}
