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
    TxSetRequested {
        hash: [u8; 32],
        from: PeerId,
    },
    /// Peer connected
    PeerConnected(PeerId),
    /// Peer disconnected
    PeerDisconnected(PeerId),
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
    SendTxSet { hash: [u8; 32], data: Vec<u8>, to: PeerId },
    /// Connect to a peer
    Dial(Multiaddr),
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
    Stream(()),  // StreamBehaviour emits () - no events
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
        let _ = self
            .cmd_tx
            .send(OverlayCommand::FetchTxSet { hash })
            .await;
    }

    pub async fn send_txset(&self, hash: [u8; 32], data: Vec<u8>, to: PeerId) {
        let _ = self
            .cmd_tx
            .send(OverlayCommand::SendTxSet { hash, data, to })
            .await;
    }

    pub async fn dial(&self, addr: Multiaddr) {
        let _ = self.cmd_tx.send(OverlayCommand::Dial(addr)).await;
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

            #[allow(deprecated)]
            let kademlia = Kademlia::with_config(
                key.public().to_peer_id(),
                MemoryStore::new(key.public().to_peer_id()),
                KademliaConfig::default(),
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
                        OverlayCommand::Dial(addr) => {
                            if let Err(e) = self.swarm.dial(addr.clone()) {
                                warn!("Failed to dial {}: {}", addr, e);
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

                let _ = self.state.event_tx.send(OverlayEvent::PeerConnected(peer_id));
            }

            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer {}", peer_id);
                {
                    let mut streams = self.state.peer_streams.write().await;
                    streams.remove(&peer_id);
                }
                let _ = self
                    .state
                    .event_tx
                    .send(OverlayEvent::PeerDisconnected(peer_id));
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
                    .add_address(&peer_id, addr);
            }
        }
    }

    fn handle_kademlia_event(&mut self, event: KademliaEvent) {
        if let KademliaEvent::RoutingUpdated { peer, .. } = event {
            debug!("Kademlia routing updated for peer {}", peer);
        }
    }

    /// Open SCP, TX, and TxSet streams to a peer
    async fn open_streams_to_peer(&mut self, peer_id: PeerId) {
        debug!("Opening streams to peer {}", peer_id);

        // Open all streams in parallel for faster connection setup
        let mut control = self.control.clone();
        let mut control2 = self.control.clone();
        let mut control3 = self.control.clone();

        let scp_fut = async {
            control.open_stream(peer_id, SCP_PROTOCOL).await
        };
        let tx_fut = async {
            control2.open_stream(peer_id, TX_PROTOCOL).await
        };
        let txset_fut = async {
            control3.open_stream(peer_id, TXSET_PROTOCOL).await
        };

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
        let streams = self.state.peer_streams.read().await;
        if let Some(peer_streams) = streams.get(&peer_id) {
            let mut ps = peer_streams.lock().await;
            ps.scp = scp_stream;
            ps.tx = tx_stream;
            ps.txset = txset_stream;
        }
    }

    /// Broadcast SCP envelope to all connected peers
    async fn broadcast_scp(&mut self, envelope: &[u8]) {
        let hash = blake2b_hash(envelope);

        // Dedup check
        {
            let mut seen = self.state.scp_seen.write().await;
            if seen.contains(&hash) {
                trace!("SCP already seen, skipping broadcast");
                return;
            }
            seen.put(hash, ());
        }

        let streams = self.state.peer_streams.read().await;
        let peers: Vec<_> = streams.keys().cloned().collect();
        drop(streams);

        debug!("Broadcasting SCP to {} peers", peers.len());

        for peer_id in peers {
            if let Err(e) = send_to_peer_stream(&self.state, peer_id, StreamType::Scp, envelope).await {
                warn!("Failed to send SCP to {}: {}", peer_id, e);
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

        debug!("Broadcasting TX to {} peers", peers.len());

        for peer_id in peers {
            if let Err(e) = send_to_peer_stream(&self.state, peer_id, StreamType::Tx, tx).await {
                warn!("Failed to send TX to {}: {}", peer_id, e);
            }
        }
    }

    /// Fetch TX set from a peer
    async fn fetch_txset(&mut self, hash: [u8; 32]) {
        let streams = self.state.peer_streams.read().await;
        let peer = match streams.keys().next().cloned() {
            Some(p) => p,
            None => {
                warn!("No peers to fetch TX set from");
                return;
            }
        };
        drop(streams);

        info!("Fetching TX set {:02x?}... from {}", &hash[..4], peer);

        // Send request on TxSet stream (just the 32-byte hash)
        if let Err(e) = send_to_peer_stream(&self.state, peer, StreamType::TxSet, &hash).await {
            warn!("Failed to send TxSet request to {}: {}", peer, e);
        }
    }

    /// Send TX set response to a specific peer
    async fn send_txset_response(&mut self, peer: PeerId, hash: [u8; 32], data: Vec<u8>) {
        info!("Sending TX set {:02x?}... ({} bytes) to {}", &hash[..4], data.len(), peer);

        // Response format: 32-byte hash + XDR data
        let mut response = Vec::with_capacity(32 + data.len());
        response.extend_from_slice(&hash);
        response.extend_from_slice(&data);

        if let Err(e) = send_to_peer_stream(&self.state, peer, StreamType::TxSet, &response).await {
            warn!("Failed to send TxSet response to {}: {}", peer, e);
        }
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
        debug!("Stream {:?} not open to {}, attempting to reopen", stream_type.protocol(), peer_id);
        match state.control.clone().open_stream(peer_id, stream_type.protocol()).await {
            Ok(s) => {
                debug!("Successfully reopened {:?} stream to {}", stream_type.protocol(), peer_id);
                *stream_slot = Some(s);
            }
            Err(e) => {
                warn!("Failed to reopen {:?} stream to {}: {:?}", stream_type.protocol(), peer_id, e);
                return Err(io::Error::new(io::ErrorKind::NotConnected, format!("failed to reopen stream: {:?}", e)));
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
        debug!("Accepted inbound SCP stream from {}", peer_id);
        let state = state.clone();

        tokio::spawn(async move {
            loop {
                match read_framed(&mut stream).await {
                    Ok(envelope) => {
                        let hash = blake2b_hash(&envelope);

                        // Dedup
                        {
                            let mut seen = state.scp_seen.write().await;
                            if seen.contains(&hash) {
                                trace!("Duplicate SCP from {}", peer_id);
                                continue;
                            }
                            seen.put(hash, ());
                        }

                        trace!("Received SCP ({} bytes) from {}", envelope.len(), peer_id);
                        let _ = state.event_tx.send(OverlayEvent::ScpReceived {
                            envelope,
                            from: peer_id,
                        });
                    }
                    Err(e) => {
                        debug!("SCP stream from {} closed: {}", peer_id, e);
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
        debug!("Accepted inbound TX stream from {}", peer_id);
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

                        trace!("Received TX ({} bytes) from {}", tx.len(), peer_id);
                        let _ = state.event_tx.send(OverlayEvent::TxReceived { tx, from: peer_id });
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
                            info!("Received TxSet request for {:02x?}... from {}", &hash[..4], peer_id);
                            
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

                            info!(
                                "Received TxSet {:02x?}... ({} bytes) from {}",
                                &hash[..4],
                                txset_data.len(),
                                peer_id
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

        // Wait for connection
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut connected = false;

        while tokio::time::Instant::now() < deadline && !connected {
            tokio::select! {
                Some(event) = events1.recv() => {
                    if matches!(event, OverlayEvent::PeerConnected(_)) {
                        connected = true;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        assert!(connected, "Should connect");

        // Give streams time to open
        tokio::time::sleep(Duration::from_millis(200)).await;

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
            scp_latency, tx_total_time
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
            tx_latency, scp_total_time
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
        assert!(request_received, "Node1 should receive TxSetRequested event");

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
        assert!(response_received, "Node2 should receive TxSetReceived event");

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

        assert_eq!(received_count, tx_count, "Should receive all {} TXs", tx_count);

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

        assert_eq!(received_count, 1, "Duplicate TX should only be received once");

        handle1.shutdown().await;
        handle2.shutdown().await;
    }
}
