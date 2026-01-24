//! Stellar Overlay Process
//!
//! A process-isolated overlay for stellar-core that handles:
//! - SCP message relay (latency-critical, via dedicated QUIC stream)
//! - Transaction flooding (via dedicated QUIC stream)
//! - Peer management
//!
//! Uses QUIC transport for true stream independence - SCP never blocked by TX.
//! Communicates with Core via Unix domain socket IPC.

mod config;
mod ipc;
mod flood;
mod http;
pub mod integrated;
pub mod libp2p_overlay;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use config::Config;
use ipc::{CoreIpc, Message, MessageType};
use integrated::{Overlay, OverlayHandle, CoreCommand};
use flood::{TxSetCache, CachedTxSet, Hash256, build_tx_set_xdr, hash_tx_set};
use libp2p_overlay::{
    create_overlay, OverlayHandle as LibP2pOverlayHandle, 
    OverlayEvent as LibP2pOverlayEvent, StellarOverlay,
};
use libp2p::identity::Keypair as Libp2pKeypair;

/// Command-line arguments
struct Args {
    config_path: Option<PathBuf>,
    socket_path: Option<PathBuf>,
    listen_mode: bool,
    peer_port: Option<u16>,
}

impl Args {
    fn parse() -> Self {
        let mut args = Args {
            config_path: None,
            socket_path: None,
            listen_mode: false,
            peer_port: None,
        };
        
        let mut iter = std::env::args().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--config" | "-c" => {
                    args.config_path = iter.next().map(PathBuf::from);
                }
                "--socket" | "-s" => {
                    args.socket_path = iter.next().map(PathBuf::from);
                }
                "--peer-port" | "-p" => {
                    args.peer_port = iter.next().and_then(|s| s.parse().ok());
                }
                "--listen" | "-l" => {
                    args.listen_mode = true;
                    // Check if next arg is the socket path (C++ passes it this way)
                    if let Some(next) = iter.next() {
                        if !next.starts_with('-') {
                            args.socket_path = Some(PathBuf::from(next));
                        }
                    }
                }
                "--help" | "-h" => {
                    eprintln!("Usage: stellar-overlay [OPTIONS] [SOCKET_PATH]");
                    eprintln!();
                    eprintln!("Options:");
                    eprintln!("  -c, --config <PATH>    Path to config file (TOML)");
                    eprintln!("  -s, --socket <PATH>    Path to Core IPC socket");
                    eprintln!("  -p, --peer-port <PORT> Port for peer TCP connections");
                    eprintln!("  -l, --listen           Listen mode (create socket, wait for Core)");
                    eprintln!("  -h, --help             Show this help");
                    eprintln!();
                    eprintln!("By default, connects to an existing socket. Use --listen to create");
                    eprintln!("the socket and wait for Core to connect (useful for testing).");
                    std::process::exit(0);
                }
                other => {
                    // Positional arg - treat as socket path for backward compat
                    if args.socket_path.is_none() {
                        args.socket_path = Some(PathBuf::from(other));
                    }
                }
            }
        }
        
        args
    }
}

/// Application state
struct App {
    #[allow(dead_code)]
    config: Config,
    core_ipc: CoreIpc,
    overlay_handle: OverlayHandle,
    /// Cache for built TX sets
    tx_set_cache: Arc<RwLock<TxSetCache>>,
    /// TX set hashes already pushed to Core (reset on ledger close)
    pushed_tx_sets: Arc<RwLock<HashSet<Hash256>>>,
    /// Current ledger sequence
    current_ledger_seq: Arc<RwLock<u32>>,
    /// libp2p overlay handle (QUIC-based SCP + TX)
    libp2p_handle: LibP2pOverlayHandle,
    /// libp2p overlay events
    libp2p_events: mpsc::UnboundedReceiver<LibP2pOverlayEvent>,
}

impl App {
    async fn new(config: Config, listen_mode: bool) -> Result<Self, Box<dyn std::error::Error>> {
        // Connect to Core (or listen for connection)
        let core_ipc = if listen_mode {
            CoreIpc::listen(&config.core_socket).await?
        } else {
            CoreIpc::connect(&config.core_socket).await?
        };
        
        // Create channels for mempool manager communication
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        
        // Create mempool manager (no network - libp2p handles all P2P)
        let mempool_manager = Overlay::new(cmd_rx);
        let overlay_handle = OverlayHandle::new(cmd_tx);
        
        // Spawn mempool manager task
        tokio::spawn(async move {
            if let Err(e) = mempool_manager.run().await {
                error!("Mempool manager error: {}", e);
            }
        });
        
        // Create libp2p QUIC overlay for SCP + TX + TxSet (unified, independent streams)
        let libp2p_keypair = Libp2pKeypair::generate_ed25519();
        let (libp2p_handle, libp2p_event_rx, libp2p_overlay) = create_overlay(libp2p_keypair)
            .map_err(|e| format!("Failed to create libp2p overlay: {}", e))?;
        
        // Use peer_port + 1000 for libp2p QUIC to avoid collision with legacy TCP
        let libp2p_port = config.peer_port + 1000;
        
        // Spawn libp2p overlay task
        tokio::spawn(async move {
            libp2p_overlay.run(libp2p_port).await;
        });
        
        info!("Started libp2p QUIC overlay on port {} (SCP + TX + TxSet streams)", libp2p_port);
        
        Ok(Self {
            config,
            core_ipc,
            overlay_handle,
            tx_set_cache: Arc::new(RwLock::new(TxSetCache::new(100))),
            pushed_tx_sets: Arc::new(RwLock::new(HashSet::new())),
            current_ledger_seq: Arc::new(RwLock::new(0)),
            libp2p_handle,
            libp2p_events: libp2p_event_rx,
        })
    }
    
    /// Main event loop - process messages from Core and overlay events
    async fn run(mut self) {
        info!("Overlay started, processing Core messages");
        
        loop {
            tokio::select! {
                // Receive message from Core
                msg = self.core_ipc.receiver.recv() => {
                    match msg {
                        Some(msg) => self.handle_core_message(msg).await,
                        None => {
                            info!("Core IPC connection closed");
                            break;
                        }
                    }
                }
                
                // Receive events from libp2p QUIC overlay (SCP + TX + TxSet)
                Some(event) = self.libp2p_events.recv() => {
                    self.handle_libp2p_event(event).await;
                }
            }
        }
        
        // Shutdown libp2p
        self.libp2p_handle.shutdown().await;
        
        info!("Overlay shutting down");
    }
    
    /// Handle an event from the libp2p QUIC overlay (SCP + TX)
    async fn handle_libp2p_event(&mut self, event: LibP2pOverlayEvent) {
        match event {
            LibP2pOverlayEvent::ScpReceived { envelope, from } => {
                debug!("Received SCP via QUIC from {}: {} bytes", from, envelope.len());
                // Forward to Core
                if let Err(e) = self.core_ipc.sender.send_scp_received(envelope, 0) {
                    error!("Failed to send SCP to Core: {}", e);
                }
            }
            LibP2pOverlayEvent::TxReceived { tx, from } => {
                debug!("Received TX via QUIC from {}: {} bytes", from, tx.len());
                // Add to mempool
                self.overlay_handle.submit_tx(tx, 0, 1);
            }
            LibP2pOverlayEvent::TxSetReceived { hash, data, from } => {
                debug!("Received TxSet via QUIC from {}: {} bytes", from, data.len());
                // Cache and notify Core
                let mut cache = self.tx_set_cache.write().await;
                cache.insert(CachedTxSet {
                    hash,
                    xdr: data.clone(),
                    ledger_seq: 0,
                    tx_hashes: vec![],
                });
                if let Err(e) = self.core_ipc.sender.send_tx_set_available(hash, data) {
                    error!("Failed to send TxSet to Core: {}", e);
                }
            }
            LibP2pOverlayEvent::TxSetRequested { hash, from } => {
                info!("Peer {} requesting TxSet {:02x?}...", from, &hash[..4]);
                // Look up in local cache and respond
                let cache = self.tx_set_cache.read().await;
                if let Some(cached) = cache.get(&hash) {
                    info!("Serving TxSet {:02x?}... ({} bytes) to {}", &hash[..4], cached.xdr.len(), from);
                    let handle = self.libp2p_handle.clone();
                    let data = cached.xdr.clone();
                    tokio::spawn(async move {
                        handle.send_txset(hash, data, from).await;
                    });
                } else {
                    debug!("TxSet {:02x?}... not in local cache, cannot serve to {}", &hash[..4], from);
                }
            }
            LibP2pOverlayEvent::PeerConnected(peer_id) => {
                info!("libp2p QUIC peer connected: {}", peer_id);
            }
            LibP2pOverlayEvent::PeerDisconnected(peer_id) => {
                info!("libp2p QUIC peer disconnected: {}", peer_id);
            }
        }
    }
    
    /// Handle a message from Core
    async fn handle_core_message(&mut self, msg: Message) {
        match msg.msg_type {
            MessageType::Shutdown => {
                info!("Shutdown requested by Core");
                // Exit the process
                std::process::exit(0);
            }
            
            MessageType::BroadcastScp => {
                // Forward SCP broadcast via libp2p QUIC (dedicated stream, no blocking)
                debug!("Received BroadcastScp from Core ({} bytes)", msg.payload.len());
                let handle = self.libp2p_handle.clone();
                let payload = msg.payload;
                tokio::spawn(async move {
                    handle.broadcast_scp(payload).await;
                });
            }
            
            MessageType::RequestNominationHash => {
                // Parse payload: [ledgerSeq:4][prevLedgerHash:32]
                if msg.payload.len() < 36 {
                    warn!("RequestNominationHash payload too short: {} bytes", msg.payload.len());
                    if let Err(e) = self.core_ipc.sender.send_nomination_hash([0u8; 32]) {
                        error!("Failed to send nomination hash: {}", e);
                    }
                    return;
                }
                
                let ledger_seq = u32::from_le_bytes(msg.payload[0..4].try_into().unwrap());
                let mut prev_hash = [0u8; 32];
                prev_hash.copy_from_slice(&msg.payload[4..36]);
                
                info!("Building TX set for ledger {} (prevHash={:?})", ledger_seq, &prev_hash[..4]);
                
                // Get top transactions from mempool
                let tx_set_cache = Arc::clone(&self.tx_set_cache);
                let core_sender = self.core_ipc.sender.clone();
                let overlay_handle = self.overlay_handle.clone();
                
                // Build TX set asynchronously
                tokio::spawn(async move {
                    // Request top transactions from overlay
                    let max_ops = 10000usize;
                    
                    let txs = match tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        overlay_handle.get_top_txs(max_ops)
                    ).await {
                        Ok(txs) => txs,
                        Err(_) => {
                            warn!("Timeout getting transactions from mempool");
                            vec![]
                        }
                    };
                    
                    info!("Building TX set with {} transactions", txs.len());
                    
                    // Sort TXs by hash for consensus determinism (matches C++ TxSetUtils::sortTxsInHashOrder)
                    let mut txs = txs;
                    txs.sort_by(|a, b| a.0.cmp(&b.0));
                    
                    // Extract TX hashes and data separately
                    let tx_hashes: Vec<[u8; 32]> = txs.iter().map(|(h, _)| *h).collect();
                    let tx_data: Vec<Vec<u8>> = txs.into_iter().map(|(_, d)| d).collect();
                    
                    // Build the TX set XDR
                    let xdr = build_tx_set_xdr(&prev_hash, &tx_data);
                    let hash = hash_tx_set(&xdr);
                    
                    info!("Built TX set: hash={:?}, xdr_size={}", &hash[..4], xdr.len());
                    
                    // Cache the TX set with TX hashes for later cleanup
                    {
                        let mut cache = tx_set_cache.write().await;
                        cache.insert(CachedTxSet {
                            hash,
                            xdr: xdr.clone(),
                            ledger_seq,
                            tx_hashes,
                        });
                    }
                    
                    // Also cache in the overlay so it can serve GET_TX_SET requests from peers
                    overlay_handle.cache_tx_set(hash, xdr.clone());
                    
                    // Send hash back to Core
                    if let Err(e) = core_sender.send_nomination_hash(hash) {
                        error!("Failed to send nomination hash: {}", e);
                    }
                });
            }
            
            MessageType::RequestTxSet => {
                // Request TX set by hash - check local cache first, then fetch from peers via libp2p
                if msg.payload.len() < 32 {
                    warn!("RequestTxSet payload too short");
                    return;
                }
                
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&msg.payload[0..32]);
                
                let tx_set_cache = Arc::clone(&self.tx_set_cache);
                let core_sender = self.core_ipc.sender.clone();
                let libp2p_handle = self.libp2p_handle.clone();
                
                tokio::spawn(async move {
                    // First check local cache
                    {
                        let cache = tx_set_cache.read().await;
                        if let Some(cached) = cache.get(&hash) {
                            info!("Sending TX set for hash {:?} ({} bytes) from local cache", &hash[..4], cached.xdr.len());
                            if let Err(e) = core_sender.send_tx_set_available(hash, cached.xdr.clone()) {
                                error!("Failed to send TX set: {}", e);
                            }
                            return;
                        }
                    }
                    
                    // Not in local cache - request from peers via libp2p
                    // The response will arrive as TxSetReceived event and be forwarded to Core
                    info!("TX set {:?} not in local cache, requesting from peers via libp2p", &hash[..4]);
                    libp2p_handle.fetch_txset(hash).await;
                });
            }
            
            MessageType::SubmitTx => {
                // Parse payload: [fee:i64][numOps:u32][txEnvelope...]
                if msg.payload.len() < 12 {
                    warn!("SubmitTx payload too short");
                    return;
                }
                
                let fee = i64::from_le_bytes(msg.payload[0..8].try_into().unwrap());
                let num_ops = u32::from_le_bytes(msg.payload[8..12].try_into().unwrap());
                let tx_data = msg.payload[12..].to_vec();
                
                debug!("Submitting TX: fee={}, numOps={}, size={}", fee, num_ops, tx_data.len());
                
                // Add to mempool
                self.overlay_handle.submit_tx(tx_data.clone(), fee as u64, num_ops);
                
                // Broadcast TX via libp2p QUIC (dedicated stream)
                let handle = self.libp2p_handle.clone();
                tokio::spawn(async move {
                    handle.broadcast_tx(tx_data).await;
                });
            }
            
            MessageType::RequestScpState => {
                // TODO: Implement - query SCP state and respond
                warn!("RequestScpState not yet implemented");
            }
            
            MessageType::LedgerClosed => {
                // Parse payload: [ledgerSeq:4][ledgerHash:32]
                if msg.payload.len() >= 4 {
                    let ledger_seq = u32::from_le_bytes(msg.payload[0..4].try_into().unwrap());
                    info!("Ledger {} closed", ledger_seq);
                    
                    let current_seq = Arc::clone(&self.current_ledger_seq);
                    let pushed = Arc::clone(&self.pushed_tx_sets);
                    let cache = Arc::clone(&self.tx_set_cache);
                    
                    tokio::spawn(async move {
                        // Update current ledger
                        *current_seq.write().await = ledger_seq;
                        
                        // Clear pushed TX sets (reset dedup tracking)
                        pushed.write().await.clear();
                        
                        // Evict old TX sets from cache
                        cache.write().await.evict_before(ledger_seq.saturating_sub(5));
                    });
                }
            }
            
            MessageType::TxSetExternalized => {
                // Parse payload: [hash:32]
                if msg.payload.len() >= 32 {
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&msg.payload[0..32]);
                    info!("TX set externalized: {:?}", &hash[..4]);
                    
                    // Look up the TX set in cache and get TX hashes, then remove from mempool
                    let cache = Arc::clone(&self.tx_set_cache);
                    let overlay_handle = self.overlay_handle.clone();
                    
                    tokio::spawn(async move {
                        let tx_hashes = {
                            let mut cache = cache.write().await;
                            cache.remove(&hash)
                        };
                        
                        // Remove TXs from mempool
                        if let Some(hashes) = tx_hashes {
                            if !hashes.is_empty() {
                                overlay_handle.remove_txs(hashes);
                            }
                        } else {
                            debug!("TX set {:?} not found in cache (may be from another node)", &hash[..4]);
                        }
                    });
                }
            }
            
            MessageType::ScpStateResponse => {
                // TODO: Forward to peer that requested it
                warn!("ScpStateResponse not yet implemented");
            }
            
            MessageType::SetPeerConfig => {
                // Parse JSON payload and configure peer connections
                if let Ok(json_str) = std::str::from_utf8(&msg.payload) {
                    info!("Received peer config: {}", json_str);
                    if let Ok(config) = serde_json::from_str::<serde_json::Value>(json_str) {
                        let known: Vec<String> = config["known_peers"].as_array()
                            .map(|v| v.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        let preferred: Vec<String> = config["preferred_peers"].as_array()
                            .map(|v| v.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        let listen_port = config["listen_port"].as_u64().unwrap_or(11625) as u16;
                        
                        info!("Parsed peer config: known={:?}, preferred={:?}, port={}", 
                              known, preferred, listen_port);
                        
                        // Connect libp2p QUIC to all known/preferred peers
                        let all_peers: Vec<_> = known.into_iter().chain(preferred.into_iter()).collect();
                        for addr_str in all_peers {
                            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                                // QUIC uses UDP, port + 1000
                                let libp2p_port = addr.port() + 1000;
                                let libp2p_addr: libp2p::Multiaddr = format!(
                                    "/ip4/{}/udp/{}/quic-v1", 
                                    addr.ip(), 
                                    libp2p_port
                                ).parse().unwrap();
                                
                                let handle = self.libp2p_handle.clone();
                                tokio::spawn(async move {
                                    handle.dial(libp2p_addr).await;
                                });
                            }
                        }
                    }
                }
            }
            
            MessageType::ConnectToPeer => {
                // Connect to a specific peer via libp2p QUIC
                if let Ok(addr_str) = std::str::from_utf8(&msg.payload) {
                    info!("Requested to connect to peer: {}", addr_str);
                    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                        // Connect libp2p QUIC (UDP, port + 1000)
                        let libp2p_port = addr.port() + 1000;
                        let libp2p_addr: libp2p::Multiaddr = format!(
                            "/ip4/{}/udp/{}/quic-v1", 
                            addr.ip(), 
                            libp2p_port
                        ).parse().unwrap();
                        
                        let handle = self.libp2p_handle.clone();
                        tokio::spawn(async move {
                            handle.dial(libp2p_addr).await;
                        });
                    } else {
                        warn!("Invalid peer address: {}", addr_str);
                    }
                }
            }
            
            _ => {
                warn!("Unexpected message type from Core: {:?}", msg.msg_type);
            }
        }
    }
}

fn setup_logging(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};
    
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));
    
    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    
    // Load config
    let mut config = if let Some(path) = &args.config_path {
        match Config::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        Config::default()
    };
    
    // Override socket path from command line
    if let Some(socket) = args.socket_path {
        config.core_socket = socket;
    }
    
    // Override peer port from command line
    if let Some(port) = args.peer_port {
        config.peer_port = port;
    }
    
    // Validate config
    if let Err(e) = config.validate() {
        eprintln!("Invalid config: {}", e);
        std::process::exit(1);
    }
    
    // Setup logging
    setup_logging(&config.log_level);
    
    info!("Stellar Overlay starting");
    info!("Core socket: {}", config.core_socket.display());
    info!("Peer port: {}", config.peer_port);
    info!("Mode: {}", if args.listen_mode { "listen (server)" } else { "connect (client)" });
    
    // Handle SIGTERM/SIGINT for graceful shutdown
    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate()
        ).expect("Failed to register SIGTERM handler");
        
        let mut sigint = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::interrupt()
        ).expect("Failed to register SIGINT handler");
        
        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    };
    
    // Create and run app
    let app = match App::new(config, args.listen_mode).await {
        Ok(app) => app,
        Err(e) => {
            error!("Failed to initialize overlay: {}", e);
            std::process::exit(1);
        }
    };
    
    // Run until shutdown signal or Core disconnects
    tokio::select! {
        _ = app.run() => {}
        _ = shutdown => {
            info!("Shutdown signal received");
        }
    }
    
    info!("Overlay stopped");
}
