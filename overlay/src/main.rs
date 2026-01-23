//! Stellar Overlay Process
//!
//! A process-isolated overlay for stellar-core that handles:
//! - SCP message relay (latency-critical)
//! - Transaction flooding (push-k/pull hybrid)
//! - Peer management
//!
//! Communicates with Core via Unix domain socket IPC.

mod config;
mod ipc;
mod scp;
mod flood;
mod peer;
mod http;
pub mod integrated;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use config::Config;
use ipc::{CoreIpc, Message, MessageType};
use integrated::{Overlay, OverlayHandle, OverlayEvent, CoreCommand};
use flood::{TxSetCache, CachedTxSet, Hash256, build_tx_set_xdr, hash_tx_set};
use peer::NoiseKeypair;

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
    overlay_events: mpsc::UnboundedReceiver<OverlayEvent>,
    /// Cache for built TX sets
    tx_set_cache: Arc<RwLock<TxSetCache>>,
    /// TX set hashes already pushed to Core (reset on ledger close)
    pushed_tx_sets: Arc<RwLock<HashSet<Hash256>>>,
    /// Current ledger sequence
    current_ledger_seq: Arc<RwLock<u32>>,
}

impl App {
    async fn new(config: Config, listen_mode: bool) -> Result<Self, Box<dyn std::error::Error>> {
        // Connect to Core (or listen for connection)
        let core_ipc = if listen_mode {
            CoreIpc::listen(&config.core_socket).await?
        } else {
            CoreIpc::connect(&config.core_socket).await?
        };
        
        // Generate keypair for Noise authentication
        let keypair = NoiseKeypair::generate();
        
        // Parse listen address
        let listen_addr: SocketAddr = format!("0.0.0.0:{}", config.peer_port).parse()?;
        
        // Create channels for overlay communication
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        
        // Create the full overlay with TCP peer support
        let overlay = Overlay::new(keypair, listen_addr, cmd_rx, event_tx);
        let overlay_handle = OverlayHandle::new(cmd_tx);
        
        // Spawn overlay task
        tokio::spawn(async move {
            if let Err(e) = overlay.run().await {
                error!("Overlay error: {}", e);
            }
        });
        
        Ok(Self {
            config,
            core_ipc,
            overlay_handle,
            overlay_events: event_rx,
            tx_set_cache: Arc::new(RwLock::new(TxSetCache::new(100))),
            pushed_tx_sets: Arc::new(RwLock::new(HashSet::new())),
            current_ledger_seq: Arc::new(RwLock::new(0)),
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
                        Some(msg) => self.handle_core_message(msg),
                        None => {
                            info!("Core IPC connection closed");
                            break;
                        }
                    }
                }
                
                // Receive events from overlay (peer connections, SCP messages)
                Some(event) = self.overlay_events.recv() => {
                    self.handle_overlay_event(event);
                }
            }
        }
        
        info!("Overlay shutting down");
    }
    
    /// Handle an event from the overlay
    fn handle_overlay_event(&mut self, event: OverlayEvent) {
        match event {
            OverlayEvent::ScpReceived { envelope, from_peer } => {
                // Forward SCP envelope to Core
                debug!("Forwarding SCP envelope ({} bytes) from peer {} to Core", 
                      envelope.len(), from_peer);
                if let Err(e) = self.core_ipc.sender.send_scp_received(envelope, from_peer) {
                    error!("Failed to send SCP to Core: {}", e);
                }
            }
            OverlayEvent::PeerConnected { peer_id, addr, public_key } => {
                info!("Peer connected: {} ({}) key={:?}", peer_id, addr, &public_key[..4]);
            }
            OverlayEvent::PeerDisconnected { peer_id } => {
                info!("Peer disconnected: {}", peer_id);
            }
        }
    }
    
    /// Handle a message from Core
    fn handle_core_message(&mut self, msg: Message) {
        match msg.msg_type {
            MessageType::Shutdown => {
                info!("Shutdown requested by Core");
                // Exit the process
                std::process::exit(0);
            }
            
            MessageType::BroadcastScp => {
                // Forward SCP broadcast to overlay for peer relay
                debug!("Received BroadcastScp from Core ({} bytes)", msg.payload.len());
                self.overlay_handle.broadcast_scp(msg.payload);
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
                    let (reply_tx, mut reply_rx) = mpsc::channel(1);
                    
                    // TODO: Make max_ops configurable from network config
                    // Use 10000 as max (should be passed from Core's config)
                    let max_ops = 10000usize;
                    
                    let _ = overlay_handle.commands.send(CoreCommand::GetTopTxs {
                        count: max_ops,
                        reply: reply_tx,
                    });
                    
                    // Wait for reply (with timeout)
                    // Returns (tx_hash, tx_data) pairs
                    let txs = match tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        reply_rx.recv()
                    ).await {
                        Ok(Some(txs)) => txs,
                        _ => {
                            warn!("Timeout or error getting transactions from mempool");
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
                    let _ = overlay_handle.commands.send(CoreCommand::CacheTxSet {
                        hash,
                        xdr: xdr.clone(),
                    });
                    
                    // Send hash back to Core
                    if let Err(e) = core_sender.send_nomination_hash(hash) {
                        error!("Failed to send nomination hash: {}", e);
                    }
                });
            }
            
            MessageType::RequestTxSet => {
                // Request TX set by hash - check local cache first, then fetch from peers
                if msg.payload.len() < 32 {
                    warn!("RequestTxSet payload too short");
                    return;
                }
                
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&msg.payload[0..32]);
                
                let tx_set_cache = Arc::clone(&self.tx_set_cache);
                let core_sender = self.core_ipc.sender.clone();
                let overlay_handle = self.overlay_handle.clone();
                
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
                    
                    // Not in local cache - request from peers
                    info!("TX set {:?} not in local cache, requesting from peers", &hash[..4]);
                    
                    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel(1);
                    let _ = overlay_handle.commands.send(CoreCommand::FetchTxSet { 
                        hash, 
                        reply: reply_tx 
                    });
                    
                    // Wait for response with timeout
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        reply_rx.recv()
                    ).await {
                        Ok(Some(Some(xdr))) => {
                            info!("Got TX set {:?} from peer ({} bytes)", &hash[..4], xdr.len());
                            // Cache it
                            {
                                let mut cache = tx_set_cache.write().await;
                                cache.insert(CachedTxSet {
                                    hash,
                                    xdr: xdr.clone(),
                                    ledger_seq: 0,
                                    tx_hashes: vec![],
                                });
                            }
                            if let Err(e) = core_sender.send_tx_set_available(hash, xdr) {
                                error!("Failed to send TX set: {}", e);
                            }
                        }
                        Ok(Some(None)) => {
                            warn!("TX set {:?} not found on any peer", &hash[..4]);
                        }
                        Ok(None) => {
                            warn!("Channel closed while fetching TX set {:?}", &hash[..4]);
                        }
                        Err(_) => {
                            warn!("Timeout fetching TX set {:?} from peers", &hash[..4]);
                        }
                    }
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
                
                // Forward to overlay for mempool insertion and flooding
                let _ = self.overlay_handle.commands.send(CoreCommand::SubmitTx {
                    data: tx_data,
                    fee: fee as u64,
                    num_ops,
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
                    let commands = self.overlay_handle.commands.clone();
                    
                    tokio::spawn(async move {
                        let tx_hashes = {
                            let mut cache = cache.write().await;
                            cache.remove(&hash)
                        };
                        
                        // Remove TXs from mempool
                        if let Some(hashes) = tx_hashes {
                            if !hashes.is_empty() {
                                let _ = commands.send(CoreCommand::RemoveTxsFromMempool {
                                    tx_hashes: hashes,
                                });
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
                        let known = config["known_peers"].as_array()
                            .map(|v| v.iter().filter_map(|s| s.as_str().map(String::from)).collect::<Vec<_>>())
                            .unwrap_or_default();
                        let preferred = config["preferred_peers"].as_array()
                            .map(|v| v.iter().filter_map(|s| s.as_str().map(String::from)).collect::<Vec<_>>())
                            .unwrap_or_default();
                        let listen_port = config["listen_port"].as_u64().unwrap_or(11625) as u16;
                        
                        info!("Parsed peer config: known={:?}, preferred={:?}, port={}", 
                              known, preferred, listen_port);
                        
                        // Send peer config to overlay for connection
                        let _ = self.overlay_handle.commands.send(CoreCommand::SetPeerConfig {
                            known_peers: known,
                            preferred_peers: preferred,
                            listen_port,
                        });
                    }
                }
            }
            
            MessageType::ConnectToPeer => {
                // Connect to a specific peer
                if let Ok(addr_str) = std::str::from_utf8(&msg.payload) {
                    info!("Requested to connect to peer: {}", addr_str);
                    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                        self.overlay_handle.connect_to(addr);
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
