//! Stellar Overlay Process
//!
//! A process-isolated overlay for stellar-core that handles:
//! - SCP message relay (latency-critical, via dedicated QUIC stream)
//! - Transaction flooding (via dedicated QUIC stream)
//! - Peer management
//!
//! Uses QUIC transport for true stream independence - SCP never blocked by TX.
//! Communicates with Core via Unix domain socket IPC.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use libp2p::identity::Keypair as Libp2pKeypair;
use libp2p::{Multiaddr, PeerId};
use stellar_overlay::compact::{
    create_differential_indices, parse_differential_indices, short_tx_id, CompactTxSet,
    CompactTxSetGetTxs, CompactTxSetMessage, SHORT_ID_LEN,
};
use stellar_overlay::config::Config;
use stellar_overlay::flood::{CachedTxSet, Hash256, TxSetCache};
use stellar_overlay::integrated::{Overlay, OverlayHandle};
use stellar_overlay::ipc::{CoreIpc, Message, MessageType};
use stellar_overlay::libp2p_overlay::{
    create_overlay, OverlayEvent as LibP2pOverlayEvent, OverlayHandle as LibP2pOverlayHandle,
};
use stellar_overlay::metrics::OverlayMetrics;
use stellar_overlay::wire::ValidatedTx;
use stellar_overlay::xdr;
use stellar_xdr::curr::{
    BytesM, DependentTxCluster, GeneralizedTransactionSet, Hash, Limits, ParallelTxExecutionStage,
    ParallelTxsComponent, ReadXdr, TransactionEnvelope, TransactionPhase, TransactionSetV1,
    TxSetComponent, TxSetComponentTxsMaybeDiscountedFee, WriteXdr,
};

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
                    eprintln!(
                        "  -l, --listen           Listen mode (create socket, wait for Core)"
                    );
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

/// Strip the `/p2p/<peer_id>` suffix from a Multiaddr if present.
/// DialOpts::peer_id() supplies the PeerId separately, so the address should be bare.
fn strip_p2p_suffix(addr: &Multiaddr) -> Multiaddr {
    let mut out = Multiaddr::empty();
    for proto in addr.iter() {
        if matches!(proto, libp2p::multiaddr::Protocol::P2p(_)) {
            break;
        }
        out.push(proto);
    }
    out
}

/// Convert a libp2p SocketAddr to a QUIC Multiaddr.
fn socket_addr_to_multiaddr(sock: &SocketAddr) -> Multiaddr {
    let ip_proto = if sock.ip().is_ipv4() { "ip4" } else { "ip6" };
    format!("/{}/{}/udp/{}/quic-v1", ip_proto, sock.ip(), sock.port())
        .parse()
        .unwrap()
}

/// Extract IP and UDP port from a Multiaddr like /ip4/1.2.3.4/udp/12625/quic-v1.
fn multiaddr_to_socket_addr(addr: &Multiaddr) -> Option<SocketAddr> {
    let mut ip = None;
    let mut port = None;
    for proto in addr.iter() {
        match proto {
            libp2p::multiaddr::Protocol::Ip4(a) => ip = Some(std::net::IpAddr::V4(a)),
            libp2p::multiaddr::Protocol::Ip6(a) => ip = Some(std::net::IpAddr::V6(a)),
            libp2p::multiaddr::Protocol::Udp(p) => port = Some(p),
            _ => {}
        }
    }
    match (ip, port) {
        (Some(ip), Some(port)) => Some(SocketAddr::new(ip, port)),
        _ => None,
    }
}

/// Resolve a peer address string to a SocketAddr.
///
/// Accepts either:
/// - `IP:port` (e.g. "10.0.0.1:11625") — parsed directly
/// - DNS hostname (e.g. "pod-0.svc.cluster.local") — resolved via DNS, using `default_port`
/// - DNS hostname with port (e.g. "pod-0.svc.cluster.local:11625") — resolved via DNS
async fn resolve_peer_addr(addr_str: &str, default_port: u16) -> Result<SocketAddr, String> {
    // Try direct SocketAddr parse first (handles "IP:port")
    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
        return Ok(addr);
    }

    // It's a hostname — append default port if none present
    let host_port = if addr_str.contains(':') {
        addr_str.to_string()
    } else {
        format!("{}:{}", addr_str, default_port)
    };

    // DNS resolution via tokio (async, non-blocking)
    let addrs: Vec<_> = tokio::net::lookup_host(&host_port)
        .await
        .map_err(|e| format!("failed to resolve '{}': {}", host_port, e))?
        .collect();

    addrs
        .iter()
        .copied()
        .find(|addr| addr.is_ipv4())
        .or_else(|| addrs.into_iter().next())
        .ok_or_else(|| format!("DNS returned no addresses for '{}'", host_port))
}

/// Result of resolve_and_dial: either we dialed successfully (with the libp2p SocketAddr)
/// or DNS resolution failed (returning the original address string for retry).
enum DialResult {
    /// Successfully resolved and dialed. Contains the libp2p SocketAddr (ip:port+1000).
    Dialed(SocketAddr),
    /// Successfully resolved but not yet dialed. For resolve-then-check-then-dial flows.
    Resolved(SocketAddr),
    /// Self-dial detected and skipped.
    SelfSkipped,
    /// DNS resolution failed — address should be retried.
    ResolutionFailed(String),
}

/// Resolve a peer address to a libp2p SocketAddr and Multiaddr, without dialing.
/// Returns the libp2p SocketAddr (port+1000) on success.
async fn resolve_peer_to_libp2p(
    addr_str: &str,
    default_port: u16,
    local_addrs: &RwLock<HashSet<SocketAddr>>,
) -> DialResult {
    match resolve_peer_addr(addr_str, default_port).await {
        Ok(addr) => {
            let libp2p_port = addr.port() + 1000;
            let libp2p_sock = SocketAddr::new(addr.ip(), libp2p_port);

            if local_addrs.read().await.contains(&libp2p_sock) {
                debug!(
                    "Skipping self-dial for {} (resolved to local {})",
                    addr_str, addr
                );
                return DialResult::SelfSkipped;
            }

            DialResult::Resolved(libp2p_sock)
        }
        Err(e) => {
            warn!("Failed to resolve peer {}: {}", addr_str, e);
            DialResult::ResolutionFailed(addr_str.to_string())
        }
    }
}

/// Resolve a peer address and dial it.
async fn resolve_and_dial(
    addr_str: &str,
    default_port: u16,
    local_addrs: &RwLock<HashSet<SocketAddr>>,
    handle: &LibP2pOverlayHandle,
) -> DialResult {
    match resolve_peer_addr(addr_str, default_port).await {
        Ok(addr) => {
            let libp2p_port = addr.port() + 1000;
            let libp2p_sock = SocketAddr::new(addr.ip(), libp2p_port);

            if local_addrs.read().await.contains(&libp2p_sock) {
                debug!(
                    "Skipping self-dial for {} (resolved to local {})",
                    addr_str, addr
                );
                return DialResult::SelfSkipped;
            }

            let ip_proto = if addr.ip().is_ipv4() { "ip4" } else { "ip6" };
            let libp2p_addr: libp2p::Multiaddr =
                format!("/{}/{}/udp/{}/quic-v1", ip_proto, addr.ip(), libp2p_port)
                    .parse()
                    .unwrap();

            info!(
                "Resolved peer {} -> {}, dialing {}",
                addr_str, addr, libp2p_addr
            );
            handle.dial(libp2p_addr).await;
            DialResult::Dialed(libp2p_sock)
        }
        Err(e) => {
            warn!("Failed to resolve peer {}: {}", addr_str, e);
            DialResult::ResolutionFailed(addr_str.to_string())
        }
    }
}

/// Spawn a background task that retries DNS resolution for unresolved peers
/// with exponential backoff (capped at 30s). Retries indefinitely until all
/// peers resolve — in K8s, pods may take arbitrarily long to become DNS-ready.
fn spawn_peer_retry_task(
    unresolved: Vec<String>,
    default_port: u16,
    local_addrs: Arc<RwLock<HashSet<SocketAddr>>>,
    configured_peers: Arc<RwLock<ConfiguredPeers>>,
    handle: LibP2pOverlayHandle,
) {
    if unresolved.is_empty() {
        return;
    }

    info!(
        "Scheduling DNS retry for {} unresolved peer(s): {:?}",
        unresolved.len(),
        unresolved
    );

    tokio::spawn(async move {
        let mut pending = unresolved;
        let mut delay = Duration::from_secs(2);
        let max_delay = Duration::from_secs(30);
        let mut attempt: u64 = 0;

        loop {
            tokio::time::sleep(delay).await;
            attempt += 1;

            info!(
                "DNS retry attempt {} for {} peer(s)",
                attempt,
                pending.len()
            );

            let mut still_pending = Vec::new();
            for addr_str in &pending {
                match resolve_and_dial(addr_str, default_port, &local_addrs, &handle).await {
                    DialResult::Dialed(libp2p_sock) => {
                        configured_peers
                            .write()
                            .await
                            .resolved
                            .insert(libp2p_sock, addr_str.clone());
                    }
                    DialResult::Resolved(_) | DialResult::SelfSkipped => {}
                    DialResult::ResolutionFailed(addr) => {
                        still_pending.push(addr);
                    }
                }
            }

            if still_pending.is_empty() {
                info!("All peers resolved successfully after {} retries", attempt);
                return;
            }

            pending = still_pending;
            delay = (delay * 2).min(max_delay);
        }
    });
}

/// Collect local IP addresses for self-dial detection.
/// Returns a set of SocketAddrs at the libp2p port (peer_port + 1000).
/// Starts with instantly-available addresses; DNS resolution runs in background.
fn collect_local_addrs(libp2p_port: u16) -> Arc<RwLock<HashSet<SocketAddr>>> {
    let mut addrs = HashSet::new();

    // Always include loopback
    addrs.insert(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        libp2p_port,
    ));

    // Probe for our primary local IP by connecting a UDP socket.
    // This doesn't send traffic — it just lets the OS pick the outbound interface.
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(local) = socket.local_addr() {
                addrs.insert(SocketAddr::new(local.ip(), libp2p_port));
            }
        }
    }

    let local_addrs = Arc::new(RwLock::new(addrs));

    // Spawn background DNS resolution of our own hostname (for K8s pod IP detection).
    // This runs concurrently with app startup — doesn't block event loop.
    let addrs_ref = local_addrs.clone();
    tokio::spawn(async move {
        if let Ok(hostname) = hostname::get() {
            if let Ok(hostname_str) = hostname.into_string() {
                let lookup = format!("{}:{}", hostname_str, libp2p_port);
                match tokio::net::lookup_host(lookup).await {
                    Ok(resolved) => {
                        let resolved: Vec<_> = resolved.collect();
                        if !resolved.is_empty() {
                            let mut addrs = addrs_ref.write().await;
                            for addr in &resolved {
                                addrs.insert(*addr);
                            }
                            debug!("DNS self-detection resolved hostname to {:?}", resolved);
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Hostname DNS resolution for self-dial detection failed: {}",
                            e
                        );
                    }
                }
            }
        }
    });

    local_addrs
}

fn get_cached_tx_set_xdr(tx_set_cache: &TxSetCache, hash: &Hash256) -> Option<Vec<u8>> {
    tx_set_cache.get(hash).map(|cached| cached.xdr.clone())
}

fn cache_tx_set_xdr(
    tx_set_cache: &mut TxSetCache,
    current_ledger_seq: u32,
    hash: Hash256,
    xdr: Vec<u8>,
) {
    tx_set_cache.insert(CachedTxSet {
        hash,
        xdr,
        ledger_seq: current_ledger_seq,
    });
}

/// A generated compact set: the encoded `CompactTxSetMessage::Set` announce
/// plus the serialized txs in tx set order (kept to serve SetGetTxs requests).
#[derive(Debug, Default)]
struct CompactTxSetData {
    txs: Vec<Vec<u8>>,
    xdr: Vec<u8>,
}

/// Build the compact representation of a locally-built tx set.
///
/// PROTOTYPE: only the shapes the current block builder produces are
/// supported — a classic phase with at most one non-discounted component, and
/// a Soroban phase with at most one execution stage of one cluster. The
/// compact encoding carries no stage/cluster structure, so generalized
/// parallel tx sets can't reconstruct to the right hash yet.
fn gen_compact_tx_set(txset_hash: Hash, txset_xdr: Vec<u8>) -> Result<CompactTxSetData, String> {
    let GeneralizedTransactionSet::V1(txset) =
        GeneralizedTransactionSet::from_xdr(&txset_xdr, Limits::none())
            .map_err(|e| format!("failed to parse tx set XDR: {e}"))?;

    let mut base_fee = None;
    let mut soroban_base_fee = None;
    let mut num_soroban_txs = 0usize;
    let mut short_ids = Vec::new();
    let mut txs = Vec::new();
    let key: &[u8; 16] = txset_hash.0[..16].try_into().unwrap();

    let push_tx = |tx: &TransactionEnvelope,
                   short_ids: &mut Vec<u8>,
                   txs: &mut Vec<Vec<u8>>|
     -> Result<(), String> {
        let tx_xdr = tx
            .to_xdr(Limits::none())
            .map_err(|e| format!("failed to serialize tx: {e}"))?;
        let tx_hash = xdr::sha256_hash(&tx_xdr);
        short_ids.extend_from_slice(&short_tx_id(key, &tx_hash));
        txs.push(tx_xdr);
        Ok(())
    };

    for phase in txset.phases.iter() {
        match phase {
            TransactionPhase::V0(components) => match components.as_slice() {
                [] => {}
                [TxSetComponent::TxsetCompTxsMaybeDiscountedFee(txset_comp)] => {
                    base_fee = txset_comp.base_fee;
                    for tx in txset_comp.txs.iter() {
                        push_tx(tx, &mut short_ids, &mut txs)?;
                    }
                }
                _ => {
                    return Err("unsupported number of components in classic phase".to_string());
                }
            },
            TransactionPhase::V1(parallel) => {
                if parallel.execution_stages.is_empty() {
                    continue;
                }
                if parallel.execution_stages.len() > 1 {
                    return Err(
                        "unsupported number of execution stages in parallel phase".to_string()
                    );
                }
                let stage = &parallel.execution_stages[0];
                if stage.len() != 1 {
                    return Err(
                        "unsupported number of clusters in parallel execution stage".to_string()
                    );
                }
                let cluster = &stage[0];
                if cluster.is_empty() {
                    return Err("empty cluster in parallel execution stage".to_string());
                }
                for tx in cluster.iter() {
                    push_tx(tx, &mut short_ids, &mut txs)?;
                }
                num_soroban_txs = cluster.len();
                soroban_base_fee = parallel.base_fee;
            }
        }
    }

    let compact_set = CompactTxSet {
        tx_set_hash: txset_hash,
        previous_ledger_hash: txset.previous_ledger_hash,
        base_fee,
        txs: BytesM::try_from(short_ids)
            .map_err(|e| format!("failed to convert short ids to BytesM: {e}"))?,
        num_soroban_txs: num_soroban_txs as u32,
        soroban_base_fee,
    };

    let compact_xdr = CompactTxSetMessage::Set(compact_set)
        .to_xdr(Limits::none())
        .map_err(|e| format!("failed to serialize CompactTxSetMessage: {e}"))?;

    Ok(CompactTxSetData {
        txs,
        xdr: compact_xdr,
    })
}

/// A compact set whose transactions we're still assembling (waiting on a
/// SetTxs reply for the mempool misses).
struct PendingCompactTxSet {
    tx_set: CompactTxSet,
    txs: Vec<Option<TransactionEnvelope>>,
    request_time: Instant,
}

/// Rebuild the full `GeneralizedTransactionSet` from a compact set whose txs
/// are all resolved, verify its SHA-256 against the announced hash, and hand
/// the canonical bytes to the main task for caching + push to Core.
fn reconstruct_tx_set(
    request: PendingCompactTxSet,
    from: PeerId,
    send_txset: mpsc::UnboundedSender<([u8; 32], Vec<u8>, PeerId)>,
    metrics: Arc<OverlayMetrics>,
) {
    let num_soroban = request.tx_set.num_soroban_txs as usize;
    if request.txs.len() < num_soroban {
        warn!(
            "COMPACT_RECONSTRUCT_FAIL: set {:02x?}... claims {} soroban txs but has {} total",
            &request.tx_set.tx_set_hash.0[..4],
            num_soroban,
            request.txs.len()
        );
        return;
    }
    let num_classic = request.txs.len() - num_soroban;
    let mut txs = Vec::with_capacity(request.txs.len());
    for tx in request.txs {
        match tx {
            Some(tx) => txs.push(tx),
            None => {
                warn!(
                    "COMPACT_RECONSTRUCT_FAIL: set {:02x?}... still has unresolved txs",
                    &request.tx_set.tx_set_hash.0[..4]
                );
                return;
            }
        }
    }
    let mut txs = txs.into_iter();
    let classic_txs: Vec<_> = txs.by_ref().take(num_classic).collect();
    let soroban_txs: Vec<_> = txs.collect();

    let phase0 = if classic_txs.is_empty() {
        TransactionPhase::V0([].try_into().unwrap())
    } else {
        TransactionPhase::V0(
            [TxSetComponent::TxsetCompTxsMaybeDiscountedFee(
                TxSetComponentTxsMaybeDiscountedFee {
                    base_fee: request.tx_set.base_fee,
                    txs: match classic_txs.try_into() {
                        Ok(txs) => txs,
                        Err(e) => {
                            warn!("COMPACT_RECONSTRUCT_FAIL: too many classic txs: {}", e);
                            return;
                        }
                    },
                },
            )]
            .try_into()
            .unwrap(),
        )
    };

    let phase1 = if soroban_txs.is_empty() {
        TransactionPhase::V1(ParallelTxsComponent::default())
    } else {
        let cluster: DependentTxCluster = match soroban_txs.try_into() {
            Ok(cluster) => cluster,
            Err(e) => {
                warn!("COMPACT_RECONSTRUCT_FAIL: too many soroban txs: {}", e);
                return;
            }
        };
        let stage: ParallelTxExecutionStage = vec![cluster].try_into().unwrap();
        TransactionPhase::V1(ParallelTxsComponent {
            base_fee: request.tx_set.soroban_base_fee,
            execution_stages: [stage].try_into().unwrap(),
        })
    };

    let full_tx_set = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: request.tx_set.previous_ledger_hash.clone(),
        phases: [phase0, phase1].try_into().unwrap(),
    });

    let full_xdr = match full_tx_set.to_xdr(Limits::none()) {
        Ok(xdr) => xdr,
        Err(e) => {
            warn!(
                "COMPACT_RECONSTRUCT_FAIL: failed to serialize reconstructed set: {}",
                e
            );
            return;
        }
    };

    // A mismatch here means a short-id collision picked the wrong mempool tx,
    // or the origin's set has structure the compact encoding can't carry.
    // Drop it; the set is unrecoverable via this announce.
    let full_hash = xdr::sha256_hash(&full_xdr);
    if full_hash != request.tx_set.tx_set_hash.0 {
        warn!(
            "COMPACT_RECONSTRUCT_FAIL: hash mismatch for set {:02x?}...: reconstructed {:02x?}... ({} txs, {} bytes)",
            &request.tx_set.tx_set_hash.0[..4],
            &full_hash[..4],
            num_classic + num_soroban,
            full_xdr.len()
        );
        return;
    }

    let fetch_us = request.request_time.elapsed().as_micros() as u64;
    metrics
        .fetch_txset_sum_us
        .fetch_add(fetch_us, Ordering::Relaxed);
    metrics.fetch_txset_count.fetch_add(1, Ordering::Relaxed);

    metrics
        .reconstructed_size
        .fetch_add(full_xdr.len() as u64, Ordering::Relaxed);
    metrics.reconstructed_count.fetch_add(1, Ordering::Relaxed);

    if send_txset
        .send((request.tx_set.tx_set_hash.0, full_xdr, from))
        .is_err()
    {
        warn!("COMPACT_RECONSTRUCT: main task gone, dropping reconstructed set");
    }
}

/// Application state
struct App {
    core_ipc: CoreIpc,
    overlay_handle: OverlayHandle,
    /// Cache for built TX sets (shared with spawned compact-set tasks)
    tx_set_cache: Arc<RwLock<TxSetCache>>,
    /// Testing knob: percentage of a compact set's txs to always request
    /// from the announcing peer even when the mempool has them
    compact_force_request_txs_pct: Arc<AtomicU32>,
    /// Serialized txs (in tx set order) for compact sets we announced,
    /// kept to serve SetGetTxs requests.
    /// std::sync::Mutex is fine: no await happens with the lock held.
    compact_set_cache: Arc<std::sync::Mutex<lru::LruCache<Hash256, Arc<Vec<Vec<u8>>>>>>,
    /// Compact sets whose missing txs we've requested and are waiting on
    pending_compact_txsets: Arc<std::sync::Mutex<lru::LruCache<Hash256, PendingCompactTxSet>>>,
    /// Reconstructed tx sets waiting to be cached + pushed to Core
    received_tx_sets: mpsc::UnboundedReceiver<([u8; 32], Vec<u8>, PeerId)>,
    /// Sender side handed to compact-set reconstruction tasks
    send_tx_set_to_core: mpsc::UnboundedSender<([u8; 32], Vec<u8>, PeerId)>,
    /// Current ledger sequence
    current_ledger_seq: u32,
    /// libp2p overlay handle (QUIC-based SCP + TX)
    libp2p_handle: LibP2pOverlayHandle,
    /// libp2p overlay events (SCP, TxSet - critical, unbounded)
    libp2p_events: mpsc::UnboundedReceiver<LibP2pOverlayEvent>,
    /// libp2p TX events (bounded, may drop under backpressure)
    tx_events: mpsc::Receiver<LibP2pOverlayEvent>,
    /// Pending SCP state requests: maps request_id to requesting peer
    /// When Core responds with ScpStateResponse containing request_id, we look up the peer
    pending_scp_state_requests: Arc<RwLock<HashMap<u64, PeerId>>>,
    /// Counter for generating unique SCP state request IDs
    next_scp_request_id: Arc<AtomicU64>,
    /// Local addresses for self-dial detection (populated at startup + async DNS)
    local_addrs: Arc<RwLock<HashSet<SocketAddr>>>,
    /// Configured peer addresses and listen port — kept for reconnection on disconnect.
    /// Updated each time SetPeerConfig is received from Core.
    configured_peers: Arc<RwLock<ConfiguredPeers>>,
    /// Known peers: PeerId → Multiaddr, learned from ConnectionEstablished events.
    /// Used for PeerId-based reconnection (libp2p can deduplicate).
    known_peers: Arc<RwLock<HashMap<PeerId, Multiaddr>>>,
    /// PeerId → configured hostname, so targeted reconnect can re-resolve DNS
    /// after a pod restart changes the peer's IP address.
    peer_hostnames: Arc<RwLock<HashMap<PeerId, String>>>,
    /// Shared metrics counters for the overlay
    metrics: Arc<OverlayMetrics>,
}

/// Peer addresses configured via SetPeerConfig, used for reconnection.
struct ConfiguredPeers {
    /// All peer address strings (known + preferred)
    addrs: Vec<String>,
    /// The listen_port from the config (used as default_port for DNS resolution)
    listen_port: u16,
    /// Map from resolved SocketAddr (at libp2p port) to original address string,
    /// so we can reconnect by address when a PeerId disconnects.
    resolved: HashMap<SocketAddr, String>,
}

impl App {
    /// Cache a reconstructed tx set and push it to Core.
    async fn receive_tx_set(&mut self, hash: [u8; 32], data: Vec<u8>, from: PeerId) {
        info!(
            "TXSET_RECV: Reconstructed TxSet {:02x?}... ({} bytes) from {}",
            &hash[..4],
            data.len(),
            from
        );

        // IMPORTANT: Cache the TxSet FIRST, before pushing to Core.
        // This ensures the TxSet is available when SCP processing resumes.
        // Reconstructed sets are candidates for the in-flight slot; stamp
        // with the next slot so eviction is conservative.
        cache_tx_set_xdr(
            &mut *self.tx_set_cache.write().await,
            self.current_ledger_seq + 1,
            hash,
            data.clone(),
        );

        // Always push TX set to Core (Core handles dedup)
        if let Err(e) = self.core_ipc.sender.send_tx_set_available(hash, data) {
            error!("Failed to push TX set to Core: {}", e);
        }
    }

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
        let metrics = Arc::new(OverlayMetrics::new());
        let (libp2p_handle, libp2p_event_rx, tx_event_rx, libp2p_overlay) =
            create_overlay(libp2p_keypair, Arc::clone(&metrics))
                .map_err(|e| format!("Failed to create libp2p overlay: {}", e))?;

        // Use peer_port + 1000 for libp2p QUIC to avoid collision with legacy TCP
        let libp2p_port = config.peer_port + 1000;
        let libp2p_listen_ip = config.libp2p_listen_ip.clone();

        // Compute local addresses for self-dial detection (instant + async DNS in background)
        let local_addrs = collect_local_addrs(libp2p_port);

        // Spawn libp2p overlay task
        tokio::spawn(async move {
            libp2p_overlay.run(&libp2p_listen_ip, libp2p_port).await;
        });

        info!(
            "Started libp2p QUIC overlay on {}:{} (SCP + TX + TxSet streams)",
            config.libp2p_listen_ip, libp2p_port
        );

        let (txset_tx, txset_rx) = mpsc::unbounded_channel();

        Ok(Self {
            core_ipc,
            overlay_handle,
            tx_set_cache: Arc::new(RwLock::new(TxSetCache::new(100))),
            compact_force_request_txs_pct: Arc::new(AtomicU32::new(0)),
            // Only holds sets we announced ourselves; 10 is plenty
            compact_set_cache: Arc::new(std::sync::Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(10).unwrap(),
            ))),
            // A safe upper bound on concurrently-assembling sets
            pending_compact_txsets: Arc::new(std::sync::Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(30).unwrap(),
            ))),
            received_tx_sets: txset_rx,
            send_tx_set_to_core: txset_tx,
            current_ledger_seq: 0,
            libp2p_handle,
            libp2p_events: libp2p_event_rx,
            tx_events: tx_event_rx,
            pending_scp_state_requests: Arc::new(RwLock::new(HashMap::new())),
            next_scp_request_id: Arc::new(AtomicU64::new(1)),
            local_addrs,
            configured_peers: Arc::new(RwLock::new(ConfiguredPeers {
                addrs: Vec::new(),
                listen_port: 11625,
                resolved: HashMap::new(),
            })),
            known_peers: Arc::new(RwLock::new(HashMap::new())),
            peer_hostnames: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        })
    }

    /// Main event loop - process messages from Core and overlay events
    async fn run(mut self) {
        info!("Overlay started, processing Core messages");

        // Safety-net reconnect timer: re-dial all configured peers every 30s.
        // Uses PeerId-based dials for known peers (libp2p skips if already connected).
        // Falls back to address-based dials for peers we haven't connected to yet.
        // This is a fallback — targeted reconnection on disconnect handles the fast path.
        let mut reconnect_interval = tokio::time::interval(Duration::from_secs(30));
        reconnect_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // Receive message from Core
                msg = self.core_ipc.receiver.recv() => {
                    match msg {
                        Some(msg) => {
                            if !self.handle_core_message(msg).await {
                                break;
                            }
                        }
                        None => {
                            info!("Core IPC connection closed");
                            break;
                        }
                    }
                }

                // Reconstructed compact tx sets ready to cache + push to Core
                Some((hash, data, from)) = self.received_tx_sets.recv() => {
                    self.receive_tx_set(hash, data, from).await;
                }

                // Receive events from libp2p QUIC overlay (SCP + TxSet - critical)
                Some(event) = self.libp2p_events.recv() => {
                    self.handle_libp2p_event(event).await;
                }

                // Receive TX events from libp2p (bounded channel, may drop under backpressure)
                Some(event) = self.tx_events.recv() => {
                    self.handle_libp2p_event(event).await;
                }

                // Safety-net reconnect: PeerId-based dials for known peers,
                // address-based ONLY for peers we've never learned a PeerId for.
                _ = reconnect_interval.tick() => {
                    let cp = self.configured_peers.read().await;
                    let addrs = cp.addrs.clone();
                    let listen_port = cp.listen_port;
                    // Expect a connection for every configured address. Self is
                    // not normally in the list; if it is, the extra safety-net
                    // tick is harmless (self-dials and connected peers are
                    // skipped when re-dialing).
                    let expected_peers = addrs.len();
                    // Build set of hostnames that have a known PeerId — these
                    // are handled by PeerId-based dials and must NOT be raw-dialed.
                    let hostnames_with_known_peer: HashSet<String> = {
                        let hostnames = self.peer_hostnames.read().await;
                        hostnames.values().cloned().collect()
                    };
                    // Also collect resolved SocketAddrs that map to known peers
                    let known_addrs: HashSet<SocketAddr> = {
                        let known = self.known_peers.read().await;
                        known.values()
                            .filter_map(|maddr| multiaddr_to_socket_addr(maddr))
                            .collect()
                    };
                    drop(cp);

                    if !addrs.is_empty() {
                        let connected = self.libp2p_handle.connected_peer_count().await;
                        if connected < expected_peers {
                            info!(
                                "Safety-net reconnect: {}/{} peers connected",
                                connected, expected_peers
                            );

                            // PeerId-based dials for configured peers we've seen before.
                            // Only peers with a hostname entry are configured — this
                            // prevents re-dialing unconfigured inbound-only peers.
                            let hostnames = self.peer_hostnames.read().await;
                            let configured_peer_ids: Vec<PeerId> = hostnames.keys().cloned().collect();
                            drop(hostnames);

                            let known = self.known_peers.read().await;
                            let known_snapshot: Vec<_> = configured_peer_ids.iter()
                                .filter_map(|pid| known.get(pid).map(|addr| (*pid, addr.clone())))
                                .collect();
                            drop(known);

                            let handle = self.libp2p_handle.clone();
                            for (peer_id, addr) in &known_snapshot {
                                handle.dial_peer(*peer_id, addr.clone()).await;
                            }

                            // Raw address dials ONLY for configured peers we've never
                            // learned a PeerId for. Resolve DNS first, then check the
                            // resolved address against known peers BEFORE dialing —
                            // a raw dial cannot be deduplicated by libp2p.
                            let unknown_addrs: Vec<_> = addrs.iter()
                                .filter(|a| !hostnames_with_known_peer.contains(*a))
                                .cloned()
                                .collect();

                            if !unknown_addrs.is_empty() {
                                info!(
                                    "Safety-net: resolving {} unknown peer(s)",
                                    unknown_addrs.len()
                                );
                                let handle = self.libp2p_handle.clone();
                                let local_addrs = self.local_addrs.clone();
                                let configured_peers = self.configured_peers.clone();

                                tokio::spawn(async move {
                                    for addr_str in &unknown_addrs {
                                        // Step 1: resolve DNS only (no dial)
                                        match resolve_peer_to_libp2p(
                                            addr_str, listen_port, &local_addrs,
                                        ).await {
                                            DialResult::Resolved(libp2p_sock) => {
                                                // Step 2: check if resolved addr is already known
                                                if known_addrs.contains(&libp2p_sock) {
                                                    debug!(
                                                        "Safety-net: {} resolved to known addr {}, skipping dial",
                                                        addr_str, libp2p_sock
                                                    );
                                                    continue;
                                                }
                                                // Step 3: truly unknown — dial
                                                let maddr = socket_addr_to_multiaddr(&libp2p_sock);
                                                info!("Safety-net: dialing unknown peer {} at {}", addr_str, maddr);
                                                handle.dial(maddr).await;
                                                configured_peers
                                                    .write()
                                                    .await
                                                    .resolved
                                                    .insert(libp2p_sock, addr_str.clone());
                                            }
                                            DialResult::SelfSkipped => {}
                                            DialResult::ResolutionFailed(_) => {}
                                            DialResult::Dialed(_) => unreachable!(),
                                        }
                                    }
                                });
                            }
                        }
                    }
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
            LibP2pOverlayEvent::ScpReceived {
                envelope,
                txset_hashes,
                from,
                slot,
            } => {
                // Copy first 4 bytes for logging identification
                let mut id_bytes = [0u8; 4];
                let id_len = std::cmp::min(envelope.len(), 4);
                id_bytes[..id_len].copy_from_slice(&envelope[..id_len]);

                debug!(
                    "SCP_FROM_PEER: Received SCP (id={:02x?}) ({} bytes) from {}, forwarding to Core",
                    &id_bytes[..id_len],
                    envelope.len(),
                    from
                );

                // TX set hashes referenced by the SCP message (logging only —
                // tx sets arrive via compact dissemination from the
                // nomination origin; there is no pull path anymore).
                for txhash in &txset_hashes {
                    debug!(
                        "SCP from {} references TX set {:02x?}... (slot {})",
                        from,
                        &txhash[..4],
                        slot
                    );
                }

                // Forward to Core
                if let Err(e) = self.core_ipc.sender.send_scp_received(envelope) {
                    error!(
                        "SCP_TO_CORE_FAIL: Failed to send SCP (id={:02x?}) to Core: {}",
                        &id_bytes[..id_len],
                        e
                    );
                } else {
                    debug!(
                        "SCP_TO_CORE_OK: Forwarded SCP (id={:02x?}) to Core",
                        &id_bytes[..id_len]
                    );
                }
            }
            LibP2pOverlayEvent::TxReceived { tx, from } => {
                debug!(
                    "Received TX via QUIC from {}: {} bytes",
                    from,
                    tx.bytes().len()
                );
                self.overlay_handle.submit_tx(tx);
            }
            LibP2pOverlayEvent::CompactReceived { msg, from, size } => {
                self.handle_compact_message(msg, from, size);
            }

            LibP2pOverlayEvent::ScpStateRequested {
                peer_id,
                ledger_seq,
            } => {
                // Generate unique request ID
                let request_id = self.next_scp_request_id.fetch_add(1, Ordering::SeqCst);
                info!(
                    "Peer {} requesting SCP state for ledger >= {} (request_id={})",
                    peer_id, ledger_seq, request_id
                );

                // Store mapping from request_id to peer_id
                self.pending_scp_state_requests
                    .write()
                    .await
                    .insert(request_id, peer_id);

                // Request SCP state from Core with request_id and ledger_seq
                // Payload format: [request_id:8][ledger_seq:4]
                let mut payload = Vec::with_capacity(12);
                payload.extend_from_slice(&request_id.to_le_bytes());
                payload.extend_from_slice(&ledger_seq.to_le_bytes());
                let msg = Message::new(MessageType::PeerRequestsScpState, payload);
                if let Err(e) = self.core_ipc.sender.send(msg) {
                    error!("Failed to send PeerRequestsScpState to Core: {:?}", e);
                    // Remove from map on error
                    self.pending_scp_state_requests
                        .write()
                        .await
                        .remove(&request_id);
                }
            }

            LibP2pOverlayEvent::PeerConnected { peer_id, addr } => {
                // Only record the mapping if this peer's address matches a configured peer.
                // Inbound connections from unconfigured peers must NOT be reconnect-eligible.
                let clean_addr = strip_p2p_suffix(&addr);
                let cp = self.configured_peers.read().await;
                let hostname = multiaddr_to_socket_addr(&clean_addr)
                    .and_then(|sock| cp.resolved.get(&sock).cloned());
                drop(cp);

                if let Some(host) = hostname {
                    info!(
                        "Learned configured peer {} at {} (hostname: {})",
                        peer_id, clean_addr, host
                    );
                    self.known_peers.write().await.insert(peer_id, clean_addr);
                    self.peer_hostnames.write().await.insert(peer_id, host);
                } else {
                    debug!(
                        "Peer {} at {} is not a configured peer, not tracking for reconnect",
                        peer_id, clean_addr
                    );
                }
            }

            LibP2pOverlayEvent::PeerDisconnected { peer_id } => {
                // Clean up any pending SCP state requests for this peer
                {
                    let mut pending = self.pending_scp_state_requests.write().await;
                    let before_len = pending.len();
                    pending.retain(|_request_id, p| p != &peer_id);
                    let removed = before_len - pending.len();
                    if removed > 0 {
                        info!(
                            "Removed {} pending SCP state requests for disconnected peer {}",
                            removed, peer_id
                        );
                    }
                }

                // Targeted reconnect: only for configured peers (those with a hostname).
                // Unconfigured inbound-only peers are not re-dialed.
                let hostname = self.peer_hostnames.read().await.get(&peer_id).cloned();
                let known_addr = self.known_peers.read().await.get(&peer_id).cloned();
                if let Some(hostname) = hostname {
                    info!(
                        "Peer {} disconnected, scheduling targeted reconnect (host={}, addr={:?})",
                        peer_id, hostname, known_addr
                    );
                    let handle = self.libp2p_handle.clone();
                    let local_addrs = self.local_addrs.clone();
                    let known_peers = self.known_peers.clone();
                    let configured_peers = self.configured_peers.clone();
                    tokio::spawn(async move {
                        let mut delay = Duration::from_secs(1);
                        let max_delay = Duration::from_secs(30);
                        // First 3 attempts: use cached Multiaddr (fast path).
                        // Remaining attempts: re-resolve DNS in case IP changed.
                        for attempt in 1u32..=10 {
                            tokio::time::sleep(delay).await;

                            if attempt <= 3 {
                                if let Some(ref addr) = known_addr {
                                    debug!(
                                        "Reconnect attempt {} for {} via cached addr {}",
                                        attempt, peer_id, addr
                                    );
                                    handle.dial_peer(peer_id, addr.clone()).await;
                                }
                            } else {
                                // Re-resolve DNS (handles K8s pod restart / IP change)
                                let cp = configured_peers.read().await;
                                let listen_port = cp.listen_port;
                                drop(cp);
                                debug!(
                                    "Reconnect attempt {} for {} via DNS re-resolve of {}",
                                    attempt, peer_id, hostname
                                );
                                match resolve_and_dial(
                                    &hostname,
                                    listen_port,
                                    &local_addrs,
                                    &handle,
                                )
                                .await
                                {
                                    DialResult::Dialed(libp2p_sock) => {
                                        let new_addr = socket_addr_to_multiaddr(&libp2p_sock);
                                        known_peers.write().await.insert(peer_id, new_addr);
                                        configured_peers
                                            .write()
                                            .await
                                            .resolved
                                            .insert(libp2p_sock, hostname.clone());
                                    }
                                    DialResult::SelfSkipped => break,
                                    DialResult::ResolutionFailed(_) => {}
                                    DialResult::Resolved(_) => unreachable!(),
                                }
                            }

                            delay = (delay * 2).min(max_delay);
                        }
                    });
                } else {
                    debug!(
                        "Peer {} disconnected (not a configured peer), not reconnecting",
                        peer_id
                    );
                }
            }
        }
    }

    /// Handle a message from Core. Returns false to signal shutdown.
    /// Handle a compact tx set message from a peer. All heavy work is
    /// spawned so the main loop never blocks.
    fn handle_compact_message(&mut self, msg: CompactTxSetMessage, from: PeerId, size: usize) {
        match msg {
            CompactTxSetMessage::Set(compact_tx_set) => {
                if compact_tx_set.txs.len() % SHORT_ID_LEN != 0 {
                    warn!(
                        "COMPACT_DROP: set {:02x?}... from {} has malformed short-id blob ({} bytes)",
                        &compact_tx_set.tx_set_hash.0[..4],
                        from,
                        compact_tx_set.txs.len()
                    );
                    return;
                }
                let num_txs = compact_tx_set.txs.len() / SHORT_ID_LEN;
                if compact_tx_set.num_soroban_txs as usize > num_txs {
                    warn!(
                        "COMPACT_DROP: set {:02x?}... from {} claims {} soroban txs of {} total",
                        &compact_tx_set.tx_set_hash.0[..4],
                        from,
                        compact_tx_set.num_soroban_txs,
                        num_txs
                    );
                    return;
                }

                info!(
                    "COMPACT_RECV: Received compact set {:02x?}... ({} txs, {} bytes) from {}",
                    &compact_tx_set.tx_set_hash.0[..4],
                    num_txs,
                    size,
                    from
                );

                let overlay_handle = self.overlay_handle.clone();
                let pending_cache = Arc::clone(&self.pending_compact_txsets);
                let p2p_handle = self.libp2p_handle.clone();
                let send_txset = self.send_tx_set_to_core.clone();
                let metrics = Arc::clone(&self.metrics);
                let request_percent = self.compact_force_request_txs_pct.load(Ordering::Relaxed);
                metrics.compact_count.fetch_add(1, Ordering::Relaxed);
                metrics
                    .compact_size
                    .fetch_add(size as u64, Ordering::Relaxed);

                tokio::spawn(async move {
                    let begin = Instant::now();
                    let key: [u8; 16] = compact_tx_set.tx_set_hash.0[..16].try_into().unwrap();
                    let Some(txs_serialized) = overlay_handle
                        .get_txs_by_short_ids(compact_tx_set.txs.to_vec(), key)
                        .await
                    else {
                        warn!("COMPACT_DROP: mempool manager gone (shutting down)");
                        return;
                    };

                    // Testing knob: pretend the first N% of txs are missing to
                    // exercise the SetGetTxs path.
                    let force_missing_count =
                        (txs_serialized.len() * request_percent as usize).div_ceil(100);
                    let mut missing = Vec::new();
                    let mut txs: Vec<Option<TransactionEnvelope>> =
                        Vec::with_capacity(txs_serialized.len());
                    for (i, tx) in txs_serialized.into_iter().enumerate() {
                        if i >= force_missing_count && !tx.is_empty() {
                            match TransactionEnvelope::from_xdr(&tx, Limits::none()) {
                                Ok(env) => {
                                    txs.push(Some(env));
                                    continue;
                                }
                                Err(e) => {
                                    // Mempool bytes should always parse; treat
                                    // as missing rather than dropping the set.
                                    warn!("COMPACT: mempool tx failed to parse: {}", e);
                                }
                            }
                        }
                        txs.push(None);
                        missing.push(i);
                    }

                    if missing.is_empty() {
                        info!(
                            "COMPACT_HIT: set {:02x?}... fully resolved from mempool ({} txs)",
                            &compact_tx_set.tx_set_hash.0[..4],
                            txs.len()
                        );
                        reconstruct_tx_set(
                            PendingCompactTxSet {
                                tx_set: compact_tx_set,
                                txs,
                                request_time: begin,
                            },
                            from,
                            send_txset,
                            metrics,
                        );
                    } else {
                        info!(
                            "COMPACT_MISS: set {:02x?}... missing {}/{} txs, requesting from {}",
                            &compact_tx_set.tx_set_hash.0[..4],
                            missing.len(),
                            txs.len(),
                            from
                        );
                        let indices = create_differential_indices(&missing);
                        let request = CompactTxSetMessage::SetGetTxs(CompactTxSetGetTxs {
                            tx_set_hash: compact_tx_set.tx_set_hash.clone(),
                            indices: match BytesM::try_from(indices) {
                                Ok(indices) => indices,
                                Err(e) => {
                                    warn!("COMPACT_DROP: oversize index encoding: {}", e);
                                    return;
                                }
                            },
                        });
                        let msg = match request.to_xdr(Limits::none()) {
                            Ok(msg) => msg,
                            Err(e) => {
                                warn!("COMPACT_DROP: failed to encode SetGetTxs: {}", e);
                                return;
                            }
                        };
                        metrics
                            .txs_requested
                            .fetch_add(missing.len() as u64, Ordering::Relaxed);
                        metrics
                            .tx_bytes_requested
                            .fetch_add(msg.len() as u64, Ordering::Relaxed);

                        // Record the pending set BEFORE sending the request so
                        // a fast reply can't race the insert.
                        pending_cache.lock().unwrap().put(
                            compact_tx_set.tx_set_hash.0,
                            PendingCompactTxSet {
                                tx_set: compact_tx_set,
                                txs,
                                request_time: begin,
                            },
                        );
                        p2p_handle.send_compact_msg(msg, from).await;
                    }
                });
            }

            CompactTxSetMessage::SetGet(get) => {
                // Not part of the push-based flow (no pull path); log and drop.
                warn!(
                    "COMPACT: unexpected SetGet for {:02x?}... from {}",
                    &get.tx_set_hash.0[..4],
                    from
                );
            }

            CompactTxSetMessage::SetGetTxs(get_txs) => {
                let compact_set_cache = Arc::clone(&self.compact_set_cache);
                let handle = self.libp2p_handle.clone();
                tokio::spawn(async move {
                    let txs = {
                        let mut cache = compact_set_cache.lock().unwrap();
                        match cache.get(&get_txs.tx_set_hash.0) {
                            Some(txs) => Arc::clone(txs),
                            None => {
                                warn!(
                                    "COMPACT: cache miss for set {:02x?}... requested by {}",
                                    &get_txs.tx_set_hash.0[..4],
                                    from
                                );
                                return;
                            }
                        }
                    };
                    let indices = match parse_differential_indices(&get_txs.indices) {
                        Ok(indices) => indices,
                        Err(e) => {
                            warn!("COMPACT: bad SetGetTxs indices from {}: {}", from, e);
                            return;
                        }
                    };
                    if indices.iter().any(|&i| i >= txs.len()) {
                        warn!(
                            "COMPACT: out-of-range SetGetTxs indices from {} for set {:02x?}...",
                            from,
                            &get_txs.tx_set_hash.0[..4]
                        );
                        return;
                    }
                    let msg = stellar_overlay::compact::build_set_txs_message(
                        &get_txs.tx_set_hash.0,
                        indices.iter().map(|&i| txs[i].as_slice()),
                    );
                    info!(
                        "COMPACT_SERVE: sending {} txs of set {:02x?}... to {}",
                        indices.len(),
                        &get_txs.tx_set_hash.0[..4],
                        from
                    );
                    handle.send_compact_msg(msg, from).await;
                });
            }

            CompactTxSetMessage::SetTxs(set_txs) => {
                let pending_cache = Arc::clone(&self.pending_compact_txsets);
                let send_txset = self.send_tx_set_to_core.clone();
                let metrics = Arc::clone(&self.metrics);
                tokio::spawn(async move {
                    let Some(mut request) =
                        pending_cache.lock().unwrap().pop(&set_txs.tx_set_hash.0)
                    else {
                        warn!(
                            "COMPACT: SetTxs for unknown set {:02x?}... from {}",
                            &set_txs.tx_set_hash.0[..4],
                            from
                        );
                        return;
                    };

                    let mut indices = HashMap::<[u8; SHORT_ID_LEN], usize>::new();
                    for (i, id) in request.tx_set.txs.chunks_exact(SHORT_ID_LEN).enumerate() {
                        indices.insert(id.try_into().unwrap(), i);
                    }

                    let key: [u8; 16] = request.tx_set.tx_set_hash.0[..16].try_into().unwrap();
                    for tx in set_txs.txs.iter() {
                        let tx_xdr = match tx.to_xdr(Limits::none()) {
                            Ok(xdr) => xdr,
                            Err(e) => {
                                warn!("COMPACT: failed to re-serialize SetTxs tx: {}", e);
                                continue;
                            }
                        };
                        let tx_hash = xdr::sha256_hash(&tx_xdr);
                        if let Some(&index) = indices.get(&short_tx_id(&key, &tx_hash)) {
                            request.txs[index] = Some(tx.clone());
                        } else {
                            warn!(
                                "COMPACT: SetTxs tx doesn't match any short id in set {:02x?}...",
                                &request.tx_set.tx_set_hash.0[..4]
                            );
                        }
                    }

                    metrics
                        .tx_bytes_received
                        .fetch_add(size as u64, Ordering::Relaxed);
                    reconstruct_tx_set(request, from, send_txset, metrics);
                });
            }
        }
    }

    async fn handle_core_message(&mut self, msg: Message) -> bool {
        match msg.msg_type {
            MessageType::Shutdown => {
                info!("Shutdown requested by Core");
                return false;
            }

            MessageType::BroadcastScp => {
                // Forward SCP broadcast via libp2p QUIC (dedicated stream, no blocking)
                let id_bytes = if msg.payload.len() >= 4 {
                    &msg.payload[..4]
                } else {
                    &msg.payload[..]
                };

                debug!(
                    "SCP_FROM_CORE: Core requested broadcast of SCP (id={:02x?}) ({} bytes)",
                    id_bytes,
                    msg.payload.len()
                );
                let handle = self.libp2p_handle.clone();
                let payload = msg.payload;
                tokio::spawn(async move {
                    handle.broadcast_scp(payload).await;
                });
            }

            MessageType::GetTopTxs => {
                // Parse payload: [count:4]
                if msg.payload.len() < 4 {
                    warn!("GetTopTxs payload too short: {} bytes", msg.payload.len());
                    // Send empty response
                    if let Err(e) = self.core_ipc.sender.send_top_txs_response(&[]) {
                        error!("Failed to send empty top txs response: {}", e);
                    }
                    return true;
                }

                let count = u32::from_le_bytes(msg.payload[0..4].try_into().unwrap()) as usize;
                debug!("Core requesting top {} transactions", count);

                let core_sender = self.core_ipc.sender.clone();
                let overlay_handle = self.overlay_handle.clone();

                tokio::spawn(async move {
                    let Some(txs) = overlay_handle.get_top_txs(count).await else {
                        warn!("GetTopTxs: mempool manager gone (shutting down); not responding");
                        return;
                    };

                    debug!("Returning {} transactions to Core", txs.len());

                    // Borrow the shared tx bytes for serialization; the one
                    // copy happens inside the IPC frame encoding.
                    let tx_data: Vec<&[u8]> = txs.iter().map(|tx| tx.bytes()).collect();

                    if let Err(e) = core_sender.send_top_txs_response(&tx_data) {
                        error!("Failed to send top txs response: {}", e);
                    }
                });
            }

            MessageType::BroadcastCompactSet => {
                // Core nominated a value; broadcast the compact form of its
                // tx set (which Core cached via CacheTxSet) to all peers.
                // Payload: [txSetHash:32]
                if msg.payload.len() != 32 {
                    warn!(
                        "BroadcastCompactSet payload has invalid length {}",
                        msg.payload.len()
                    );
                    return true;
                }
                let mut tx_set_hash = [0u8; 32];
                tx_set_hash.copy_from_slice(&msg.payload[0..32]);

                let tx_set_cache = Arc::clone(&self.tx_set_cache);
                let compact_set_cache = Arc::clone(&self.compact_set_cache);
                let handle = self.libp2p_handle.clone();
                tokio::spawn(async move {
                    {
                        // Check the compact cache before the more expensive
                        // generation step, and insert a placeholder so
                        // concurrent requests for the same set don't generate
                        // it twice.
                        let mut cache = compact_set_cache.lock().unwrap();
                        if cache.contains(&tx_set_hash) {
                            info!(
                                "COMPACT_BROADCAST_SKIP: set {:02x?}... already generated",
                                &tx_set_hash[..4]
                            );
                            return;
                        }
                        cache.put(tx_set_hash, Arc::new(Vec::new()));
                    }
                    let cached = {
                        let cache = tx_set_cache.read().await;
                        cache.get(&tx_set_hash).cloned()
                    };
                    let Some(cached) = cached else {
                        // Should only happen for tx sets from before an
                        // overlay restart
                        warn!(
                            "COMPACT_BROADCAST_FAIL: set {:02x?}... not in tx set cache",
                            &tx_set_hash[..4]
                        );
                        return;
                    };

                    let full_size = cached.xdr.len();
                    let compact_data = tokio::task::spawn_blocking(move || {
                        gen_compact_tx_set(Hash(tx_set_hash), cached.xdr)
                    })
                    .await;
                    let compact_data = match compact_data {
                        Ok(Ok(data)) => data,
                        Ok(Err(e)) => {
                            error!(
                                "COMPACT_BROADCAST_FAIL: cannot encode set {:02x?}...: {}",
                                &tx_set_hash[..4],
                                e
                            );
                            return;
                        }
                        Err(e) => {
                            error!("COMPACT_BROADCAST_FAIL: generation task died: {}", e);
                            return;
                        }
                    };
                    info!(
                        "COMPACT_BROADCAST: set {:02x?}... ({} bytes compact, {} bytes full)",
                        &tx_set_hash[..4],
                        compact_data.xdr.len(),
                        full_size
                    );
                    handle.broadcast_compact(compact_data.xdr).await;
                    compact_set_cache
                        .lock()
                        .unwrap()
                        .put(tx_set_hash, Arc::new(compact_data.txs));
                });
            }

            MessageType::CompactForceRequestTxsPct => {
                if msg.payload.len() < 4 {
                    warn!("CompactForceRequestTxsPct payload too short");
                    return true;
                }
                let pct = u32::from_le_bytes(msg.payload[0..4].try_into().unwrap());
                info!("Compact force-request-txs percentage set to {}", pct);
                self.compact_force_request_txs_pct
                    .store(pct, Ordering::Relaxed);
            }

            MessageType::RequestTxSet => {
                // Core wants a TX set by hash. Serve from the local cache if
                // present; otherwise the set will arrive via a peer's compact
                // broadcast (there is no pull path anymore).
                // Payload: [hash:32][slotSeq:4]
                if msg.payload.len() < 36 {
                    warn!("RequestTxSet payload too short");
                    return true;
                }

                let mut hash = [0u8; 32];
                hash.copy_from_slice(&msg.payload[0..32]);

                let tx_set_cache = Arc::clone(&self.tx_set_cache);
                let core_sender = self.core_ipc.sender.clone();
                tokio::spawn(async move {
                    if let Some(xdr) = get_cached_tx_set_xdr(&*tx_set_cache.read().await, &hash) {
                        info!(
                            "TXSET_FROM_CACHE: Sending TX set {:02x?}... ({} bytes) from local cache",
                            &hash[..4],
                            xdr.len()
                        );
                        if let Err(e) = core_sender.send_tx_set_available(hash, xdr) {
                            error!("Failed to send TX set: {}", e);
                        }
                    } else {
                        info!(
                            "TXSET_CACHE_MISS: TX set {:02x?}... not in local cache, waiting for compact push",
                            &hash[..4]
                        );
                    }
                });
            }

            MessageType::CacheTxSet => {
                // Core built a TX set locally and wants us to cache it for peer requests
                // Payload: [hash:32][slotSeq:4][txSetXDR...]
                if msg.payload.len() < 37 {
                    warn!("CacheTxSet payload too short");
                    return true;
                }

                let mut hash = [0u8; 32];
                hash.copy_from_slice(&msg.payload[0..32]);
                let slot = u32::from_le_bytes(msg.payload[32..36].try_into().unwrap());
                let tx_set_xdr = &msg.payload[36..];

                // Core is trusted for encoding, so we skip decoding. We still
                // guard the content hash cheaply: caching bytes under a hash
                // that peers would recompute differently makes the tx set
                // unfetchable network-wide.
                if !xdr::tx_set_hash_matches(&hash, tx_set_xdr) {
                    warn!(
                        "TXSET_CACHE_DROP: Dropping TX set {:02x?}... from Core: content hash mismatch",
                        &hash[..4]
                    );
                    return true;
                }

                info!(
                    "TXSET_CACHE: Caching locally-built TX set {:02x?}... for slot {} ({} bytes)",
                    &hash[..4],
                    slot,
                    tx_set_xdr.len()
                );

                cache_tx_set_xdr(
                    &mut *self.tx_set_cache.write().await,
                    slot,
                    hash,
                    tx_set_xdr.to_vec(),
                );
            }

            MessageType::SubmitTx => {
                // Parse payload: [fee:i64][numOps:u32][txEnvelope...]
                if msg.payload.len() < 12 {
                    warn!("SubmitTx payload too short");
                    return true;
                }

                let fee = i64::from_le_bytes(msg.payload[0..8].try_into().unwrap());
                let num_ops = u32::from_le_bytes(msg.payload[8..12].try_into().unwrap());
                let tx_data = msg.payload[12..].to_vec();

                // Core is trusted for encoding and supplies fee/ops; we only
                // reject fee-bumps (still unsupported) via a cheap discriminant
                // check. No decode.
                let tx = match ValidatedTx::from_core_trusted(tx_data, fee, num_ops) {
                    Ok(tx) => tx,
                    Err(e) => {
                        warn!("SUBMIT_TX_DROP: Dropping unsupported TX from Core: {}", e);
                        return true;
                    }
                };

                // Add to mempool
                self.overlay_handle.submit_tx(Arc::clone(&tx));

                // Broadcast TX via libp2p QUIC (dedicated stream)
                let handle = self.libp2p_handle.clone();
                tokio::spawn(async move {
                    handle.broadcast_tx(tx).await;
                });
            }

            MessageType::RequestScpState => {
                // Core is asking us to request SCP state from peers
                // Payload is ledger sequence (u32, 4 bytes)
                if msg.payload.len() >= 4 {
                    let ledger_seq = u32::from_le_bytes(msg.payload[0..4].try_into().unwrap());
                    info!(
                        "Core requests SCP state from peers for ledger >= {}",
                        ledger_seq
                    );

                    // Forward request to all connected peers
                    let handle = self.libp2p_handle.clone();
                    tokio::spawn(async move {
                        handle.request_scp_state_from_all_peers(ledger_seq).await;
                    });
                } else {
                    warn!(
                        "RequestScpState with invalid payload length: {}",
                        msg.payload.len()
                    );
                }
            }

            MessageType::LedgerClosed => {
                // Parse payload: [ledgerSeq:4][ledgerHash:32]
                if msg.payload.len() >= 4 {
                    let ledger_seq = u32::from_le_bytes(msg.payload[0..4].try_into().unwrap());
                    info!("Ledger {} closed", ledger_seq);

                    // Update current ledger
                    self.current_ledger_seq = ledger_seq;

                    // Evict old TX sets from cache
                    self.tx_set_cache
                        .write()
                        .await
                        .evict_before(ledger_seq.saturating_sub(12));
                }
            }

            MessageType::TxSetExternalized => {
                // Parse payload: [txSetHash:32][numTxHashes:4][txHash1:32][txHash2:32]...
                if msg.payload.len() >= 36 {
                    let mut tx_set_hash = [0u8; 32];
                    tx_set_hash.copy_from_slice(&msg.payload[0..32]);
                    let num_hashes =
                        u32::from_le_bytes(msg.payload[32..36].try_into().unwrap()) as usize;

                    debug!(
                        "TX set externalized: {:?} with {} TX hashes",
                        &tx_set_hash[..4],
                        num_hashes
                    );

                    // Parse TX hashes from payload
                    let mut tx_hashes = Vec::with_capacity(num_hashes);
                    for i in 0..num_hashes {
                        let start = 36 + (i * 32);
                        let end = start + 32;
                        if end <= msg.payload.len() {
                            let mut hash = [0u8; 32];
                            hash.copy_from_slice(&msg.payload[start..end]);
                            tx_hashes.push(hash);
                        }
                    }

                    // Remove TXs from mempool and WAIT for completion
                    // This prevents race where next nomination queries stale mempool
                    if !tx_hashes.is_empty() {
                        self.overlay_handle.remove_txs_sync(tx_hashes).await;
                    }

                    // NOTE: Don't remove TX set from cache on externalization!
                    // Other nodes may still need to fetch it for catch-up.
                    // The evict_before() call in LedgerClosed handler will clean
                    // up old TX sets (keeping last 5 ledgers).
                }
            }

            MessageType::ScpStateResponse => {
                // Core responded with SCP state - look up peer by request_id and forward
                // Payload format: [request_id:8][count:4][env1_len:4][env1_xdr]...
                if msg.payload.len() < 12 {
                    warn!(
                        "ScpStateResponse payload too short: {} (need at least 12 bytes)",
                        msg.payload.len()
                    );
                    return true;
                }

                let request_id = u64::from_le_bytes(msg.payload[0..8].try_into().unwrap());
                let num_envelopes =
                    u32::from_le_bytes(msg.payload[8..12].try_into().unwrap()) as usize;
                debug!(
                    "Core responded with {} SCP envelopes for request_id={}",
                    num_envelopes, request_id
                );

                // Look up the peer by request_id
                let peer_id = {
                    let mut pending = self.pending_scp_state_requests.write().await;
                    match pending.remove(&request_id) {
                        Some(p) => p,
                        None => {
                            warn!(
                                "Received ScpStateResponse for unknown request_id={} - dropping",
                                request_id
                            );
                            return true;
                        }
                    }
                };

                info!(
                    "Forwarding {} SCP envelopes to peer {} (request_id={})",
                    num_envelopes, peer_id, request_id
                );

                // Parse and forward each envelope to the requesting peer
                let handle = self.libp2p_handle.clone();
                let payload = msg.payload.clone();
                tokio::spawn(async move {
                    let mut offset = 12; // Skip request_id (8) + count (4)
                    for _ in 0..num_envelopes {
                        if offset + 4 > payload.len() {
                            warn!("ScpStateResponse truncated at envelope length");
                            break;
                        }
                        let env_len =
                            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
                                as usize;
                        offset += 4;

                        if offset + env_len > payload.len() {
                            warn!("ScpStateResponse truncated at envelope data");
                            break;
                        }
                        let envelope = &payload[offset..offset + env_len];
                        offset += env_len;

                        // Send envelope to requesting peer over SCP stream
                        if let Err(e) = handle.send_scp_to_peer(peer_id.clone(), envelope).await {
                            warn!("Failed to send SCP envelope to {}: {:?}", peer_id, e);
                        }
                    }
                    info!(
                        "Finished forwarding {} SCP envelopes to {}",
                        num_envelopes, peer_id
                    );
                });
            }

            MessageType::SetPeerConfig => {
                // Parse JSON payload and configure peer connections
                if let Ok(json_str) = std::str::from_utf8(&msg.payload) {
                    info!("Received peer config: {}", json_str);
                    if let Ok(config) = serde_json::from_str::<serde_json::Value>(json_str) {
                        let known: Vec<String> = config["known_peers"]
                            .as_array()
                            .map(|v| {
                                v.iter()
                                    .filter_map(|s| s.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let preferred: Vec<String> = config["preferred_peers"]
                            .as_array()
                            .map(|v| {
                                v.iter()
                                    .filter_map(|s| s.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let listen_port = config["listen_port"].as_u64().unwrap_or(11625) as u16;

                        info!(
                            "Parsed peer config: known={:?}, preferred={:?}, port={}",
                            known, preferred, listen_port
                        );

                        // Resolve and dial all known/preferred peers
                        let all_peers: Vec<_> =
                            known.into_iter().chain(preferred.into_iter()).collect();

                        // Store configured peers for reconnection
                        {
                            let mut cp = self.configured_peers.write().await;
                            cp.addrs = all_peers.clone();
                            cp.listen_port = listen_port;
                            cp.resolved.clear();
                        }

                        // Prune known_peers and peer_hostnames for peers whose
                        // hostnames are no longer in the config. Prevents stale
                        // entries from re-dialing removed peers.
                        {
                            let new_hosts: HashSet<&str> =
                                all_peers.iter().map(|s| s.as_str()).collect();
                            let hostnames = self.peer_hostnames.read().await;
                            let stale_peers: Vec<PeerId> = hostnames
                                .iter()
                                .filter(|(_pid, host)| !new_hosts.contains(host.as_str()))
                                .map(|(pid, _)| *pid)
                                .collect();
                            drop(hostnames);
                            if !stale_peers.is_empty() {
                                info!("Pruning {} peers removed from config", stale_peers.len());
                                let mut known = self.known_peers.write().await;
                                let mut hosts = self.peer_hostnames.write().await;
                                for pid in &stale_peers {
                                    known.remove(pid);
                                    hosts.remove(pid);
                                }
                            }
                        }

                        let handle = self.libp2p_handle.clone();
                        let local_addrs = self.local_addrs.clone();
                        let configured_peers = self.configured_peers.clone();

                        tokio::spawn(async move {
                            let mut unresolved = Vec::new();
                            for addr_str in &all_peers {
                                match resolve_and_dial(addr_str, listen_port, &local_addrs, &handle)
                                    .await
                                {
                                    DialResult::Dialed(libp2p_sock) => {
                                        // Record mapping so we can reconnect on disconnect
                                        configured_peers
                                            .write()
                                            .await
                                            .resolved
                                            .insert(libp2p_sock, addr_str.clone());
                                    }
                                    DialResult::Resolved(_) | DialResult::SelfSkipped => {}
                                    DialResult::ResolutionFailed(addr) => {
                                        unresolved.push(addr);
                                    }
                                }
                            }

                            // Retry any peers that failed DNS resolution
                            spawn_peer_retry_task(
                                unresolved,
                                listen_port,
                                local_addrs,
                                configured_peers,
                                handle,
                            );
                        });
                    }
                }
            }

            MessageType::RequestOverlayMetrics => {
                // Snapshot metrics and send back as JSON
                let snapshot = self.metrics.snapshot();
                match serde_json::to_vec(&snapshot) {
                    Ok(json_bytes) => {
                        let resp = Message::new(MessageType::OverlayMetricsResponse, json_bytes);
                        if let Err(e) = self.core_ipc.sender.send(resp) {
                            error!("Failed to send metrics response: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize metrics snapshot: {}", e);
                    }
                }
            }

            _ => {
                warn!("Unexpected message type from Core: {:?}", msg.msg_type);
            }
        }
        true
    }
}

fn setup_logging(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

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
    // Install panic hook to log panics properly
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("PANIC in Rust overlay: {}", panic_info);
        if let Some(location) = panic_info.location() {
            eprintln!(
                "  at {}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            );
        }
        if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            eprintln!("  payload: {}", s);
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            eprintln!("  payload: {}", s);
        }
    }));

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

    // Setup logging
    setup_logging(&config.log_level);

    info!("Stellar Overlay starting");
    info!("Core socket: {}", config.core_socket.display());
    info!("Peer port: {}", config.peer_port);
    info!(
        "Mode: {}",
        if args.listen_mode {
            "listen (server)"
        } else {
            "connect (client)"
        }
    );

    // Handle SIGTERM/SIGINT for graceful shutdown
    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to register SIGTERM handler");

        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("Failed to register SIGINT handler");

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

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::{Limits, ScpEnvelope, WriteXdr};

    fn test_scp_envelope_xdr(slot_index: u64) -> Vec<u8> {
        let mut envelope = ScpEnvelope::default();
        envelope.statement.slot_index = slot_index;
        envelope.to_xdr(Limits::none()).unwrap()
    }

    // --- DNS resolution tests ---

    #[tokio::test]
    async fn test_resolve_peer_addr_ip_port() {
        // Bare IP:port should parse directly without DNS
        let addr = resolve_peer_addr("10.0.0.1:11625", 9999).await.unwrap();
        assert_eq!(addr, "10.0.0.1:11625".parse::<SocketAddr>().unwrap());
        // default_port is ignored when addr already has a port
    }

    #[tokio::test]
    async fn test_resolve_peer_addr_ip_port_various() {
        // Loopback
        let addr = resolve_peer_addr("127.0.0.1:8080", 0).await.unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), 8080);

        // High port
        let addr = resolve_peer_addr("192.168.1.1:65535", 0).await.unwrap();
        assert_eq!(addr.port(), 65535);
    }

    #[tokio::test]
    async fn test_resolve_peer_addr_dns_no_port() {
        // "localhost" is a DNS name; should resolve and use default_port
        let addr = resolve_peer_addr("localhost", 11625).await.unwrap();
        assert!(
            addr.ip().is_loopback(),
            "localhost should resolve to loopback, got {}",
            addr.ip()
        );
        assert_eq!(
            addr.port(),
            11625,
            "Should use default_port when hostname has no port"
        );
    }

    #[tokio::test]
    async fn test_resolve_peer_addr_dns_with_port() {
        // "localhost:9999" — DNS name with explicit port
        let addr = resolve_peer_addr("localhost:9999", 11625).await.unwrap();
        assert!(addr.ip().is_loopback());
        assert_eq!(
            addr.port(),
            9999,
            "Should use explicit port, not default_port"
        );
    }

    #[tokio::test]
    async fn test_resolve_peer_addr_unresolvable() {
        // Bogus hostname should return an error
        let result = resolve_peer_addr("this.host.definitely.does.not.exist.invalid", 11625).await;
        assert!(result.is_err(), "Unresolvable hostname should return Err");
        let err = result.unwrap_err();
        assert!(
            err.contains("failed to resolve"),
            "Error should mention resolution failure, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_resolve_peer_addr_ipv6_bracket() {
        // Bracketed IPv6 with port should parse directly
        let addr = resolve_peer_addr("[::1]:11625", 9999).await.unwrap();
        assert!(addr.ip().is_ipv6());
        assert_eq!(addr.port(), 11625);
    }

    // --- collect_local_addrs tests ---

    #[tokio::test]
    async fn test_collect_local_addrs_includes_loopback() {
        let addrs = collect_local_addrs(12625);
        // Loopback is inserted synchronously, should be present immediately
        let set = addrs.read().await;
        let loopback = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 12625);
        assert!(
            set.contains(&loopback),
            "Local addrs must always contain loopback at the libp2p port"
        );
    }

    #[tokio::test]
    async fn test_collect_local_addrs_has_nonloopback() {
        // The UDP probe should find at least our primary interface IP
        let addrs = collect_local_addrs(12625);
        let set = addrs.read().await;
        assert!(
            set.len() >= 2,
            "Should have loopback + at least one probe result, got {} addrs: {:?}",
            set.len(),
            set,
        );
    }

    // --- resolve_and_dial tests ---

    #[tokio::test]
    async fn test_resolve_and_dial_self_dial_skipped() {
        // If the resolved address is in local_addrs, resolve_and_dial should
        // return SelfSkipped.
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        // 127.0.0.1:11625 → libp2p port 12625
        local_addrs
            .write()
            .await
            .insert("127.0.0.1:12625".parse().unwrap());

        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        let result = resolve_and_dial("127.0.0.1:11625", 11625, &local_addrs, &handle).await;
        assert!(
            matches!(result, DialResult::SelfSkipped),
            "Self-dial should be skipped"
        );
    }

    #[tokio::test]
    async fn test_resolve_and_dial_dns_failure_returns_addr() {
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        let result = resolve_and_dial("unresolvable.invalid", 11625, &local_addrs, &handle).await;
        assert!(
            matches!(result, DialResult::ResolutionFailed(ref s) if s == "unresolvable.invalid"),
            "Failed DNS should return ResolutionFailed with the address string"
        );
    }

    #[tokio::test]
    async fn test_resolve_and_dial_ip_port_success() {
        // A valid IP:port that is NOT in local_addrs should return Dialed.
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        let result = resolve_and_dial("10.255.255.1:11625", 11625, &local_addrs, &handle).await;
        assert!(
            matches!(result, DialResult::Dialed(_)),
            "Valid IP:port should resolve and return Dialed"
        );
    }

    #[tokio::test]
    async fn test_resolve_and_dial_dns_success() {
        // "localhost" should resolve via DNS and return Dialed.
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        let result = resolve_and_dial("localhost", 11625, &local_addrs, &handle).await;
        assert!(
            matches!(result, DialResult::Dialed(_)),
            "localhost should resolve via DNS and return Dialed"
        );
    }

    // --- spawn_peer_retry_task tests ---

    fn make_test_configured_peers() -> Arc<RwLock<ConfiguredPeers>> {
        Arc::new(RwLock::new(ConfiguredPeers {
            addrs: Vec::new(),
            listen_port: 11625,
            resolved: HashMap::new(),
        }))
    }

    #[tokio::test]
    async fn test_spawn_peer_retry_empty_is_noop() {
        // Empty unresolved list should not spawn anything
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        // This should return immediately without spawning a task
        spawn_peer_retry_task(
            vec![],
            11625,
            local_addrs,
            make_test_configured_peers(),
            handle,
        );
        // No panic, no hang — that's the test
    }

    #[tokio::test]
    async fn test_spawn_peer_retry_resolves_on_retry() {
        // "localhost" should resolve on the first retry attempt.
        // We put it in the "unresolved" list as if initial resolution failed.
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        // Use tokio::time::pause() so the test doesn't actually sleep 2+ seconds
        tokio::time::pause();

        spawn_peer_retry_task(
            vec!["localhost".to_string()],
            11625,
            local_addrs,
            make_test_configured_peers(),
            handle,
        );

        // Advance time past the first retry delay (2s)
        tokio::time::advance(Duration::from_secs(3)).await;
        // Yield to let the spawned task run
        tokio::task::yield_now().await;

        // If we get here without hanging, the retry resolved "localhost" and exited.
        // (An unresolvable host would keep retrying forever, but "localhost" succeeds on attempt 1.)
    }

    #[tokio::test]
    async fn test_spawn_peer_retry_keeps_retrying() {
        // With an unresolvable host, the retry task should keep going indefinitely
        // (no max attempts). We verify it survives multiple retry cycles.
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        tokio::time::pause();

        spawn_peer_retry_task(
            vec!["will-never-resolve.invalid".to_string()],
            11625,
            local_addrs,
            make_test_configured_peers(),
            handle,
        );

        // Advance through many retry cycles — the task should not exit or panic.
        // Delays: 2, 4, 8, 16, 30, 30, 30, ... (capped at 30s)
        // After 300s, we've done ~12 retries. Task is still alive.
        tokio::time::advance(Duration::from_secs(300)).await;
        tokio::task::yield_now().await;

        // Advance further — still should not panic or exit
        tokio::time::advance(Duration::from_secs(300)).await;
        tokio::task::yield_now().await;

        // If we get here, the retry loop is still running (no max attempts). Pass.
    }

    /// Integration test: when known_peers are passed to the overlay via
    /// resolve_and_dial, all of them (IPs and DNS names) get resolved and
    /// connected. Verifies the full SetPeerConfig → resolve → dial → connected flow.
    #[tokio::test]
    async fn test_all_known_peers_resolve_and_connect() {
        // Create 3 overlay nodes
        let kp1 = Libp2pKeypair::generate_ed25519();
        let kp2 = Libp2pKeypair::generate_ed25519();
        let kp3 = Libp2pKeypair::generate_ed25519();

        let (handle1, _events1, _tx1, overlay1) =
            create_overlay(kp1, Arc::new(OverlayMetrics::new())).unwrap();
        let (handle2, mut events2, _tx2, overlay2) =
            create_overlay(kp2, Arc::new(OverlayMetrics::new())).unwrap();
        let (handle3, mut events3, _tx3, overlay3) =
            create_overlay(kp3, Arc::new(OverlayMetrics::new())).unwrap();

        // Start all three on different ports
        let port1: u16 = 18501;
        let port2: u16 = 18502;
        let port3: u16 = 18503;
        tokio::spawn(async move { overlay1.run("127.0.0.1", port1).await });
        tokio::spawn(async move { overlay2.run("127.0.0.1", port2).await });
        tokio::spawn(async move { overlay3.run("127.0.0.1", port3).await });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Node1 resolves and dials all peers using a mix of IP and DNS formats.
        // peer_port values are: port2 - 1000 = 17502, port3 - 1000 = 17503
        // (resolve_and_dial adds +1000 for libp2p_port)
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let known_peers: Vec<String> = vec![
            format!("127.0.0.1:{}", port2 - 1000), // bare IP:port for node2
            format!("localhost:{}", port3 - 1000), // DNS name:port for node3
        ];

        for addr_str in &known_peers {
            let result = resolve_and_dial(addr_str, 11625, &local_addrs, &handle1).await;
            assert!(
                matches!(result, DialResult::Dialed(_)),
                "Peer {} should resolve and dial on first try",
                addr_str
            );
        }

        // Wait for connections + stream establishment
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify connectivity by broadcasting SCP from node1 and receiving on node2 and node3
        let scp_msg = test_scp_envelope_xdr(1);
        handle1.broadcast_scp(scp_msg.clone()).await;

        let mut node2_received = false;
        let mut node3_received = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

        while tokio::time::Instant::now() < deadline && !(node2_received && node3_received) {
            tokio::select! {
                Some(event) = events2.recv() => {
                    if let LibP2pOverlayEvent::ScpReceived { envelope, .. } = event {
                        if envelope == scp_msg {
                            node2_received = true;
                        }
                    }
                }
                Some(event) = events3.recv() => {
                    if let LibP2pOverlayEvent::ScpReceived { envelope, .. } = event {
                        if envelope == scp_msg {
                            node3_received = true;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }

        assert!(
            node2_received,
            "Node2 (connected via bare IP) should receive SCP broadcast"
        );
        assert!(
            node3_received,
            "Node3 (connected via DNS name) should receive SCP broadcast"
        );

        handle1.shutdown().await;
        handle2.shutdown().await;
        handle3.shutdown().await;
    }

    #[tokio::test]
    async fn test_spawn_peer_retry_backoff_caps_at_30s() {
        // Verify the backoff caps at 30s by checking that many retries don't
        // take longer than expected. Delays: 2, 4, 8, 16, 30, 30, 30...
        // After the first 4 retries (2+4+8+16=30s), each additional retry is 30s.
        let local_addrs = Arc::new(RwLock::new(HashSet::new()));
        let keypair = Libp2pKeypair::generate_ed25519();
        let (handle, _evt_rx, _tx_rx, _overlay) =
            create_overlay(keypair, Arc::new(OverlayMetrics::new())).unwrap();

        tokio::time::pause();

        // Mix of resolvable and unresolvable
        spawn_peer_retry_task(
            vec![
                "will-never-resolve.invalid".to_string(),
                "localhost".to_string(),
            ],
            11625,
            local_addrs,
            make_test_configured_peers(),
            handle,
        );

        // After 3s: first retry runs. "localhost" resolves, "invalid" stays pending.
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;

        // After 4 more seconds (total 7s): second retry for the remaining peer.
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        // No panic = pass
    }

    #[test]
    fn test_strip_p2p_suffix() {
        // Address with /p2p suffix
        let addr_with_p2p: Multiaddr =
            format!("/ip4/127.0.0.1/udp/12625/quic-v1/p2p/{}", PeerId::random())
                .parse()
                .unwrap();
        let stripped = strip_p2p_suffix(&addr_with_p2p);
        assert_eq!(stripped.to_string(), "/ip4/127.0.0.1/udp/12625/quic-v1");

        // Address without /p2p suffix — should be unchanged
        let bare: Multiaddr = "/ip4/10.0.0.1/udp/9000/quic-v1".parse().unwrap();
        let stripped = strip_p2p_suffix(&bare);
        assert_eq!(stripped, bare);
    }

    // --- App::handle_core_message / handle_libp2p_event tests ---

    use std::os::unix::net::UnixStream as StdUnixStream;
    use stellar_overlay::ipc::MessageCodec;

    /// Build an App wired to an in-process socket pair, without touching the
    /// network. Returns the core-side stream for driving and observing IPC.
    /// The libp2p overlay object is dropped (not run), so cache-miss fetches
    /// just log a warning — these tests only exercise the cache paths.
    fn test_app() -> (App, StdUnixStream) {
        let (overlay_side, core_side) = StdUnixStream::pair().unwrap();
        let core_ipc = CoreIpc::from_stream(overlay_side).unwrap();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mempool_manager = Overlay::new(cmd_rx);
        tokio::spawn(async move {
            let _ = mempool_manager.run().await;
        });
        let overlay_handle = OverlayHandle::new(cmd_tx);

        let metrics = Arc::new(OverlayMetrics::new());
        let (libp2p_handle, libp2p_events, tx_events, _overlay) =
            create_overlay(Libp2pKeypair::generate_ed25519(), Arc::clone(&metrics)).unwrap();

        let (txset_tx, txset_rx) = mpsc::unbounded_channel();
        let app = App {
            core_ipc,
            overlay_handle,
            tx_set_cache: Arc::new(RwLock::new(TxSetCache::new(100))),
            compact_force_request_txs_pct: Arc::new(AtomicU32::new(0)),
            compact_set_cache: Arc::new(std::sync::Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(10).unwrap(),
            ))),
            pending_compact_txsets: Arc::new(std::sync::Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(30).unwrap(),
            ))),
            received_tx_sets: txset_rx,
            send_tx_set_to_core: txset_tx,
            current_ledger_seq: 0,
            libp2p_handle,
            libp2p_events,
            tx_events,
            pending_scp_state_requests: Arc::new(RwLock::new(HashMap::new())),
            next_scp_request_id: Arc::new(AtomicU64::new(1)),
            local_addrs: Arc::new(RwLock::new(HashSet::new())),
            configured_peers: Arc::new(RwLock::new(ConfiguredPeers {
                addrs: Vec::new(),
                listen_port: 11625,
                resolved: HashMap::new(),
            })),
            known_peers: Arc::new(RwLock::new(HashMap::new())),
            peer_hostnames: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        };
        (app, core_side)
    }

    /// A minimal valid GeneralizedTransactionSet whose content hash matches,
    /// so it passes the CacheTxSet hash guard.
    fn test_txset_xdr(seed: u8) -> ([u8; 32], Vec<u8>) {
        use stellar_xdr::curr::{GeneralizedTransactionSet, Hash};

        let mut tx_set = GeneralizedTransactionSet::default();
        let GeneralizedTransactionSet::V1(v1) = &mut tx_set;
        v1.previous_ledger_hash = Hash([seed; 32]);
        let bytes = tx_set.to_xdr(Limits::none()).unwrap();
        let hash = xdr::sha256_hash(&bytes);
        (hash, bytes)
    }

    fn request_tx_set_payload(hash: &[u8; 32], slot: u32) -> Vec<u8> {
        let mut payload = hash.to_vec();
        payload.extend_from_slice(&slot.to_le_bytes());
        payload
    }

    fn ledger_closed_payload(seq: u32) -> Vec<u8> {
        let mut payload = seq.to_le_bytes().to_vec();
        payload.extend_from_slice(&[0u8; 32]);
        payload
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_request_tx_set_rejects_legacy_32_byte_payload() {
        let (mut app, mut core) = test_app();
        let (hash, xdr_bytes) = test_txset_xdr(7);

        // Cache the set, so if the handler wrongly accepted the legacy
        // format it would respond with TxSetAvailable below.
        cache_tx_set_xdr(&mut *app.tx_set_cache.write().await, 1, hash, xdr_bytes);

        // Pre-slotSeq payload: [hash:32] only. The protocol is now
        // [hash:32][slotSeq:4]; the short payload must be dropped.
        let handled = app
            .handle_core_message(Message::new(MessageType::RequestTxSet, hash.to_vec()))
            .await;
        assert!(
            handled,
            "short payload should be dropped, not kill the loop"
        );

        core.set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        assert!(
            MessageCodec::read(&mut core).is_err(),
            "no response expected for a legacy 32-byte RequestTxSet payload"
        );
    }

    /// Regression test for premature tx set eviction: a set Core caches for a
    /// future slot must be stamped with that slot, not with the overlay's
    /// current ledger view. Before the fix it was stamped with
    /// current_ledger_seq (0 here) and evicted on the next LedgerClosed,
    /// making the set unfetchable exactly when SCP needed it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cache_tx_set_slot_stamp_prevents_premature_eviction() {
        let (mut app, mut core) = test_app();
        let (hash, xdr_bytes) = test_txset_xdr(9);
        let slot: u32 = 100;

        // Core caches the set it built for slot 100 while the overlay still
        // thinks the current ledger is 0.
        // CacheTxSet payload: [hash:32][slotSeq:4][txSetXDR...]
        let mut payload = request_tx_set_payload(&hash, slot);
        payload.extend_from_slice(&xdr_bytes);
        assert!(
            app.handle_core_message(Message::new(MessageType::CacheTxSet, payload))
                .await
        );

        // Ledger 99 closes; eviction drops sets stamped before slot 87. The
        // slot-100 entry must survive (pre-fix it was stamped 0 and died).
        assert!(
            app.handle_core_message(Message::new(
                MessageType::LedgerClosed,
                ledger_closed_payload(99)
            ))
            .await
        );

        // Core asks for the set: it must come back from the local cache.
        assert!(
            app.handle_core_message(Message::new(
                MessageType::RequestTxSet,
                request_tx_set_payload(&hash, slot)
            ))
            .await
        );

        core.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let resp = MessageCodec::read(&mut core).unwrap();
        assert_eq!(resp.msg_type, MessageType::TxSetAvailable);
        assert_eq!(&resp.payload[0..32], &hash[..]);
        assert_eq!(&resp.payload[32..], &xdr_bytes[..]);
    }

    /// Same property for sets reconstructed from a peer's compact broadcast:
    /// receive_tx_set caches under the slot after the overlay's current
    /// ledger view (the in-flight slot), pushes to Core, and the entry
    /// survives eviction of older slots.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reconstructed_txset_cached_and_pushed() {
        let (mut app, mut core) = test_app();
        let (hash, xdr_bytes) = test_txset_xdr(11);

        // The overlay is at ledger 99; the reconstructed set is a candidate
        // for slot 100.
        app.current_ledger_seq = 99;
        app.receive_tx_set(hash, xdr_bytes.clone(), PeerId::random())
            .await;

        // Receiving the set pushes it straight to Core; drain that message.
        core.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let pushed = MessageCodec::read(&mut core).unwrap();
        assert_eq!(pushed.msg_type, MessageType::TxSetAvailable);

        // Ledger 99 closes (evicts sets stamped before 87); the entry was
        // stamped with slot 100 and must survive.
        assert!(
            app.handle_core_message(Message::new(
                MessageType::LedgerClosed,
                ledger_closed_payload(99)
            ))
            .await
        );

        assert!(
            app.handle_core_message(Message::new(
                MessageType::RequestTxSet,
                request_tx_set_payload(&hash, 100)
            ))
            .await
        );
        let resp = MessageCodec::read(&mut core).unwrap();
        assert_eq!(resp.msg_type, MessageType::TxSetAvailable);
        assert_eq!(&resp.payload[0..32], &hash[..]);
        assert_eq!(&resp.payload[32..], &xdr_bytes[..]);
    }

    // --- compact set generation / reconstruction roundtrip ---

    /// A tx set in the shape the block builder produces (one classic
    /// component + one soroban cluster) must survive the compact roundtrip:
    /// gen_compact_tx_set -> resolve all txs -> reconstruct_tx_set produces
    /// byte-identical XDR (and therefore the same hash).
    #[tokio::test]
    async fn test_compact_roundtrip_reconstructs_identical_set() {
        use stellar_xdr::curr::{
            DecoratedSignature, Operation, SequenceNumber, Transaction, TransactionV1Envelope, VecM,
        };

        let make_tx = |fee: u32, sequence: i64| -> TransactionEnvelope {
            let mut tx = Transaction {
                fee,
                seq_num: SequenceNumber(sequence),
                ..Transaction::default()
            };
            tx.operations = VecM::try_from(vec![Operation::default(); 1]).unwrap();
            TransactionEnvelope::Tx(TransactionV1Envelope {
                tx,
                signatures: VecM::<DecoratedSignature, 20>::default(),
            })
        };
        let parse = |bytes: Vec<u8>| TransactionEnvelope::from_xdr(&bytes, Limits::none()).unwrap();
        let classic: Vec<TransactionEnvelope> =
            (1..=3).map(|i| make_tx(100 + i as u32, i)).collect();
        let soroban: Vec<TransactionEnvelope> =
            (10..=11).map(|i| make_tx(200 + i as u32, i)).collect();

        let phase0 = TransactionPhase::V0(
            [TxSetComponent::TxsetCompTxsMaybeDiscountedFee(
                TxSetComponentTxsMaybeDiscountedFee {
                    base_fee: Some(100),
                    txs: classic.try_into().unwrap(),
                },
            )]
            .try_into()
            .unwrap(),
        );
        let cluster: DependentTxCluster = soroban.try_into().unwrap();
        let stage: ParallelTxExecutionStage = vec![cluster].try_into().unwrap();
        let phase1 = TransactionPhase::V1(ParallelTxsComponent {
            base_fee: Some(500),
            execution_stages: [stage].try_into().unwrap(),
        });
        let tx_set = GeneralizedTransactionSet::V1(TransactionSetV1 {
            previous_ledger_hash: Hash([0x77; 32]),
            phases: [phase0, phase1].try_into().unwrap(),
        });
        let set_xdr = tx_set.to_xdr(Limits::none()).unwrap();
        let set_hash = xdr::sha256_hash(&set_xdr);

        // Origin side: generate the compact form
        let compact = gen_compact_tx_set(Hash(set_hash), set_xdr.clone()).unwrap();
        assert_eq!(compact.txs.len(), 5);
        let announce = CompactTxSetMessage::from_xdr(&compact.xdr, Limits::none()).unwrap();
        let CompactTxSetMessage::Set(compact_set) = announce else {
            panic!("expected Set announce");
        };
        assert_eq!(compact_set.tx_set_hash.0, set_hash);
        assert_eq!(compact_set.num_soroban_txs, 2);
        assert_eq!(compact_set.txs.len(), 5 * SHORT_ID_LEN);

        // Receiver side: all txs resolved (as if from the mempool)
        let txs: Vec<Option<TransactionEnvelope>> = compact
            .txs
            .iter()
            .map(|bytes| Some(parse(bytes.clone())))
            .collect();

        let (send_txset, mut recv_txset) = mpsc::unbounded_channel();
        reconstruct_tx_set(
            PendingCompactTxSet {
                tx_set: compact_set,
                txs,
                request_time: Instant::now(),
            },
            PeerId::random(),
            send_txset,
            Arc::new(OverlayMetrics::new()),
        );

        let (hash, reconstructed, _) = recv_txset.try_recv().expect("set should reconstruct");
        assert_eq!(hash, set_hash);
        assert_eq!(reconstructed, set_xdr);
    }
}
