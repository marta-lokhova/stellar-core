//! Mempool manager that handles transaction storage and TX set building.
//!
//! Network communication is handled by the libp2p QUIC overlay.
//! This module provides:
//! - Transaction mempool (fee-ordered, with dedup)
//! - TX set caching for consensus
//! - Core command handling for mempool operations

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, trace};

use crate::flood::{Mempool, compute_tx_hash};

/// Peer ID type
pub type PeerId = u64;

/// Commands from Core to Overlay
#[derive(Debug, Clone)]
pub enum CoreCommand {
    /// Broadcast SCP envelope to all peers (handled by libp2p)
    BroadcastScp { envelope: Vec<u8> },
    
    /// Submit a transaction for flooding
    SubmitTx { 
        data: Vec<u8>,
        fee: u64,
        num_ops: u32,
    },
    
    /// Request top N transactions by fee
    GetTopTxs { 
        count: usize,
        reply: mpsc::Sender<Vec<([u8; 32], Vec<u8>)>>,
    },
    
    /// Connect to a peer (handled by libp2p)
    ConnectTo { addr: SocketAddr },
    
    /// Configure peers (handled by libp2p)
    SetPeerConfig {
        known_peers: Vec<String>,
        preferred_peers: Vec<String>,
        listen_port: u16,
    },
    
    /// Remove transactions from mempool (after ledger close)
    RemoveTxsFromMempool {
        tx_hashes: Vec<[u8; 32]>,
    },
    
    /// Fetch a TX set from peers by hash (libp2p handles network)
    FetchTxSet {
        hash: [u8; 32],
        reply: mpsc::Sender<Option<Vec<u8>>>,
    },

    /// Cache a locally-built TX set
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

/// Mempool manager (no longer handles network connections).
pub struct Overlay {
    /// Commands from Core
    core_commands: mpsc::UnboundedReceiver<CoreCommand>,
    
    /// TX mempool
    mempool: Arc<RwLock<Mempool>>,
    
    /// Local TX set cache (hash -> XDR)
    local_tx_sets: Arc<RwLock<HashMap<[u8; 32], Vec<u8>>>>,
}

impl Overlay {
    /// Create a new mempool manager.
    pub fn new(
        core_commands: mpsc::UnboundedReceiver<CoreCommand>,
    ) -> Self {
        Self {
            core_commands,
            mempool: Arc::new(RwLock::new(Mempool::new(100000, Duration::from_secs(300)))),
            local_tx_sets: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Run the mempool manager.
    pub async fn run(mut self) -> std::io::Result<()> {
        info!("Mempool manager started (libp2p handles networking)");
        
        while let Some(cmd) = self.core_commands.recv().await {
            self.handle_core_command(cmd).await;
        }
        
        info!("Mempool manager shutting down");
        Ok(())
    }
    
    /// Handle a command from Core.
    async fn handle_core_command(&self, cmd: CoreCommand) {
        match cmd {
            CoreCommand::BroadcastScp { .. } => {
                trace!("BroadcastScp ignored (handled by libp2p)");
            }
            
            CoreCommand::SubmitTx { data, fee, num_ops } => {
                let hash = compute_tx_hash(&data);
                debug!("[SubmitTx] TX: hash={:?}, size={}, fee={}, ops={}", 
                      &hash[..4], data.len(), fee, num_ops);
                
                let mut mempool = self.mempool.write().await;
                let entry = crate::flood::TxEntry {
                    data,
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
            
            CoreCommand::GetTopTxs { count, reply } => {
                let mempool = self.mempool.read().await;
                let top_hashes = mempool.top_by_fee(count);
                let txs: Vec<([u8; 32], Vec<u8>)> = top_hashes
                    .iter()
                    .filter_map(|h| mempool.get(h).map(|e| (*h, e.data.clone())))
                    .collect();
                let _ = reply.send(txs).await;
            }
            
            CoreCommand::ConnectTo { .. } => {
                trace!("ConnectTo ignored (handled by libp2p)");
            }
            
            CoreCommand::SetPeerConfig { .. } => {
                trace!("SetPeerConfig ignored (handled by libp2p)");
            }
            
            CoreCommand::RemoveTxsFromMempool { tx_hashes } => {
                let mut mempool = self.mempool.write().await;
                let count = tx_hashes.len();
                for hash in tx_hashes {
                    mempool.remove(&hash);
                }
                info!("Removed {} TXs from mempool", count);
            }
            
            CoreCommand::FetchTxSet { hash, reply } => {
                let cache = self.local_tx_sets.read().await;
                if let Some(xdr) = cache.get(&hash) {
                    let _ = reply.send(Some(xdr.clone())).await;
                } else {
                    let _ = reply.send(None).await;
                }
            }
            
            CoreCommand::CacheTxSet { hash, xdr } => {
                info!("Caching TX set {:?} ({} bytes)", &hash[..4], xdr.len());
                let mut cache = self.local_tx_sets.write().await;
                cache.insert(hash, xdr);
            }
        }
    }
    
    /// Get mempool reference (for testing)
    pub fn mempool(&self) -> &Arc<RwLock<Mempool>> {
        &self.mempool
    }
    
    /// Get TX set cache reference (for testing)
    pub fn tx_set_cache(&self) -> &Arc<RwLock<HashMap<[u8; 32], Vec<u8>>>> {
        &self.local_tx_sets
    }
}

/// Handle for sending commands to the mempool manager.
#[derive(Clone)]
pub struct OverlayHandle {
    cmd_tx: mpsc::UnboundedSender<CoreCommand>,
}

impl OverlayHandle {
    /// Create a new handle.
    pub fn new(cmd_tx: mpsc::UnboundedSender<CoreCommand>) -> Self {
        Self { cmd_tx }
    }
    
    /// Submit a transaction.
    pub fn submit_tx(&self, data: Vec<u8>, fee: u64, num_ops: u32) {
        let _ = self.cmd_tx.send(CoreCommand::SubmitTx { data, fee, num_ops });
    }
    
    /// Get top transactions by fee.
    pub async fn get_top_txs(&self, count: usize) -> Vec<([u8; 32], Vec<u8>)> {
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        let _ = self.cmd_tx.send(CoreCommand::GetTopTxs { count, reply: reply_tx });
        reply_rx.recv().await.unwrap_or_default()
    }
    
    /// Remove transactions from mempool.
    pub fn remove_txs(&self, tx_hashes: Vec<[u8; 32]>) {
        let _ = self.cmd_tx.send(CoreCommand::RemoveTxsFromMempool { tx_hashes });
    }
    
    /// Cache a TX set.
    pub fn cache_tx_set(&self, hash: [u8; 32], xdr: Vec<u8>) {
        let _ = self.cmd_tx.send(CoreCommand::CacheTxSet { hash, xdr });
    }
    
    /// Fetch a TX set from cache.
    pub async fn fetch_tx_set(&self, hash: [u8; 32]) -> Option<Vec<u8>> {
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        let _ = self.cmd_tx.send(CoreCommand::FetchTxSet { hash, reply: reply_tx });
        reply_rx.recv().await.flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_submit_tx_adds_to_mempool() {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let overlay = Overlay::new(cmd_rx);
        let handle = OverlayHandle::new(cmd_tx);
        
        // Start overlay in background
        let mempool = overlay.mempool.clone();
        tokio::spawn(async move {
            let _ = overlay.run().await;
        });
        
        // Submit a TX
        handle.submit_tx(vec![1, 2, 3], 100, 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Verify it's in mempool
        let mp = mempool.read().await;
        assert_eq!(mp.len(), 1);
    }
    
    #[tokio::test]
    async fn test_get_top_txs() {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let overlay = Overlay::new(cmd_rx);
        let handle = OverlayHandle::new(cmd_tx);
        
        tokio::spawn(async move {
            let _ = overlay.run().await;
        });
        
        // Submit TXs with different fees
        handle.submit_tx(vec![1], 100, 1);
        handle.submit_tx(vec![2], 500, 1);
        handle.submit_tx(vec![3], 200, 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Get top 2
        let top = handle.get_top_txs(2).await;
        assert_eq!(top.len(), 2);
        // First should be highest fee
        assert_eq!(top[0].1, vec![2]);
    }
    
    #[tokio::test]
    async fn test_remove_txs() {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let overlay = Overlay::new(cmd_rx);
        let handle = OverlayHandle::new(cmd_tx);
        let mempool = overlay.mempool.clone();
        
        tokio::spawn(async move {
            let _ = overlay.run().await;
        });
        
        // Submit TXs
        handle.submit_tx(vec![1], 100, 1);
        handle.submit_tx(vec![2], 200, 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Remove first TX
        let hash1 = compute_tx_hash(&[1]);
        handle.remove_txs(vec![hash1]);
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Only one should remain
        let mp = mempool.read().await;
        assert_eq!(mp.len(), 1);
    }
    
    #[tokio::test]
    async fn test_cache_and_fetch_tx_set() {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let overlay = Overlay::new(cmd_rx);
        let handle = OverlayHandle::new(cmd_tx);
        
        tokio::spawn(async move {
            let _ = overlay.run().await;
        });
        
        let hash = [42u8; 32];
        let xdr = vec![1, 2, 3, 4, 5];
        
        // Cache it
        handle.cache_tx_set(hash, xdr.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Fetch it
        let result = handle.fetch_tx_set(hash).await;
        assert_eq!(result, Some(xdr));
        
        // Fetch non-existent
        let result = handle.fetch_tx_set([0u8; 32]).await;
        assert_eq!(result, None);
    }
}
