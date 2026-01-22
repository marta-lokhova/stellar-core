//! Flood coordinator for transaction propagation.
//!
//! Implements hybrid push-k/pull strategy:
//! - Push full TX to k random peers immediately (low latency)
//! - Advert hash to remaining peers (bandwidth efficient)
//! - Respond to demands with full TX

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use super::mempool::{Mempool, TxEntry, TxHash, compute_tx_hash};
use crate::peer::{PeerId, PeerManagerHandle};

/// Number of peers to push full TX to
const PUSH_K: usize = 8;

/// Batch adverts every this duration
const ADVERT_BATCH_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum hashes per advert batch
const MAX_ADVERTS_PER_BATCH: usize = 100;

/// How long to wait for a demanded TX before timing out
const DEMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// Commands to the flood coordinator
#[derive(Debug)]
pub enum FloodCommand {
    /// New transaction received (from peer or local submission)
    NewTx {
        data: Vec<u8>,
        from_peer: PeerId,
    },
    /// Advert received from peer (list of hashes)
    AdvertReceived {
        hashes: Vec<TxHash>,
        from_peer: PeerId,
    },
    /// Demand received from peer (they want these TXs)
    DemandReceived {
        hashes: Vec<TxHash>,
        from_peer: PeerId,
    },
    /// Get transactions for nomination (by fee)
    GetTopTxs {
        count: usize,
        reply: mpsc::Sender<Vec<TxHash>>,
    },
    /// Transaction confirmed (remove from mempool)
    TxConfirmed {
        hash: TxHash,
    },
}

/// Tracks pending demands we've sent
struct PendingDemand {
    hash: TxHash,
    sent_at: Instant,
    from_peer: PeerId,
}

/// Tracks adverts waiting to be batched
struct PendingAdvert {
    hash: TxHash,
    for_peers: Vec<PeerId>,
}

/// Coordinates transaction flooding across peers.
pub struct FloodCoordinator {
    /// Transaction mempool
    mempool: Mempool,
    
    /// Commands channel
    commands: mpsc::Receiver<FloodCommand>,
    
    /// Handle to send messages to peers
    peer_handle: PeerManagerHandle,
    
    /// Connected peer IDs
    peers: HashSet<PeerId>,
    
    /// Hashes we've already seen (to avoid redundant processing)
    seen_hashes: HashSet<TxHash>,
    
    /// Pending adverts to batch
    pending_adverts: HashMap<TxHash, HashSet<PeerId>>,
    
    /// Outstanding demands we've sent
    pending_demands: HashMap<TxHash, PendingDemand>,
    
    /// Last time we flushed adverts
    last_advert_flush: Instant,
}

impl FloodCoordinator {
    pub fn new(
        commands: mpsc::Receiver<FloodCommand>,
        peer_handle: PeerManagerHandle,
        mempool_size: usize,
        mempool_age: Duration,
    ) -> Self {
        Self {
            mempool: Mempool::new(mempool_size, mempool_age),
            commands,
            peer_handle,
            peers: HashSet::new(),
            seen_hashes: HashSet::new(),
            pending_adverts: HashMap::new(),
            pending_demands: HashMap::new(),
            last_advert_flush: Instant::now(),
        }
    }
    
    /// Add a peer to the coordinator's peer set.
    pub fn add_peer(&mut self, peer_id: PeerId) {
        self.peers.insert(peer_id);
    }
    
    /// Remove a peer from the coordinator's peer set.
    pub fn remove_peer(&mut self, peer_id: PeerId) {
        self.peers.remove(&peer_id);
        // Clean up any pending adverts for this peer
        for peers in self.pending_adverts.values_mut() {
            peers.remove(&peer_id);
        }
    }
    
    /// Run the flood coordinator event loop.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(ADVERT_BATCH_INTERVAL);
        
        loop {
            tokio::select! {
                Some(cmd) = self.commands.recv() => {
                    self.handle_command(cmd).await;
                }
                _ = interval.tick() => {
                    self.flush_adverts().await;
                    self.cleanup_expired_demands();
                }
            }
        }
    }
    
    async fn handle_command(&mut self, cmd: FloodCommand) {
        match cmd {
            FloodCommand::NewTx { data, from_peer } => {
                self.handle_new_tx(data, from_peer).await;
            }
            FloodCommand::AdvertReceived { hashes, from_peer } => {
                self.handle_adverts(hashes, from_peer).await;
            }
            FloodCommand::DemandReceived { hashes, from_peer } => {
                self.handle_demands(hashes, from_peer).await;
            }
            FloodCommand::GetTopTxs { count, reply } => {
                let top = self.mempool.top_by_fee(count);
                let _ = reply.send(top).await;
            }
            FloodCommand::TxConfirmed { hash } => {
                self.mempool.remove(&hash);
                self.seen_hashes.remove(&hash);
            }
        }
    }
    
    /// Handle a new transaction (from peer or local).
    async fn handle_new_tx(&mut self, data: Vec<u8>, from_peer: PeerId) {
        let hash = compute_tx_hash(&data);
        
        // Check if we've seen this transaction
        if self.seen_hashes.contains(&hash) {
            trace!("Already seen TX {:?}", &hash[..4]);
            return;
        }
        self.seen_hashes.insert(hash);
        
        // Clear any pending demand for this hash
        self.pending_demands.remove(&hash);
        
        // Parse TX metadata (simplified - in reality would parse XDR)
        let tx_entry = TxEntry {
            data: data.clone(),
            hash,
            source_account: [0u8; 32], // TODO: parse from XDR
            sequence: 0,               // TODO: parse from XDR
            fee: 100,                  // TODO: parse from XDR
            num_ops: 1,                // TODO: parse from XDR
            received_at: Instant::now(),
            from_peer,
        };
        
        // Add to mempool
        if !self.mempool.insert(tx_entry) {
            return; // Duplicate (shouldn't happen given seen_hashes check)
        }
        
        debug!("New TX {:?}, propagating to {} peers", &hash[..4], self.peers.len());
        
        // Select k random peers to push to (excluding sender)
        let eligible_peers: Vec<PeerId> = self.peers
            .iter()
            .filter(|&&p| p != from_peer)
            .copied()
            .collect();
        
        let push_peers = select_random_k(&eligible_peers, PUSH_K);
        let advert_peers: Vec<PeerId> = eligible_peers
            .iter()
            .filter(|p| !push_peers.contains(p))
            .copied()
            .collect();
        
        // Push full TX to k peers
        for &peer_id in &push_peers {
            // TODO: Actually send the TX message
            // For now just trace
            trace!("Would push TX {:?} to peer {}", &hash[..4], peer_id);
        }
        
        // Queue adverts for remaining peers
        if !advert_peers.is_empty() {
            self.pending_adverts
                .entry(hash)
                .or_default()
                .extend(advert_peers);
        }
    }
    
    /// Handle adverts from a peer.
    async fn handle_adverts(&mut self, hashes: Vec<TxHash>, from_peer: PeerId) {
        for hash in hashes {
            // Skip if we already have this TX
            if self.seen_hashes.contains(&hash) {
                continue;
            }
            
            // Skip if we already have a pending demand
            if self.pending_demands.contains_key(&hash) {
                continue;
            }
            
            // Send demand to the peer
            trace!("Demanding TX {:?} from peer {}", &hash[..4], from_peer);
            self.pending_demands.insert(hash, PendingDemand {
                hash,
                sent_at: Instant::now(),
                from_peer,
            });
            
            // TODO: Actually send DEMAND message to peer
        }
    }
    
    /// Handle demands from a peer.
    async fn handle_demands(&mut self, hashes: Vec<TxHash>, from_peer: PeerId) {
        for hash in hashes {
            if let Some(tx) = self.mempool.get(&hash) {
                // Send full TX to peer
                trace!("Sending demanded TX {:?} to peer {}", &hash[..4], from_peer);
                // TODO: Actually send TX message
            } else {
                trace!("Don't have demanded TX {:?}", &hash[..4]);
            }
        }
    }
    
    /// Flush batched adverts to peers.
    async fn flush_adverts(&mut self) {
        if self.pending_adverts.is_empty() {
            return;
        }
        
        // Group hashes by peer
        let mut by_peer: HashMap<PeerId, Vec<TxHash>> = HashMap::new();
        
        for (hash, peers) in self.pending_adverts.drain() {
            for peer_id in peers {
                by_peer.entry(peer_id).or_default().push(hash);
            }
        }
        
        // Send batched adverts
        for (peer_id, hashes) in by_peer {
            // Split into batches if too large
            for chunk in hashes.chunks(MAX_ADVERTS_PER_BATCH) {
                trace!("Sending {} adverts to peer {}", chunk.len(), peer_id);
                // TODO: Actually send ADVERT message
            }
        }
        
        self.last_advert_flush = Instant::now();
    }
    
    /// Clean up demands that have timed out.
    fn cleanup_expired_demands(&mut self) {
        let now = Instant::now();
        self.pending_demands.retain(|hash, demand| {
            if now.duration_since(demand.sent_at) > DEMAND_TIMEOUT {
                warn!("Demand for TX {:?} timed out", &hash[..4]);
                false
            } else {
                true
            }
        });
    }
}

/// Handle for sending commands to the flood coordinator.
#[derive(Clone)]
pub struct FloodCoordinatorHandle {
    commands: mpsc::Sender<FloodCommand>,
}

impl FloodCoordinatorHandle {
    pub fn new(commands: mpsc::Sender<FloodCommand>) -> Self {
        Self { commands }
    }
    
    /// Submit a new transaction.
    pub async fn submit_tx(&self, data: Vec<u8>, from_peer: PeerId) -> Result<(), ()> {
        self.commands
            .send(FloodCommand::NewTx { data, from_peer })
            .await
            .map_err(|_| ())
    }
    
    /// Process received adverts.
    pub async fn advert_received(&self, hashes: Vec<TxHash>, from_peer: PeerId) -> Result<(), ()> {
        self.commands
            .send(FloodCommand::AdvertReceived { hashes, from_peer })
            .await
            .map_err(|_| ())
    }
    
    /// Process received demands.
    pub async fn demand_received(&self, hashes: Vec<TxHash>, from_peer: PeerId) -> Result<(), ()> {
        self.commands
            .send(FloodCommand::DemandReceived { hashes, from_peer })
            .await
            .map_err(|_| ())
    }
    
    /// Mark a transaction as confirmed (remove from mempool).
    pub async fn tx_confirmed(&self, hash: TxHash) -> Result<(), ()> {
        self.commands
            .send(FloodCommand::TxConfirmed { hash })
            .await
            .map_err(|_| ())
    }
    
    /// Get top transactions by fee.
    pub async fn get_top_txs(&self, count: usize) -> Result<Vec<TxHash>, ()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.commands
            .send(FloodCommand::GetTopTxs { count, reply: tx })
            .await
            .map_err(|_| ())?;
        rx.recv().await.ok_or(())
    }
}

/// Select k random elements from a slice.
fn select_random_k<T: Clone>(items: &[T], k: usize) -> Vec<T> {
    let mut rng = rand::thread_rng();
    let k = k.min(items.len());
    
    // Use partial shuffle for efficiency
    let mut items: Vec<T> = items.to_vec();
    let (selected, _) = items.partial_shuffle(&mut rng, k);
    selected.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_select_random_k() {
        let items = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        
        let selected = select_random_k(&items, 3);
        assert_eq!(selected.len(), 3);
        
        // All selected items should be from original
        for item in &selected {
            assert!(items.contains(item));
        }
    }
    
    #[test]
    fn test_select_random_k_exceeds_size() {
        let items = vec![1, 2, 3];
        
        let selected = select_random_k(&items, 10);
        assert_eq!(selected.len(), 3); // capped at items.len()
    }
    
    #[tokio::test]
    async fn test_flood_coordinator_new_tx() {
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let (peer_cmd_tx, _peer_cmd_rx) = mpsc::channel(100);
        let peer_handle = PeerManagerHandle::new(peer_cmd_tx);
        
        let mut coordinator = FloodCoordinator::new(
            cmd_rx,
            peer_handle,
            1000,
            Duration::from_secs(300),
        );
        
        // Add some peers
        coordinator.add_peer(1);
        coordinator.add_peer(2);
        coordinator.add_peer(3);
        
        // Submit a transaction
        let tx_data = b"test transaction data".to_vec();
        let hash = compute_tx_hash(&tx_data);
        
        coordinator.handle_new_tx(tx_data.clone(), 0).await;
        
        // Should be in mempool
        assert!(coordinator.mempool.contains(&hash));
        
        // Should be in seen_hashes
        assert!(coordinator.seen_hashes.contains(&hash));
        
        // Submit same TX again - should be ignored
        coordinator.handle_new_tx(tx_data, 0).await;
        assert_eq!(coordinator.mempool.len(), 1);
    }
    
    #[tokio::test]
    async fn test_flood_coordinator_adverts() {
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let (peer_cmd_tx, _peer_cmd_rx) = mpsc::channel(100);
        let peer_handle = PeerManagerHandle::new(peer_cmd_tx);
        
        let mut coordinator = FloodCoordinator::new(
            cmd_rx,
            peer_handle,
            1000,
            Duration::from_secs(300),
        );
        
        // Receive an advert for unknown TX
        let hash = compute_tx_hash(b"unknown tx");
        coordinator.handle_adverts(vec![hash], 42).await;
        
        // Should have a pending demand
        assert!(coordinator.pending_demands.contains_key(&hash));
        assert_eq!(coordinator.pending_demands.get(&hash).unwrap().from_peer, 42);
    }
    
    #[tokio::test]
    async fn test_flood_coordinator_get_top_txs() {
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let (peer_cmd_tx, _peer_cmd_rx) = mpsc::channel(100);
        let peer_handle = PeerManagerHandle::new(peer_cmd_tx);
        
        let mut coordinator = FloodCoordinator::new(
            cmd_rx,
            peer_handle,
            1000,
            Duration::from_secs(300),
        );
        
        // Add some transactions
        for i in 0..5 {
            let tx_data = format!("tx{}", i).into_bytes();
            coordinator.handle_new_tx(tx_data, 0).await;
        }
        
        // Get top 3
        let top = coordinator.mempool.top_by_fee(3);
        assert_eq!(top.len(), 3);
    }
    
    #[tokio::test]
    async fn test_flood_coordinator_handle() {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(100);
        let handle = FloodCoordinatorHandle::new(cmd_tx);
        
        // Test submit_tx
        let tx_data = b"test".to_vec();
        handle.submit_tx(tx_data.clone(), 42).await.unwrap();
        
        match cmd_rx.recv().await.unwrap() {
            FloodCommand::NewTx { data, from_peer } => {
                assert_eq!(data, tx_data);
                assert_eq!(from_peer, 42);
            }
            _ => panic!("Wrong command"),
        }
        
        // Test tx_confirmed
        let hash = [1u8; 32];
        handle.tx_confirmed(hash).await.unwrap();
        
        match cmd_rx.recv().await.unwrap() {
            FloodCommand::TxConfirmed { hash: h } => {
                assert_eq!(h, hash);
            }
            _ => panic!("Wrong command"),
        }
    }
}
