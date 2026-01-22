//! Peer store for tracking known peers and connection history.
//!
//! Stores:
//! - Known peer addresses
//! - Success/failure counts
//! - Backoff state

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{debug, trace};

/// Backoff parameters
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF_EXPONENT: u32 = 10;
const MAX_BACKOFF: Duration = Duration::from_secs(10240); // 10 * 2^10

/// Information about a known peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Peer address
    pub addr: SocketAddr,
    
    /// Number of successful connections
    pub success_count: u32,
    
    /// Number of consecutive failures
    pub failure_count: u32,
    
    /// When we last successfully connected
    pub last_success: Option<Instant>,
    
    /// When we last failed to connect
    pub last_failure: Option<Instant>,
    
    /// Is this a preferred peer (never evict, connect first)
    pub preferred: bool,
    
    /// When we can next attempt connection
    pub next_attempt_after: Option<Instant>,
}

impl PeerInfo {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            success_count: 0,
            failure_count: 0,
            last_success: None,
            last_failure: None,
            preferred: false,
            next_attempt_after: None,
        }
    }
    
    pub fn new_preferred(addr: SocketAddr) -> Self {
        let mut info = Self::new(addr);
        info.preferred = true;
        info
    }
    
    /// Record a successful connection.
    pub fn record_success(&mut self) {
        self.success_count = self.success_count.saturating_add(1);
        self.failure_count = 0;
        self.last_success = Some(Instant::now());
        self.next_attempt_after = None;
    }
    
    /// Record a failed connection attempt.
    pub fn record_failure(&mut self) {
        self.failure_count = self.failure_count.saturating_add(1);
        self.last_failure = Some(Instant::now());
        self.update_backoff();
    }
    
    /// Update backoff timer based on failure count.
    fn update_backoff(&mut self) {
        // Exponential backoff: 10s * 2^min(failures, 10)
        let exponent = self.failure_count.min(MAX_BACKOFF_EXPONENT);
        let backoff_secs = INITIAL_BACKOFF.as_secs() * (1u64 << exponent);
        let backoff = Duration::from_secs(backoff_secs.min(MAX_BACKOFF.as_secs()));
        
        // Add some randomization (±10%)
        let jitter_range = backoff.as_millis() / 10;
        let jitter = if jitter_range > 0 {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let jitter_ms = rng.gen_range(0..jitter_range as u64);
            Duration::from_millis(jitter_ms)
        } else {
            Duration::ZERO
        };
        
        self.next_attempt_after = Some(Instant::now() + backoff + jitter);
        trace!(
            "Peer {} backoff: {:?} (failures: {})",
            self.addr, backoff, self.failure_count
        );
    }
    
    /// Check if we can attempt connection now.
    pub fn can_attempt(&self) -> bool {
        match self.next_attempt_after {
            None => true,
            Some(t) => Instant::now() >= t,
        }
    }
}

/// Store of known peers.
pub struct PeerStore {
    /// Peers by address
    peers: HashMap<SocketAddr, PeerInfo>,
    
    /// Maximum number of peers to store
    max_peers: usize,
}

impl PeerStore {
    pub fn new(max_peers: usize) -> Self {
        Self {
            peers: HashMap::with_capacity(max_peers),
            max_peers,
        }
    }
    
    /// Add a peer to the store.
    /// Returns true if newly added, false if already present.
    pub fn add(&mut self, addr: SocketAddr) -> bool {
        if self.peers.contains_key(&addr) {
            return false;
        }
        
        // Evict if at capacity (evict worst non-preferred peer)
        while self.peers.len() >= self.max_peers {
            if !self.evict_one() {
                // Can't evict (all preferred) - reject new peer
                debug!("Cannot add peer {}: store full with preferred peers", addr);
                return false;
            }
        }
        
        self.peers.insert(addr, PeerInfo::new(addr));
        true
    }
    
    /// Add a preferred peer.
    pub fn add_preferred(&mut self, addr: SocketAddr) {
        if let Some(info) = self.peers.get_mut(&addr) {
            info.preferred = true;
        } else {
            // Force add even if at capacity
            self.peers.insert(addr, PeerInfo::new_preferred(addr));
        }
    }
    
    /// Get peer info.
    pub fn get(&self, addr: &SocketAddr) -> Option<&PeerInfo> {
        self.peers.get(addr)
    }
    
    /// Get mutable peer info.
    pub fn get_mut(&mut self, addr: &SocketAddr) -> Option<&mut PeerInfo> {
        self.peers.get_mut(addr)
    }
    
    /// Record successful connection.
    pub fn record_success(&mut self, addr: &SocketAddr) {
        if let Some(info) = self.peers.get_mut(addr) {
            info.record_success();
        }
    }
    
    /// Record failed connection.
    pub fn record_failure(&mut self, addr: &SocketAddr) {
        if let Some(info) = self.peers.get_mut(addr) {
            info.record_failure();
        }
    }
    
    /// Get peers eligible for connection (not in backoff).
    pub fn eligible_for_connection(&self) -> Vec<&PeerInfo> {
        let mut eligible: Vec<&PeerInfo> = self.peers
            .values()
            .filter(|p| p.can_attempt())
            .collect();
        
        // Sort: preferred first, then by success count (descending)
        eligible.sort_by(|a, b| {
            b.preferred.cmp(&a.preferred)
                .then_with(|| b.success_count.cmp(&a.success_count))
        });
        
        eligible
    }
    
    /// Get preferred peers.
    pub fn preferred(&self) -> Vec<&PeerInfo> {
        self.peers.values().filter(|p| p.preferred).collect()
    }
    
    /// Number of stored peers.
    pub fn len(&self) -> usize {
        self.peers.len()
    }
    
    /// Is the store empty?
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
    
    /// Evict the worst non-preferred peer.
    fn evict_one(&mut self) -> bool {
        // Find worst non-preferred peer (most failures, oldest last success)
        let worst = self.peers
            .iter()
            .filter(|(_, info)| !info.preferred)
            .min_by(|(_, a), (_, b)| {
                // Higher failure count is worse
                // Lower success count is worse
                a.success_count.cmp(&b.success_count)
                    .then_with(|| b.failure_count.cmp(&a.failure_count))
            })
            .map(|(addr, _)| *addr);
        
        if let Some(addr) = worst {
            debug!("Evicting peer {}", addr);
            self.peers.remove(&addr);
            true
        } else {
            false
        }
    }
    
    /// Add multiple peers from a PEERS message.
    pub fn add_from_peers_message(&mut self, addrs: &[SocketAddr]) {
        for &addr in addrs {
            self.add(addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    
    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }
    
    #[test]
    fn test_add_peer() {
        let mut store = PeerStore::new(100);
        let addr = make_addr(11625);
        
        assert!(store.add(addr));
        assert!(!store.add(addr)); // duplicate
        
        assert_eq!(store.len(), 1);
        assert!(store.get(&addr).is_some());
    }
    
    #[test]
    fn test_success_failure_tracking() {
        let mut store = PeerStore::new(100);
        let addr = make_addr(11625);
        
        store.add(addr);
        store.record_success(&addr);
        
        let info = store.get(&addr).unwrap();
        assert_eq!(info.success_count, 1);
        assert_eq!(info.failure_count, 0);
        assert!(info.last_success.is_some());
        
        store.record_failure(&addr);
        let info = store.get(&addr).unwrap();
        assert_eq!(info.success_count, 1);
        assert_eq!(info.failure_count, 1);
        assert!(info.last_failure.is_some());
    }
    
    #[test]
    fn test_backoff() {
        let mut info = PeerInfo::new(make_addr(11625));
        
        assert!(info.can_attempt());
        
        info.record_failure();
        assert!(!info.can_attempt());
        
        // After enough time, should be able to attempt
        info.next_attempt_after = Some(Instant::now() - Duration::from_secs(1));
        assert!(info.can_attempt());
    }
    
    #[test]
    fn test_exponential_backoff() {
        let mut info = PeerInfo::new(make_addr(11625));
        
        // First failure: ~10s backoff
        info.record_failure();
        let backoff1 = info.next_attempt_after.unwrap() - Instant::now();
        
        // Reset and try again with more failures
        info.record_failure();
        let backoff2 = info.next_attempt_after.unwrap() - Instant::now();
        
        // Second failure should have longer backoff
        assert!(backoff2 > backoff1);
    }
    
    #[test]
    fn test_preferred_peers() {
        let mut store = PeerStore::new(100);
        let addr1 = make_addr(11625);
        let addr2 = make_addr(11626);
        
        store.add(addr1);
        store.add_preferred(addr2);
        
        let preferred = store.preferred();
        assert_eq!(preferred.len(), 1);
        assert_eq!(preferred[0].addr, addr2);
    }
    
    #[test]
    fn test_eviction() {
        let mut store = PeerStore::new(3);
        
        // Fill the store
        store.add(make_addr(1));
        store.add(make_addr(2));
        store.add(make_addr(3));
        
        // Make addr3 have worst stats
        store.record_failure(&make_addr(3));
        store.record_failure(&make_addr(3));
        
        // Add another peer - should evict addr3
        assert!(store.add(make_addr(4)));
        
        assert_eq!(store.len(), 3);
        assert!(store.get(&make_addr(3)).is_none());
        assert!(store.get(&make_addr(4)).is_some());
    }
    
    #[test]
    fn test_preferred_never_evicted() {
        let mut store = PeerStore::new(2);
        
        store.add_preferred(make_addr(1));
        store.add_preferred(make_addr(2));
        
        // Try to add non-preferred - should fail (all are preferred)
        assert!(!store.add(make_addr(3)));
        assert_eq!(store.len(), 2);
    }
    
    #[test]
    fn test_eligible_for_connection() {
        let mut store = PeerStore::new(100);
        
        store.add(make_addr(1));
        store.add_preferred(make_addr(2));
        store.add(make_addr(3));
        
        // Put addr3 in backoff
        store.record_failure(&make_addr(3));
        
        let eligible = store.eligible_for_connection();
        
        // Should have 2 eligible (addr1 and addr2)
        assert_eq!(eligible.len(), 2);
        
        // Preferred should be first
        assert!(eligible[0].preferred);
        assert_eq!(eligible[0].addr, make_addr(2));
    }
    
    #[test]
    fn test_add_from_peers_message() {
        let mut store = PeerStore::new(100);
        
        let addrs = vec![
            make_addr(1),
            make_addr(2),
            make_addr(3),
        ];
        
        store.add_from_peers_message(&addrs);
        
        assert_eq!(store.len(), 3);
        assert!(store.get(&make_addr(1)).is_some());
        assert!(store.get(&make_addr(2)).is_some());
        assert!(store.get(&make_addr(3)).is_some());
    }
}
