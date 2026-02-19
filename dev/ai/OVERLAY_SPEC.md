# Stellar Rust Overlay - Exact Specification

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           stellar-core (C++)                                │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │     SCP     │    │   Herder    │    │   Ledger    │    │    HTTP     │  │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘  │
│         │                  │                  │                  │          │
│         └──────────────────┴──────────────────┴──────────────────┘          │
│                                     │                                       │
│                              Unix Socket IPC                                │
│                                     │                                       │
└─────────────────────────────────────┼───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Rust Overlay Process                                 │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                          main.rs (App)                               │   │
│  │  - Core IPC handler (Unix socket)                                    │   │
│  │  - libp2p event handler                                              │   │
│  │  - TX set cache (fetched from peers)                                 │   │
│  │  - SCP state request tracking (request_id → peer_id)                │   │
│  └───────────────────────────────┬─────────────────────────────────────┘   │
│                                  │                                          │
│         ┌────────────────────────┼────────────────────────┐                │
│         ▼                        ▼                        ▼                │
│  ┌─────────────┐    ┌────────────────────────┐    ┌─────────────┐         │
│  │  Mempool    │    │   libp2p_overlay.rs    │    │   IPC       │         │
│  │ (integrated │    │                        │    │ (messages,  │         │
│  │    .rs)     │    │  - QUIC transport      │    │  transport) │         │
│  │             │    │  - 3 stream types      │    │             │         │
│  │  - TxEntry  │    │  - Kademlia DHT        │    │  - Message  │         │
│  │  - fee sort │    │  - Identify            │    │  - Framing  │         │
│  │  - dedup    │    │  - Flooding logic      │    │             │         │
│  └─────────────┘    └────────────────────────┘    └─────────────┘         │
│                                  │                                          │
│                                  ▼                                          │
│                     ┌─────────────────────────┐                            │
│                     │   libp2p Swarm          │                            │
│                     │                         │                            │
│                     │  ┌─────────────────┐    │                            │
│                     │  │ StreamBehaviour │    │                            │
│                     │  └─────────────────┘    │                            │
│                     │  ┌─────────────────┐    │                            │
│                     │  │    Kademlia     │    │                            │
│                     │  └─────────────────┘    │                            │
│                     │  ┌─────────────────┐    │                            │
│                     │  │    Identify     │    │                            │
│                     │  └─────────────────┘    │                            │
│                     └───────────┬─────────────┘                            │
│                                 │                                          │
└─────────────────────────────────┼──────────────────────────────────────────┘
                                  │ QUIC (UDP)
                                  ▼
                    ┌─────────────────────────────┐
                    │       P2P Network           │
                    │  (other stellar-core nodes) │
                    └─────────────────────────────┘
```

---

## 2. Transport Layer

### 2.1 QUIC Configuration

| Parameter | Value | Notes |
|-----------|-------|-------|
| Transport | QUIC over UDP | libp2p `.with_quic_config()` |
| Listen address | Configurable | `config.libp2p_listen_ip` (default: `0.0.0.0`) |
| Port | `config.peer_port + 1000` | Avoids legacy TCP collision |
| Idle connection timeout | 300s | `with_idle_connection_timeout` |
| Keep-alive interval | 15s | `quic_config.keep_alive_interval` |
| Max idle timeout | 60s | `quic_config.max_idle_timeout` |

### 2.2 Stream Protocols

Three independent QUIC streams per peer connection:

| Protocol | Purpose | Typical Size | Priority |
|----------|---------|--------------|----------|
| `/stellar/scp/1.0.0` | SCP consensus messages | ~500 bytes | **CRITICAL** |
| `/stellar/tx/1.0.0` | Transaction flooding | ~1 KB | Normal |
| `/stellar/txset/1.0.0` | TX set request/response | Up to 16 MB | Critical |

### 2.3 Message Framing

All streams use length-prefixed framing:
```
┌─────────────────┬──────────────────────────────────┐
│  Length (4B BE) │  Payload (up to 16 MB)           │
└─────────────────┴──────────────────────────────────┘
```

- **MAX_MESSAGE_SIZE**: 16 MB
- **Encoding**: Big-endian for length prefix

---

## 3. Network Behaviours

### 3.1 Combined Behaviour (`StellarBehaviour`)

```rust
struct StellarBehaviour {
    stream: StreamBehaviour,     // libp2p-stream for 3 protocols
    kademlia: Kademlia<MemoryStore>,
    identify: Identify,
}
```

### 3.2 Kademlia Configuration

| Setting | Value | Notes |
|---------|-------|-------|
| Mode | **Server** (forced) | Required for peer discovery in test networks |
| Store | `MemoryStore` | In-memory routing table |
| Bootstrap trigger | On first peer identification (Identify protocol) | Via `BootstrapKademlia` command |
| Periodic re-bootstrap | Default 5 minutes | libp2p Kademlia default |

**Critical**: Kademlia defaults to Client mode and only switches to Server when external address is confirmed. In localhost test networks, this never happens, so nodes must be forced to Server mode immediately.

### 3.3 Identify Configuration

| Setting | Value |
|---------|-------|
| Protocol | `/stellar/1.0.0` |
| Action on Received | Add peer addresses to Kademlia, auto-dial if not connected |

---

## 4. Shared State (`SharedState`)

All async handlers share this state via `Arc`:

```rust
struct SharedState {
    // Per-peer outbound streams (SCP/TX/TxSet)
    peer_streams: RwLock<HashMap<PeerId, Arc<PeerOutboundStreams>>>,
    
    // Deduplication caches (LRU)
    scp_seen: RwLock<LruCache<[u8;32], ()>>,        // 10,000 entries
    tx_seen: RwLock<LruCache<[u8;32], ()>>,         // 100,000 entries
    
    // Flooding tracking
    scp_sent_to: RwLock<LruCache<[u8;32], HashSet<PeerId>>>,  // 10,000
    tx_sent_to: RwLock<LruCache<[u8;32], HashSet<PeerId>>>,   // 100,000
    
    // TX set fetching
    txset_sources: RwLock<LruCache<[u8;32], PeerId>>,         // 1,000
    pending_txset_requests: RwLock<HashMap<[u8;32], PeerId>>,
    
    // Communication channels
    event_tx: mpsc::UnboundedSender<OverlayEvent>,  // Critical path (SCP, TxSet)
    tx_event_tx: mpsc::Sender<OverlayEvent>,        // Bounded channel (TX, backpressure OK)
    tx_dropped_count: AtomicU64,                    // Dropped TX event counter
    control: Control,                               // For reopening streams
    
    // INV/GETDATA flooding state
    inv_batcher: RwLock<InvBatcher>,                // Batches INV announcements per peer
    inv_tracker: RwLock<InvTracker>,                // Tracks TX sources for routing
    pending_getdata: RwLock<PendingRequests>,        // Outstanding GETDATA requests
    tx_buffer: RwLock<TxBuffer>,                    // TX data for serving GETDATA
    inbound_peers: RwLock<HashSet<PeerId>>,         // Track inbound connections
}
```

---

## 5. Stream Management

### 5.1 Per-Peer Streams

```rust
struct PeerOutboundStreams {
    scp: Mutex<Option<Stream>>,
    tx: Mutex<Option<Stream>>,
    txset: Mutex<Option<Stream>>,
}
```

**Locking**: Each stream has its own `Mutex` to avoid head-of-line blocking.
A large TxSet write does not block SCP sends to the same peer.

### 5.2 Stream Lifecycle

```
ConnectionEstablished
        │
        ▼
┌───────────────────────────────────┐
│  open_streams_to_peer()           │
│  - Opens SCP, TX, TxSet           │
│    streams in parallel            │
│  - Stores in peer_streams         │
│  - Sends SCP state request        │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  Normal Operation                 │
│  - send_to_peer_stream()          │
│    reopens if closed              │
│  - try_send_to_existing_          │
│    stream() fails if closed       │
└───────────────────────────────────┘
        │
        ▼
ConnectionClosed
        │
        ▼
┌───────────────────────────────────┐
│  Cleanup                          │
│  - Remove from peer_streams       │
│  - Emit PeerDisconnected          │
└───────────────────────────────────┘
```

### 5.3 Send Functions

| Function | Behavior | Used By |
|----------|----------|---------|
| `send_to_peer_stream()` | Reopens stream if closed, holds mutex during reopen + write | Broadcast, directed sends |
| `try_send_to_existing_stream()` | Fails immediately if closed, no reopen | Flood forwarding |

**BUG in `try_send_to_existing_stream`**: Failed sends still mark peer in `sent_to` cache, causing message loss with no retry.

---

## 6. Flooding Protocol

### 6.1 SCP Flooding

```
Node A originates SCP message
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  BroadcastScp(envelope)                                          │
│  1. hash = blake2b(envelope)                                     │
│  2. Check scp_seen → skip if seen                                │
│  3. Add to scp_seen                                              │
│  4. Get all connected peers                                      │
│  5. Add ALL peers to scp_sent_to[hash] (BEFORE sending)  ← BUG   │
│  6. FOR EACH peer (SEQUENTIAL):                          ← BUG   │
│       await send_to_peer_stream(peer, SCP, envelope)             │
└──────────────────────────────────────────────────────────────────┘
```

```
Node B receives SCP from peer A
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  handle_inbound_scp_streams() (spawned per-peer)                 │
│  1. Read message from stream                                     │
│  2. If len == 4 → SCP state request, emit event                  │
│  3. hash = blake2b(envelope)                                     │
│  4. Check scp_seen → skip if duplicate                           │
│  5. Add to scp_seen                                              │
│  6. Emit ScpReceived event to Core                               │
│  7. FLOOD:                                                       │
│     a. Lock scp_sent_to + peer_streams                           │
│     b. Find peers not in sent_to[hash], excluding sender         │
│     c. Add found peers + sender to sent_to[hash]                 │
│     d. Drop locks                                                │
│     e. spawn task to forward to each peer via                    │
│        try_send_to_existing_stream()                             │
└──────────────────────────────────────────────────────────────────┘
```

### 6.2 TX Flooding (INV/GETDATA)

TX flooding uses a bandwidth-efficient INV/GETDATA protocol, unlike SCP
which uses direct broadcast:

```
Node A receives/submits a TX
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  broadcast_tx(tx)                                                │
│  1. hash = blake2b(tx)                                           │
│  2. Check tx_seen → skip if seen                                 │
│  3. Add to tx_seen, store in tx_buffer for GETDATA responses     │
│  4. Create InvEntry { hash, fee_per_op }                         │
│  5. Add to InvBatcher for each peer                              │
│  6. When batch full or timeout, send InvBatch on TX stream       │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Peer receives InvBatch                                          │
│  1. For each hash in batch:                                      │
│     a. Check tx_seen → skip if already known                     │
│     b. Record source in inv_tracker                              │
│     c. Add to pending GETDATA requests                           │
│  2. Send GetData request for wanted hashes                       │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Original peer receives GetData                                  │
│  1. Look up each hash in tx_buffer                               │
│  2. Send full Tx message with data                               │
└──────────────────────────────────────────────────────────────────┘
```

TX stream message types (1-byte discriminant):
- `0x01` Tx: Full transaction data
- `0x02` InvBatch: Batch of TX hash announcements
- `0x03` GetData: Request specific TXs by hash

### 6.3 TX Set Fetching

```
Core requests TX set
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  RequestTxSet from Core (hash)                                   │
│  1. Check local tx_set_cache → send immediately if found         │
│  2. Add hash to pending_core_txset_requests                      │
│  3. Call libp2p_handle.fetch_txset(hash)                         │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  fetch_txset(hash) in libp2p_overlay                             │
│  1. Check pending_txset_requests → skip if already fetching      │
│  2. Add to pending_txset_requests                                │
│  3. Check txset_sources for known source peer                    │
│  4. If source disconnected or unknown, pick any peer             │
│  5. Send 32-byte hash request on TxSet stream                    │
│  6. If send fails, remove from pending                           │
│  BUG: No timeout, no retry to other peers                        │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Peer receives request (32 bytes)                                │
│  1. Emit TxSetRequested event                                    │
│  2. main.rs looks up in tx_set_cache                             │
│  3. If found, send_txset(hash, data, peer)                       │
│     Response format: [hash:32][xdr_data...]                      │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Receive TX set response (>32 bytes)                             │
│  1. Parse hash from first 32 bytes                               │
│  2. Remove from pending_txset_requests                           │
│  3. Emit TxSetReceived event                                     │
│  4. main.rs checks pending_core_txset_requests                   │
│  5. If Core waiting, send TxSetAvailable via IPC                 │
│  6. Cache in tx_set_cache                                        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 7. Core ↔ Overlay IPC

### 7.1 Transport

- **Protocol**: Unix domain socket
- **Path**: Configurable, default varies
- **Message format**: `[type:u32 NE][length:u32 NE][payload]` (NE = native-endian)
- **Max payload**: 16 MB

### 7.2 Message Types

#### Core → Overlay

| Type | ID | Payload | Description |
|------|-----|---------|-------------|
| BroadcastScp | 1 | `[scp_envelope]` | Broadcast SCP to all peers |
| GetTopTxs | 2 | `[count:4]` | Request top N TXs by fee |
| RequestScpState | 3 | (empty) | Request SCP state from peers |
| LedgerClosed | 4 | `[ledger_seq:4 LE]` | Notify ledger close |
| TxSetExternalized | 5 | `[txset_hash:32][num_hashes:4 LE][tx_hash:32]...` | TX set applied |
| ScpStateResponse | 6 | `[request_id:8 LE][count:4 LE][env_len:4 LE][env]...` | SCP state for peer |
| Shutdown | 7 | (empty) | Shutdown overlay |
| SetPeerConfig | 8 | `JSON` | Configure peer addresses |
| SubmitTx | 10 | `[fee:i64][num_ops:u32][tx_envelope]` | Submit TX |
| RequestTxSet | 11 | `[hash:32]` | Request TX set by hash |
| CacheTxSet | 12 | `[hash:32][txset_xdr]` | Cache locally-built TX set |

#### Overlay → Core

| Type | ID | Payload | Description |
|------|-----|---------|-------------|
| ScpReceived | 100 | `[scp_envelope]` | SCP from network |
| TopTxsResponse | 101 | `[count:4][len:4][tx]...` | Response to GetTopTxs |
| PeerRequestsScpState | 102 | (empty) | Peer wants SCP state |
| TxSetAvailable | 103 | `[hash:32][txset_xdr]` | Fetched TX set |
| QuorumSetAvailable | 104 | `[...]` | Quorum set from peer |

### 7.3 SCP State Request Flow

```
Peer A requests SCP state from Peer B
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Peer A: send 4-byte ledger_seq (LE) on SCP stream               │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Peer B: handle_inbound_scp_streams                              │
│  1. Detect 4-byte message = SCP state request                    │
│  2. Emit ScpStateRequested event with peer_id                    │
│  3. main.rs generates request_id, stores mapping:                │
│     pending_scp_state_requests[request_id] = peer_id             │
│  4. Send PeerRequestsScpState to Core via IPC                    │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Core: Processes request, sends ScpStateResponse                 │
│  Payload: [request_id:8 LE][count:4 LE][env1_len:4 LE][env1]... │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Peer B: main.rs receives ScpStateResponse                       │
│  1. Look up peer_id via request_id from payload                  │
│  2. Parse envelopes from payload                                 │
│  3. Send each envelope to peer via send_scp_to_peer()            │
└──────────────────────────────────────────────────────────────────┘
```

---

## 8. Mempool (`integrated.rs`)

### 8.1 Data Structures

```rust
struct TxEntry {
    data: Vec<u8>,           // Raw TX bytes
    hash: [u8; 32],          // SHA256 of data
    source_account: [u8; 32], // BUG: Always [0;32], not parsed
    sequence: u64,            // BUG: Always 0, not parsed
    fee: u64,                 // From SubmitTx message
    num_ops: u32,             // From SubmitTx message
    received_at: Instant,
    from_peer: u64,
}

struct Mempool {
    by_hash: HashMap<TxHash, TxEntry>,
    by_fee: BTreeSet<FeePriority>,       // Descending fee/op
    by_account: HashMap<AccountId, Vec<TxHash>>,
    max_size: 100,000,
    max_age: 300s,
}
```

### 8.2 Fee Priority Ordering

```rust
// Higher fee per op = higher priority
// Comparison: fee1/ops1 vs fee2/ops2
//           = fee1*ops2 vs fee2*ops1 (avoid division)

struct FeePriority {
    fee: u64,
    num_ops: u32,
    hash: [u8; 32],  // Tie-breaker
}

impl Ord for FeePriority {
    // Order: fee/ops DESC, then num_ops ASC, then hash ASC
}
```

### 8.3 Operations

| Operation | Complexity | Notes |
|-----------|------------|-------|
| `insert()` | O(log n) | Evicts lowest fee if at capacity |
| `remove()` | O(log n) | Updates all 3 indices |
| `top_by_fee(n)` | O(n) | Iterator over BTreeSet |
| `by_account()` | O(k log k) | k = TXs from account |
| `evict_expired()` | O(n) | Scans all TXs |

---

## 9. TX Set Building (`flood/txset.rs`)

### 9.1 GeneralizedTransactionSet XDR Format (v1)

```
GeneralizedTransactionSet {
    v: 1 (u32 BE)
    v1TxSet: TransactionSetV1 {
        previousLedgerHash: Hash (32 bytes)
        phases: [TransactionPhase; 2] {
            // Phase 0: CLASSIC (sequential)
            discriminant: 0 (u32 BE)
            components_len: 0 or 1 (u32 BE)
            [if non-empty:
                component_discriminant: 0 (TXSET_COMP_TXS_MAYBE_DISCOUNTED_FEE)
                baseFee: 0 (not present)
                txs_len: N (u32 BE)
                txs: [TransactionEnvelope; N]
            ]
            
            // Phase 1: SOROBAN (parallel, empty)
            discriminant: 1 (u32 BE)  // parallelTxsComponent
            baseFee: 0 (not present)
            executionStages_len: 0 (u32 BE)
        }
    }
}
```

### 9.2 TX Set Cache

```rust
struct TxSetCache {
    by_hash: HashMap<Hash256, CachedTxSet>,
    max_size: 100,  // Configurable
}

struct CachedTxSet {
    hash: [u8; 32],
    xdr: Vec<u8>,
    ledger_seq: u32,
    tx_hashes: Vec<[u8; 32]>,
}
```

Eviction: On insert if at capacity, removes arbitrary entry.

---

## 10. Event Loop (`main.rs`)

### 10.1 Main Select Loop

```rust
loop {
    tokio::select! {
        // IPC from Core
        msg = core_ipc.receiver.recv() => {
            handle_core_message(msg).await;
        }
        
        // Critical events from libp2p (SCP, TxSet) — unbounded
        Some(event) = libp2p_events.recv() => {
            handle_libp2p_event(event).await;
        }
        
        // TX events from libp2p — bounded (backpressure OK)
        Some(event) = tx_events.recv() => {
            handle_libp2p_event(event).await;
        }
        
        // Periodic reconnect to configured peers
        _ = reconnect_interval.tick() => {
            attempt_reconnect_configured_peers().await;
        }
    }
}
```

Critical events (SCP, TxSet) use an unbounded channel to ensure they
are never dropped. TX events use a bounded channel with backpressure;
dropped TX events are counted via `tx_dropped_count`.

### 10.2 libp2p Overlay Event Loop

```rust
loop {
    tokio::select! {
        // Swarm events (connections, behaviours)
        event = swarm.select_next_some() => {
            handle_swarm_event(event).await;  // BUG: Blocking awaits
        }
        
        // Commands from main.rs
        Some(cmd) = cmd_rx.recv() => {
            match cmd {
                BroadcastScp(env) => broadcast_scp(&env).await,  // BUG: Sequential
                BroadcastTx(tx) => broadcast_tx(&tx).await,      // BUG: Sequential
                FetchTxSet{hash} => fetch_txset(hash).await,
                // ...
            }
        }
    }
}
```

---

## 11. Known Issues and TODOs

### 11.1 Data Integrity

| Issue | Location | Impact |
|-------|----------|--------|
| TX source_account not parsed | `integrated.rs` | Account tracking broken (hardcoded to [0;32]) |
| TX sequence not parsed | `integrated.rs` | Sequence validation broken (hardcoded to 0) |
| TX fee from Core only | `main.rs` | Network TXs received via INV have fee_per_op=0 |
| INV fee_per_op always 0 | `libp2p_overlay.rs` broadcast_tx | TODO: pass actual fee |

### 11.2 Reliability

| Issue | Location | Impact |
|-------|----------|--------|
| No TxSet fetch timeout | `fetch_txset()` | Pending request never cleaned up if peer disconnects |
| sent_to marked before send | `broadcast_scp()` | If send fails, message won't be retried to that peer |

### 11.3 Not Implemented

| Feature | Notes |
|---------|-------|
| Survey protocol | No support |
| Soroban TX set phase | SOROBAN phase always empty in GeneralizedTransactionSet |
| TX validation | No signature/balance checks in overlay |
| Topology optimization | No Tier1 full-mesh, no watcher topic filtering |

---

## 12. Constants Summary

| Constant | Value | Location |
|----------|-------|----------|
| MAX_MESSAGE_SIZE | 16 MB | `libp2p_overlay.rs:40` |
| SCP_SEEN_CACHE | 10,000 | `SharedState::new()` |
| TX_SEEN_CACHE | 100,000 | `SharedState::new()` |
| TXSET_SOURCES_CACHE | 1,000 | `SharedState::new()` |
| CMD_CHANNEL_SIZE | 256 | `create_overlay()` |
| MEMPOOL_MAX_SIZE | 100,000 | `integrated.rs` |
| MEMPOOL_MAX_AGE | 300s | `integrated.rs` |
| TXSET_CACHE_SIZE | 100 | `main.rs` |
| IDLE_CONN_TIMEOUT | 300s | `create_overlay()` |
| QUIC_KEEP_ALIVE | 15s | `create_overlay()` |
| QUIC_MAX_IDLE | 60s | `create_overlay()` |
| IPC_MAX_PAYLOAD | 16 MB | `ipc/messages.rs` |

---

## 13. Hash Functions

| Usage | Algorithm | Output |
|-------|-----------|--------|
| SCP/TX dedup | Blake2b | 32 bytes |
| TX hash | SHA256 | 32 bytes |
| TX set hash | SHA256 | 32 bytes |

---

## 14. Async Task Spawning

| Task | Lifetime | Purpose |
|------|----------|---------|
| `handle_inbound_scp_streams` | Per protocol | Accept SCP streams |
| `handle_inbound_tx_streams` | Per protocol | Accept TX streams |
| `handle_inbound_txset_streams` | Per protocol | Accept TxSet streams |
| SCP stream reader | Per peer | Read SCP messages |
| TX stream reader | Per peer | Read TX messages |
| TxSet stream reader | Per peer | Read TxSet messages |
| SCP flood forward | Per received SCP | Forward to other peers |
| TX flood forward | Per received TX | Forward to other peers |
| Kademlia bootstrap | Once on first Identify | Triggered via BootstrapKademlia command |

---

## 15. File Structure

```
overlay/src/
├── main.rs                 # Entry point, App state, core IPC event loop
├── lib.rs                  # Library exports
├── libp2p_overlay.rs       # P2P networking (~1600 lines)
├── integrated.rs           # Mempool manager
├── config.rs               # Configuration (~260 lines)
├── flood/
│   ├── mod.rs              # Module exports
│   ├── mempool.rs          # TX mempool (~630 lines)
│   ├── txset.rs            # TX set building (~433 lines)
│   ├── inv_batcher.rs      # Batches INV announcements per peer
│   ├── inv_tracker.rs      # Tracks TX sources for GETDATA routing
│   ├── inv_messages.rs     # INV/GETDATA wire format (~300 lines)
│   ├── pending_requests.rs # GETDATA timeout tracking
│   ├── tx_buffer.rs        # TX storage for serving GETDATA
│   └── tx_xdr.rs           # TX metadata parsing
├── ipc/
│   ├── mod.rs              # Module exports
│   ├── messages.rs         # IPC message types (~424 lines)
│   └── transport.rs        # Unix socket transport
└── http/
    └── mod.rs              # HTTP endpoints (minimal)

overlay/tests/
├── e2e_binary.rs           # Binary spawn tests
└── kademlia_test.rs        # Kademlia DHT discovery tests
```

---

## 16. Dependencies

Key Cargo dependencies:
- `libp2p` (0.54+): QUIC, Kademlia, Identify, Stream
- `tokio`: Async runtime
- `blake2`, `sha2`: Hash functions
- `lru`: Dedup caches
- `serde_json`: Config parsing
- `tracing`: Logging

---

## 17. Configuration (`config.rs`)

### 17.1 Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `core_socket` | PathBuf | `/tmp/stellar-overlay.sock` | IPC socket path |
| `listen_addr` | SocketAddr | `0.0.0.0:11625` | Network listen address |
| `libp2p_listen_ip` | String | `0.0.0.0` | IP for libp2p QUIC listener |
| `peer_port` | u16 | 11625 | Base port (libp2p uses +1000) |
| `target_outbound_peers` | usize | 8 | Outbound connection target |
| `max_inbound_peers` | usize | 64 | Max inbound connections |
| `max_mempool_size` | usize | 100000 | Max TXs in mempool |
| `tx_push_peer_count` | usize | 8 | Peers to push INV announcements to |
| `known_peers` | Vec<SocketAddr> | [] | Bootstrap peer addresses |
| `preferred_peers` | Vec<SocketAddr> | [] | Preferred peer addresses |
| `http_addr` | Option<SocketAddr> | `127.0.0.1:11626` | HTTP status endpoint |
| `log_level` | String | `info` | Logging level |

### 17.2 Usage

```rust
// In main.rs
let config = Config::from_file("overlay.toml")?;
overlay.run(&config.libp2p_listen_ip, config.peer_port + 1000).await;
```
