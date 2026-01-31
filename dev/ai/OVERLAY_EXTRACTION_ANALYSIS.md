# Overlay Extraction Analysis Report

**Date:** 2026-01-12
**Goal:** Extract overlay into standalone library for experimentation with new overlay implementations (e.g., libp2p)

## Executive Summary

Extracting the overlay into a standalone library is **feasible but requires significant refactoring**. The main challenge isn't the overlay code itself—it's reasonably well-organized—but rather the **bidirectional coupling** with Herder and the **deep integration with Application state**. There's no single clean cut today; instead, there are approximately **15-20 integration points** that need to be addressed.

---

## 1. Current Overlay Architecture

### Key Classes
| Class | Responsibility | File |
|-------|---------------|------|
| `OverlayManager` | Main interface, peer lifecycle, flooding coordination | `OverlayManager.h` |
| `OverlayManagerImpl` | Implementation with `Floodgate`, `TxDemandsManager`, `SurveyManager` | `OverlayManagerImpl.cpp` |
| `Peer` | Single connection, message handling, flow control | `Peer.cpp` (~2000 lines) |
| `TCPPeer` | ASIO-based TCP transport | `TCPPeer.cpp` |
| `PeerAuth` | curve25519 key exchange, HMAC keys | `PeerAuth.cpp` |
| `PeerManager` | Peer database storage/retrieval | `PeerManager.cpp` |
| `Floodgate` | Broadcast deduplication | `Floodgate.cpp` |
| `FlowControl` | Per-peer rate limiting | `FlowControl.cpp` |
| `TxAdverts` / `TxDemandsManager` | Pull mode implementation | `TxAdverts.cpp`, `TxDemandsManager.cpp` |
| `BanManager` | Node banning | `BanManager.h` |
| `SurveyManager` | Network topology surveys | `SurveyManager.cpp` |

### Source Stats
- ~25 source files in `src/overlay/`
- ~8,000 lines of code (excluding tests)

---

## 2. Dependencies: Overlay → Core

These are things the overlay currently **pulls from** core:

### 2.1 Herder (HEAVY COUPLING) ⚠️

| Method | Called From | Purpose |
|--------|-------------|---------|
| `recvTransaction()` | `OverlayManagerImpl.cpp:1228` | Pass received tx to tx queue |
| `recvSCPEnvelope()` | `Peer.cpp:1662` | Pass SCP messages to consensus |
| `recvTxSet()` | `Peer.cpp:1501, 1510` | Pass fetched tx sets to Herder |
| `recvSCPQuorumSet()` | `Peer.cpp:1618` | Pass fetched quorum sets to Herder |
| `getTxSet()` | `Peer.cpp:1460` | Serve tx set data to peers |
| `getQSet()` | `Peer.cpp:1597` | Serve quorum set data to peers |
| `peerDoesntHave()` | `Peer.cpp:1408` | Inform ItemFetcher about missing data |
| `sendSCPStateToPeer()` | `Peer.cpp:1677` | Send SCP state on GET_SCP_STATE request |
| `getMinLedgerSeqToAskPeers()` | `Peer.cpp:1966` | Query after authentication |
| `isBannedTx()` | `TxDemandsManager.cpp:73` | Check tx status before demanding |
| `getTx()` | `TxDemandsManager.cpp:74` | Check if tx already known |
| `getMaxQueueSizeOps()` | `TxDemandsManager.cpp:50-51` | Flow control limits |
| `getMaxQueueSizeSorobanOps()` | `TxDemandsManager.cpp:53-55` | Soroban flow control |

**Pain Point**: Overlay directly calls into Herder for ~12 different operations. These calls are scattered across `Peer.cpp` (~2000 lines) and `TxDemandsManager.cpp`.

### 2.2 LedgerManager (MODERATE COUPLING)

| Method | Location | Purpose |
|--------|----------|---------|
| `isSynced()` | `Peer.cpp:1157` | Gate tx processing during catchup |
| `getExpectedLedgerCloseTime()` | `Peer.cpp:1418`, `TxDemandsManager.cpp:49` | Rate limiting calculations |
| `getLastMaxTxSetSizeOps()` | `FlowControl.cpp:472`, `TxAdverts.cpp:111,126` | Flow control limits |
| `getLastClosedLedgerHeader()` | `Peer.cpp:1483`, `TxAdverts.cpp:197` | Protocol version checks |
| `getLastClosedSorobanNetworkConfig()` | `TxAdverts.cpp:203` | Soroban-specific limits |

**Pain Point**: Overlay needs ledger state for flow control decisions.

### 2.3 BanManager (LIGHT COUPLING)

| Method | Location | Purpose |
|--------|----------|---------|
| `isBanned(NodeID)` | `Peer.cpp:1783` | Checked during HELLO handshake |

### 2.4 Database (MODERATE COUPLING)

`PeerManager.cpp` uses SOCI directly for peer storage:
- Uses `Application::getDatabase().getMiscSession()`
- ~10 SQL queries for CRUD operations
- Tables: `peers` (ip, port, numfailures, nextattempt, type)

Key methods:
- `load()` - Load peer record
- `store()` - Save peer record
- `loadRandomPeers()` - Get peers for connection attempts
- `getPeersToSend()` - Get peers to share with other nodes
- `removePeersWithManyFailures()` - Cleanup

### 2.5 Config (PERVASIVE)

Used throughout overlay for ~30+ configuration values:

**Identity & Network:**
- `PEER_PORT` - Listening port
- `NODE_SEED` - Node's secret key
- `NETWORK_PASSPHRASE` - Network identifier

**Peer Limits:**
- `TARGET_PEER_CONNECTIONS` - Desired outbound connections
- `MAX_ADDITIONAL_PEER_CONNECTIONS` - Extra allowed connections
- `MAX_PENDING_CONNECTIONS` - Max pending handshakes

**Timeouts:**
- `PEER_TIMEOUT` - Idle timeout for authenticated peers
- `PEER_AUTHENTICATION_TIMEOUT` - Handshake timeout
- `PEER_STRAGGLER_TIMEOUT` - Slow peer detection

**Flow Control:**
- `PEER_FLOOD_READING_CAPACITY` - Messages before SEND_MORE needed
- `FLOOD_OP_RATE_PER_LEDGER` - Tx flooding rate
- `FLOOD_SOROBAN_RATE_PER_LEDGER` - Soroban tx rate
- `FLOOD_DEMAND_PERIOD_MS` - Pull mode timing
- `FLOOD_DEMAND_BACKOFF_DELAY_MS` - Retry backoff

**Protocol:**
- `OVERLAY_PROTOCOL_VERSION` - Current protocol version
- `OVERLAY_PROTOCOL_MIN_VERSION` - Minimum supported version
- `LEDGER_PROTOCOL_VERSION` - Ledger protocol version

**Peer Lists:**
- `KNOWN_PEERS` - Bootstrap peers
- `PREFERRED_PEERS` - Preferred peer addresses
- `PREFERRED_PEER_KEYS` - Preferred peer public keys

**Misc:**
- `MAX_SLOTS_TO_REMEMBER` - SCP slot history
- `BACKGROUND_OVERLAY_PROCESSING` - Enable overlay thread
- `BACKGROUND_TX_SIG_VERIFICATION` - Background signature checks

### 2.6 Crypto & XDR (DEEP COUPLING)

| Usage | Location | Purpose |
|-------|----------|---------|
| Network ID | `Peer.cpp:136`, `Peer.cpp:1114` | Message signing/verification |
| `TransactionFrameBase::makeTransactionFromWire()` | `Peer.cpp:176` | Create tx domain objects |
| `TxSetXDRFrame::makeFromWire()` | `Peer.cpp:1500, 1509` | Create tx set objects |
| SCP signature verification | `Peer.cpp:1110-1116` | Verify SCP envelopes in background |
| BLAKE2/SHA256 hashing | Throughout | Message deduplication |
| curve25519 | `PeerAuth.cpp` | Key exchange |
| HMAC-SHA256 | `Hmac.cpp` | Message authentication |

### 2.7 BucketManager (UNEXPECTED COUPLING)

| Usage | Location | Purpose |
|-------|----------|---------|
| `SearchableSnapshotConstPtr` | `Peer.cpp:69-127` | Background signature verification |
| `LedgerSnapshot` | `Peer.cpp:71` | Account lookups for sig verification |
| `copySearchableLiveBucketListSnapshot()` | `OverlayManagerImpl.cpp:1443` | Get bucket snapshot |

This coupling exists because the overlay thread can verify transaction signatures in the background before passing to main thread, which requires looking up account signers.

### 2.8 VirtualClock & Timers

All overlay timers use `VirtualClock` from Application:
- `VirtualTimer` for periodic tasks
- `VirtualClock::time_point` for timestamps
- Used in: `Peer`, `OverlayManagerImpl`, `TxDemandsManager`, `TxAdverts`

### 2.9 Scheduler

Message processing uses Application's scheduler for prioritization:
- `Peer.cpp:1127` - `postOnMainThread` with `Scheduler::ActionType`
- Different queues: AUTH, CTRL, TX, SCPQ, SCP, MISC

---

## 3. Dependencies: Core → Overlay

These are things core **calls into** overlay:

### 3.1 Herder → Overlay

| Caller | Method | Purpose |
|--------|--------|---------|
| `PendingEnvelopes.cpp:561` | `broadcastMessage()` | Broadcast SCP envelopes |
| `HerderImpl.cpp:558` | `broadcastMessage()` | Broadcast SCP envelopes |
| `TransactionQueue.cpp:1061` | `broadcastMessage()` | Flood transactions |
| `HerderImpl` | `clearLedgersBelow()` | Cleanup on ledger close |
| `HerderImpl` | `forgetFloodedMsg()` | Remove discarded messages |

### 3.2 Application Lifecycle

| Caller | Method | Purpose |
|--------|--------|---------|
| `ApplicationImpl.cpp:767` | `start()` | Start overlay |
| `ApplicationImpl.cpp:843` | `shutdown()` | Stop overlay |

### 3.3 CommandHandler (HTTP API)

Various overlay info exposed via `/peers`, `/info`, etc.

---

## 4. Message Flow Analysis

### 4.1 Incoming Transaction Flow
```
TCPPeer::readHandler
  → Peer::recvAuthenticatedMessage  (background thread possible)
  → [Background: verify HMAC, optionally verify tx signatures]
  → postOnMainThread(recvMessage)
  → Peer::recvRawMessage
  → Peer::recvTransaction
  → OverlayManager::recvTransaction
  → Herder::recvTransaction          ← COUPLING POINT
  → TransactionQueue::add
```

### 4.2 Outgoing Transaction Flow
```
TransactionQueue::broadcast
  → OverlayManager::broadcastMessage
  → Floodgate::broadcast
  → [For each peer not already knowing message]
  → Peer::sendMessage
  → FlowControl queue
  → sendAuthenticatedMessage
  → TCPPeer::sendMessage (async write)
```

### 4.3 SCP Message Flow
```
Peer::recvSCPMessage
  → OverlayManager::recvFloodedMsgID (track who sent it)
  → Herder::recvSCPEnvelope          ← COUPLING POINT
  → [SCP processing]
  → Herder emits new envelope
  → OverlayManager::broadcastMessage
  → Floodgate::broadcast
  → [flood to peers]
```

### 4.4 Pull Mode Flow (Transactions)
```
// Receiving advert
Peer::recvFloodAdvert
  → TxAdverts::queueIncomingAdvert

// Demanding
TxDemandsManager::demand (on timer)
  → [Check if tx known/banned via Herder]
  → Peer::sendTxDemand

// Receiving demand
Peer::recvFloodDemand
  → OverlayManager::recvTxDemand
  → TxDemandsManager::recvTxDemand
  → [Lookup tx in Herder, send if found]
```

---

## 5. Proposed Interface Boundary

For a clean extraction, the library needs a **callback-based interface** rather than direct subsystem access.

### 5.1 Overlay Library Interface (what library exposes)

```cpp
// What the library provides to core
class OverlayLibrary {
public:
    virtual ~OverlayLibrary() = default;

    // Lifecycle
    virtual void start() = 0;
    virtual void shutdown() = 0;

    // Broadcast a message to all peers
    // Returns true if sent to at least one peer
    virtual bool broadcastMessage(
        std::vector<uint8_t> const& msg,
        std::optional<Hash> const& hash) = 0;

    // Cleanup old state
    virtual void clearLedgersBelow(uint32_t ledgerSeq) = 0;
    virtual void forgetFloodedMsg(Hash const& msgID) = 0;

    // Peer info
    virtual std::vector<PeerInfo> getAuthenticatedPeers() const = 0;
    virtual int getAuthenticatedPeersCount() const = 0;
    virtual int getPendingPeersCount() const = 0;
};
```

### 5.2 Callbacks Interface (what core provides to library)

```cpp
// Callbacks that core implements, library calls
class OverlayCallbacks {
public:
    virtual ~OverlayCallbacks() = default;

    // === Incoming data notifications ===

    // Transaction received from peer
    virtual void onTransactionReceived(
        std::vector<uint8_t> const& txEnvelope,
        PeerId peer,
        Hash const& hash) = 0;

    // SCP envelope received from peer
    virtual void onSCPEnvelopeReceived(
        std::vector<uint8_t> const& envelope,
        PeerId peer,
        Hash const& hash) = 0;

    // Transaction set received (response to GET_TX_SET)
    virtual void onTxSetReceived(
        Hash const& hash,
        std::vector<uint8_t> const& txSet) = 0;

    // Quorum set received (response to GET_SCP_QUORUMSET)
    virtual void onQuorumSetReceived(
        Hash const& hash,
        std::vector<uint8_t> const& qSet) = 0;

    // Peer doesn't have requested data
    virtual void onPeerDoesntHave(
        MessageType type,
        Hash const& hash,
        PeerId peer) = 0;

    // Peer wants our SCP state
    virtual void onGetSCPState(
        uint32_t ledgerSeq,
        PeerId peer) = 0;

    // === Data requests (library asks core for data) ===

    // Get tx set to serve to peer (nullopt if not found)
    virtual std::optional<std::vector<uint8_t>> getTxSet(
        Hash const& hash) = 0;

    // Get quorum set to serve to peer (nullopt if not found)
    virtual std::optional<std::vector<uint8_t>> getQuorumSet(
        Hash const& hash) = 0;

    // Get SCP state to send to peer
    virtual std::vector<std::vector<uint8_t>> getSCPState(
        uint32_t ledgerSeq) = 0;

    // === Status queries ===

    // Is node synced with network?
    virtual bool isSynced() const = 0;

    // Is this transaction banned?
    virtual bool isTxBanned(Hash const& hash) const = 0;

    // Do we already have this transaction?
    virtual bool hasTx(Hash const& hash) const = 0;

    // Get a transaction we have (for responding to demands)
    virtual std::optional<std::vector<uint8_t>> getTx(
        Hash const& hash) const = 0;

    // === Rate limiting parameters ===

    virtual std::chrono::milliseconds getLedgerCloseTime() const = 0;
    virtual uint32_t getMaxTxSetSizeOps() const = 0;
    virtual uint32_t getMaxQueueSizeOps() const = 0;
    virtual uint32_t getMaxQueueSizeSorobanOps() const = 0;
};
```

### 5.3 Configuration Struct

```cpp
struct OverlayConfig {
    // Identity
    uint16_t peerPort;
    SecretKey nodeSeed;  // Or just the public key + signing callback
    Hash networkId;
    std::string networkPassphrase;

    // Peer limits
    uint32_t targetPeerConnections;
    uint32_t maxAdditionalPeerConnections;
    uint32_t maxPendingInboundConnections;
    uint32_t maxPendingOutboundConnections;

    // Timeouts (seconds)
    uint32_t peerTimeout;
    uint32_t peerAuthTimeout;
    uint32_t peerStragglerTimeout;

    // Flow control
    uint32_t peerFloodReadingCapacity;
    double floodOpRatePerLedger;
    double floodSorobanRatePerLedger;
    std::chrono::milliseconds floodDemandPeriod;
    std::chrono::milliseconds floodDemandBackoffDelay;

    // Protocol versions
    uint32_t overlayProtocolVersion;
    uint32_t overlayProtocolMinVersion;
    uint32_t ledgerProtocolVersion;

    // Peer lists
    std::vector<std::string> knownPeers;
    std::vector<std::string> preferredPeers;
    std::vector<PublicKey> preferredPeerKeys;

    // Optional features
    bool enablePullMode;
    bool enableBackgroundProcessing;
};
```

### 5.4 Peer Storage Interface

```cpp
// Abstract peer persistence (so library doesn't need database)
class PeerStorage {
public:
    virtual ~PeerStorage() = default;

    struct PeerRecord {
        std::string ip;
        uint16_t port;
        uint32_t numFailures;
        std::chrono::system_clock::time_point nextAttempt;
        PeerType type;  // INBOUND, OUTBOUND, PREFERRED
    };

    virtual std::optional<PeerRecord> load(
        std::string const& ip, uint16_t port) = 0;

    virtual void store(
        std::string const& ip, uint16_t port,
        PeerRecord const& record) = 0;

    virtual std::vector<PeerRecord> loadRandomPeers(
        PeerType type, size_t count) = 0;

    virtual void removePeersWithManyFailures(size_t minFailures) = 0;
};
```

---

## 6. Pain Points & Tech Debt

### 6.1 Critical Issues (Must Fix for Extraction)

| Issue | Location | Impact | Effort |
|-------|----------|--------|--------|
| **Application& everywhere** | All overlay classes | Can't instantiate without full Application | Medium |
| **Direct Herder calls** | `Peer.cpp` (12 methods) | Tight consensus coupling | Medium |
| **TransactionFrame creation** | `Peer.cpp:176` | Creates domain objects, needs network ID | Low |
| **SCP signature verification** | `Peer.cpp:1110-1116` | Needs network ID, crypto | Low |
| **Background signature verification** | `Peer.cpp:63-127` | Uses `LedgerSnapshot` from BucketManager | High |
| **Database coupling** | `PeerManager.cpp` | Direct SOCI usage | Medium |

### 6.2 Moderate Issues

| Issue | Location | Impact | Effort |
|-------|----------|--------|--------|
| **VirtualClock coupling** | All timers | Need async runtime abstraction | Medium |
| **Config pervasiveness** | ~30 config values | Need config struct extraction | Low |
| **Metrics (medida)** | Throughout | Need metrics abstraction | Low |
| **Scheduler integration** | `Peer.cpp:1127` | Message prioritization tied to app scheduler | Medium |

### 6.3 Architectural Observations

1. **AppConnector is a step in the right direction** (`src/main/AppConnector.h`)
   - Already tries to isolate subsystem access
   - But exposes too much: Herder, LedgerManager, BanManager, OverlayManager
   - Could be refactored to implement `OverlayCallbacks`

2. **Overlay thread already exists**
   - `BACKGROUND_OVERLAY_PROCESSING` config enables background thread
   - `postOnOverlayThread` / `postOnMainThread` pattern already established
   - Shows code is already async-aware

3. **Message types are well-defined**
   - XDR in `src/protocol-curr/xdr/Stellar-overlay.x`
   - Provides clean protocol specification
   - ~20 message types, well-documented

4. **Flooding logic is reasonably isolated**
   - `Floodgate` - deduplication, ~150 lines
   - `FlowControl` - rate limiting, ~500 lines
   - `TxAdverts` - pull mode adverts, ~250 lines
   - These could move to library relatively easily

5. **Survey system is optional**
   - `SurveyManager` is ~1500 lines
   - Used for network topology analysis
   - Could be left in core initially

---

## 7. Recommended Extraction Strategy

### Phase 1: Create Abstraction Layer (C++ side)

**Goal:** Decouple overlay from direct subsystem access without changing behavior.

1. Define `OverlayCallbacks` interface in C++ (as shown above)
2. Create `OverlayConfig` struct with all needed config values
3. Create `PeerStorage` interface for peer persistence
4. Modify `AppConnector` to implement `OverlayCallbacks`
5. Replace direct Herder/LedgerManager calls with callback invocations
6. Replace direct database calls with `PeerStorage` interface

**Estimated changes:**
- New files: ~3 (interfaces)
- Modified files: ~10 (Peer.cpp, OverlayManagerImpl.cpp, TxDemandsManager.cpp, etc.)
- Lines changed: ~500-800

### Phase 2: Extract Database

1. Implement `PeerStorage` interface with current SQLite backend
2. Move peer table DDL to storage implementation
3. Test that peer persistence still works

**Estimated changes:**
- New files: 1 (SQLitePeerStorage)
- Modified files: 2 (PeerManager.cpp, OverlayManagerImpl.cpp)
- Lines changed: ~200

### Phase 3: Prepare Rust FFI Boundary

1. Choose FFI approach: **`cxx`** recommended (see section 8)
2. Define Rust traits matching C++ interfaces
3. Create bridge types for Hash, PublicKey, etc.
4. Implement C++ shim that calls Rust

**Estimated new code:**
- `src/rust/overlay_bridge.rs` - FFI definitions
- `src/overlay/RustOverlayAdapter.cpp` - C++ adapter

### Phase 4: Implement Rust Library

Order of implementation:
1. Config and types
2. `PeerAuth` - key exchange (can use `x25519-dalek`, `hmac`)
3. `TCPPeer` - tokio TCP, message framing
4. `Floodgate` - deduplication logic
5. `FlowControl` - rate limiting
6. `OverlayManager` - peer lifecycle
7. (Optional) Pull mode - `TxAdverts`, `TxDemandsManager`

**Estimated Rust code:** ~4000-6000 lines

### Phase 5: Integration & Testing

1. Feature flag to switch between C++ and Rust overlay
2. Run both in parallel, compare behavior
3. Extensive testing on testnet
4. Gradual rollout

---

## 8. Rust FFI Recommendation

Given constraints (simplest, safest, tokio-based):

### Recommended: `cxx` crate

The `cxx` crate provides safe C++/Rust interop with:
- Compile-time type checking
- No manual unsafe blocks for common cases
- Exception safety (C++ exceptions become Rust panics)
- Automatic memory management
- Good documentation

**Example bridge definition:**

```rust
// src/rust/overlay_ffi.rs

#[cxx::bridge(namespace = "stellar::rust")]
mod ffi {
    // Shared types
    struct OverlayConfig {
        peer_port: u16,
        node_seed: [u8; 32],
        network_id: [u8; 32],
        // ... other fields
    }

    struct PeerInfo {
        id: [u8; 32],
        address: String,
        is_inbound: bool,
    }

    // Rust types exposed to C++
    extern "Rust" {
        type RustOverlay;

        fn create_overlay(
            config: &OverlayConfig,
            callbacks: Pin<&mut OverlayCallbacks>,
        ) -> Result<Box<RustOverlay>>;

        fn start(self: &RustOverlay) -> Result<()>;
        fn shutdown(self: &RustOverlay);
        fn broadcast_message(
            self: &RustOverlay,
            msg: &[u8],
            hash: &[u8; 32]
        ) -> bool;
        fn get_authenticated_peers(self: &RustOverlay) -> Vec<PeerInfo>;
    }

    // C++ types/functions called from Rust
    unsafe extern "C++" {
        include!("overlay/OverlayCallbacks.h");

        type OverlayCallbacks;

        fn on_transaction_received(
            self: Pin<&mut OverlayCallbacks>,
            tx: &[u8],
            peer_id: &[u8; 32],
            hash: &[u8; 32],
        );

        fn on_scp_envelope_received(
            self: Pin<&mut OverlayCallbacks>,
            envelope: &[u8],
            peer_id: &[u8; 32],
            hash: &[u8; 32],
        );

        fn is_synced(self: &OverlayCallbacks) -> bool;
        fn is_tx_banned(self: &OverlayCallbacks, hash: &[u8; 32]) -> bool;
        // ... etc
    }
}
```

### Alternative: cbindgen + bindgen

More manual but more flexible:
- `cbindgen` generates C headers from Rust
- `bindgen` generates Rust bindings from C headers
- Requires more unsafe code
- Better for complex lifetime scenarios

### Alternative: UniFFI

Mozilla's cross-language bindings generator:
- Define interface in UDL (like IDL)
- Generates bindings for multiple languages
- Overkill if only targeting C++

---

## 9. Effort Estimates

| Area | Complexity | Estimated Time | Notes |
|------|------------|----------------|-------|
| Define callback interface | Low | 1-2 days | Clean abstraction design |
| Refactor Peer.cpp to use callbacks | Medium | 3-5 days | ~20 call sites |
| Extract PeerManager storage | Medium | 2-3 days | Abstract SOCI |
| Extract Config | Low | 1 day | Mechanical |
| Setup cxx bridge | Medium | 2-3 days | Boilerplate, build integration |
| Port PeerAuth to Rust | Low | 2-3 days | Standard crypto |
| Port TCPPeer to Rust/tokio | Medium | 5-7 days | Well-defined scope |
| Port Floodgate logic | Medium | 3-4 days | State machine |
| Port FlowControl | Medium | 3-4 days | Rate limiting |
| Port pull mode | High | 5-7 days | Complex state machines |
| Integration testing | High | 5-10 days | Correctness verification |

**Total estimate:** 4-8 weeks for MVP (without pull mode)

---

## 10. Open Questions

1. **Pull mode in library?**
   - You mentioned "not necessarily needed"
   - Removing it simplifies: no `TxAdverts`, `TxDemandsManager`
   - But current network uses it heavily

2. **Peer storage ownership?**
   - Option A: Library owns storage (simpler library interface)
   - Option B: Core provides storage via callback (library is stateless)
   - Recommendation: Option A with pluggable backend

3. **Survey system?**
   - `SurveyManager` is ~1500 lines
   - Not critical for basic overlay function
   - Recommendation: Leave in core initially, add later

4. **Background signature verification?**
   - Currently uses `LedgerSnapshot` for account lookups
   - Options:
     - A) Pass raw bytes, let core verify (simpler library)
     - B) Library verifies with callback for account data (more complex)
   - Recommendation: Option A initially

5. **libp2p integration?**
   - If considering libp2p, interface should be more abstract
   - libp2p has own peer discovery (DHT, mDNS)
   - libp2p has own pubsub (gossipsub)
   - Would need to map Stellar message types to libp2p topics
   - Recommendation: Design interface to be transport-agnostic

---

## 11. File Reference

Key files to understand:

| File | Purpose | Lines |
|------|---------|-------|
| `src/overlay/OverlayManager.h` | Main interface | 220 |
| `src/overlay/OverlayManagerImpl.cpp` | Implementation | 1450 |
| `src/overlay/Peer.h` | Peer interface | 550 |
| `src/overlay/Peer.cpp` | Peer implementation | 2050 |
| `src/overlay/TCPPeer.cpp` | TCP transport | 400 |
| `src/overlay/PeerAuth.cpp` | Authentication | 200 |
| `src/overlay/PeerManager.cpp` | Peer storage | 650 |
| `src/overlay/Floodgate.cpp` | Broadcast dedup | 150 |
| `src/overlay/FlowControl.cpp` | Rate limiting | 550 |
| `src/overlay/TxAdverts.cpp` | Pull mode adverts | 250 |
| `src/overlay/TxDemandsManager.cpp` | Pull mode demands | 200 |
| `src/main/AppConnector.h` | App access abstraction | 100 |
| `src/protocol-curr/xdr/Stellar-overlay.x` | Protocol definition | 360 |

---

## 12. Next Steps

1. **Review this document** - Validate understanding, answer open questions
2. **Decide on scope** - With/without pull mode, survey, etc.
3. **Prototype callback interface** - Small C++ change to validate approach
4. **Setup Rust build** - Integrate cxx into stellar-core build
5. **Implement incrementally** - Start with PeerAuth, then TCP, then flooding
