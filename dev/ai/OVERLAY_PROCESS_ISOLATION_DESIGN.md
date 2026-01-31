# Overlay Process Isolation Design

**Date**: 2026-01-27 (Updated - Message Flooding Bugfixes)
**Status**: ✅ **PROTOTYPE WORKING** - SCP Consensus + TX Flooding + Message Flooding Achieved!
**Previous Work**: See OVERLAY_EXTRACTION_ANALYSIS.md, TRANSPORT_REDESIGN.md
**Recent Bugfixes**: See RUST_OVERLAY_BUGFIXES_JAN27_2026.md for detailed analysis

---

## ✅ Implementation Status (2026-01-21)

### Milestones Achieved

1. **SCP Consensus** - Two stellar-core nodes reach consensus using Rust overlay
   - Full message flow: Core → IPC → Rust → QUIC → Peer → Rust → IPC → Core
   - E2E test passes: reaches ledger 5+ on both nodes
   - 10-node network consensus test passes

2. **TX Set Support** - Core builds TX sets using transactions from Rust mempool
   - Core requests top N transactions via `GET_TOP_TXS`
   - Overlay returns top transactions from mempool (fee-per-op ordered)
   - Core builds `GeneralizedTransactionSet` locally
   - TX set caching in Rust for peer requests
   - CLASSIC phase implemented (TODO: SOROBAN)

### What's Implemented
| Component | Status | Notes |
|-----------|--------|-------|
| IPC Protocol | ✅ Working | Unix sockets (SOCK_STREAM), length-prefix framing |
| SCP Relay | ✅ Working | Broadcasts Core→Rust→Peers, receives Peers→Rust→Core |
| Peer Connections | ✅ Working | QUIC transport with TLS 1.3 (via libp2p) |
| TX Set Support | ✅ Working | Core builds TX sets from mempool TXs, caching for peers |
| TX Flooding | ✅ Working | Peer-to-peer flooding with deduplication |
| Mempool | ✅ Working | Fee-per-op ordered, eviction, dedup |
| Peer Management | ✅ Working | Kademlia DHT for discovery, manual bootstrap |
| Stream Independence | ✅ Working | Dedicated QUIC streams (SCP/TX/TxSet) via libp2p-stream |

### Key Bugs Found & Fixed

#### Bug #1: StellarMessage Format Mismatch (Critical)
**Symptom**: SCP messages sent to peers were not recognized as SCP.

**Root Cause**: Format mismatch between IPC and peer-to-peer protocols:
- IPC protocol: Raw `SCPEnvelope` XDR bytes  
- Peer protocol: `StellarMessage` union with 4-byte discriminant

**Fix**: 
```rust
// On broadcast (Core → Peers): Add StellarMessage header
let mut stellar_msg = vec![0u8, 0, 0, 10]; // SCP_MESSAGE = 10
stellar_msg.extend_from_slice(&envelope);

// On receive (Peers → Core): Strip header
let scp_envelope = plaintext[4..].to_vec();
```

**Learning**: When bridging protocols, explicitly document and test the wire format at each boundary.

#### Bug #2: Socket Path Collision
**Symptom**: Multiple nodes in simulation collided on socket path.

**Root Cause**: Socket path was only using PID, not unique per-node identifier.

**Fix**: Use `HTTP_PORT` in path: `/tmp/stellar-overlay-{pid}-{HTTP_PORT}.sock`

#### Bug #3: Simulation stopOverlayTick Segfault  
**Symptom**: SIGSEGV when stopping overlay tick in OVER_TCP mode.

**Root Cause**: `static_cast<OverlayManagerImpl&>` on `RustOverlayManager`.

**Fix**: Use `dynamic_cast` with null check.

#### Bug #4: Missing Message Flooding (CRITICAL - Jan 27, 2026)
**Symptom**: 10-node consensus test timed out. Only 23 SCP messages received (expected ~3000).

**Root Cause**: Nodes received SCP/TX messages from peers but didn't forward to other peers. This broke gossip protocol in partially connected networks (ring topology).

**Example Failure**:
```
Network: A --- B --- C
         |           |
         D           E

- A broadcasts M to B and D
- B receives M but doesn't forward → C and E never get M
- Consensus stalls because C and E miss critical messages
```

**Fix**: Added flooding logic in `handle_inbound_scp_streams()` and `handle_inbound_tx_streams()`:
- On receive: spawn task to forward message to all peers except sender
- Added LRU caches to track which peers already have each message (avoid duplicate sends)
- Use `try_send_to_existing_stream()` to only send to peers with open streams (no stream reopening during flood)

**Result**: SCP receives jumped from 23 → 3780, consensus progresses correctly.

**Learning**: Gossip requires BOTH origin broadcast AND message forwarding by receivers. Flooding should be opportunistic (only use existing streams, don't trigger connection opens).

#### Bug #5: crankUntil Predicate Bug (Test Framework - Jan 27, 2026)
**Symptom**: Test kept cranking past target ledger, throwing "overshoot" error at ledger 9 when checking for ledger 5.

**Root Cause**: `Simulation::haveAllExternalized()` used exact equality `min == num` instead of `min >= num`. Fast-advancing nodes jumped from ledger 4 to 6 during the 1-second check interval, missing the window where min==5.

**Fix**: Changed predicate to use `>=` for monotonically increasing values:
```cpp
// Before:
return (min == num) && ((max - min) <= maxSpread);

// After:
return (min >= num) && ((max - min) <= maxSpread);
```

**Learning**: Use `>=` not `==` for state progression checks (ledgers, timestamps). Exact equality creates race conditions in fast-changing systems.

---

## Executive Summary

This document captures a new architecture for stellar-core's overlay system that achieves:

1. **Process isolation** - Overlay runs as a separate process from Core
2. **Clean API** - Minimal, well-defined IPC boundary
3. **Zero TX copying** - Transactions never cross the process boundary in normal flow
4. **Latency optimization** - Critical SCP path is ~1KB of data

---

## High-Level Goals (Ordered by Importance)

### Goal #1: Minimize SCP Latency
Design transport that minimizes time between SCP message originating on a node and being received by peer. Eliminate all head-of-line blocking for latency-critical traffic.

### Goal #2: Extensibility  
Clean, language-agnostic API between overlay and core. Should be able to plug in different overlay implementations (including Rust) without integration pain.

### Goal #3: Maintainability
Aggressively reduce complexity. Replace over-engineered in-house solutions with libraries where possible.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  CORE PROCESS                          OVERLAY PROCESS                       │
│  ════════════                          ═══════════════                       │
│                                                                              │
│  ┌─────────────────┐                   ┌─────────────────────────────────┐  │
│  │ SCP State       │◄── SCP envelopes ─┤ Network Layer                   │  │
│  │ Machine         │─── SCP envelopes ►│ (peers, transport)              │  │
│  └────────┬────────┘                   └─────────────────────────────────┘  │
│           │                                          ▲                       │
│           │ "nominate"                               │ txs from peers        │
│           │                                          │                       │
│           │              ┌─────────────┐             │                       │
│           └─────────────►│ "give hash" │◄────────────┘                       │
│                          └──────┬──────┘                                     │
│                                 │                                            │
│                                 ▼                                            │
│                          ┌─────────────────────────────────┐                │
│                          │ Mempool                         │                │
│                          │ - validates against snapshot    │◄── user submit │
│                          │ - assembles TX sets             │                │
│                          │ - tracks TX set hashes seen     │                │
│                          └──────────────┬──────────────────┘                │
│                                         │                                    │
│  ┌─────────────────┐                    │ optimistic push                   │
│  │ Execution       │◄───────────────────┘ (TX sets seen in SCP)             │
│  │ Engine          │                                                        │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           │ new ledger                                                       │
│           ▼                                                                  │
│  ┌─────────────────┐     snapshot      ┌─────────────────────────────────┐  │
│  │ Ledger State    │──────────────────►│ Ledger Snapshot (read-only)     │  │
│  │ (buckets)       │   (after close)   │ or: getLedgerEntry RPC calls    │  │
│  └─────────────────┘                   └─────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. Separate Process (Not In-Process Library)

**Decision**: Overlay runs as a separate OS process communicating via IPC.

**Rationale**:
| Aspect | Separate Process | In-Process Library |
|--------|------------------|-------------------|
| Crash isolation | ✅ Overlay crash doesn't kill Core | ❌ Shared fate |
| Independent updates | ✅ Can update overlay without Core restart | ❌ Linked together |
| Language flexibility | ✅ Any language | ✅ Rust via FFI |
| Debugging | ⚠️ Harder (two processes) | ✅ Single process |
| Latency (SCP) | ⚠️ ~5μs IPC overhead | ✅ ~100ns function call |
| Complexity | ⚠️ IPC protocol, process management | ✅ Simpler |

**The 5μs IPC overhead is acceptable** - it's 0.005% of a 100ms network RTT. Measured latency in stress tests confirms SCP is not bottlenecked by IPC.

### 2. Transactions Never Cross Boundary (Normal Flow)

**Decision**: Overlay owns the mempool. Transactions stay in overlay process.

**How it works**:
1. Users submit TXs to Overlay (new API endpoint)
2. Overlay validates TXs against ledger snapshot
3. Core requests top N transactions via `GET_TOP_TXS`
4. Overlay returns top transactions (fee-per-op ordered)
5. **Core builds the TX set locally** and nominates the hash
6. Overlay tracks TX set hashes seen in SCP messages
7. When Overlay sees a new TX set hash, it fetches from peers via `REQUEST_TX_SET`
8. Overlay sends fetched TX sets to Core via `TX_SET_AVAILABLE`
9. Core caches TX sets for execution

**Result**: 10MB TX sets don't block the critical SCP path. Core only needs TX bytes when executing - fetched asynchronously.

### 3. Ledger Snapshot Access

**Decision**: Overlay accesses ledger state via:
- Shared bucket files (buckets are immutable, can open separate read stream)
- OR `getLedgerEntry` RPC calls to Core (simpler for MVP)

**Not needed**: Copying entire ledger state across process boundary.

### 4. Survey System Removed

**Decision**: Survey system is out of scope. Consider it deprecated for this design.

---

## What Crosses the Process Boundary

### Critical Path (Latency-Sensitive)

| Direction | Data | Size | Notes |
|-----------|------|------|-------|
| Core → Overlay | SCP envelope to broadcast | ~500 bytes | Must be immediate |
| Overlay → Core | SCP envelope received | ~500 bytes | Must be immediate |
| Core → Overlay | "Give me nomination hash" | ~100 bytes | Must be immediate |
| Overlay → Core | Nomination hash response | ~100 bytes | Must be immediate |

**Total critical path: ~1KB** - Unix sockets are fine.

### Non-Critical Path (Can Be Async/Optimistic)

| Direction | Data | Size | Notes |
|-----------|------|------|-------|
| Overlay → Core | TX set bytes | Up to 10MB | Pushed optimistically before needed |
| Overlay → Core | Quorum set | Small | Rare, OK to copy |
| Core → Overlay | Ledger closed notification | Small | Background |
| Core → Overlay | SCP state (on peer request) | ~100 envelopes | On demand |

### Never Crosses (Normal Flow)

| Data | Notes |
|------|-------|
| Individual transactions | Stay in overlay mempool |
| TX flooding traffic | Handled entirely in overlay |
| Peer connection state | Overlay internal |

---

## IPC Protocol Design

### Message Format

```
┌──────────────────────────────────────────────────────────────────┐
│  0      1      2      3      4      5      6      7     ...     │
├──────────────────────────────────────────────────────────────────┤
│    Message Type (u32)    │    Payload Length (u32)    │ Payload │
└──────────────────────────────────────────────────────────────────┘
```

Uses **SOCK_STREAM** Unix sockets with length-prefix framing (not SOCK_SEQPACKET).

### Core → Overlay Messages

```rust
enum CoreToOverlay {
    // ═══ CRITICAL PATH ═══
    
    /// Broadcast this SCP envelope to all peers
    BroadcastScp { 
        envelope: Vec<u8>,  // XDR bytes
    },
    
    /// Request top N transactions from mempool for nomination
    GetTopTxs { 
        count: u32,
    },
    
    /// Request current SCP state (peer asked via GET_SCP_STATE)
    RequestScpState {
        ledger_seq: u32,
        request_id: u64,
    },
    
    // ═══ NON-CRITICAL ═══
    
    /// Ledger closed, here's the new state
    LedgerClosed {
        ledger_seq: u32,
        ledger_hash: [u8; 32],
    },
    
    /// We externalized this TX set, drop TXs from mempool
    TxSetExternalized { 
        tx_set_hash: [u8; 32],
        tx_hashes: Vec<[u8; 32]>,
    },
    
    /// Response: here's the SCP state you requested
    ScpStateResponse {
        request_id: u64,
        envelopes: Vec<Vec<u8>>,  // XDR bytes
    },
    
    /// Submit a transaction for flooding
    SubmitTx {
        fee: i64,
        num_ops: u32,
        tx_envelope: Vec<u8>,  // XDR
    },
    
    /// Request a TX set by hash (async - response via TX_SET_AVAILABLE)
    RequestTxSet {
        hash: [u8; 32],
    },
    
    /// Cache a locally-built TX set so Rust can serve it to peers
    CacheTxSet {
        hash: [u8; 32],
        tx_set_xdr: Vec<u8>,
    },
    
    /// Configure peer addresses for Kademlia DHT bootstrap
    SetPeerConfig {
        known_peers: Vec<String>,
        preferred_peers: Vec<String>,
        listen_port: u16,
    },
    
    /// Lifecycle
    Shutdown,
}
```

### Overlay → Core Messages

```rust
enum OverlayToCore {
    // ═══ CRITICAL PATH ═══
    
    /// Received SCP envelope from network
    ScpReceived { 
        envelope: Vec<u8>,  // XDR bytes
        from_peer: PeerId,
    },
    
    /// Response to GET_TOP_TXS request
    TopTxsResponse {
        count: u32,
        txs: Vec<(i64, u32, Vec<u8>)>,  // (fee, numOps, txEnvelopeXDR)
    },
    
    /// Peer requested SCP state
    PeerRequestsScpState {
        ledger_seq: u32,
        peer_id: PeerId,
        request_id: u64,
    },
    
    // ═══ NON-CRITICAL ═══
    
    /// TX set fetched from peer (response to REQUEST_TX_SET)
    TxSetAvailable {
        hash: [u8; 32],
        tx_set_xdr: Vec<u8>,
    },
    
    /// Here's a quorum set referenced in SCP
    QuorumSetAvailable {
        hash: [u8; 32],
        qset: Vec<u8>,  // XDR bytes
    },
}
```

---

## Overlay Internal Architecture

The overlay process uses **libp2p** with **QUIC transport** for networking:

```
┌─────────────────────────────────────────────────────────────────┐
│                     OVERLAY PROCESS                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    FLOODING LAYER                          │ │
│  │  - TX mempool (validation, dedup, assembly)                │ │
│  │  - SCP message relay (no interpretation, just forward)     │ │
│  │  - TX set caching (built by Core, served to peers)         │ │
│  │  - Quorum set caching                                      │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              PEER MANAGEMENT LAYER (libp2p)                │ │
│  │  - Kademlia DHT for peer discovery                         │ │
│  │  - Identify protocol for peer info exchange                │ │
│  │  - Connection lifecycle (connect, auth, disconnect)        │ │
│  │  - Peer state management                                   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │            TRANSPORT LAYER (libp2p QUIC)                   │ │
│  │  - QUIC connections with TLS 1.3 encryption                │ │
│  │  - Independent stream per protocol (SCP/TX/TxSet)          │ │
│  │  - 4-byte length prefix per message                        │ │
│  │  - No head-of-line blocking between streams                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### libp2p-stream Protocol Independence

Each peer connection maintains three **persistent bidirectional streams**:

```
Peer A ←─────────────────────────────────────────────────────→ Peer B
        
        SCP Stream:     /stellar/scp/1.0.0
        TX Stream:      /stellar/tx/1.0.0
        TxSet Stream:   /stellar/txset/1.0.0
```

**Key Property**: QUIC provides independent loss recovery per stream. If a packet is lost on the TX stream, the SCP stream is **completely unaffected**.

**Benefits over TCP**:
- No head-of-line blocking between protocols
- 0-RTT connection resumption
- Automatic congestion control per stream
- Built-in encryption (TLS 1.3) - no separate crypto layer needed

### Layer Responsibilities

#### Transport Layer
- **Input**: Raw bytes to send, connection requests
- **Output**: Raw bytes received, connection events
- **Implementation**: libp2p QUIC transport
- **No knowledge of**: Message types, SCP, transactions

#### Peer Management Layer  
- **Input**: "Connect to these addresses", "Disconnect peer X"
- **Output**: "Peer connected", "Peer disconnected", authenticated peer list
- **Implementation**: Kademlia DHT + Identify protocol
- **Handles**: Peer discovery, connection limits, peer database
- **No knowledge of**: Message content beyond peer identity

#### Flooding Layer
- **Input**: Messages from peers, commands from Core
- **Output**: Messages to peers, events to Core
- **Handles**: Mempool, TX validation, broadcast dedup, TX set caching, **message forwarding**
- **Knows about**: Message types, but NOT SCP semantics (just relay)

#### Message Flooding Implementation (Added Jan 27, 2026)

**Design Principle**: Gossip requires both origin broadcast AND message forwarding by receivers.

**Flooding Flow**:
```
Peer A sends message M to Node B
   ↓
Node B receives M on inbound stream
   ↓
Node B deduplicates (already seen?)
   ↓
Node B forwards to Core (local processing)
   ↓
Node B spawns flood task:
   - Get peers who haven't received M yet
   - Send M to each peer (only if stream already open)
   - Track sends in LRU cache
```

**Key Implementation Details**:

1. **Deduplication at Two Layers**:
   - **Receive-side**: Check if message already seen (LRU cache of message hashes)
   - **Send-side**: Track which peers we've sent each message to (LRU cache of hash → Set<PeerId>)
   - Result: O(n) sends instead of O(n²)

2. **Opportunistic Flooding**:
   - Uses `try_send_to_existing_stream()` - only sends if stream already open
   - Does NOT trigger stream opens during flood
   - Prevents connection churn under high load
   - Separates stream lifecycle from message forwarding

3. **Async Spawning**:
   - Flooding spawned as separate task (doesn't block receive loop)
   - Each message floods in parallel
   - Backpressure handled at stream level (QUIC flow control)

**Code Location**: `overlay/src/libp2p_overlay.rs`
- Lines 897-926: `try_send_to_existing_stream()` helper
- Lines 1049-1091: SCP flooding logic
- Lines 1171-1206: TX flooding logic

**Why This Matters**: In partially connected networks (e.g., ring topology), messages only reach directly connected peers without forwarding. The 10-node test uses ring topology with cross-connections - without flooding, nodes 3+ hops away never receive messages.

---

## Policies That Change

### Current: Overlay has SCP visibility

The current overlay queries Herder for:
- `trackingConsensusLedgerIndex()` - used in Floodgate
- `isNewerNominationOrBallotSt()` - used to keep newer SCP messages
- `getMinLedgerSeqToAskPeers()` - authentication handshake
- `getMinLedgerSeqToRemember()` - FlowControl cleanup

### New: Overlay is SCP-agnostic

**SCP messages are just opaque bytes to relay.** Policies change to:

| Old Policy | New Policy |
|------------|------------|
| Keep newer SCP messages | Keep ALL SCP messages (they bypass FlowControl anyway per Phase 1) |
| Cleanup based on ledger seq | Core sends `LedgerClosed` → Overlay cleans up |
| Min ledger for peer handshake | Core provides this in config |
| Tracking consensus index | Not needed - SCP is Core's problem |

**Key insight**: SCP messages are small and critical. Don't drop them, don't rate-limit them, just relay them immediately.

---

## TX Validation in Overlay

Overlay validates transactions against ledger snapshot:
- Signature verification
- Sequence number check
- Fee check
- Balance check

**Snapshot access options** (in order of simplicity):

1. **RPC to Core**: Call `getLedgerEntry` endpoint (exists today)
2. **Shared bucket files**: Open same bucket files as Core (buckets are immutable)
3. **Snapshot delta updates**: Core pushes deltas after each ledger close

For MVP, option 1 is simplest. Optimization can come later.

---

## Flow Diagrams

### Normal Flow: Local TX Submission → Nomination

```
User                    Overlay                           Core
  │                        │                                │
  │── submit TX ──────────►│                                │
  │                        │                                │
  │                        │ [add to mempool]               │
  │                        │                                │
  │                        │ [flood to peers]               │
  │                        │                                │
  │                        │◄── GET_TOP_TXS(N) ─────────────│
  │                        │                                │
  │                        │ [get top N by fee-per-op]      │
  │                        │                                │
  │                        │── TOP_TXS_RESPONSE(TXs) ──────►│
  │                        │                                │
  │                        │                                │ [build TX set locally]
  │                        │                                │ [compute hash]
  │                        │                                │ [SCP nominates hash]
```

### Normal Flow: SCP Message Relay

```
Peer A                  Overlay                           Core
  │                        │                                │
  │── SCP envelope ───────►│                                │
  │   (QUIC SCP stream)    │── SCP_RECEIVED ───────────────►│
  │                        │                                │ [SCP processes]
  │                        │                                │
  │                        │◄── BROADCAST_SCP ──────────────│
  │                        │                                │
  │◄── SCP envelope ───────│ (to all other peers            │
  │   (QUIC SCP stream)    │  via QUIC SCP stream)          │
```

### TX Set Fetch Flow

```
Peer B                  Overlay                           Core
  │                        │                                │
  │── SCP nominate(H) ────►│                                │
  │   (QUIC SCP stream)    │                                │
  │                        │ [H is new, need TX set]        │
  │                        │                                │
  │── GET_TX_SET(H) ◄──────│                                │
  │   (QUIC TxSet stream)  │                                │
  │── TX_SET(H, bytes) ───►│                                │
  │   (QUIC TxSet stream)  │                                │
  │                        │── TX_SET_AVAILABLE(H, bytes) ─►│ [cache for execution]
  │                        │                                │
  │                        │── SCP_RECEIVED(nominate H) ───►│
  │                        │                                │ [SCP processes]
  │                        │                                │
  ... later ...
  │                        │                                │
  │                        │                                │ [externalize H]
  │                        │                                │ [execute - TX set
  │                        │                                │  already cached!]
```

---

## IPC Implementation

### Current: Unix Domain Sockets (SOCK_STREAM)

```
Core ←──── Unix Socket (SOCK_STREAM + length-prefix framing) ────► Overlay
```

- **Socket Type**: `SOCK_STREAM` (not SEQPACKET)
- **Framing**: 4-byte length prefix + 4-byte type + payload
- **Latency**: ~5μs for small messages (measured)
- **Throughput**: More than sufficient for critical path (~1KB/message)
- **Complexity**: Low - standard Unix socket programming
- **Debugging**: Easy (can use socat, strace)

**Why not SOCK_SEQPACKET?**
- SOCK_STREAM is more portable and better tested
- Length-prefix framing is trivial (~10 lines of code)
- No measurable performance difference for our message sizes

### Future: Shared Memory + Ring Buffers (If Needed)

```
Core ←──── Shared Memory (SPSC rings) ────► Overlay
```

- **Latency**: ~100ns-1μs
- **Complexity**: High (proper memory barriers, synchronization)
- **When needed**: If profiling shows Unix socket is a bottleneck

**Current Assessment**: Unix sockets are NOT a bottleneck. Stress tests show SCP latency is dominated by network RTT, not IPC.

---

## Migration Strategy

### Phase 1: Define IPC Protocol
- Finalize message types
- Implement IPC layer in Core (C++)
- Create overlay stub process (Rust) that echoes messages

### Phase 2: Extract Flooding Layer
- Move mempool to overlay process
- Move TX validation to overlay (using RPC for ledger access)
- Keep peer management in Core temporarily

### Phase 3: Extract Peer Management
- Move peer database to overlay
- Move connection lifecycle to overlay
- Move authentication to overlay

### Phase 4: Extract Transport
- Move TCP handling to overlay
- Core only talks to overlay via IPC

### Phase 5: Optimize
- Replace RPC ledger access with shared buckets
- Add shared memory IPC if needed
- Consider QUIC transport

---

## Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| TX validation location | Overlay (using ledger snapshot) |
| TX submission endpoint | Overlay (new API) |
| TX set assembly | Overlay builds, returns hash to Core |
| Quorum sets | Pass to Core (copy is fine, rare) |
| GET_SCP_STATE | Overlay asks Core, Core responds |
| Item tracking (DONT_HAVE) | Overlay internal, Core doesn't care |
| Survey system | Out of scope, ignore completely |

---

## Benefits of This Design

### 1. Clean Separation
- Core = SCP state machine + execution engine
- Overlay = networking + mempool
- Clear contract between them

### 2. Independent Scaling
- Can run multiple overlay processes for different peer sets
- Can update overlay without restarting Core

### 3. Crash Isolation
- Overlay crash doesn't lose SCP state
- Core crash doesn't lose pending TXs (overlay can replay)

### 4. Language Flexibility
- Overlay can be pure Rust
- No FFI complexity for TX handling
- Only simple IPC protocol

### 5. Testability
- Can test overlay with mock Core
- Can test Core with mock overlay
- Clear interface to verify

---

## References

- Previous analysis: `dev/ai/OVERLAY_EXTRACTION_ANALYSIS.md`
- Transport redesign (Phases 1-2 done): `dev/ai/TRANSPORT_REDESIGN.md`
- Alternative transport designs: `dev/ai/TRANSPORT_ALTERNATIVE_DESIGNS.md`
- Socket debugging learnings: `dev/ai/CRITICAL_SOCKET_DEBUG_LEARNINGS.md`
- NASA complexity study: https://www.nasa.gov/wp-content/uploads/2015/04/418878main_fswc_final_report.pdf

---

## Implementation Learnings (2026-01-27)

### What Worked Well

1. **TDD Approach** - Writing tests first (E2E consensus test) provided clear success criteria and caught integration bugs early.

2. **Unix Socket IPC** - Simple, reliable, ~5μs latency is negligible. `SOCK_STREAM` with length-prefix framing worked well.

3. **QUIC Transport via libp2p** - Stream independence works as designed. Packet loss on TX stream doesn't affect SCP stream. TLS 1.3 encryption built-in.

4. **libp2p-stream** - Persistent bidirectional streams (SCP/TX/TxSet) eliminated need for custom protocol multiplexing.

5. **Kademlia DHT** - Peer discovery works without manual peer gossip. Bootstrap from KNOWN_PEERS, then automatic discovery.

6. **Async Rust with Tokio** - Natural fit for concurrent peer connections. `mpsc` channels for Core↔Overlay communication worked perfectly.

7. **Simulation Framework** - Using `OVER_TCP` mode for E2E testing was invaluable. Real QUIC + real processes = high confidence.

### What Was Harder Than Expected

1. **Wire Format Alignment** - Multiple encoding layers (IPC XDR, StellarMessage XDR, QUIC framing) made debugging difficult. Each boundary needs explicit format documentation.

2. **TX Set Format for Protocol ≥23** - Soroban phase must use parallel format (v=1), not sequential (v=0). Validation happens in `TxSetFrame.cpp:checkValidSoroban()`.

3. **Process Lifecycle** - Coordinating startup (overlay must listen before Core connects), shutdown (graceful IPC close), and crash handling.

4. **Logging Across Processes** - Rust and C++ logs interleave. Timestamps help but correlation is still tricky.

5. **libp2p Port Mapping** - Rust overlay uses QUIC port = peer_port + 1000. Tests must configure KNOWN_PEERS with C++ peer port, Rust converts it.

### Architecture Validation

| Design Decision | Validated? | Notes |
|-----------------|------------|-------|
| Separate process | ✅ | Clean separation, easy debugging, IPC overhead negligible |
| QUIC transport | ✅ | Stream independence proven in tests, no head-of-line blocking |
| Unix socket IPC | ✅ | Fast enough, simple, not a bottleneck in stress tests |
| SCP messages are opaque | ✅ | Overlay just relays, no SCP logic needed |
| Core builds TX sets | ✅ | GetTopTxs flow simpler than hash-based nomination |
| Kademlia DHT | ✅ | Peer discovery works, no need for Stellar-style gossip |
| Async Rust | ✅ | Natural fit for networking |

### Code Quality Observations

1. **Message Type Alignment** - Rust and C++ enums match exactly (verified via tests). Critical for correctness.

2. **Error Handling** - Some `let _ = channel.send()` patterns silently drop errors. Consider logging failures.

3. **Dead Code** - TX flooding code existed but wasn't wired initially. Now fully integrated and tested.

### Recommended Review Patterns

When reviewing overlay code, check:

1. **Format at boundaries** - Is raw XDR or StellarMessage expected? Document explicitly.
2. **Message type alignment** - Do Rust enum values match C++ enum values exactly?
3. **QUIC stream selection** - Is message going to correct stream (SCP/TX/TxSet)?
4. **Graceful shutdown** - Does the overlay handle IPC close? Does Core handle overlay crash?
5. **Fee-per-op ordering** - Are TXs sorted correctly for nomination?

---

## Current Status (2026-01-27)

### Completed ✅
1. ~~Finalize IPC message types~~ - All 12 Core→Overlay and 5 Overlay→Core messages defined
2. ~~Prototype IPC layer~~ - Unix socket IPC working
3. ~~Basic SCP relay working~~ - SCP consensus achieved
4. ~~Wire TX flooding~~ - TX flooding peer-to-peer with dedup
5. ~~Peer discovery~~ - Kademlia DHT implemented
6. ~~Multi-hop testing~~ - 10-node network consensus test passes

### In Progress ⚠️
7. **Crash recovery** - Detect overlay crash, attempt restart (manual recovery only)
8. **Metrics** - IPC latency, message counts, peer stats (not exposed to C++)

### Future 🚧
9. **Soroban support** - Parallel TX execution phase (CLASSIC only for now)
10. **Production hardening** - Rate limiting, backpressure, resource limits
11. **Performance tuning** - Profile and optimize critical paths

---

## Test Inventory (117+ Total Tests)

### Rust Unit Tests (99 tests)

| Module | Tests | What They Cover |
|--------|-------|-----------------|
| **Config** | 13 | Default config, TOML parsing, validation (min/max values, invalid input) |
| **Mempool** | 20 | Insert, evict, dedup, fee ordering, by-account queries, stress (10K TXs) |
| **TX Set** | 13 | Building, hashing, caching, eviction, determinism |
| **IPC Messages** | 11 | Serialization roundtrip, error handling (truncated, invalid, oversized) |
| **IPC Transport** | 10 | Unix socket send/receive, connection close, all message types |
| **Integrated** | 12 | SubmitTx, GetTopTxs, fee-per-op ordering, CacheTxSet flow |
| **libp2p Overlay** | 20 | SCP relay (2-node, 3-node), TX broadcast, TX set fetch, stream independence, dedup, disconnect detection |

**Key Coverage:**
- ✅ All IPC message types serialize/deserialize correctly
- ✅ Mempool handles edge cases (zero fee, capacity, eviction)
- ✅ TX set hashing is deterministic
- ✅ SCP not blocked by TX flood (stream independence proof)
- ✅ Large TX sets (16MB) don't block SCP stream
- ✅ Message flooding with deduplication (Jan 27, 2026)

### C++ Integration Tests (18 tests)

| Test Name | Tag | What It Tests |
|-----------|-----|---------------|
| `OverlayIPC connects to Rust overlay` | `[overlay-ipc-rust][.]` | Basic IPC connection |
| `OverlayIPC broadcasts SCP to Rust overlay` | `[overlay-ipc][.]` | SCP broadcast Core→Overlay→Peer |
| `OverlayIPC receives SCP from Rust overlay` | `[overlay-ipc][.]` | SCP receive Peer→Overlay→Core |
| `OverlayIPC ledger close notification` | `[overlay-ipc][.]` | LedgerClosed message |
| `Two Cores communicate via Rust overlays` | `[overlay-ipc][.]` | Two Core instances via Rust overlays |
| `Rust overlay SCP consensus` | `[overlay-ipc][.]` | Full SCP consensus via Rust overlay |
| `Rust overlay get top transactions` | `[overlay-ipc][.]` | GET_TOP_TXS request/response |
| `Rust overlay TX submission` | `[overlay-ipc][.]` | SubmitTx via IPC |
| `Rust overlay TX inclusion` | `[overlay-ipc][.]` | TX included in top TXs |
| `Rust overlay TX fee per op inclusion` | `[overlay-ipc][.]` | Fee-per-op ordering |
| `Rust overlay mempool eviction` | `[overlay-ipc][.]` | Mempool eviction at capacity |
| `Rust overlay TX deduplication` | `[overlay-ipc][.]` | TX dedup |
| `Rust overlay mempool clear on externalize` | `[overlay-ipc][.]` | Mempool clear after externalization |
| `Rust overlay TX flooding between peers` | `[overlay-ipc][.]` | TX flooding peer-to-peer |
| `Rust overlay TX included in ledger` | `[overlay-ipc][.]` | End-to-end TX inclusion in ledger |
| `Rust overlay SCP latency under TX load` | `[overlay-ipc-stress]` | SCP latency stress test (no head-of-line blocking) |
| `Rust overlay 10-node network consensus` | `[overlay-ipc]` | **10-node ring topology consensus** (Added Jan 27, 2026) |
| `IPC payload size benchmark` | `[overlay-ipc-rust][.][benchmark]` | IPC performance benchmarking |

**Key Coverage:**
- ✅ Full SCP consensus (2-node, 10-node with ring topology)
- ✅ End-to-end TX flow (submit → flood → include in ledger)
- ✅ Stress test: SCP under TX load (verifies stream independence)
- ✅ Message flooding in partially connected networks (Jan 27, 2026)
- ✅ IPC performance benchmarked

### Test Coverage Summary

| Functionality | Rust Tests | C++ Tests | Total | Status |
|---------------|------------|-----------|-------|--------|
| **Configuration** | 13 | 0 | 13 | ✅ Well tested |
| **Mempool** | 20 | 4 | 24 | ✅ Well tested |
| **TX Sets** | 13 | 0 | 13 | ✅ Well tested |
| **IPC Protocol** | 21 | 6 | 27 | ✅ Well tested |
| **SCP Relay** | 5 | 5 | 10 | ✅ Well tested |
| **TX Flooding** | 8 | 3 | 11 | ✅ Well tested |
| **Message Flooding** | 0 | 1 | 1 | ✅ Covered (10-node test) |
| **Peer Management** | 4 | 0 | 4 | ⚠️ Basic coverage |
| **Integration/E2E** | 12 | 4 | 16 | ✅ Well tested |
| **Stress/Benchmark** | 1 | 2 | 3 | ⚠️ Limited |
| **TOTAL** | **99** | **18** | **117** | ✅ |

### Coverage Gaps

**Well Tested ✅**
- Mempool operations (insert, evict, dedup, ordering)
- TX set building and caching
- IPC message serialization
- SCP relay (2-node, 3-node, 10-node)
- TX flooding with dedup
- **Message flooding in partially connected networks (Jan 27, 2026)**
- Stream independence (SCP not blocked by TX flood)
- **Smart deduplication (sender-side tracking, Jan 27, 2026)**

**Needs More Tests ⚠️**
- Kademlia DHT peer discovery (only basic tests)
- Crash recovery and reconnection
- Network partition scenarios
- Byzantine peer behavior
- Resource exhaustion (memory, connections)
- **Message flooding under various topologies** (only ring topology tested)

**Not Tested ❌**
- QUIC 0-RTT connection resumption
- Kademlia DHT churn (peers joining/leaving rapidly)
- Survey system (out of scope)
- Shared bucket file access for ledger state
