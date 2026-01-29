# Rust Overlay Design

**Updated**: 2026-01-27 (Message Flooding Implementation)  
**Status**: 117+ tests passing (99 Rust + 18 C++), prototype working  

---

## Overview

The Rust overlay is a separate process that handles all peer-to-peer networking for stellar-core. It communicates with the C++ core via Unix domain socket IPC.

**Key Properties:**
- Process isolation: overlay crash doesn't crash core
- Latency-sensitive traffic (SCP, TX SET) is never blocked by lower priority traffic (TX)
- Free parallelism as SCP runs completely independently
---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        stellar-core (C++)                        │
│                                                                  │
│  ┌────────────────┐    ┌─────────────────────────────────────┐  │
│  │  HerderImpl    │───▶│  RustOverlayManager                 │  │
│  │  (SCP logic)   │    │   └─ OverlayIPC (Unix socket)       │  │
│  └────────────────┘    └─────────────────────────────────────┘  │
│                                      │                           │
└──────────────────────────────────────│───────────────────────────┘
                                       │ Unix socket IPC
                                       │ (length-prefixed messages)
┌──────────────────────────────────────│───────────────────────────┐
│                        stellar-overlay (Rust)                     │
│                                      │                           │
│  ┌───────────────────────────────────▼──────────────────────────┐│
│  │                     Core IPC Handler                         ││
│  │  • Reads/writes IPC messages                                 ││
│  │  • Routes to libp2p overlay                                  ││
│  └──────────────────────────────────┬───────────────────────────┘│
│                                     │                            │
│  ┌──────────────────────────────────▼───────────────────────────┐│
│  │                   libp2p Swarm (QUIC)                        ││
│  │                                                              ││
│  │  Behaviours:                                                 ││
│  │  • libp2p-stream: SCP/TX/TxSet protocol streams              ││
│  │  • Kademlia: peer discovery DHT                              ││
│  │  • Identify: peer info exchange                              ││
│  └──────────────────────────────────────────────────────────────┘│
│                              │                                   │
│              ┌───────────────┼───────────────┐                   │
│              │               │               │                   │
│              ▼               ▼               ▼                   │
│         [Peer 1]        [Peer 2]        [Peer 3]                │
│         SCP stream      SCP stream      SCP stream              │
│         TX stream       TX stream       TX stream               │
│         TxSet stream    TxSet stream    TxSet stream            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Transport: QUIC

QUIC provides everything we need in a single protocol:
- **Encryption**: TLS 1.3 built-in (no separate Noise layer)
- **Multiplexing**: Multiple independent streams per connection
- **Stream independence**: Lost packet on TX stream doesn't block SCP stream
- **0-RTT**: Fast reconnection to known peers

libp2p address format: `/ip4/<addr>/udp/<port>/quic-v1`

---

## Stream Protocols

Three dedicated streams per peer, using libp2p-stream:

| Protocol | Purpose | Message Size | Priority |
|----------|---------|--------------|----------|
| `/stellar/scp/1.0.0` | Consensus messages | ~500B | Critical |
| `/stellar/tx/1.0.0` | Transaction flooding | ~1KB | Normal |
| `/stellar/txset/1.0.0` | TX set fetch | Up to 16MB | Critical |

Each stream uses 4-byte length prefix framing.

TxSet is critical because nodes must fetch nominated TX sets to participate in consensus voting.
---

## Peer Discovery: Kademlia DHT

Kademlia provides decentralized peer discovery:
- Bootstrap nodes configured via `known_peers`
- Nodes join DHT and discover peers automatically
- No Stellar-style peer gossip needed (no backward compat)

---

## IPC Messages

Core → Overlay:
| Type | ID | Purpose |
|------|-----|---------|
| BroadcastScp | 1 | Broadcast SCP envelope to peers |
| GetTopTxs | 2 | Request top N transactions from mempool |
| RequestScpState | 3 | Ask peers for SCP state (when catching up) |
| LedgerClosed | 4 | Notify ledger state change |
| TxSetExternalized | 5 | Drop TXs for externalized TX set |
| ScpStateResponse | 6 | Response to peer's SCP state request |
| Shutdown | 7 | Graceful shutdown |
| SetPeerConfig | 8 | Configure bootstrap nodes (from KNOWN_PEERS) |
| SubmitTx | 10 | Submit TX from `tx` endpoint for flooding |
| RequestTxSet | 11 | Request TX set by hash (async fetch) |
| CacheTxSet | 12 | Cache a locally-built TX set for serving to peers |

Overlay → Core:
| Type | ID | Purpose |
|------|-----|---------|
| ScpReceived | 100 | SCP envelope from network |
| TopTxsResponse | 101 | Response to GetTopTxs with transaction data |
| PeerRequestsScpState | 102 | Peer requested SCP state |
| TxSetAvailable | 103 | TX set fetched from peer |
| QuorumSetAvailable | 104 | Quorum set referenced in SCP |

---

## Code Structure

```
overlay/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point, CLI args, main loop
│   ├── lib.rs            # Public module exports
│   ├── config.rs         # Configuration parsing (TOML)
│   ├── libp2p_overlay.rs # libp2p swarm, stream handling (1400 LOC)
│   ├── integrated.rs     # High-level overlay API
│   ├── ipc/
│   │   ├── mod.rs
│   │   ├── messages.rs   # IPC message types
│   │   └── transport.rs  # Unix socket read/write
│   ├── flood/
│   │   ├── mod.rs
│   │   ├── mempool.rs    # Fee-ordered TX mempool
│   │   └── txset.rs      # TX set building/caching
│   └── http/
│       └── mod.rs        # HTTP status endpoint (minimal)
└── tests/
    └── e2e_binary.rs     # Binary integration tests
```

---

## Test Coverage (117 tests)

### Rust Unit Tests (99 tests)

**Config (13 tests):**
- Default config, TOML parsing, validation

**Mempool (20 tests):**
- Insert, evict, dedup, fee ordering, by-account queries
- Stress test: 10,000 TX inserts

**TX Set (13 tests):**
- Building, hashing, caching, eviction

**IPC Messages (11 tests):**
- Serialization roundtrip for all message types
- Error handling (truncated, invalid, oversized)

**IPC Transport (10 tests):**
- Unix socket send/receive
- Connection close detection
- All message types verified

**Integrated (12 tests):**
- SubmitTx → mempool
- GetTopTxs → response
- TX ordering by fee-per-op
- CacheTxSet → fetch flow

**libp2p Overlay (20 tests):**
- Two overlays connect and exchange SCP
- Three-node SCP relay (triangle topology)
- TX broadcast and deduplication
- TX set fetch with source tracking
- SCP not blocked by TX flood (and vice versa)
- Peer disconnect detection
- Large TX set doesn't block SCP (stream independence proof)

### C++ Integration Tests (18 tests)

**OverlayIPC Tests (17 tests):**
- Basic IPC connection
- SCP broadcast and receive
- Two cores communicate via Rust overlays
- Full SCP consensus (2-node)
- **Full SCP consensus (10-node ring topology)**
- TX submission, flooding, inclusion in ledger
- Mempool operations (eviction, dedup, clear on externalize)
- Stress: SCP latency under TX load (verifies stream independence)

**Benchmark (1 test):**
- IPC payload size benchmark

**What Tests Verify**:
- ✅ SCP messages propagate through partially connected networks (ring topology)
- ✅ Message flooding works with smart deduplication
- ✅ Opportunistic flooding doesn't cause connection churn
- ✅ Stream independence: SCP not blocked by TX flood and vice versa
- ✅ 10-node consensus with Kademlia discovery

---

## Opportunities for Improvement

### Connectivity Topology

Currently peer management is outsourced to lipp2p GossipSub default implementaiton. Peer discovery is done via Kademlia DHT. This needs to be optimized for Stellar network characteristics: Tier1 full connectivity, watchers subscribing to certain topics like EXTERNALIZE messages only, etc.

### Mempool

Transactions aren't validated in the Rust overlay, only basic deduplication and fee ordering is done. Full validation is done at the block layer by SCP (so mempool DoS needs to be addressed).

### Survey

Not supported at all at the moment.

### Soroban Support

Not supported at all at the moment.

