# Rust Overlay Design

**Status**: Prototype — 133+ tests passing (115+ Rust + 18 C++)

---

## Overview

The Rust overlay is a separate process that handles all peer-to-peer networking for stellar-core. It communicates with the C++ core via Unix domain socket IPC.

**Key Properties:**
- Process isolation: overlay crash doesn't crash core
- Latency-sensitive traffic (SCP, TX SET) is never blocked by lower priority traffic (TX) via independent QUIC streams
- TX flooding uses bandwidth-efficient INV/GETDATA protocol
- Peer discovery via Kademlia DHT — no legacy peer gossip

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

| Protocol | Purpose | Message Format | Priority |
|----------|---------|---------------|----------|
| `/stellar/scp/1.0.0` | Consensus messages | Length-prefixed SCP envelopes (~500B) | Critical |
| `/stellar/tx/1.0.0` | Transaction flooding | INV/GETDATA protocol (typed messages) | Normal |
| `/stellar/txset/1.0.0` | TX set fetch | Request: 32-byte hash; Response: hash+XDR | Critical |

SCP and TxSet streams use 4-byte length-prefixed framing.
TX stream uses a typed message format: `[type:1][payload...]` with types Tx (0x01), InvBatch (0x02), GetData (0x03).

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
│   ├── main.rs              # Entry point, App state, core IPC event loop
│   ├── lib.rs               # Public module exports
│   ├── config.rs            # Configuration parsing (TOML)
│   ├── libp2p_overlay.rs    # libp2p swarm, stream handling (~1600 LOC)
│   ├── integrated.rs        # Mempool manager, TX set cache wrapper
│   ├── ipc/
│   │   ├── mod.rs
│   │   ├── messages.rs      # IPC message types and binary codec
│   │   └── transport.rs     # Unix socket read/write
│   ├── flood/
│   │   ├── mod.rs
│   │   ├── mempool.rs       # Fee-ordered TX mempool (~630 LOC)
│   │   ├── txset.rs         # TX set XDR building/caching
│   │   ├── inv_batcher.rs   # Batches INV announcements per peer
│   │   ├── inv_tracker.rs   # Tracks TX sources for GETDATA routing
│   │   ├── inv_messages.rs  # INV/GETDATA wire format encoding
│   │   ├── pending_requests.rs  # GETDATA timeout tracking
│   │   ├── tx_buffer.rs     # TX storage for serving GETDATA responses
│   │   └── tx_xdr.rs        # TX metadata parsing helpers
│   └── http/
│       └── mod.rs           # HTTP status endpoint (minimal)
└── tests/
    ├── e2e_binary.rs        # Binary integration tests
    └── kademlia_test.rs     # Kademlia DHT discovery tests
```

---

## Test Coverage (133+ tests)

### Rust Unit Tests (115+ tests)

**Config (13 tests):**
- Default config, TOML parsing, validation

**Mempool (19 tests):**
- Insert, evict, dedup, fee ordering, by-account queries
- Stress test: 10,000 TX inserts

**TX Set (12 tests):**
- XDR building, hashing, caching, eviction

**IPC Messages (11 tests):**
- Serialization roundtrip for all message types
- Error handling (truncated, invalid, oversized)

**INV/GETDATA (51 tests):**
- inv_messages (18): Wire format encoding/decoding
- inv_batcher (11): Batching and timeout
- inv_tracker (12): Source tracking and round-robin
- pending_requests (10): Timeout and retry logic

**TX Buffer (10 tests):**
- Storage, expiry, GETDATA response serving

**libp2p Overlay (1 test):**
- Integration test

**E2E / Kademlia (6 tests):**
- Binary spawn tests, Kademlia peer discovery

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

---

## Known Limitations

### Not Yet Implemented
- **Survey support**: No survey protocol
- **Soroban TX set phase**: SOROBAN phase in GeneralizedTransactionSet is always empty
- **TX validation**: Only deduplication and fee ordering; no signature/balance checks
- **TX fee from network**: TXs received from peers have fee=0 (only locally submitted TXs have fee metadata)
- **TX source_account/sequence parsing**: Hardcoded to zeros (TODO in code)

### Peer Management
Peer discovery relies on Kademlia DHT. Topology optimization for Stellar
network characteristics (Tier1 full connectivity, watcher nodes) is not
yet implemented.

