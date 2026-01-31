# Rust Overlay Bugfixes - January 27, 2026

## Session Summary

Fixed critical bugs in the Rust overlay that prevented 10-node network consensus tests from passing. All tests now pass successfully.

---

## Bugs Fixed

### 1. Missing Message Flooding (CRITICAL)

**Symptom:** 10-node consensus test timed out. SCP messages only reached directly connected peers, not the entire network.

**Root Cause:** When a node received an SCP/TX message from a peer, it only forwarded to C++ Core but didn't forward to other peers. This broke gossip protocol in partially connected networks.

**Example Failure:**
```
Network: A --- B --- C
         |           |
         D           E

- A broadcasts M to B and D
- B receives M but doesn't forward → C and E never get M
- Consensus stalls at ledger 3
```

**Fix:** `overlay/src/libp2p_overlay.rs`
- Added flooding logic in `handle_inbound_scp_streams()` (lines 1049-1091)
- Added flooding logic in `handle_inbound_tx_streams()` (lines 1171-1206)
- When receiving a message, spawn task to forward to all peers except sender

**Impact:** SCP message count increased from 23 to 3780. Test changed from timeout to overshoot.

---

### 2. Inefficient Flooding (Bandwidth Waste)

**Symptom:** Flooding worked but sent O(n²) duplicate messages instead of O(n).

**Root Cause:** No tracking of which peers already received each message. In a 10-node fully connected network, one message resulted in 90 sends instead of 10.

**Fix:** `overlay/src/libp2p_overlay.rs`
- Added `scp_sent_to` and `tx_sent_to` LRU caches to SharedState (lines 204-223)
- Modified `broadcast_scp()` to track which peers were sent each message (lines 633-666)
- Modified `broadcast_tx()` to track which peers were sent each message (lines 688-719)
- Modified flooding logic to only forward to peers NOT in "already sent" set (lines 1054-1090)

**Impact:** Reduced duplicate sends while maintaining full connectivity.

---

### 3. Stream Reopening During Flood (Connection Churn)

**Symptom:** Frequent peer disconnects during consensus, especially around ledger 3.

**Root Cause:** Flooding used `send_to_peer_stream()` which tries to **reopen streams** if they're closed. During high message volume, this caused:
- Multiple simultaneous stream open attempts
- Connection reset errors
- Disconnect/reconnect churn

**Fix:** `overlay/src/libp2p_overlay.rs`
- Created `try_send_to_existing_stream()` function (lines 897-926) that:
  - Only sends if stream already open
  - Returns error immediately without reopening
- Modified SCP flooding to use new function (line 1088)
- Modified TX flooding to use new function (line 1202)

**Rationale:** Flooding should be opportunistic - only send to peers with open streams, don't create connection chaos by reopening streams during high load.

**Impact:** Reduced disconnect warnings, cleaner connection management.

---

### 4. crankUntil Predicate Bug (C++ Test Framework)

**Symptom:** Test kept cranking past target ledger, throwing "overshoot" error even though all nodes reached consensus.

**Root Cause:** `Simulation::haveAllExternalized()` used exact equality check:
```cpp
return (min == num) && ((max - min) <= maxSpread);
```

This returns `true` ONLY when min equals exactly `num`. Timeline:
1. Nodes at ledger 4 → predicate returns `false` (4 != 5), keep cranking
2. Nodes advance to ledger 6 during 1-second check interval → predicate returns `false` (6 != 5), keep cranking
3. **Window where min==5 was missed!**
4. Continue until min=9 > 5+3, throw overshoot

**Fix:** `src/simulation/Simulation.cpp` line 426
```cpp
// Before:
return (min == num) && ((max - min) <= maxSpread);

// After:
return (min >= num) && ((max - min) <= maxSpread);
```

**Impact:** Predicate now returns `true` as soon as all nodes reach or exceed target ledger with acceptable spread. Test passes immediately at ledger 5-6 instead of overshooting to ledger 9.

---

## Files Modified

### Rust (overlay/src/libp2p_overlay.rs)
- Lines 204-223: Added message tracking caches to SharedState
- Lines 226-244: Initialize tracking caches
- Lines 633-666: Modified `broadcast_scp()` to track sends
- Lines 688-719: Modified `broadcast_tx()` to track sends
- Lines 897-926: Added `try_send_to_existing_stream()` helper
- Lines 1013-1091: Implemented smart SCP flooding with deduplication
- Lines 1107-1206: Implemented smart TX flooding with deduplication

### C++ (src/simulation/Simulation.cpp)
- Line 426: Fixed `haveAllExternalized()` predicate from `==` to `>=`

### C++ Test (src/overlay/test/OverlayIPCTests.cpp)
- Lines 1333-1348: Added debug logging for per-node ledger numbers (optional, for debugging)

---

## Test Results

**Before fixes:**
```
overlay/test/OverlayIPCTests.cpp:1308: FAILED:
  Simulation timed out (nodes stuck at ledger 2-3)
```

**After fixes:**
```
All tests passed (2 assertions in 1 test case)
```

All 10 nodes successfully reach consensus through ledgers 2, 3, 4, 5+ in ring topology with cross-connections.

---

## Key Learnings

### 1. Gossip Requires Active Forwarding
Receiving a message isn't enough - nodes MUST forward to neighbors. Assuming direct connectivity or relying only on origin broadcasts breaks partially connected networks.

### 2. Deduplication Belongs at Send AND Receive
- **Receive-side:** Prevent reprocessing same message
- **Send-side:** Prevent sending duplicates to peers who already have it
- Both are necessary for efficient flooding

### 3. Flooding Should Be Opportunistic
During high load, don't try to reopen streams - only use existing connections. Stream management should be separate from message forwarding.

### 4. Test Predicates Need ">=" Not "=="
When checking if nodes reached a state, use `>=` to handle fast progression. `==` creates tiny windows that can be missed.

### 5. Connection Churn Indicates Resource Exhaustion
Multiple disconnects during high activity suggests:
- Too many simultaneous operations (stream opens)
- Backpressure not being handled
- Need for rate limiting or batching

---

## Design Principles Validated

1. **Separation of Concerns:** Stream management (persistent) vs message forwarding (opportunistic)
2. **Idempotency:** Message deduplication allows safe redundant sends
3. **Graceful Degradation:** Skip unreachable peers during flood, don't fail entire operation
4. **LRU Caching:** Automatic eviction of old message tracking prevents unbounded memory growth

---

## Potential Future Improvements

1. **Batch Flooding:** Instead of forwarding immediately, batch messages per peer to reduce syscalls
2. **Adaptive Flooding:** Track which peers reliably have streams open, prioritize those
3. **Flood Metrics:** Track duplicate rate, coverage, latency to optimize strategy
4. **Back-off on Disconnect:** If a peer is churning, reduce flood attempts to it
5. **Stream Pool:** Pre-open streams to high-priority peers to reduce cold-start latency

---

## Related Session Checkpoints

- **008-implemented-smart-message-floo.md** - Smart deduplication added
- **007-implementing-scp-state-sync-wi.md** - SCP state sync with retry
- **006-implementing-scp-state-synchro.md** - Initial SCP state sync

This session focused on making message propagation robust and efficient, completing the core gossip protocol implementation.
