# Rust Overlay Debugging Session - January 26, 2026

## Session Summary

Fixed build errors and debugged Rust overlay integration tests. All 14 overlay-ipc tests now pass.

## Bugs Found & Fixed

| Bug | Severity | Root Cause | Fix | Discovery Method |
|-----|----------|------------|-----|------------------|
| TxSet validation failure | High | Soroban phase using v=0 (sequential) instead of v=1 (parallel) for protocol ≥23 | Changed `txset.rs` discriminant from v=0 to v=1 | Logs showing "invalid txSet" |
| Tests stuck waiting | High | Missing KNOWN_PEERS config - nodes couldn't discover each other | Added peer config to test setup | Logs showing "No known peers" |
| Build error: const modification | Medium | Simulation.cpp calling `clear()`/`push_back()` on const `Config&` | Removed dead code (was never working) | Compiler error |
| Build error: wrong base class | Medium | ConsoleReporterWithSum inheriting from AbstractPollingReporter | Changed to MetricProcessor | Compiler error |
| DB migration crash | Medium | Migration trying to drop non-existent tables (peers/ban) | Added table existence check | Runtime error |

## Key Learnings

### 1. TxSet Format for Protocol ≥ 23

**Critical knowledge for Rust overlay**:

```
GeneralizedTransactionSet {
  v: 1,
  v1TxSet: {
    previousLedgerHash: Hash,
    phases: [
      Phase 0 (Classic): TransactionPhase v=0 (v0Components)
      Phase 1 (Soroban): TransactionPhase v=1 (parallelTxsComponent)  // MUST be v=1!
    ]
  }
}
```

- Protocol 23+ requires Soroban phase to use **parallel format (v=1)**, not sequential (v=0)
- Even empty Soroban phases must use v=1 with empty `executionStages`
- Validation in `TxSetFrame.cpp:checkValidSoroban()` lines 1785-1795
- Constant: `PARALLEL_SOROBAN_PHASE_PROTOCOL_VERSION = V_23`

### 2. Simulation Framework Virtual Clock

**Not a bug, but confusing in logs**:

- Simulation uses VirtualClock in REAL_TIME mode but advances in 1-second discrete ticks
- `crankAllNodes()` sets `nextTick = now + 1 second` (Simulation.cpp:271-272)
- `postOnMainThread` schedules via virtual clock, causing ~1-2s apparent delays
- Logs showing "SCPReceived executed after 1.999s" are normal simulation behavior

### 3. Rust Overlay Port Mapping

The Rust overlay uses different ports than C++ Core expects:
- Stellar peer port (e.g., 11626) → QUIC port = peer_port + 1000 (e.g., 12626)
- `SetPeerConfig` in `main.rs:475-488` does the conversion when dialing
- Tests must configure KNOWN_PEERS with the C++ peer port, Rust converts it

### 4. Test Configuration Pattern

Multi-node overlay tests require explicit KNOWN_PEERS configuration:

```cpp
// Example from "TX included in ledger" test
cfg.PEER_PORT = basePort;
cfg.KNOWN_PEERS.clear();
for (size_t j = 0; j < NUM_NODES; j++) {
    if (i != j) {
        cfg.KNOWN_PEERS.push_back("127.0.0.1:" + std::to_string(basePort + j));
    }
}
```

## What Went Well

1. **Parallel tool calls** - Running multiple views/greps simultaneously sped up investigation
2. **Test-driven debugging** - Running specific failing tests rather than full suite
3. **Log analysis** - Found root causes by tracing through INFO/DEBUG logs
4. **Reading existing code** - Found validation logic in TxSetFrame.cpp that revealed format requirements

## What Could Have Been Better

| Issue | What Happened | Better Approach |
|-------|---------------|-----------------|
| Slow SCP investigation | Spent time analyzing 20s ledger closes | Should have checked simulation clock behavior first |
| Missing KNOWN_PEERS | Didn't notice test config differences initially | Compare working vs failing test configs side-by-side |
| TxSet format | Trial and error on XDR structure | Read `checkValidSoroban()` first to understand requirements |

## Remaining Work

### RequestScpState Implementation (Optional)

Currently causes slow initial consensus - nodes can't catch up on missed SCP messages during connection race.

- IPC types defined: `REQUEST_SCP_STATE (3)`, `PEER_REQUESTS_SCP_STATE (102)`, `SCP_STATE_RESPONSE (6)`
- C++ side ready: `setOnScpStateRequest` callback at `RustOverlayManager.cpp:59-62`
- `getSCPStateForPeer` returns current SCP envelopes (`HerderImpl.cpp:977-1002`)
- Rust side stub at `main.rs:395-398` just logs warning

**Implementation plan**:
1. Forward `REQUEST_SCP_STATE` from Core to peer via SCP stream
2. On receiving peer: send `PEER_REQUESTS_SCP_STATE` to Core
3. Core responds with `SCP_STATE_RESPONSE` containing envelopes
4. Forward response back to requesting peer

## Files Modified

| File | Change |
|------|--------|
| `overlay/src/flood/txset.rs` | Changed Soroban phase from v=0 to v=1 |
| `src/overlay/test/OverlayIPCTests.cpp` | Added KNOWN_PEERS to 2 tests |
| `src/simulation/Simulation.cpp` | Removed dead KNOWN_PEERS code, fixed ConsoleReporterWithSum |
| `src/database/Database.cpp` | Added missing table check in migration |

## Test Coverage Status

All 14 overlay-ipc tests pass:
- Basic IPC: startup, shutdown, messaging
- SCP consensus: 2-node consensus with Rust overlay
- TX handling: submit, flood, include in ledger
- Stress: 4-node SCP under TX load

## Useful Commands

```bash
# Run overlay-ipc tests
./src/stellar-core test -a "[overlay-ipc]"

# Build Rust overlay
cd overlay && cargo build --release

# Run specific test with verbose output
./src/stellar-core test "Rust overlay SCP consensus" 2>&1 | tee test.log
```
