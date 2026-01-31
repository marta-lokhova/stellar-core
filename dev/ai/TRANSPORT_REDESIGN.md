# Transport Layer Redesign Plan for stellar-core

## Problem Statement

SCP messages and TX_SETs must NEVER be stalled in ANY queue. Currently they pass through multiple queuing points that can add unbounded latency.

---

## Complete Queue Analysis (Deep Dive)

### ALL Queuing Points for SCP_MESSAGE

| # | Queue | Location | Can Block SCP? | Worst Case Latency |
|---|-------|----------|---------------|-------------------|
| 1 | **FlowControl mOutboundQueues[0]** | FlowControl.cpp:412 | **YES** | **UNBOUNDED** (waits for SEND_MORE) |
| 2 | **Scheduler pending queue** | Timer.cpp:440 | YES | Seconds under load |
| 3 | **Scheduler named queues** | Scheduler.cpp:259 | YES | Fair queuing competition |
| 4 | **TCPPeer mWriteQueue** | TCPPeer.cpp:236 | YES | FIFO, up to 1MB batched |
| 5 | **TCP kernel buffer** | OS | YES | 256KB / bandwidth |

### Critical Finding: FlowControl is the BIGGEST Problem

```
FlowControl::getNextBatchToSend() [FlowControl.cpp:178]
    if (!hasOutboundCapacity(msg, guard)) {
        // MESSAGE BLOCKED HERE - WAITS FOR SEND_MORE FROM PEER
        mNoOutboundCapacity = std::make_optional<...>(now);
        break;  // <-- SCP message stuck indefinitely!
    }
```

**Even at priority 0, SCP waits for peer's SEND_MORE capacity. Remote peer controls when we can send!**

### Key Discovery: TX_SET Already Bypasses FlowControl!

```cpp
// Peer.cpp:892-908
if (OverlayManager::isFloodMessage(*msg)) {
    mFlowControl->addMsgAndMaybeTrimQueue(msg);  // SCP goes here (blocked)
} else {
    sendAuthenticatedMessage(msg);  // TX_SET goes here (direct!)
}
```

`isFloodMessage()` returns true for: SCP_MESSAGE, TRANSACTION, FLOOD_DEMAND, FLOOD_ADVERT
TX_SET is NOT a flood message → bypasses FlowControl entirely!

---

## Clarified Requirements

- **Critical messages**: SCP_MESSAGE, TX_SET, GET_TX_SET, SCP_QUORUMSET, GET_SCP_QUORUMSET
- **Goal**: These messages should NEVER be stalled in ANY queue
- **Latency target**: <1ms for critical messages
- **Platform**: Linux-only acceptable
- **Backward compat**: Not required - can break protocol
- **FlowControl**: Can be dropped entirely for critical messages
- **Feature parity**: NOT required

---

## Radical New Architecture: Zero-Queue Critical Path

### Design Principle

**Critical messages bypass ALL intermediate queues and go directly to socket.**

```
┌─────────────────────────────────────────────────────────────────────┐
│                        NEW MESSAGE FLOW                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  CRITICAL PATH (SCP, TX_SET, GET_TX_SET):                           │
│  ┌──────────┐                              ┌──────────────────────┐ │
│  │ Herder/  │ ──── DIRECT ──────────────── │ Critical Socket      │ │
│  │ App      │      (no queues)             │ (dedicated, 8KB buf) │ │
│  └──────────┘                              └──────────────────────┘ │
│                                                                      │
│  BULK PATH (TX, FLOOD_ADVERT, FLOOD_DEMAND):                        │
│  ┌──────────┐    ┌────────────┐    ┌─────────┐    ┌──────────────┐ │
│  │ App      │ ── │ FlowControl│ ── │WriteQueue│ ── │ Bulk Socket  │ │
│  └──────────┘    │ (optional) │    │(batched) │    │ (64KB buf)   │ │
│                  └────────────┘    └─────────┘    └──────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### How Each Blocking Point is Eliminated

| Queue | Current | New Design |
|-------|---------|------------|
| FlowControl | SCP waits for SEND_MORE | **BYPASSED** - critical messages skip FlowControl |
| Scheduler | Posts to main thread | **BYPASSED** - direct call, no scheduling |
| mWriteQueue | FIFO batching | **BYPASSED** - synchronous write to dedicated socket |
| TCP buffer | 256KB shared | **ISOLATED** - dedicated 8KB buffer for critical |

---

## Implementation: Phased Approach

### Phase 1: Remove SCP from FlowControl (IMMEDIATE WIN - ~5 lines)

**The simplest, highest-impact change:**

```cpp
// OverlayManagerImpl.cpp - BEFORE
bool OverlayManager::isFloodMessage(StellarMessage const& msg) {
    return msg.type() == SCP_MESSAGE ||      // <-- REMOVE THIS
           msg.type() == TRANSACTION ||
           msg.type() == FLOOD_DEMAND ||
           msg.type() == FLOOD_ADVERT;
}

// OverlayManagerImpl.cpp - AFTER
bool OverlayManager::isFloodMessage(StellarMessage const& msg) {
    return msg.type() == TRANSACTION ||
           msg.type() == FLOOD_DEMAND ||
           msg.type() == FLOOD_ADVERT;
    // SCP_MESSAGE removed - now bypasses FlowControl like TX_SET!
}
```

#### Full Impact Analysis of `isFloodMessage()` Change

**17 usages analyzed:**

| Location | Impact | Safe? |
|----------|--------|-------|
| `Peer.cpp:892` | **DESIRED**: SCP bypasses FlowControl queues | ✅ |
| `Peer.cpp:200,1314,1436` | Only in `#ifdef BUILD_TESTS` | ✅ |
| `FlowControl.cpp:128,238,359,433` | Assertions - SCP won't enter these paths | ✅ |
| `FlowControlCapacity.cpp:134` | SCP won't consume outbound flood capacity | ✅ Good |
| `FlowControlCapacity.cpp:153` | SCP won't consume inbound flood capacity | ✅ Good |
| `FlowControlCapacity.cpp:184` | SCP won't release flood capacity | ✅ |
| `TCPPeer.cpp:371` | SCP won't go through FlowControl cleanup | ✅ |
| `OverlayManagerImpl.cpp:1368` | SCP won't be in flood metrics | ⚠️ Metrics only |
| `LoopbackPeer.cpp:352` | Test code | ✅ |
| `OverlayManagerTests.cpp:57` | Test code | ✅ |

**Key findings:**
1. **Floodgate::broadcast() is UNAFFECTED** - checks `msg->type() == SCP_MESSAGE` directly (line 138)
2. **Inbound SCP still works** - uses `mTotalCapacity` (all messages) not `mFloodCapacity`
3. **Only cosmetic change**: SCP duplicate detection metrics lost (minor)

**Backpressure**: Use socket buffer monitoring instead of SEND_MORE:
```cpp
bool TCPPeer::canSendCritical() {
    int pending;
    ioctl(socket_fd, SIOCOUTQ, &pending);  // Linux: get pending bytes
    return pending < CRITICAL_THRESHOLD;    // e.g., 32KB
}
```

### Phase 2: Dedicated Critical Socket (Full Isolation)

**Add second TCP connection per peer for critical messages:**

```cpp
class TCPPeer : public Peer {
    // Existing socket becomes bulk-only
    SocketPtr mBulkSocket;      // TRANSACTION, FLOOD_*

    // NEW: Dedicated critical socket
    SocketPtr mCriticalSocket;  // SCP, TX_SET, GET_TX_SET

    void sendCriticalMessage(std::shared_ptr<StellarMessage const> msg) {
        // Direct serialize + write - NO queues
        auto xdr = xdr::xdr_to_msg(authenticateMessage(*msg));
        asio::write(*mCriticalSocket, asio::buffer(xdr->raw_data(), xdr->raw_size()));
    }
};
```

**Socket configuration:**
- Critical: SO_SNDBUF=8KB, TCP_NODELAY=ON, dedicated thread
- Bulk: SO_SNDBUF=64KB, TCP_NODELAY=OFF (batched), thread pool

### Phase 3: Thread Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      THREADING MODEL                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────────┐                                     │
│  │  Critical Thread   │  ← Dedicated, low-latency           │
│  │  (1 thread)        │  ← All critical sockets             │
│  │  io_context_crit   │  ← Never blocks on bulk work        │
│  └────────────────────┘                                     │
│                                                              │
│  ┌────────────────────┐                                     │
│  │  Bulk Thread Pool  │  ← 4 threads, shared                │
│  │  (4 threads)       │  ← All bulk sockets                 │
│  │  io_context_bulk   │  ← FlowControl, batching            │
│  └────────────────────┘                                     │
│                                                              │
│  ┌────────────────────┐                                     │
│  │  Main Thread       │  ← Application logic                │
│  │  VirtualClock      │  ← Scheduler, timers                │
│  └────────────────────┘                                     │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## New Backpressure Mechanism (Replacing SEND_MORE for Critical)

**Current**: Peer sends SEND_MORE to grant capacity → sender waits if no capacity
**Problem**: Unbounded latency waiting for SEND_MORE

**New for Critical Path**: Socket buffer monitoring + DROP CONNECTION if peer too slow

```cpp
class CriticalBackpressure {
    static constexpr size_t BUFFER_WARN = 16 * 1024;   // 16KB
    static constexpr size_t BUFFER_DROP = 64 * 1024;   // 64KB - DROP PEER

    void checkBeforeSend(int socket_fd) {
        int pending;
        ioctl(socket_fd, SIOCOUTQ, &pending);

        if (pending > BUFFER_DROP) {
            // Peer too slow - DROP CONNECTION
            // Fast nodes shouldn't wait for slow ones
            dropPeer("critical socket backed up - peer too slow");
            throw PeerDropped{};
        }
        if (pending > BUFFER_WARN) {
            CLOG_WARNING(Overlay, "Critical socket backlog: {} bytes", pending);
        }
        // Always send - never wait
    }
};
```

**Philosophy**: If a peer can't keep up with consensus traffic, drop them. Fast nodes shouldn't be held back by slow ones.

**For Bulk Path**: Keep existing SEND_MORE flow control (transactions can wait).

---

## Inbound Path: HIGH_PRIORITY Scheduler Queue

**Current inbound flow:**
```
Network → recvAuthenticatedMessage() → postOnMainThread("SCP", NORMAL_ACTION) → Scheduler
```

**Problem**: SCP competes with TX, FLOOD_* in fair scheduler. Under load, SCP waits.

**New**: Add HIGH_PRIORITY queue that ALWAYS executes first.

```cpp
// Scheduler.h - Add new queue type
enum class QueuePriority {
    HIGH_PRIORITY,   // NEW: SCP messages - always runs first
    NORMAL,          // Existing queues
};

// Scheduler.cpp - Modify runOne()
ActionQueue* Scheduler::selectNextQueue() {
    // ALWAYS check HIGH_PRIORITY first
    if (!mHighPriorityQueue.empty()) {
        return &mHighPriorityQueue;
    }
    // Then fall back to existing LAS algorithm for normal queues
    return selectByLeastAttainedService();
}
```

**Usage in Peer.cpp:**
```cpp
void Peer::recvAuthenticatedMessage(...) {
    if (msg.type() == SCP_MESSAGE || msg.type() == TX_SET || ...) {
        // HIGH_PRIORITY queue - always executes first
        mAppConnector.postOnMainThread(callback, "SCP_HP", HIGH_PRIORITY);
    } else {
        // Normal path
        mAppConnector.postOnMainThread(callback, queueName, actionType);
    }
}
```

**Impact**: Inbound SCP messages are processed before any bulk traffic, even under heavy load.

---

## Protocol Changes

### Option A: Same Port, Message-Based Routing (Simpler)

No protocol change needed. Just change `isFloodMessage()` and message routing in code.

### Option B: Dual Port (Full Isolation)

```
PEER_PORT (11625):     Critical socket (SCP, TX_SET, control)
PEER_PORT+1 (11626):   Bulk socket (TX, FLOOD_*)
```

**Handshake:**
1. Connect to PEER_PORT, complete HELLO/AUTH
2. If peer advertises `DUAL_SOCKET` capability in HELLO
3. Connect to PEER_PORT+1, send `ASSOCIATE_SESSION{session_id}`
4. Peer links sockets, derives HMAC key for bulk socket

---

## Message Classification

| Message Type | Path | FlowControl | Socket | Backpressure |
|-------------|------|-------------|--------|--------------|
| SCP_MESSAGE | Critical | NO | Critical | Buffer monitor |
| TX_SET | Critical | NO | Critical | Buffer monitor |
| GENERALIZED_TX_SET | Critical | NO | Critical | Buffer monitor |
| GET_TX_SET | Critical | NO | Critical | Buffer monitor |
| SCP_QUORUMSET | Critical | NO | Critical | Buffer monitor |
| GET_SCP_QUORUMSET | Critical | NO | Critical | Buffer monitor |
| HELLO/AUTH | Critical | NO | Critical | Buffer monitor |
| ERROR_MSG | Critical | NO | Critical | Buffer monitor |
| SEND_MORE | Critical | NO | Critical | Buffer monitor |
| TRANSACTION | Bulk | YES | Bulk | SEND_MORE |
| FLOOD_ADVERT | Bulk | YES | Bulk | SEND_MORE |
| FLOOD_DEMAND | Bulk | YES | Bulk | SEND_MORE |
| GET_PEERS/PEERS | Bulk | NO | Bulk | None |

---

## Key Files to Modify

### Phase 1: Remove SCP from FlowControl (~5 lines)
| File | Changes |
|------|---------|
| `src/overlay/OverlayManagerImpl.cpp:1119` | Remove SCP_MESSAGE from `isFloodMessage()` |

### Phase 2: HIGH_PRIORITY Scheduler Queue
| File | Changes |
|------|---------|
| `src/util/Scheduler.h` | Add `HIGH_PRIORITY` queue type, `mHighPriorityQueue` |
| `src/util/Scheduler.cpp:259` | Modify `runOne()` to check HIGH_PRIORITY first |
| `src/overlay/Peer.cpp:1050-1127` | Route SCP to HIGH_PRIORITY queue on receive |
| `src/util/Timer.h/cpp` | Add HIGH_PRIORITY parameter to `postAction()` |

### Phase 3: Dedicated Critical Socket
| File | Changes |
|------|---------|
| `src/overlay/TCPPeer.h/cpp` | Add `mCriticalSocket`, `sendCriticalMessage()`, buffer monitoring |
| `src/overlay/Peer.cpp:821-910` | Route critical messages to dedicated socket |
| `src/overlay/PeerDoor.cpp` | Listen on PEER_PORT+1 for bulk connections |
| `src/main/ApplicationImpl.cpp` | Add critical io_context + dedicated thread |
| `src/main/Config.h` | Add `PEER_PORT_BULK`, `ENABLE_DUAL_SOCKET` |
| `src/overlay/FlowControl.cpp` | Remove SCP from priority queues (cleanup)

---

## Verification Plan

1. **Unit Test**: Send SCP while bulk queue is full → SCP arrives immediately
2. **Integration Test**: Measure SCP latency under heavy TX load
3. **Stress Test**: 100 peers, flood bulk channel, measure critical latency
4. **Metric**: Add `overlay.critical_send_latency_us` histogram
5. **Backward Compat**: Test with single-socket peer (fall back to shared)

---

## Latency Analysis

**Current (worst case):**
```
SCP message latency =
    FlowControl wait (UNBOUNDED - waits for SEND_MORE) +
    Scheduler queue (seconds under load) +
    mWriteQueue batch (up to 1MB) +
    TCP buffer drain (256KB / bandwidth = 20ms at 100Mbps)
= UNBOUNDED (could be minutes if peer stops sending SEND_MORE)
```

**After Phase 1 (remove SCP from FlowControl):**
```
SCP message latency =
    mWriteQueue batch (up to 1MB) +
    TCP buffer drain (256KB / bandwidth)
= ~25ms worst case
```

**After Phase 2 (dedicated socket):**
```
SCP message latency =
    XDR serialize (~10μs) +
    Critical socket buffer (8KB / bandwidth = 0.6ms at 100Mbps)
= <1ms
```

---

## Design Decisions (Finalized)

1. **Phase 1**: Remove SCP_MESSAGE from `isFloodMessage()` (immediate, ~5 lines)
2. **Phase 2**: Add HIGH_PRIORITY scheduler queue for inbound SCP
3. **Phase 3**: Dedicated critical socket on PEER_PORT+1
4. **Backpressure for critical**: DROP CONNECTION if peer too slow (buffer > 64KB)
5. **Backpressure for bulk**: Keep existing SEND_MORE flow control
6. **Threading**: Dedicated thread for critical I/O
7. **Backward compat**: Fall back to shared socket for old peers

## Summary: Complete Solution

**Outbound SCP path (after all phases):**
```
Herder::broadcast() → Peer::sendCriticalMessage() → mCriticalSocket → wire
                      (no FlowControl)              (no mWriteQueue)
                      (no Scheduler)                (8KB buffer)
```

**Inbound SCP path (after all phases):**
```
wire → mCriticalSocket → recvMessage() → postOnMainThread(HIGH_PRIORITY) → immediate processing
       (dedicated)       (no batching)    (always first in scheduler)
```

**Result**: SCP messages touch ZERO intermediate queues and are always processed first.
