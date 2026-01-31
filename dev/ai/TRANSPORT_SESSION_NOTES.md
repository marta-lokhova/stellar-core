# Transport Redesign Session Notes

**Date**: 2026-01-14
**Status**: Phases 1 & 2 complete, Phase 3 pending

This document captures implementation details, codebase knowledge, and context NOT in the main plan (TRANSPORT_REDESIGN.md).

---

## Changes Made So Far

### Phase 1: Remove SCP from FlowControl

**File**: `src/overlay/OverlayManagerImpl.cpp:1118-1130`

```cpp
// BEFORE:
bool isFlood = msg.type() == SCP_MESSAGE || msg.type() == TRANSACTION ||
               msg.type() == FLOOD_DEMAND || msg.type() == FLOOD_ADVERT;

// AFTER:
// NOTE: SCP_MESSAGE intentionally NOT included here.
// SCP messages bypass FlowControl to ensure consensus traffic
// is never stalled waiting for SEND_MORE from peers.
// See dev/ai/TRANSPORT_REDESIGN.md for design rationale.
bool isFlood = msg.type() == TRANSACTION || msg.type() == FLOOD_DEMAND ||
               msg.type() == FLOOD_ADVERT;
```

**Impact**: SCP messages now take the same path as TX_SET - direct to `sendAuthenticatedMessage()`, bypassing FlowControl queues entirely.

### Phase 2: HIGH_PRIORITY Scheduler Queue

**File 1**: `src/util/Scheduler.h`

Added to ActionType enum (line 112-120):
```cpp
enum class ActionType
{
    // HIGH_PRIORITY actions are always executed before any other actions.
    // Used for consensus-critical messages (SCP) that must never be delayed.
    // See dev/ai/TRANSPORT_REDESIGN.md for design rationale.
    HIGH_PRIORITY_ACTION,
    NORMAL_ACTION,
    DROPPABLE_ACTION
};
```

Added member variable (line 136-138):
```cpp
// HIGH_PRIORITY actions are stored separately and always run first.
// Simple FIFO queue - no LAS algorithm needed for critical messages.
std::deque<Action> mHighPriorityActions;
```

**File 2**: `src/util/Scheduler.cpp`

Modified `enqueue()` (line 226-242) - routes HIGH_PRIORITY to separate queue:
```cpp
if (type == ActionType::HIGH_PRIORITY_ACTION)
{
    mStats.mActionsEnqueued++;
    mHighPriorityActions.emplace_back(std::move(action));
    mSize += 1;
    return;
}
```

Modified `runOne()` (line 268-287) - checks high-priority first:
```cpp
// HIGH_PRIORITY actions ALWAYS run first, before any normal queues.
if (!mHighPriorityActions.empty())
{
    ZoneScoped;
    ZoneText("HIGH_PRIORITY", 13);
    mSize -= 1;
    mStats.mActionsDequeued++;
    auto action = std::move(mHighPriorityActions.front());
    mHighPriorityActions.pop_front();
    mCurrentActionType = ActionType::HIGH_PRIORITY_ACTION;
    action();
    mCurrentActionType = ActionType::NORMAL_ACTION;
    return 1;
}
```

Modified `shutdown()` (line 198-212) - clears high-priority queue:
```cpp
mHighPriorityActions.clear();
```

**File 3**: `src/overlay/Peer.cpp:1084-1097`

Changed queue selection for consensus messages:
```cpp
// consensus, self - critical messages use HIGH_PRIORITY
case SCP_MESSAGE:
case TX_SET:
case GENERALIZED_TX_SET:
case SCP_QUORUMSET:
    cat = "SCP";
    // HIGH_PRIORITY ensures these are processed before any other messages.
    // See dev/ai/TRANSPORT_REDESIGN.md for design rationale.
    type = Scheduler::ActionType::HIGH_PRIORITY_ACTION;
    break;

case DONT_HAVE:
    cat = "SCP";
    break;
```

---

## Deep Codebase Knowledge

### Message Flow: Outbound SCP (BEFORE changes)

```
HerderSCPDriver::emitEnvelope() [src/herder/HerderSCPDriver.cpp:168]
    ↓
HerderImpl::broadcast() [src/herder/HerderImpl.cpp:545]
    - Creates StellarMessage with type SCP_MESSAGE
    ↓
OverlayManager::broadcastMessage() [src/overlay/OverlayManagerImpl.cpp:1277]
    ↓
Floodgate::broadcast() [src/overlay/Floodgate.cpp:85-167]
    - Tracks which peers have been told (mFloodMap)
    - For SCP_MESSAGE: calls peer->sendMessage() SYNCHRONOUSLY (line 140)
    - For other types: posts async via mApp.postOnMainThread()
    ↓
Peer::sendMessage() [src/overlay/Peer.cpp:821-910]
    - If isFloodMessage(): goes to FlowControl (BLOCKED HERE)
    - Else: goes directly to sendAuthenticatedMessage()
    ↓
FlowControl::addMsgAndMaybeTrimQueue() [src/overlay/FlowControl.cpp:412]
    - Adds to mOutboundQueues[priority]
    - Priority 0 = SCP_MESSAGE
    ↓
FlowControl::getNextBatchToSend() [src/overlay/FlowControl.cpp:163-215]
    - BLOCKS if !hasOutboundCapacity() - waits for SEND_MORE from peer!
    ↓
Peer::sendAuthenticatedMessage() [src/overlay/Peer.cpp:913-953]
    - XDR serialization
    - Posts to background thread if enabled
    ↓
TCPPeer::sendMessage() [src/overlay/TCPPeer.cpp:227-243]
    - Adds to mWriteQueue (FIFO)
    ↓
TCPPeer::messageSender() [src/overlay/TCPPeer.cpp:296-394]
    - Batches up to MAX_BATCH_WRITE_COUNT (1024) or MAX_BATCH_WRITE_BYTES (1MB)
    - Issues asio::async_write()
```

### Message Flow: Outbound SCP (AFTER Phase 1)

```
HerderSCPDriver::emitEnvelope()
    ↓
HerderImpl::broadcast()
    ↓
Floodgate::broadcast()  [UNCHANGED - still handles SCP specially at line 138]
    ↓
Peer::sendMessage()
    - isFloodMessage() now returns FALSE for SCP_MESSAGE
    - Goes DIRECTLY to sendAuthenticatedMessage()  ← BYPASS FlowControl!
    ↓
TCPPeer::sendMessage() → mWriteQueue → async_write
```

### Message Flow: Inbound SCP

```
TCPPeer::startRead() [src/overlay/TCPPeer.cpp:544-778]
    - Async read from socket
    - Hybrid sync/async approach for efficiency
    ↓
TCPPeer::recvMessage() [src/overlay/TCPPeer.cpp:781-815]
    - XDR deserialization
    ↓
Peer::recvAuthenticatedMessage() [src/overlay/Peer.cpp:1010-1131]
    - HMAC verification
    - Creates CapacityTrackedMessage
    - Selects queue name and ActionType based on message type
    - Posts to main thread via postOnMainThread()
    ↓
VirtualClock::postAction() [src/util/Timer.cpp:431-467]
    - Adds to mPendingActionQueue (thread-safe)
    ↓
VirtualClock::crank() [src/util/Timer.cpp:345-428]
    - Transfers pending queue to Scheduler
    ↓
Scheduler::runOne() [src/util/Scheduler.cpp:259-327]
    - AFTER Phase 2: checks mHighPriorityActions FIRST
    - Then falls back to LAS algorithm
    ↓
Peer::recvMessage() [src/overlay/Peer.cpp:1135-1177]
    - Dispatches to specific handler
```

### Key Data Structures

**FlowControl Outbound Queues** (`src/overlay/FlowControl.h:85-91`):
```cpp
// Priority queues (array of deques):
// Index 0: SCP_MESSAGE (highest)
// Index 1: TRANSACTION
// Index 2: FLOOD_DEMAND
// Index 3: FLOOD_ADVERT (lowest)
FloodQueues<QueuedOutboundMessage> mOutboundQueues;
```

**TCPPeer Write Queue** (`src/overlay/TCPPeer.h:33`):
```cpp
std::deque<TimestampedMessage> mWriteQueue;
```

**Scheduler Queue Map** (`src/util/Scheduler.h:137`):
```cpp
std::map<std::pair<std::string, ActionType>, Qptr> mAllActionQueues;
```

### Threading Model

```
Main Thread:
  - VirtualClock with Scheduler
  - Application logic
  - Message processing (recvMessage handlers)

Overlay Thread (optional, BACKGROUND_OVERLAY_PROCESSING):
  - All peer socket I/O
  - XDR serialization
  - Single io_context shared by all peers

Worker Threads (WORKER_THREADS):
  - Background work via mWorkerIOContext

Eviction Thread:
  - Background eviction scanner
```

**Key io_contexts** (`src/main/ApplicationImpl.cpp:85-104`):
- `mIOContext` - Main thread (in VirtualClock)
- `mWorkerIOContext` - Worker thread pool
- `mOverlayIOContext` - Overlay thread (if BACKGROUND_OVERLAY_PROCESSING)
- `mEvictionIOContext` - Eviction thread

### isFloodMessage() Usage Analysis

All 17 usages analyzed - safe to remove SCP_MESSAGE:

| File:Line | Purpose | Impact of Change |
|-----------|---------|------------------|
| `Peer.cpp:892` | Route to FlowControl or direct | **DESIRED**: SCP bypasses FlowControl |
| `Peer.cpp:200,1314,1436` | `#ifdef BUILD_TESTS` only | No production impact |
| `FlowControl.cpp:128,238,359,433` | Assertions | SCP won't enter these paths |
| `FlowControlCapacity.cpp:134,153,184` | Capacity tracking | SCP won't consume flood capacity |
| `TCPPeer.cpp:371` | Write handler cleanup | Safe - no capacity to release |
| `OverlayManagerImpl.cpp:1368` | Metrics (recordMessageMetric) | Minor: SCP not in flood metrics |
| `LoopbackPeer.cpp:352` | Test code | Test-only |
| `OverlayManagerTests.cpp:57` | Test code | Test-only |

**Critical finding**: `Floodgate::broadcast()` (line 138) checks `msg->type() == SCP_MESSAGE` directly - NOT affected by isFloodMessage() change!

### Scheduler Algorithm

**LAS (Least Attained Service)** algorithm:
- Each queue tracks `mTotalService` (accumulated runtime in nanoseconds)
- `runOne()` picks queue with lowest `mTotalService`
- Prevents starvation while ensuring fairness
- Our HIGH_PRIORITY queue bypasses this entirely

**Floor mechanism** (prevents burst monopolization):
```cpp
auto minTotalService = mMaxTotalService - mLatencyWindow;
```
New queues get "credit" up to `mLatencyWindow` (default 5 seconds).

### Configuration Constants

**FlowControl** (`src/main/Config.h`):
- `MAX_BATCH_WRITE_COUNT` = 1024 messages
- `MAX_BATCH_WRITE_BYTES` = 1MB
- `OUTBOUND_TX_QUEUE_BYTE_LIMIT` = 3MB
- `FLOW_CONTROL_SEND_MORE_BATCH_SIZE` = messages before SEND_MORE

**TCP** (`src/overlay/TCPPeer.h`):
- `BUFSZ` = 0x40000 (256KB buffer)

**Scheduler** (`src/util/Timer.cpp:22`):
- `mLatencyWindow` = 5 seconds

---

## Important File Locations

### Overlay/Transport
- `src/overlay/Peer.h/cpp` - Base peer class, message routing
- `src/overlay/TCPPeer.h/cpp` - TCP socket implementation
- `src/overlay/FlowControl.h/cpp` - Priority queues, SEND_MORE flow control
- `src/overlay/FlowControlCapacity.h/cpp` - Capacity tracking
- `src/overlay/Floodgate.h/cpp` - Broadcast logic
- `src/overlay/PeerDoor.h/cpp` - Connection acceptor
- `src/overlay/OverlayManagerImpl.h/cpp` - Overlay coordination

### Scheduling/Threading
- `src/util/Scheduler.h/cpp` - LAS scheduler implementation
- `src/util/Timer.h/cpp` - VirtualClock, crank loop
- `src/main/ApplicationImpl.cpp` - Thread creation, io_contexts

### Herder/Consensus
- `src/herder/HerderImpl.cpp` - broadcast() entry point
- `src/herder/HerderSCPDriver.cpp` - emitEnvelope()

---

## Build Commands

```bash
# Build stellar-core
mkcore

# This is an alias that runs make with the right flags
```

---

## Phase 3 Considerations (For Tomorrow)

### Dual Socket Architecture

Need to implement:
1. Second TCP socket per peer (PEER_PORT+1 for bulk traffic)
2. Socket association handshake (session ID)
3. Separate io_context + thread for critical sockets
4. Buffer monitoring for backpressure (ioctl SIOCOUTQ)
5. Drop peer if critical socket backs up

### Key Files to Modify for Phase 3
- `src/overlay/TCPPeer.h/cpp` - Add mCriticalSocket, mBulkSocket
- `src/overlay/PeerDoor.cpp` - Listen on second port
- `src/main/ApplicationImpl.cpp` - Add critical io_context + thread
- `src/main/Config.h` - Add PEER_PORT_BULK config
- `src/xdr/Stellar-overlay.x` - Add CONNECT_SECONDARY message (if needed)

### Backpressure Implementation

```cpp
// Linux-specific socket buffer check
int pending;
ioctl(socket_fd, SIOCOUTQ, &pending);
if (pending > 64 * 1024) {
    dropPeer("critical socket backed up");
}
```

---

## Test Considerations

After changes, these areas need testing:
1. SCP messages still broadcast correctly (Floodgate unchanged)
2. HIGH_PRIORITY actions run before normal actions
3. FlowControl still works for TX/FLOOD_* messages
4. No regression in consensus timing

Existing tests to check:
- `src/overlay/test/OverlayManagerTests.cpp`
- `src/overlay/test/FlowControlTests.cpp`
- `src/herder/test/` - Consensus tests
