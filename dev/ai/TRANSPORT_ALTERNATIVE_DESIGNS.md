# Alternative Transport Architectures for Eliminating Head-of-Line Blocking

## Executive Summary

This document proposes three radically different transport architectures to achieve <1ms latency for SCP consensus messages. The current architecture has five queuing points that can cause unbounded latency:

1. **FlowControl outbound queues** - Wait for `SEND_MORE` from peer (unbounded)
2. **Scheduler pending queue** - Can be seconds under load
3. **Scheduler named queues** - Fair queuing competition with other work
4. **TCPPeer mWriteQueue** - FIFO, up to 1MB batched
5. **TCP kernel buffer** - 256KB

## Current System Analysis

### Message Characteristics

| Message Type | Typical Size | Frequency | Latency Requirement |
|-------------|--------------|-----------|---------------------|
| SCP_MESSAGE | 200-800 bytes | ~10-50/sec during consensus | <10ms ideal, <100ms tolerable |
| TRANSACTION | 100-100KB | 100s-1000s/sec | Best effort |
| TX_SET | Up to 10MB | 1 per ledger | Before timeout |
| FLOOD_ADVERT | Small | High | Medium |
| FLOOD_DEMAND | Small | High | Medium |

### SCP Timing Constraints

From `NetworkConfig.h`:
- **Nomination timeout initial**: 750-2500ms (configurable)
- **Ballot timeout initial**: 750-2500ms (configurable)
- **Target ledger close**: 5000ms (pre-v23)
- **Consensus stuck timeout**: Defined in Herder

SCP messages must arrive within timeout windows for consensus to proceed. A 100ms message delay can cause ballot counter increments and potentially cascade to consensus failures.

### Network Topology

- **TARGET_PEER_CONNECTIONS**: Typically 8-20 outbound peers
- **Additional inbound**: Can be significant
- **Quorum dependencies**: Messages from specific validator nodes are critical

### Current Queuing Analysis

```
                    CURRENT MESSAGE FLOW

  SCP Message Generated
         |
         v
  +------------------+
  | FlowControl      |  <- Priority queue (SCP=0, TX=1, DEMAND=2, ADVERT=3)
  | Outbound Queue   |  <- BUT: waits for SEND_MORE (unbounded!)
  +------------------+
         |
         v
  +------------------+
  | Scheduler Queue  |  <- LAS algorithm, fair but adds latency
  +------------------+
         |
         v
  +------------------+
  | TCPPeer          |  <- mWriteQueue (FIFO, up to 1MB batch)
  | mWriteQueue      |  <- No priority differentiation
  +------------------+
         |
         v
  +------------------+
  | TCP Kernel       |  <- 256KB buffer (BUFSZ)
  | Buffer           |  <- FIFO, no control
  +------------------+
         |
         v
       Network
```

**Key Problem**: Even though FlowControl has priority queues, the bottleneck is `hasOutboundCapacity()` - SCP messages still wait for `SEND_MORE` from the peer before they can be sent.

---

## Design A: QUIC Multi-Stream Transport

### Overview

Replace TCP with QUIC, using separate streams for different message priorities. QUIC provides:
- Built-in stream multiplexing without head-of-line blocking
- Per-stream flow control (independent of other streams)
- 0-RTT connection establishment for reconnections
- Built-in TLS 1.3

### Architecture

```
                    QUIC MULTI-STREAM ARCHITECTURE

                    +-------------------------+
                    |     Application         |
                    +-------------------------+
                    | Stream Router           |
                    | (message -> stream ID)  |
                    +----+--------+--------+--+
                         |        |        |
              +----------+   +----+----+   +----------+
              |              |         |              |
         +----v----+    +----v----+   +----v----+   +----v----+
         | Stream 0|    | Stream 1|   | Stream 2|   | Stream N|
         |   SCP   |    | TX_SET  |   |   TX    |   |  Bulk   |
         | Priority|    | Urgent  |   | Normal  |   |         |
         +---------+    +---------+   +---------+   +---------+
              |              |             |             |
              +------+-------+------+------+------+------+
                     |              |             |
                +----v--------------v-------------v----+
                |           QUIC Layer                 |
                | - Per-stream congestion control      |
                | - Per-stream flow control            |
                | - 0-RTT reconnection                 |
                +--------------------------------------+
                                |
                          +-----v-----+
                          |    UDP    |
                          +-----------+
```

### How It Eliminates Each Queuing Point

| Current Queue | QUIC Solution |
|---------------|---------------|
| FlowControl outbound | **Eliminated**: Each stream has independent flow control. SCP stream never waits for TX stream's `SEND_MORE` |
| Scheduler pending | **Eliminated**: Direct write to QUIC stream, no scheduler involvement for network IO |
| Scheduler named queues | **Eliminated**: Same as above |
| TCPPeer mWriteQueue | **Replaced**: Per-stream queues, SCP has dedicated stream with smallest queue |
| TCP kernel buffer | **Mitigated**: UDP + QUIC prioritization. Can use `SO_PRIORITY` for SCP packets |

### Stream Assignment

```cpp
enum class QUICStreamType : uint64_t {
    SCP_CRITICAL = 0,        // SCP messages, highest priority
    SCP_QUORUM_SET = 1,      // Quorum set requests/responses
    TX_SET = 2,              // Transaction sets (per ledger)
    TRANSACTION = 3,         // Individual transactions
    FLOOD_CONTROL = 4,       // Adverts and demands
    PEER_MANAGEMENT = 5,     // Hello, Auth, Peers, etc.
    SURVEY = 6,              // Survey messages
    BULK = 7                 // Everything else
};
```

### Implementation Options

1. **quiche (Cloudflare)** - Rust library, can expose C API
2. **msquic (Microsoft)** - C library, cross-platform
3. **ngtcp2** - C library, used by curl
4. **Quinn (Rust)** - If considering Rust rewrite

### Backpressure Mechanism

```
Per-stream backpressure (independent):

Stream 0 (SCP):
  - Window size: 64KB (small, fast ACKs)
  - Max queued: 10 messages
  - On overflow: Drop oldest non-externalize message

Stream 3 (TX):
  - Window size: 1MB
  - Max queued: 1000 messages
  - On overflow: Drop oldest (current behavior)
```

### Threading Model

```
Option 1: Single-threaded event loop
+------------------+
|   Main Thread    |
| +--------------+ |
| | QUIC Engine  | |
| |   (quiche)   | |
| +--------------+ |
| | Poll-based   | |
| | multiplexing | |
| +--------------+ |
+------------------+

Option 2: Dedicated QUIC thread
+------------------+     +------------------+
|   Main Thread    |     |   QUIC Thread    |
| +--------------+ |     | +--------------+ |
| | Application  | |<--->| | QUIC Engine  | |
| | Logic        | | MQ  | +--------------+ |
| +--------------+ |     | |   io_uring   | |
+------------------+     | | (Linux only) | |
                         | +--------------+ |
                         +------------------+
```

### Pros and Cons

**Pros:**
- Stream multiplexing eliminates HOL blocking at protocol level
- Per-stream flow control means SCP never waits for TX capacity
- 0-RTT enables fast reconnection
- Built-in encryption (no separate HMAC layer needed)
- Active ecosystem with production-proven libraries
- UDP allows kernel bypass later if needed

**Cons:**
- Protocol change (breaks compatibility with older nodes)
- QUIC adds ~20ms initial handshake (but 0-RTT mitigates)
- More CPU for crypto (but modern CPUs have AES-NI)
- Less tooling/debugging support than TCP
- NAT traversal can be tricky (but validators usually have public IPs)

### Implementation Complexity

| Component | Effort | Risk |
|-----------|--------|------|
| QUIC library integration | 2 weeks | Medium |
| Stream routing logic | 1 week | Low |
| Connection management | 2 weeks | Medium |
| Testing infrastructure | 2 weeks | Medium |
| Protocol negotiation | 1 week | Low |
| **Total** | **8 weeks** | **Medium** |

---

## Design B: Dual-Channel Architecture (UDP + TCP)

### Overview

Use UDP for SCP messages (small, latency-critical) and TCP for bulk data (large, reliability-critical). This is simpler than QUIC while achieving the main goal.

### Architecture

```
                    DUAL-CHANNEL ARCHITECTURE

                    +-------------------------+
                    |     Application         |
                    +----------+--------------+
                               |
                    +----------v--------------+
                    |    Message Router       |
                    |  (classify by type)     |
                    +----+---------------+----+
                         |               |
              +----------+               +----------+
              |                                     |
         +----v----+                          +-----v----+
         | UDP     |                          | TCP      |
         | Channel |                          | Channel  |
         +---------+                          +----------+
         |         |                          |          |
         | - SCP   |                          | - TX_SET |
         | - PING  |                          | - TX     |
         | - HELLO |                          | - BULK   |
         +---------+                          +----------+
              |                                     |
         +----v----+                          +-----v----+
         | sendto()|                          | write()  |
         | recvfrom|                          | read()   |
         +---------+                          +----------+
              |                                     |
              +------------------+------------------+
                                 |
                            Network
```

### Message Classification

```cpp
bool shouldUseUDP(StellarMessage const& msg) {
    switch (msg.type()) {
        case SCP_MESSAGE:           return true;   // Critical, small
        case GET_SCP_QUORUM_SET:    return true;   // Critical
        case SCP_QUORUM_SET:        return msg.size() < 4096;  // Small sets
        case HELLO:                 return true;   // Connection setup
        case AUTH:                  return true;   // Connection setup
        case SEND_MORE_EXTENDED:    return true;   // Flow control
        case DONT_HAVE:             return true;   // Small
        case ERROR_MSG:             return true;   // Small
        case PEERS:                 return true;   // Small
        default:                    return false;  // TCP for bulk
    }
}
```

### UDP Protocol Design

```
UDP SCP Message Format:
+------------------+------------------+------------------+
|  Magic (4B)      |  Sequence (8B)   |  Msg Type (4B)   |
+------------------+------------------+------------------+
|  Payload Length (4B)                |  HMAC (32B)      |
+------------------+------------------+------------------+
|                  Payload (variable)                    |
+--------------------------------------------------------+

Reliability for SCP (application-level):
- Each SCP message has sequence number
- Receiver sends ACK (piggyback on next message)
- Sender retransmits after timeout (configurable, default 50ms)
- Max retransmits: 3 (then fall back to TCP)
- Duplicate detection via sequence number cache
```

### How It Eliminates Each Queuing Point

| Current Queue | Dual-Channel Solution |
|---------------|----------------------|
| FlowControl outbound | **Bypassed for UDP**: SCP goes directly to socket, no SEND_MORE wait |
| Scheduler pending | **Bypassed for UDP**: Direct sendto() call |
| Scheduler named queues | **Bypassed for UDP**: No scheduler involvement |
| TCPPeer mWriteQueue | **Split**: UDP has no write queue (immediate send); TCP keeps existing behavior |
| TCP kernel buffer | **Split**: UDP uses its own small buffer; TCP unchanged |

### Implementation

```cpp
class DualChannelPeer : public Peer {
private:
    // UDP channel for SCP
    asio::ip::udp::socket mUdpSocket;
    uint64_t mUdpSeqOut{0};
    uint64_t mUdpSeqIn{0};
    std::unordered_set<uint64_t> mPendingAcks;

    // TCP channel (existing)
    std::shared_ptr<TCPPeer::SocketType> mTcpSocket;

public:
    void sendSCPMessage(SCPEnvelope const& env) {
        // Serialize
        auto bytes = xdr::xdr_to_opaque(env);

        // Build UDP frame
        UDPFrame frame;
        frame.seq = ++mUdpSeqOut;
        frame.type = SCP_MESSAGE;
        frame.payload = std::move(bytes);
        frame.hmac = computeHmac(frame);

        // Send immediately - no queue!
        mUdpSocket.send(asio::buffer(xdr::xdr_to_opaque(frame)));

        // Schedule retransmit timer
        scheduleRetransmit(frame.seq, 50ms);
    }

    void onUdpReceive(UDPFrame const& frame) {
        // Verify HMAC
        if (!verifyHmac(frame)) {
            return;  // Drop silently
        }

        // Dedup
        if (frame.seq <= mUdpSeqIn) {
            return;  // Already processed
        }
        mUdpSeqIn = frame.seq;

        // Process immediately on receive thread
        processMessage(frame);
    }
};
```

### Backpressure Mechanism

**UDP Channel:**
- No explicit backpressure (fire and forget)
- Kernel will drop packets if socket buffer full
- Application retransmits on timeout
- Natural rate limiting through retransmit window

**TCP Channel:**
- Existing FlowControl mechanism
- Unchanged from current behavior

### Threading Model

```
+------------------+     +------------------+
|   Main Thread    |     | Network Thread   |
|                  |     |                  |
| +----------+     |     | +------------+   |
| |SCP Logic |-----|---->| |UDP Socket  |   |
| +----------+     | Q   | |  (direct)  |   |
|                  |     | +------------+   |
| +----------+     |     |                  |
| |FlowCtrl  |-----|---->| +------------+   |
| +----------+     | Q   | |TCP Socket  |   |
|                  |     | | (batched)  |   |
+------------------+     | +------------+   |
                         +------------------+

Key insight: SCP messages bypass FlowControl entirely
and go directly to network thread for UDP send.
```

### Pros and Cons

**Pros:**
- Simpler than QUIC (no new protocol stack)
- UDP gives complete control over latency
- Easy to reason about (two separate paths)
- Can implement incrementally
- Fallback to TCP for unreliable networks
- Works with existing firewall rules (if UDP allowed)

**Cons:**
- NAT/firewall may block UDP (need TCP fallback)
- Custom reliability layer adds complexity
- Two connections per peer (more resources)
- Packet loss requires application-level handling
- No built-in encryption (need to maintain HMAC)
- MTU limitations (SCP messages might fragment)

### Implementation Complexity

| Component | Effort | Risk |
|-----------|--------|------|
| UDP socket management | 1 week | Low |
| Reliability layer | 2 weeks | Medium |
| Message routing | 1 week | Low |
| Fallback to TCP | 1 week | Low |
| Testing | 2 weeks | Medium |
| **Total** | **7 weeks** | **Medium** |

---

## Design C: io_uring Kernel Bypass with Priority Queues

### Overview

Use Linux's io_uring for zero-copy, kernel-bypassing network IO with explicit priority queues. This is the most radical approach, achieving sub-millisecond latency through:

1. Zero-copy send/receive
2. Batched system calls
3. Priority-aware submission
4. Poll-mode operation (no interrupts)

### Architecture

```
                    IO_URING ARCHITECTURE (Linux only)

                    +-------------------------+
                    |     Application         |
                    +-------------------------+
                    | Priority Dispatcher     |
                    | (SCP > TX_SET > TX)     |
                    +----+--------+--------+--+
                         |        |        |
              +----------+   +----+----+   +----------+
              | HIGH     |   | MEDIUM  |   | LOW      |
              | Priority |   | Priority|   | Priority |
              | Ring     |   | Ring    |   | Ring     |
              +----|-----+   +----|----+   +----|-----+
                   |              |             |
              +----v--------------v-------------v----+
              |           io_uring                   |
              | +----------------------------------+ |
              | |    Submission Queue (SQ)        | |
              | | [SCP] [SCP] [TX] [TX] [TX]...   | |
              | +----------------------------------+ |
              |                                      |
              | +----------------------------------+ |
              | |    Completion Queue (CQ)        | |
              | | [done] [done] [done]...         | |
              | +----------------------------------+ |
              +--------------------------------------+
                                |
                          +-----v-----+
                          |  Kernel   |
                          |  (TCP)    |
                          +-----------+
                                |
                          +-----v-----+
                          |    NIC    |
                          +-----------+
```

### Priority Ring Design

```cpp
class PriorityIOUring {
private:
    static constexpr int NUM_PRIORITY_LEVELS = 4;

    struct PriorityRing {
        int priority;
        std::deque<io_uring_sqe> pending;
        uint32_t inflight{0};
        uint32_t maxInflight;
    };

    std::array<PriorityRing, NUM_PRIORITY_LEVELS> mRings;
    io_uring mRing;

public:
    void submitSCPMessage(int fd, const void* data, size_t len) {
        // Priority 0 (highest)
        auto& ring = mRings[0];

        // Get SQE directly
        io_uring_sqe* sqe = io_uring_get_sqe(&mRing);
        io_uring_prep_send(sqe, fd, data, len, MSG_DONTWAIT);
        sqe->flags |= IOSQE_IO_LINK;  // Link for ordering
        io_uring_sqe_set_data(sqe, makeContext(PRIORITY_HIGH, fd));

        // Submit immediately for high priority
        io_uring_submit(&mRing);
    }

    void submitBulkMessage(int fd, const void* data, size_t len) {
        // Priority 3 (lowest) - batch these
        auto& ring = mRings[3];

        io_uring_sqe sqe;
        io_uring_prep_send(&sqe, fd, data, len, 0);
        ring.pending.push_back(sqe);

        // Batch submit when queue is large enough
        if (ring.pending.size() >= 32) {
            flushRing(3);
        }
    }

    void poll() {
        // Process completions
        io_uring_cqe* cqe;
        while (io_uring_peek_cqe(&mRing, &cqe) == 0) {
            auto ctx = (Context*)io_uring_cqe_get_data(cqe);
            handleCompletion(ctx, cqe->res);
            io_uring_cqe_seen(&mRing, cqe);
        }

        // Resubmit lower priority work if high priority is empty
        for (int i = 0; i < NUM_PRIORITY_LEVELS; i++) {
            if (mRings[i].pending.empty()) continue;
            if (i > 0 && !mRings[0].pending.empty()) break;  // Higher priority waiting
            flushRing(i);
        }
    }
};
```

### How It Eliminates Each Queuing Point

| Current Queue | io_uring Solution |
|---------------|-------------------|
| FlowControl outbound | **Replaced**: Priority submission rings with preemption |
| Scheduler pending | **Eliminated**: Direct io_uring submission |
| Scheduler named queues | **Eliminated**: Same as above |
| TCPPeer mWriteQueue | **Replaced**: io_uring SQ with priority sorting |
| TCP kernel buffer | **Optimized**: Zero-copy with `IORING_OP_SEND_ZC` |

### Zero-Copy Send

```cpp
void sendZeroCopy(int fd, std::shared_ptr<Buffer> buf) {
    io_uring_sqe* sqe = io_uring_get_sqe(&mRing);

    // Zero-copy send (kernel >= 6.0)
    io_uring_prep_send_zc(sqe, fd, buf->data(), buf->size(), 0, 0);

    // Buffer must stay alive until CQE with IORING_CQE_F_NOTIF
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    io_uring_sqe_set_data(sqe, new ZeroCopyContext(buf));

    io_uring_submit(&mRing);
}
```

### Backpressure Mechanism

```
Priority-aware backpressure:

1. Per-priority inflight limits:
   - Priority 0 (SCP): max 100 inflight
   - Priority 1 (TX_SET): max 50 inflight
   - Priority 2 (TX): max 200 inflight
   - Priority 3 (BULK): max 500 inflight

2. When limit reached:
   - Queue in userspace ring
   - Higher priority can preempt lower priority submissions

3. Completion-driven flow:
   - Each completion enables one new submission
   - Priority ordering maintained

4. TCP backpressure:
   - io_uring respects TCP window
   - EAGAIN surfaced through CQE
   - Automatic requeue with IORING_OP_POLL_ADD
```

### Threading Model

```
Option 1: Dedicated io_uring thread
+------------------+     +------------------+
|   Main Thread    |     | io_uring Thread  |
|                  |     |                  |
| +------------+   |     | +------------+   |
| | SCP Logic  |   |     | | io_uring   |   |
| +------------+   |     | | poll loop  |   |
|      |           |     | +------------+   |
|      v           |     |      ^    |      |
| +------------+   |     |      |    v      |
| | Lock-free  |========>| +------------+   |
| | Queue      |   |     | | SQ batching|   |
| +------------+   |     | +------------+   |
+------------------+     +------------------+

Option 2: Single-threaded with IORING_SETUP_SQPOLL
+------------------+
|   Main Thread    |
|                  |
| +------------+   |     +------------------+
| | Application|   |     | Kernel SQ Poll   |
| +------------+   |     | Thread (hidden)  |
|      |           |     +------------------+
|      v           |            ^
| +------------+   |            |
| | io_uring   |===============>|
| | submit     |   |
| +------------+   |
+------------------+
Note: SQPOLL mode lets kernel poll the SQ,
reducing syscall overhead to near zero.
```

### Implementation

```cpp
class IOUringPeer : public Peer {
private:
    io_uring mRing;
    std::array<PriorityQueue, 4> mPriorityQueues;

    void setup() {
        io_uring_params params = {};
        params.flags = IORING_SETUP_SQPOLL |   // Kernel-side polling
                       IORING_SETUP_SQ_AFF |    // Pin to CPU
                       IORING_SETUP_CQSIZE;     // Custom CQ size
        params.sq_thread_idle = 1000;           // 1ms idle before sleep

        io_uring_queue_init_params(4096, &mRing, &params);

        // Register buffers for zero-copy
        registerBuffers();
    }

    void sendSCP(SCPEnvelope const& env) {
        auto buf = serialize(env);

        io_uring_sqe* sqe = io_uring_get_sqe(&mRing);
        io_uring_prep_send(sqe, mFd, buf.data(), buf.size(), MSG_DONTWAIT);

        // High priority flag (kernel >= 5.11)
        sqe->ioprio = IORING_PRIORITY_HIGH;

        // Submit immediately
        io_uring_submit(&mRing);
    }

    void poll() {
        io_uring_cqe* cqes[32];
        int n = io_uring_peek_batch_cqe(&mRing, cqes, 32);

        for (int i = 0; i < n; i++) {
            handleCompletion(cqes[i]);
        }
        io_uring_cq_advance(&mRing, n);
    }
};
```

### Pros and Cons

**Pros:**
- Lowest possible latency (sub-millisecond achievable)
- Zero-copy eliminates memory bandwidth bottleneck
- Batching reduces syscall overhead
- Kernel polling mode (SQPOLL) approaches DPDK performance
- Still uses standard TCP (firewall-friendly)
- Incremental adoption possible

**Cons:**
- Linux-only (requires kernel >= 5.11, ideally >= 6.0)
- Complex API with many edge cases
- Debugging is harder (async completion model)
- Memory management complexity (buffer lifetime)
- Less mature ecosystem than traditional sockets
- May require kernel configuration (CAP_SYS_NICE for SQPOLL)

### Implementation Complexity

| Component | Effort | Risk |
|-----------|--------|------|
| io_uring setup | 1 week | Medium |
| Priority queue integration | 2 weeks | High |
| Zero-copy buffer management | 2 weeks | High |
| Completion handling | 1 week | Medium |
| Error recovery | 2 weeks | High |
| Testing & debugging | 3 weeks | High |
| **Total** | **11 weeks** | **High** |

---

## Comparison Matrix

| Criterion | Design A (QUIC) | Design B (UDP+TCP) | Design C (io_uring) |
|-----------|-----------------|--------------------|--------------------|
| **Latency Improvement** | 10-50x | 20-100x | 50-200x |
| **Complexity** | Medium | Low-Medium | High |
| **Portability** | Cross-platform | Cross-platform | Linux only |
| **Protocol Compatibility** | Breaking | Breaking | Compatible* |
| **Implementation Time** | 8 weeks | 7 weeks | 11 weeks |
| **Risk** | Medium | Medium | High |
| **Ecosystem Maturity** | Good | Excellent | Growing |
| **Debugging** | Medium | Easy | Hard |

*io_uring is wire-compatible but requires modern Linux kernel

---

## Recommended Approach: Hybrid Design A+C

For production deployment, I recommend a phased approach combining QUIC for protocol benefits with io_uring for performance:

### Phase 1: QUIC Migration (8 weeks)
- Replace TCP with QUIC
- Implement stream multiplexing
- Eliminate FlowControl SEND_MORE blocking for SCP
- **Measurable goal**: <50ms p99 SCP latency

### Phase 2: io_uring Optimization (6 weeks)
- Use io_uring for QUIC UDP socket operations
- Implement priority submission
- Enable zero-copy where possible
- **Measurable goal**: <5ms p99 SCP latency

### Phase 3: Kernel Bypass (Optional, 8+ weeks)
- DPDK or AF_XDP for ultra-low latency
- Dedicated network thread on isolated CPU
- **Measurable goal**: <1ms p99 SCP latency

---

## Appendix A: Message Size Analysis

From codebase analysis:

```cpp
// Maximum message sizes (Peer.h)
static size_t const MAX_MESSAGE_SIZE = 1024 * 1024 * 16;     // 16 MB
static size_t const MAX_TX_SET_ALLOWANCE = 1024 * 1024 * 10; // 10 MB

// SCP message composition:
// - SCPEnvelope ~= SCPStatement + Signature(64 bytes)
// - SCPStatement ~= NodeID(32) + slotIndex(8) + pledges(variable)
// - Pledges contain Value which is opaque_vec<> (typically hash references)

// Typical SCP message sizes:
// - Nominate: 200-500 bytes (depends on candidate count)
// - Prepare: 300-600 bytes
// - Confirm: 200-400 bytes
// - Externalize: 200-400 bytes
```

---

## Appendix B: Current Priority System

From `FlowControl.cpp`:

```cpp
uint32_t FlowControl::getMessagePriority(StellarMessage const& msg) {
    switch (msg.type()) {
    case SCP_MESSAGE:   return 0;  // Highest
    case TRANSACTION:   return 1;
    case FLOOD_DEMAND:  return 2;
    case FLOOD_ADVERT:  return 3;  // Lowest
    default:
        throw std::runtime_error("Unknown message type");
    }
}
```

The priority is used for queue ordering but NOT for bypassing flow control - that's the core problem.

---

## Appendix C: Scheduler Analysis

The current Scheduler (`Scheduler.cpp`) uses a Least Attained Service (LAS) algorithm:

1. Actions are grouped into named queues
2. Each queue tracks total CPU time consumed
3. Queue with least total service runs next
4. HIGH_PRIORITY_ACTION bypasses this entirely

The recent addition of `HIGH_PRIORITY_ACTION` helps but doesn't address network queuing:

```cpp
// From Scheduler.cpp
if (!mHighPriorityActions.empty()) {
    // HIGH_PRIORITY actions ALWAYS run first
    auto action = std::move(mHighPriorityActions.front());
    mHighPriorityActions.pop_front();
    action();
    return 1;
}
```

This only helps with CPU scheduling, not network IO.

---

## Appendix D: References

1. QUIC RFC 9000: https://www.rfc-editor.org/rfc/rfc9000
2. io_uring documentation: https://kernel.dk/io_uring.pdf
3. quiche (Cloudflare QUIC): https://github.com/cloudflare/quiche
4. liburing: https://github.com/axboe/liburing
5. msquic: https://github.com/microsoft/msquic
