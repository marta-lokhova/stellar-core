Primary Tasks Spawned at Startup

  ┌─────────────────┬──────────────────┬───────────────────────────────────────────────────────────┐
  │ Task            │ Spawned At       │ Purpose                                                   │
  ├─────────────────┼──────────────────┼───────────────────────────────────────────────────────────┤
  │ App::run        │ main.rs:902      │ Main event loop (tokio::select! over IPC + libp2p events) │
  ├─────────────────┼──────────────────┼───────────────────────────────────────────────────────────┤
  │ Mempool Manager │ main.rs:196      │ Handles CoreCommand channel, owns mempool                 │
  ├─────────────────┼──────────────────┼───────────────────────────────────────────────────────────┤
  │ libp2p Overlay  │ main.rs:212      │ QUIC swarm event loop, peer management                    │
  ├─────────────────┼──────────────────┼───────────────────────────────────────────────────────────┤
  │ IPC Reader      │ transport.rs:183 │ spawn_blocking reads from Unix socket                     │
  ├─────────────────┼──────────────────┼───────────────────────────────────────────────────────────┤
  │ IPC Writer      │ transport.rs:189 │ spawn_blocking writes to Unix socket                      │
  └─────────────────┴──────────────────┴───────────────────────────────────────────────────────────┘

  libp2p Overlay Internal Tasks (spawned in libp2p_overlay.rs)

  ┌───────────────────────┬────────────────────────────┬────────────────────────────────────────────────┐
  │ Task                  │ Spawned At                 │ Purpose                                        │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ SCP Inbound Handler   │ line 381                   │ Accepts SCP streams, spawns per-peer readers   │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ TX Inbound Handler    │ line 382                   │ Accepts TX streams, spawns per-peer readers    │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ TxSet Inbound Handler │ line 383                   │ Accepts TxSet streams, spawns per-peer readers │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ Per-Peer SCP Reader   │ line 1050                  │ Reads SCP messages from one peer               │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ Per-Peer TX Reader    │ line 1173                  │ Reads TX messages from one peer                │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ Per-Peer TxSet Reader │ line 1256                  │ Reads TxSet messages from one peer             │
  ├───────────────────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ Per-Peer Send Tasks   │ lines 685, 742, 1147, 1233 │ Spawned for parallel broadcast/flood           │
  └───────────────────────┴────────────────────────────┴────────────────────────────────────────────────┘

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  2. Threading Model

    ┌─────────────────────────────────────────────────────────────────┐
    │                    Tokio Runtime (multi-threaded)               │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
    │  │   App::run      │  │ Mempool Manager │  │ libp2p Overlay  │ │
    │  │   (select!)     │  │ (cmd_rx.recv)   │  │ (swarm.select!) │ │
    │  └────────┬────────┘  └─────────────────┘  └────────┬────────┘ │
    │           │                                         │          │
    │           │ mpsc channels                           │          │
    │           ▼                                         ▼          │
    │  ┌─────────────────┐                    ┌───────────────────┐  │
    │  │ IPC Reader/     │                    │ Per-Peer Inbound  │  │
    │  │ Writer          │                    │ Stream Handlers   │  │
    │  │ (spawn_blocking)│                    │ (per peer×3)      │  │
    │  └─────────────────┘                    └───────────────────┘  │
    │                                                                 │
    │  ┌─────────────────────────────────────────────────────────┐   │
    │  │           Per-Send Tasks (spawned dynamically)          │   │
    │  │   - Broadcast SCP to N peers → N tasks                  │   │
    │  │   - Flood TX to M peers → M tasks                       │   │
    │  │   - Forward SCP/TX → 1 task per peer                    │   │
    │  └─────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────┘

  Key Points:

    - Tokio uses work-stealing scheduler across multiple OS threads
    - All tasks are cooperative (yield at .await points)
    - spawn_blocking moves IPC I/O to dedicated blocking threadpool
    - No dedicated threads per peer—all async tasks

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  3. Memory Model & Shared Data

  Shared State Containers

  ┌─────────────────────────────┬───────────────────────┬──────────────────────────────────────────────────────────┬───────────────────┐
  │ Container                   │ Location              │ Lock Type                                                │ Contents          │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ tx_set_cache                │ main.rs:163           │ Arc<RwLock<TxSetCache>>                                  │ Cached TX sets    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ pushed_tx_sets              │ main.rs:165           │ Arc<RwLock<HashSet>>                                     │ Dedup tracking    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ current_ledger_seq          │ main.rs:167           │ Arc<RwLock<u32>>                                         │ Current ledger    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ pending_core_txset_requests │ main.rs:173           │ Arc<RwLock<HashSet>>                                     │ Awaiting fetch    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ pending_scp_state_requests  │ main.rs:176           │ Arc<RwLock<VecDeque>>                                    │ FIFO queue        │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ mempool                     │ integrated.rs:89      │ Arc<RwLock<Mempool>>                                     │ TX storage        │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ local_tx_sets               │ integrated.rs:92      │ Arc<RwLock<HashMap>>                                     │ Local TX sets     │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ peer_streams                │ libp2p_overlay.rs:218 │ RwLock<HashMap<PeerId, Arc<Mutex<PeerOutboundStreams>>>> │ Per-peer streams  │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ scp_seen                    │ libp2p_overlay.rs:220 │ RwLock<LruCache>                                         │ SCP dedup (10K)   │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ tx_seen                     │ libp2p_overlay.rs:222 │ RwLock<LruCache>                                         │ TX dedup (100K)   │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ scp_sent_to                 │ libp2p_overlay.rs:224 │ RwLock<LruCache>                                         │ Flood tracking    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ tx_sent_to                  │ libp2p_overlay.rs:226 │ RwLock<LruCache>                                         │ Flood tracking    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ txset_sources               │ libp2p_overlay.rs:228 │ RwLock<LruCache>                                         │ Source mapping    │
  ├─────────────────────────────┼───────────────────────┼──────────────────────────────────────────────────────────┼───────────────────┤
  │ pending_txset_requests      │ libp2p_overlay.rs:230 │ RwLock<HashSet>                                          │ In-flight fetches │
  └─────────────────────────────┴───────────────────────┴──────────────────────────────────────────────────────────┴───────────────────┘

  Channel Communication

  ┌───────────────────┬───────────────────┬──────────────────┬─────────────────────────┐
  │ Channel           │ Type              │ Direction        │ Purpose                 │
  ├───────────────────┼───────────────────┼──────────────────┼─────────────────────────┤
  │ Core IPC inbound  │ mpsc::unbounded   │ IPC Reader → App │ Messages from C++       │
  ├───────────────────┼───────────────────┼──────────────────┼─────────────────────────┤
  │ Core IPC outbound │ mpsc::unbounded   │ App → IPC Writer │ Messages to C++         │
  ├───────────────────┼───────────────────┼──────────────────┼─────────────────────────┤
  │ libp2p events     │ mpsc::unbounded   │ libp2p → App     │ Network events          │
  ├───────────────────┼───────────────────┼──────────────────┼─────────────────────────┤
  │ libp2p commands   │ mpsc::Sender(256) │ App → libp2p     │ Broadcast/dial commands │
  ├───────────────────┼───────────────────┼──────────────────┼─────────────────────────┤
  │ mempool commands  │ mpsc::unbounded   │ App → Mempool    │ TX operations           │
  └───────────────────┴───────────────────┴──────────────────┴─────────────────────────┘

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  4. What Runs in Parallel vs Serial

  Parallel Execution

    ✓ Different peers' inbound readers run in parallel
    ✓ Broadcast send tasks to different peers run in parallel
    ✓ App::run, Mempool Manager, libp2p Overlay all run concurrently
    ✓ IPC Reader and Writer run concurrently (separate spawn_blocking)
    ✓ SCP, TX, TxSet inbound stream handlers run concurrently

  Serialized Execution

    ✗ App::run main loop: one event at a time (select! picks one branch)
    ✗ Mempool mutations: single task processes CoreCommand sequentially
    ✗ Per-peer outbound writes: Mutex<PeerOutboundStreams> serializes SCP/TX/TxSet
    ✗ pending_scp_state_requests: FIFO queue processed in order
    ✗ Each RwLock write blocks other writers (but allows concurrent readers)

  Serialization Diagram

    Peer A Outbound:          Peer B Outbound:
    ┌─────────────────┐       ┌─────────────────┐
    │ Mutex acquired  │       │ Mutex acquired  │  ← PARALLEL (different peers)
    │  - SCP write    │       │  - TX write     │
    │  - TX write     │       │  - SCP write    │
    │  - TxSet write  │       │  - TxSet write  │
    └─────────────────┘       └─────────────────┘
            │                         │
            └─────────┬───────────────┘
                      │
            ┌─────────▼─────────┐
            │ SERIAL within     │  ← SERIAL (same peer)
            │ same peer's Mutex │
            └───────────────────┘

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  5. Potential Races & Concurrency Issues

  Issue 1: Per-Peer Mutex Causes Head-of-Line Blocking

  Location: libp2p_overlay.rs:95-108

    struct PeerOutboundStreams {
        scp: Option<Stream>,
        tx: Option<Stream>,
        txset: Option<Stream>,
    }

  Problem: Single Mutex protects all three streams. A large TxSet write (up to 16MB) blocks SCP sends to the same peer.

  Severity: Medium - violates stream independence goal for same-peer communication.

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Issue 2: FIFO Queue Without Correlation

  Location: main.rs:176, 676-685

    pending_scp_state_requests: Arc<RwLock<VecDeque<PeerId>>>
    // ...
    let peer_id = pending.pop_front(); // No correlation to response!

  Problem: Core responds with ScpStateResponse but overlay uses FIFO to determine recipient. If responses arrive out-of-order, wrong peer gets the state.

  Severity: High - can cause consensus corruption in edge cases.

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Issue 3: No Timeout on pending_txset_requests

  Location: libp2p_overlay.rs:754-762

    pending.insert(hash);
    // ... send request ...
    // No cleanup if peer disconnects or times out!

  Problem: If peer disconnects after request sent, hash stays in pending_txset_requests forever, blocking future fetches.

  Mitigation exists: PeerDisconnected event cleans up pending_scp_state_requests but NOT pending_txset_requests.

  Severity: Medium - causes stuck fetches.

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Issue 4: Unbounded Channels Under Load

  Location: Multiple

    mpsc::unbounded_channel::<Message>()  // IPC
    mpsc::unbounded_channel()             // libp2p events

  Problem: Under heavy TX flood, unbounded channels can grow indefinitely, causing memory exhaustion.

  Severity: Low-Medium - only under extreme load.

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Issue 5: Race in TX Set Cache After Fetch

  Location: main.rs:326-354

    let was_pending = pending.remove(&hash);  // Atomic
    if was_pending {
        // Send to Core
    }
    // Also cache (separate operation)
    cache.insert(...);

  Problem: Between pending.remove() and cache.insert(), another request for same hash could arrive and find neither pending nor cached.

  Severity: Low - window is microseconds, Core retries.

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Issue 6: Duplicate SCP State Responses

  Location: libp2p_overlay.rs:1054-1068

    if envelope.len() == 4 {
        // SCP state request - no dedup
        state.event_tx.send(OverlayEvent::ScpStateRequested { ... });
    }

  Problem: If peer sends multiple state requests quickly, all get forwarded to Core and queued. Responses go to wrong peers due to Issue 2.

  Severity: Medium - compounds Issue 2.

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  6. Deadlock Analysis

  Lock Acquisition Order

  Most lock acquisitions follow safe patterns:

    Pattern 1: peer_streams.read() → peer_streams[peer].lock()
    Pattern 2: scp_sent_to.write() + peer_streams.read() (same scope)
    Pattern 3: pending_txset_requests.write() alone

  Potential Deadlock Scenario (Theoretical)

    Task A (flooding):                    Task B (connection close):
    1. scp_sent_to.write().await         1. peer_streams.write().await
    2. peer_streams.read().await         2. (cleanup)

       ↑                                    ↑
       └─────── Could deadlock if ──────────┘
                order reversed

  Current Code: libp2p_overlay.rs:1107-1111 acquires both locks:

    let mut sent_to = state.scp_sent_to.write().await;
    let streams = state.peer_streams.read().await;

  Analysis: RwLock read doesn't block other reads, and write vs read only blocks if write is waiting. Tokio's async locks yield, so true deadlock is unlikely but possible under
  pathological scheduling.

  Verdict: Low deadlock risk due to:

    - No nested lock acquisition in opposite orders
    - Async locks yield rather than spin
    - Short critical sections

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  7. Safety Evaluation Summary

  ┌─────────────────────┬───────────────┬────────────────────────────────────────────┐
  │ Aspect              │ Rating        │ Notes                                      │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Data Race Safety    │ ✅ Excellent  │ Rust's ownership + RwLock/Mutex guarantees │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Deadlock Risk       │ ✅ Low        │ Consistent lock ordering, async yields     │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Memory Safety       │ ✅ Excellent  │ No unsafe code, Arc for sharing            │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Backpressure        │ ⚠️ Weak       │ Unbounded channels can grow                │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Request Correlation │ ❌ Poor       │ FIFO queue is fragile                      │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Timeout Handling    │ ⚠️ Incomplete │ Missing for txset fetches                  │
  ├─────────────────────┼───────────────┼────────────────────────────────────────────┤
  │ Stream Independence │ ⚠️ Partial    │ Per-peer mutex serializes                  │
  └─────────────────────┴───────────────┴────────────────────────────────────────────┘

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  8. Recommendations

    - Split per-peer mutex into three separate mutexes for SCP/TX/TxSet streams
    - Add request IDs to SCP state requests for proper correlation
    - Add timeout task for pending_txset_requests cleanup
    - Bounded channels with backpressure for IPC and events
    - Clean up pending_txset_requests on PeerDisconnected
