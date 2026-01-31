# Critical Socket Implementation: Debugging Learnings

## Session Date: 2026-01-16

### Problem Statement
Tests were failing with "unexpected auth sequence" and "Broken pipe" errors when trying to use the newly implemented critical socket feature for the transport redesign.

### Root Causes Identified

#### 1. Address Parsing Issue
**Problem**: `mIPAddress` field only contained IP address without port for incoming connections (REMOTE_CALLED_US role).

**Why it happened**: 
- For outgoing connections: `mIPAddress = address.toString()` includes "IP:PORT"
- For incoming connections: `mIPAddress = ep.address().to_string()` only includes IP

**Fix**: Use `getAddress()` method which returns the peer's advertised listening address from the HELLO message (set during authentication).

**Learning**: Don't assume string formats are consistent across different code paths. Use proper accessors that return structured data.

#### 2. Race Condition in Handshake
**Problem**: Client set `mCriticalSocketConnected = true` immediately after sending association message, but server's async_read hadn't completed yet. Client tried to send critical messages before server was ready, resulting in "Broken pipe".

**Why it happened**:
- Async operations complete at unpredictable times
- No synchronization between client and server socket setup

**Fix**: Implemented ACK protocol:
1. Client sends association message
2. Client waits for ACK byte (0x06)
3. Server reads association, links socket, sends ACK
4. Only after receiving ACK does client set `mCriticalSocketConnected = true`

**Learning**: In distributed systems, never assume the remote end is ready just because you completed a local operation. Always use explicit synchronization (handshakes, ACKs, etc.).

#### 3. Asymmetric Role Handling
**Problem**: Both client and server tried to initiate critical socket connections to each other, causing conflicts.

**Why it happened**: `onAuthenticated()` was called on both sides without role checking.

**Fix**: Only WE_CALLED_REMOTE initiates; REMOTE_CALLED_US accepts via PeerDoor.

**Learning**: In bidirectional protocols, clearly define which role initiates which connections to avoid conflicts and race conditions.

#### 4. Incomplete Implementation
**Problem**: Critical socket had send logic but no receive logic - messages went into a black hole.

**Why it happened**: The header declared `startCriticalRead()` and `criticalReadHandler()` but they were never implemented.

**Fix**: Implemented full read loop:
- `startCriticalRead()` - async_read for 4-byte header
- Parse message length from header
- async_read for message body
- Deserialize XDR and post to main thread
- Loop back to read next message

**Learning**: When implementing bidirectional communication, always implement both send AND receive paths. Don't assume one-way communication will work.

#### 5. Threading and Async I/O Complexity
**Problem**: Multiple threading contexts made debugging difficult:
- Main thread (authentication, message dispatch)
- Overlay IO thread (socket operations)
- Async callbacks (could run on either)

**Why it was tricky**:
- `releaseAssert(threadIsMain())` checks helped identify threading assumptions
- Async operations don't guarantee ordering
- Synchronous `asio::write()` from potentially wrong thread

**Fix**: 
- Used `postOnMainThread()` for message processing
- Made async operations explicit with callbacks
- Added `releaseAssert()` checks to document threading requirements

**Learning**: In async I/O code, always be explicit about which thread operations run on. Document threading requirements with assertions.

### Design Patterns Used

#### 1. Association Protocol
Instead of trying to magically match incoming critical sockets to existing peers, use an explicit association message containing the peer's public key. This allows the server to look up the peer in its authenticated peers map and link the sockets.

#### 2. ACK Protocol
Simple but effective: client waits for single-byte ACK before proceeding. This provides a synchronization point and ensures both sides are ready before data transfer begins.

#### 3. State Machine
Socket connection states:
- Not connected
- Connecting (async_connect in progress)
- Association sent (waiting for ACK)
- Connected (ACK received, ready for data)

#### 4. Graceful Degradation
If critical socket fails, don't crash - just fall back to bulk socket. This was already in the design but important to preserve.

### Testing Insights

1. **Error messages are gold**: "Cannot parse address for critical socket: 127.0.0.1" immediately told us the port was missing.

2. **Sequence matters**: "Critical socket ready" followed immediately by "Broken pipe" indicated a timing/race issue.

3. **Log everything during development**: The INFO-level logging added at the end helps track:
   - Which messages use critical vs bulk socket
   - Connection establishment sequence
   - Data flow

### Remaining Work

1. **Proper error handling**: Currently drops messages if critical socket unavailable. Should fall back to bulk socket properly.

2. **Metrics**: Add metrics for:
   - Critical socket latency
   - Messages sent via critical vs bulk
   - Critical socket failures/fallbacks

3. **Testing**: Need unit tests specifically for:
   - Critical socket handshake
   - Fallback behavior
   - Edge cases (peer drops during handshake, etc.)

### Key Takeaways

1. **Async I/O is hard**: Every async operation is a potential race condition. Use explicit synchronization.

2. **Test incrementally**: Don't implement entire bidirectional protocol at once. Test connection, then send, then receive separately.

3. **Document threading**: Use assertions to document which thread operations expect to run on.

4. **Observability**: Good logging is essential for debugging distributed systems.

5. **Defensive programming**: Check all assumptions (socket connected, address has port, etc.).

### References

- Design document: `/Users/marta/Documents/dev/stellar-core/dev/ai/TRANSPORT_REDESIGN.md`
- Key files modified:
  - `src/overlay/TCPPeer.cpp` - Critical socket implementation
  - `src/overlay/TCPPeer.h` - Method declarations
  - `src/overlay/PeerDoor.cpp` - Critical socket acceptor
  - `src/overlay/Peer.cpp` - Message routing logic
  - `src/overlay/OverlayManagerImpl.cpp` - Message classification

### Next Steps

1. Compile and test the current implementation
2. Run the specific test: `stellar-core test -a "overlay parallel processing" -c "background ledger close"`
3. Verify critical socket is being used via log messages
4. Measure latency improvement for SCP messages
5. Add proper fallback to bulk socket (currently drops messages)
6. Add metrics and monitoring
