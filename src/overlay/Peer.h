#pragma once

// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/asio.h" // IWYU pragma: keep
#include "database/Database.h"
#include "lib/json/json.h"
#include "medida/timer.h"
#include "overlay/OverlayAppConnector.h"
#include "overlay/PeerBareAddress.h"
#include "util/NonCopyable.h"
#include "util/Timer.h"
#include "xdrpp/message.h"

namespace stellar
{

typedef std::shared_ptr<SCPQuorumSet> SCPQuorumSetPtr;

static size_t const MAX_MESSAGE_SIZE = 1024 * 1024 * 16;     // 16 MB
static size_t const MAX_TX_SET_ALLOWANCE = 1024 * 1024 * 10; // 10 MB
static size_t const MAX_SOROBAN_BYTE_ALLOWANCE =
    MAX_TX_SET_ALLOWANCE / 2; // 5 MB
static size_t const MAX_CLASSIC_BYTE_ALLOWANCE =
    MAX_TX_SET_ALLOWANCE / 2; // 5 MB

static_assert(MAX_TX_SET_ALLOWANCE >=
              MAX_SOROBAN_BYTE_ALLOWANCE + MAX_CLASSIC_BYTE_ALLOWANCE);

// max tx size is 100KB
static const uint32_t MAX_CLASSIC_TX_SIZE_BYTES = 100 * 1024;

class Application;
class LoopbackPeer;
struct OverlayMetrics;
class FlowControl;
class TxAdverts;

// Peer class represents a connected peer (either inbound or outbound)
//
// Connection steps:
//   A initiates a TCP connection to B
//   Once the connection is established, A sends HELLO(CertA,NonceA)
//     HELLO message includes A's listening port and ledger information
//   B now has IP and listening port of A, sends HELLO(CertB,NonceB) back
//   A sends AUTH(signed([seq=0], keyAB))
//     Peers use `seq` counter to prevent message replays
//   B verifies A's AUTH message and does the following:
//     sends AUTH(signed([seq=0], keyBA)) back
//     sends a list of other peers to try
//     maybe disconnects (if no connection slots are available)
//
// keyAB and keyBA are per-connection HMAC keys derived from non-interactive
// ECDH on random curve25519 keys conveyed in CertA and CertB (certs signed by
// Node Ed25519 keys) the result of which is then fed through HKDF with the
// per-connection nonces. See PeerAuth.h.
//
// If any verify step fails, the peer disconnects immediately.

class Peer : public std::enable_shared_from_this<Peer>,
             public NonMovableOrCopyable
{

  public:
    static constexpr std::chrono::seconds PEER_SEND_MODE_IDLE_TIMEOUT =
        std::chrono::seconds(60);
    static constexpr std::chrono::nanoseconds PEER_METRICS_DURATION_UNIT =
        std::chrono::milliseconds(1);
    static constexpr std::chrono::nanoseconds PEER_METRICS_RATE_UNIT =
        std::chrono::seconds(1);
    static constexpr uint32_t FIRST_VERSION_SUPPORTING_FLOW_CONTROL_IN_BYTES =
        28;
    static constexpr uint32_t FIRST_VERSION_REQUIRED_FOR_PROTOCOL_20 = 32;

    // The reporting will be based on the previous
    // PEER_METRICS_WINDOW_SIZE-second time window.
    static constexpr std::chrono::seconds PEER_METRICS_WINDOW_SIZE =
        std::chrono::seconds(300);

    typedef std::shared_ptr<Peer> pointer;

    enum PeerState
    {
        CONNECTING = 0,
        CONNECTED = 1,
        GOT_HELLO = 2,
        GOT_AUTH = 3,
        CLOSING = 4
    };

    static inline int
    format_as(PeerState const& s)
    {
        return static_cast<int>(s);
    }

    enum PeerRole
    {
        REMOTE_CALLED_US,
        WE_CALLED_REMOTE
    };

    static inline std::string
    format_as(PeerRole const& r)
    {
        return (r == REMOTE_CALLED_US) ? "REMOTE_CALLED_US"
                                       : "WE_CALLED_REMOTE";
    }

    enum class DropDirection
    {
        REMOTE_DROPPED_US,
        WE_DROPPED_REMOTE
    };

    struct PeerMetrics
    {
        PeerMetrics(VirtualClock::time_point connectedTime);
        std::atomic<uint64_t> mMessageRead;
        std::atomic<uint64_t> mMessageWrite;
        std::atomic<uint64_t> mByteRead;
        std::atomic<uint64_t> mByteWrite;
        std::atomic<uint64_t> mAsyncRead;
        std::atomic<uint64_t> mAsyncWrite;
        std::atomic<uint64_t> mMessageDrop;

        medida::Timer mMessageDelayInWriteQueueTimer;
        medida::Timer mMessageDelayInAsyncWriteTimer;
        medida::Timer mAdvertQueueDelay;
        medida::Timer mPullLatency;

        std::atomic<uint64_t> mDemandTimeouts;
        std::atomic<uint64_t> mUniqueFloodBytesRecv;
        std::atomic<uint64_t> mDuplicateFloodBytesRecv;
        std::atomic<uint64_t> mUniqueFetchBytesRecv;
        std::atomic<uint64_t> mDuplicateFetchBytesRecv;

        std::atomic<uint64_t> mUniqueFloodMessageRecv;
        std::atomic<uint64_t> mDuplicateFloodMessageRecv;
        std::atomic<uint64_t> mUniqueFetchMessageRecv;
        std::atomic<uint64_t> mDuplicateFetchMessageRecv;

        std::atomic<uint64_t> mTxHashReceived;
        std::atomic<uint64_t> mTxDemandSent;

        std::atomic<VirtualClock::time_point> mConnectedTime;

        std::atomic<uint64_t> mMessagesFulfilled;
        std::atomic<uint64_t> mBannedMessageUnfulfilled;
        std::atomic<uint64_t> mUnknownMessageUnfulfilled;
    };

    struct TimestampedMessage
    {
        VirtualClock::time_point mEnqueuedTime;
        VirtualClock::time_point mIssuedTime;
        VirtualClock::time_point mCompletedTime;
        void recordWriteTiming(OverlayMetrics& metrics,
                               PeerMetrics& peerMetrics);
        xdr::msg_ptr mMessage;
    };

  protected:
    class MsgCapacityTracker : private NonMovableOrCopyable
    {
        std::weak_ptr<Peer> mWeakPeer;
        StellarMessage mMsg;
        std::atomic<bool> mFinished{false};

      public:
        MsgCapacityTracker(std::weak_ptr<Peer> peer, StellarMessage const& msg);
        StellarMessage const& getMessage();
        std::weak_ptr<Peer> getPeer();
        // Since MsgCapacityTracker is passed around between thread and stored
        // by ASIO in lambdas, do not rely on RAII to trigger completion.
        // Instead, explicitly call `finish`.
        void finish();
    };

    OverlayAppConnector mAppConnector;

    Hash const mNetworkID;
    std::shared_ptr<FlowControl> mFlowControl;
    std::atomic<VirtualClock::time_point> mLastRead;
    std::atomic<VirtualClock::time_point> mLastWrite;
    std::atomic<VirtualClock::time_point> mEnqueueTimeOfLastWrite;

    PeerRole const mRole;
    OverlayMetrics& mOverlayMetrics;
    PeerMetrics mPeerMetrics;

    // Mutex to protect PeerState, which can be accessed and modified from
    // multiple threads
    std::recursive_mutex mutable mStateMutex;

    HmacSha256Key mSendMacKey;
    HmacSha256Key mRecvMacKey;
    std::atomic<uint64_t> mSendMacSeq{0};
    std::atomic<uint64_t> mRecvMacSeq{0};
    // Does local node have capacity to read from this peer
    bool canRead() const;
    // helper method to acknowledge that some bytes were received
    void receivedBytes(size_t byteCount, bool gotFullMessage);
    virtual bool
    useBackgroundThread() const
    {
        return mAppConnector.getConfig()
            .EXPERIMENTAL_BACKGROUND_OVERLAY_PROCESSING;
    }

    void initialize(PeerBareAddress const& address);
    void setState(std::lock_guard<std::recursive_mutex>& stateGuard,
                  PeerState newState);
    bool shouldAbort(std::lock_guard<std::recursive_mutex>& stateGuard) const;
    void shutdownAndRemovePeer(std::string const& reason,
                               DropDirection dropDirection);

    // Subclasses should only use these methods to load and modify peer state,
    // signatured hint that callers need to grab mStateMutex.
    PeerState
    getState(std::lock_guard<std::recursive_mutex>& stateGuard) const
    {
        return mState;
    }

    void recvAuthenticatedMessage(AuthenticatedMessage&& msg);
    // These exist mostly to be overridden in TCPPeer and callable via
    // shared_ptr<Peer> as a captured shared_from_this().
    virtual void connectHandler(asio::error_code const& ec);

    void maybeExecuteInBackground(std::string const& jobName,
                                  std::function<void(std::shared_ptr<Peer>)> f);
    VirtualTimer&
    getRecurrentTimer()
    {
        releaseAssert(threadIsMain());
        return mRecurringTimer;
    }

  private:
    PeerState mState;
    NodeID mPeerID;
    uint256 mSendNonce;
    uint256 mRecvNonce;

    std::string mRemoteVersion;
    uint32_t mRemoteOverlayMinVersion;
    std::optional<uint32_t> mRemoteOverlayVersion;
    PeerBareAddress mAddress;

    VirtualClock::time_point mCreationTime;
    VirtualTimer mRecurringTimer;
    VirtualTimer mDelayedExecutionTimer;

    std::shared_ptr<TxAdverts> mTxAdverts;

    static Hash pingIDfromTimePoint(VirtualClock::time_point const& tp);
    void pingPeer();
    void maybeProcessPingResponse(Hash const& id);
    VirtualClock::time_point mPingSentTime;
    std::chrono::milliseconds mLastPing;

    void recvRawMessage(StellarMessage const& msg);

    virtual void recvError(StellarMessage const& msg);
    void updatePeerRecordAfterEcho();
    void updatePeerRecordAfterAuthentication();
    void recvAuth(StellarMessage const& msg);
    void recvDontHave(StellarMessage const& msg);
    void recvGetPeers(StellarMessage const& msg);
    void recvHello(Hello const& elo);
    void recvPeers(StellarMessage const& msg);
    void recvSurveyRequestMessage(StellarMessage const& msg);
    void recvSurveyResponseMessage(StellarMessage const& msg);
    void recvSendMore(StellarMessage const& msg);

    void recvGetTxSet(StellarMessage const& msg);
    void recvTxSet(StellarMessage const& msg);
    void recvGeneralizedTxSet(StellarMessage const& msg);
    void recvTransaction(StellarMessage const& msg);
    void recvGetSCPQuorumSet(StellarMessage const& msg);
    void recvSCPQuorumSet(StellarMessage const& msg);
    void recvSCPMessage(StellarMessage const& msg);
    void recvGetSCPState(StellarMessage const& msg);
    void recvFloodAdvert(StellarMessage const& msg);
    void recvFloodDemand(StellarMessage const& msg);

    void sendHello();
    void sendAuth();
    void sendSCPQuorumSet(SCPQuorumSetPtr qSet);
    void sendDontHave(MessageType type, uint256 const& itemID);
    void sendPeers();
    void sendError(ErrorCode error, std::string const& message);

    void recvMessage(std::shared_ptr<MsgCapacityTracker> msgTracker);

    // NB: This is a move-argument because the write-buffer has to travel
    // with the write-request through the async IO system, and we might have
    // several queued at once. We have carefully arranged this to not copy
    // data more than the once necessary into this buffer, but it can't be
    // put in a reused/non-owned buffer without having to buffer/queue
    // messages somewhere else. The async write request will point _into_
    // this owned buffer. This is really the best we can do.
    virtual void sendMessage(xdr::msg_ptr&& xdrBytes) = 0;
    virtual void scheduleRead() = 0;
    virtual void
    connected()
    {
    }

    virtual AuthCert getAuthCert();

    void startRecurrentTimer();

    void recurrentTimerExpired(asio::error_code const& error);
    std::chrono::seconds getIOTimeout() const;

    void sendAuthenticatedMessage(std::shared_ptr<StellarMessage const> msg);
    void beginMessageProcessing(StellarMessage const& msg);
    void endMessageProcessing(StellarMessage const& msg);

  public:
    /* The following functions must all be called from the main thread (they all
     * contain releaseAssert(threadIsMain())) */
    Peer(Application& app, PeerRole role);

    void cancelTimers();

    std::string msgSummary(StellarMessage const& stellarMsg);
    void sendGetTxSet(uint256 const& setID);
    void sendGetQuorumSet(uint256 const& setID);
    void sendGetPeers();
    void sendGetScpState(uint32 ledgerSeq);
    void sendErrorAndDrop(ErrorCode error, std::string const& message);
    void sendTxDemand(TxDemandVector&& demands);
    // Queue up an advert to send, return true if the advert was queued, and
    // false otherwise (if advert is a duplicate, for example)
    bool sendAdvert(Hash const& txHash);
    void sendSendMore(uint32_t numMessages);
    void sendSendMore(uint32_t numMessages, uint32_t numBytes);

    virtual void sendMessage(std::shared_ptr<StellarMessage const> msg,
                             bool log = true);

    PeerRole
    getRole() const
    {
        releaseAssert(threadIsMain());
        return mRole;
    }

    std::chrono::seconds getLifeTime() const;
    std::chrono::milliseconds getPing() const;

    std::string const&
    getRemoteVersion() const
    {
        releaseAssert(threadIsMain());
        return mRemoteVersion;
    }

    std::optional<uint32_t>
    getRemoteOverlayVersion() const
    {
        releaseAssert(threadIsMain());
        return mRemoteOverlayVersion;
    }

    PeerBareAddress const&
    getAddress()
    {
        releaseAssert(threadIsMain());
        return mAddress;
    }

    NodeID
    getPeerID()
    {
        releaseAssert(threadIsMain());
        return mPeerID;
    }

    std::string const& toString();

    void startExecutionDelayedTimer(
        VirtualClock::duration d, std::function<void()> const& onSuccess,
        std::function<void(asio::error_code)> const& onFailure);
    Json::Value getJsonInfo(bool compact) const;
    void handleMaxTxSizeIncrease(uint32_t increase);
    virtual ~Peer()
    {
        releaseAssert(threadIsMain());
    }

    // Pull Mode facade methods , must be called from the main thread.
    // Queue up transaction hashes for processing again. This method is normally
    // called if a previous demand failed or timed out.
    void retryAdvert(std::list<Hash>& hashes);
    // Does this peer have any transaction hashes to process?
    bool hasAdvert();
    // Pop the next transaction hash to process
    std::pair<Hash, std::optional<VirtualClock::time_point>> popAdvert();
    // Clear pull mode state below `ledgerSeq`
    void clearBelow(uint32_t ledgerSeq);

    /* The following functions can be called from background thread, so they
     * must be thread-safe */
    bool isConnected(std::lock_guard<std::recursive_mutex>& stateGuard) const;
    bool
    isAuthenticated(std::lock_guard<std::recursive_mutex>& stateGuard) const;

    PeerMetrics&
    getPeerMetrics()
    {
        // PeerMetrics is thread-safe
        return mPeerMetrics;
    }

    virtual void drop(std::string const& reason,
                      DropDirection dropDirection) = 0;

    friend class LoopbackPeer;
    friend class PeerStub;

#ifdef BUILD_TESTS
    std::shared_ptr<FlowControl>
    getFlowControl() const
    {
        return mFlowControl;
    }
    bool isAuthenticatedForTesting() const;
    bool shouldAbortForTesting() const;
    bool isConnectedForTesting() const;
#endif

    // Public thread-safe methods that access Peer's state
    void
    assertAuthenticated() const
    {
        std::lock_guard<std::recursive_mutex> guard(mStateMutex);
        releaseAssert(isAuthenticated(guard));
    }

    void
    assertShuttingDown() const
    {
        std::lock_guard<std::recursive_mutex> guard(mStateMutex);
        releaseAssert(mState == CLOSING);
    }

    // equivalent to isAuthenticated being an atomic flag; i.e. it can be safely
    // loaded, but once it's loaded, there are no guarantees on its value. If
    // the code block depends on isAuthenticated value being constant, use
    // `doIfAuthenticated`
    bool
    isAuthenticatedAtomic() const
    {
        std::lock_guard<std::recursive_mutex> guard(mStateMutex);
        return isAuthenticated(guard);
    }

    void
    doIfAuthenticated(std::function<void()> f)
    {
        std::lock_guard<std::recursive_mutex> guard(mStateMutex);
        if (isAuthenticated(guard))
        {
            f();
        }
    }
};
}
