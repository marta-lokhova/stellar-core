// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "overlay/RustOverlayManager.h"
#include "overlay/RandomPeerSource.h"
#include "herder/Herder.h"
#include "herder/TxSetFrame.h"
#include "main/Application.h"
#include "util/Logging.h"
#include "xdr/Stellar-overlay.h"
#include <fmt/format.h>

namespace stellar
{

RustOverlayManager::RustOverlayManager(Application& app)
    : mApp(app)
    , mOverlayMetrics(app)
    , mPeerManager(app)
    , mAuth(app)
    , mSurveyManager(std::make_shared<SurveyManager>(app))
    , mLiveInboundPeersCounter(std::make_shared<int>(0))
{
    auto const& cfg = mApp.getConfig();
    
    // Generate socket path based on PID and HTTP_PORT for uniqueness
    std::string socketPath = cfg.OVERLAY_SOCKET_PATH;
    if (socketPath.empty())
    {
        socketPath = fmt::format("/tmp/stellar-overlay-{}-{}.sock", 
                                 getpid(), cfg.HTTP_PORT);
    }
    
    // Use configured binary path or default
    std::string binaryPath = cfg.OVERLAY_BINARY_PATH;
    if (binaryPath.empty())
    {
        binaryPath = "stellar-overlay";
    }
    
    CLOG_INFO(Overlay, "Creating RustOverlayManager with socket={}, binary={}, port={}",
              socketPath, binaryPath, cfg.PEER_PORT);
    
    mOverlayIPC = std::make_unique<OverlayIPC>(socketPath, binaryPath, cfg.PEER_PORT);
}

RustOverlayManager::~RustOverlayManager()
{
    shutdown();
}

void
RustOverlayManager::start()
{
    CLOG_INFO(Overlay, "Starting RustOverlayManager");
    
    // Set up callback for received SCP envelopes - route to Herder
    mOverlayIPC->setOnSCPReceived([this](SCPEnvelope const& env) {
        // Called from reader thread, post to main thread
        mApp.postOnMainThread(
            [this, env]() {
                mApp.getHerder().recvSCPEnvelope(env);
            },
            "RustOverlayManager: SCPReceived");
    });
    
    if (!mOverlayIPC->start())
    {
        CLOG_ERROR(Overlay, "Failed to start Rust overlay process");
        throw std::runtime_error("Failed to start Rust overlay");
    }
    
    // Configure the overlay with peer settings
    auto const& cfg = mApp.getConfig();
    mOverlayIPC->setPeerConfig(
        cfg.KNOWN_PEERS,
        cfg.PREFERRED_PEERS,
        cfg.PEER_PORT
    );
    
    CLOG_INFO(Overlay, "RustOverlayManager started, peer_port={}", cfg.PEER_PORT);
}

void
RustOverlayManager::shutdown()
{
    if (mShuttingDown.exchange(true))
    {
        return;
    }
    
    CLOG_INFO(Overlay, "Shutting down RustOverlayManager");
    if (mOverlayIPC)
    {
        mOverlayIPC->shutdown();
    }
}

bool
RustOverlayManager::isShuttingDown() const
{
    return mShuttingDown.load();
}

void
RustOverlayManager::connectTo(PeerBareAddress const& address)
{
    if (mShuttingDown.load())
    {
        return;
    }
    
    std::string addrStr = fmt::format("{}:{}", address.getIP(), address.getPort());
    CLOG_DEBUG(Overlay, "RustOverlayManager::connectTo {}", addrStr);
    mOverlayIPC->connectToPeer(addrStr);
}

bool
RustOverlayManager::broadcastMessage(std::shared_ptr<StellarMessage const> msg,
                                     std::optional<Hash> const hash)
{
    if (mShuttingDown.load() || !mOverlayIPC->isConnected())
    {
        return false;
    }
    
    if (msg->type() == SCP_MESSAGE)
    {
        return mOverlayIPC->broadcastSCP(msg->envelope());
    }
    else if (msg->type() == TRANSACTION)
    {
        // TODO: Implement TX broadcast via IPC
        CLOG_WARNING(Overlay, "TX broadcast via RustOverlay not yet implemented");
        return false;
    }
    
    // Other message types not supported
    return false;
}

void
RustOverlayManager::clearLedgersBelow(uint32_t ledgerSeq, uint32_t lclSeq)
{
    // Forward to Rust overlay via IPC
    if (mOverlayIPC && mOverlayIPC->isConnected())
    {
        // Notify ledger closed (ledger cleanup handled in Rust)
        Hash dummyHash; // TODO: Get actual LCL hash
        mOverlayIPC->notifyLedgerClosed(lclSeq, dummyHash);
    }
}

// Flood gate - minimal implementations
bool
RustOverlayManager::recvFloodedMsgID(Peer::pointer peer, Hash const& msgID)
{
    // Rust overlay handles flood dedup
    return true;
}

void
RustOverlayManager::recvTransaction(TransactionFrameBasePtr transaction,
                                    Peer::pointer peer, Hash const& index)
{
    // Rust overlay manages transactions
}

void
RustOverlayManager::forgetFloodedMsg(Hash const& msgID)
{
    // Rust overlay handles this
}

void
RustOverlayManager::recvTxDemand(FloodDemand const& dmd, Peer::pointer peer)
{
    // Rust overlay handles TX demands
}

// Peer management - return empty (peers managed by Rust)
std::vector<Peer::pointer>
RustOverlayManager::getRandomAuthenticatedPeers()
{
    return mEmptyPeerList;
}

std::vector<Peer::pointer>
RustOverlayManager::getRandomInboundAuthenticatedPeers()
{
    return mEmptyPeerList;
}

std::vector<Peer::pointer>
RustOverlayManager::getRandomOutboundAuthenticatedPeers()
{
    return mEmptyPeerList;
}

Peer::pointer
RustOverlayManager::getConnectedPeer(PeerBareAddress const& address)
{
    return nullptr;
}

void
RustOverlayManager::maybeAddInboundConnection(Peer::pointer peer)
{
    // Not applicable - Rust overlay manages connections
}

bool
RustOverlayManager::addOutboundConnection(Peer::pointer peer)
{
    // Not applicable - Rust overlay manages connections
    return false;
}

void
RustOverlayManager::removePeer(Peer* peer)
{
    // Not applicable - Rust overlay manages peers
}

bool
RustOverlayManager::acceptAuthenticatedPeer(Peer::pointer peer)
{
    // Not applicable
    return false;
}

bool
RustOverlayManager::isPreferred(Peer* peer) const
{
    return false;
}

bool
RustOverlayManager::isPossiblyPreferred(std::string const& ip) const
{
    return false;
}

bool
RustOverlayManager::haveSpaceForConnection(std::string const& ip) const
{
    return true;
}

std::vector<Peer::pointer> const&
RustOverlayManager::getInboundPendingPeers() const
{
    return mEmptyPeerList;
}

std::vector<Peer::pointer> const&
RustOverlayManager::getOutboundPendingPeers() const
{
    return mEmptyPeerList;
}

std::vector<Peer::pointer>
RustOverlayManager::getPendingPeers() const
{
    return mEmptyPeerList;
}

std::shared_ptr<int>
RustOverlayManager::getLiveInboundPeersCounter() const
{
    return mLiveInboundPeersCounter;
}

int
RustOverlayManager::getPendingPeersCount() const
{
    return 0;
}

std::map<NodeID, Peer::pointer> const&
RustOverlayManager::getInboundAuthenticatedPeers() const
{
    return mEmptyPeerMap;
}

std::map<NodeID, Peer::pointer> const&
RustOverlayManager::getOutboundAuthenticatedPeers() const
{
    return mEmptyPeerMap;
}

std::map<NodeID, Peer::pointer>
RustOverlayManager::getAuthenticatedPeers() const
{
    return mEmptyPeerMap;
}

int
RustOverlayManager::getAuthenticatedPeersCount() const
{
    return 0;
}

std::set<Peer::pointer>
RustOverlayManager::getPeersKnows(Hash const& h)
{
    return {};
}

// Metrics and managers
OverlayMetrics&
RustOverlayManager::getOverlayMetrics()
{
    return mOverlayMetrics;
}

PeerAuth&
RustOverlayManager::getPeerAuth()
{
    return mAuth;
}

PeerManager&
RustOverlayManager::getPeerManager()
{
    return mPeerManager;
}

SurveyManager&
RustOverlayManager::getSurveyManager()
{
    return *mSurveyManager;
}

void
RustOverlayManager::recordMessageMetric(StellarMessage const& stellarMsg,
                                        Peer::pointer peer)
{
    // Metrics handled by Rust overlay
}

uint32_t
RustOverlayManager::getFlowControlBytesTotal() const
{
    return 0;
}

bool
RustOverlayManager::checkScheduledAndCache(std::shared_ptr<CapacityTrackedMessage> tracker)
{
    return false;
}

SearchableSnapshotConstPtr&
RustOverlayManager::getOverlayThreadSnapshot()
{
    return mOverlayThreadSnapshot;
}

std::optional<std::pair<TxSetXDRFrameConstPtr, Hash>>
RustOverlayManager::getTxSetForNomination(uint32_t ledgerSeq, Hash const& prevLedgerHash)
{
    if (!mOverlayIPC || mShuttingDown)
    {
        return std::nullopt;
    }
    
    CLOG_DEBUG(Overlay, "Requesting nomination hash from Rust overlay for ledger {}", ledgerSeq);
    
    // Request nomination hash from Rust overlay
    Hash txSetHash = mOverlayIPC->requestNominationHash(ledgerSeq, prevLedgerHash);
    
    // Check if we got a valid hash
    Hash emptyHash;
    std::memset(emptyHash.data(), 0, emptyHash.size());
    if (txSetHash == emptyHash)
    {
        CLOG_WARNING(Overlay, "Rust overlay returned empty nomination hash");
        return std::nullopt;
    }
    
    CLOG_DEBUG(Overlay, "Got nomination hash from Rust overlay: {}",
               binToHex(txSetHash).substr(0, 8));
    
    // Request the actual TX set
    auto txSetOpt = mOverlayIPC->getTxSet(txSetHash);
    if (!txSetOpt)
    {
        CLOG_WARNING(Overlay, "Failed to get TX set for hash {}", 
                     binToHex(txSetHash).substr(0, 8));
        return std::nullopt;
    }
    
    // Wrap in TxSetXDRFrame
    auto txSetFrame = TxSetXDRFrame::makeFromWire(*txSetOpt);
    
    CLOG_INFO(Overlay, "Got TX set from Rust overlay: hash={}", 
              binToHex(txSetHash).substr(0, 8));
    
    return std::make_pair(txSetFrame, txSetHash);
}

void
RustOverlayManager::broadcastTransaction(TransactionEnvelope const& tx,
                                         int64_t fee, uint32_t numOps)
{
    if (mOverlayIPC && !mShuttingDown)
    {
        CLOG_DEBUG(Overlay, "Forwarding TX to Rust overlay: fee={}, numOps={}",
                   fee, numOps);
        mOverlayIPC->submitTransaction(tx, fee, numOps);
    }
}

void
RustOverlayManager::notifyTxSetExternalized(Hash const& txSetHash)
{
    if (mOverlayIPC && !mShuttingDown)
    {
        CLOG_DEBUG(Overlay, "Notifying Rust overlay of externalized TX set: {}",
                   binToHex(txSetHash).substr(0, 8));
        mOverlayIPC->notifyTxSetExternalized(txSetHash);
    }
}

} // namespace stellar
