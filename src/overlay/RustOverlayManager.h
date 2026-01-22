// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "overlay/OverlayManager.h"
#include "overlay/OverlayIPC.h"
#include "overlay/OverlayMetrics.h"
#include "overlay/PeerAuth.h"
#include "overlay/PeerManager.h"
#include "overlay/SurveyManager.h"

namespace stellar
{

/**
 * RustOverlayManager delegates peer management to an external Rust process.
 * 
 * This class implements the OverlayManager interface but routes all
 * networking through the Rust overlay via IPC:
 * - connectTo() -> sends CONNECT_TO_PEER IPC message
 * - broadcastMessage() -> sends SCP/TX via IPC
 * - Peer queries return empty (peers are managed by Rust)
 */
class RustOverlayManager : public OverlayManager
{
  public:
    RustOverlayManager(Application& app);
    ~RustOverlayManager();

    // Core methods - delegate to Rust overlay
    void start() override;
    void shutdown() override;
    bool isShuttingDown() const override;
    
    void connectTo(PeerBareAddress const& address) override;
    bool broadcastMessage(std::shared_ptr<StellarMessage const> msg,
                         std::optional<Hash> const hash = std::nullopt) override;

    void clearLedgersBelow(uint32_t ledgerSeq, uint32_t lclSeq) override;
    
    // Flood gate - minimal implementations
    bool recvFloodedMsgID(Peer::pointer peer, Hash const& msgID) override;
    void recvTransaction(TransactionFrameBasePtr transaction,
                        Peer::pointer peer, Hash const& index) override;
    void forgetFloodedMsg(Hash const& msgID) override;
    void recvTxDemand(FloodDemand const& dmd, Peer::pointer peer) override;
    
    // Peer management - return empty (peers managed by Rust)
    std::vector<Peer::pointer> getRandomAuthenticatedPeers() override;
    std::vector<Peer::pointer> getRandomInboundAuthenticatedPeers() override;
    std::vector<Peer::pointer> getRandomOutboundAuthenticatedPeers() override;
    Peer::pointer getConnectedPeer(PeerBareAddress const& address) override;
    void maybeAddInboundConnection(Peer::pointer peer) override;
    bool addOutboundConnection(Peer::pointer peer) override;
    void removePeer(Peer* peer) override;
    bool acceptAuthenticatedPeer(Peer::pointer peer) override;
    bool isPreferred(Peer* peer) const override;
    bool isPossiblyPreferred(std::string const& ip) const override;
    bool haveSpaceForConnection(std::string const& ip) const override;
    
    std::vector<Peer::pointer> const& getInboundPendingPeers() const override;
    std::vector<Peer::pointer> const& getOutboundPendingPeers() const override;
    std::vector<Peer::pointer> getPendingPeers() const override;
    std::shared_ptr<int> getLiveInboundPeersCounter() const override;
    int getPendingPeersCount() const override;
    std::map<NodeID, Peer::pointer> const& getInboundAuthenticatedPeers() const override;
    std::map<NodeID, Peer::pointer> const& getOutboundAuthenticatedPeers() const override;
    std::map<NodeID, Peer::pointer> getAuthenticatedPeers() const override;
    int getAuthenticatedPeersCount() const override;
    
    std::set<Peer::pointer> getPeersKnows(Hash const& h) override;
    
    // Metrics and managers
    OverlayMetrics& getOverlayMetrics() override;
    PeerAuth& getPeerAuth() override;
    PeerManager& getPeerManager() override;
    SurveyManager& getSurveyManager() override;
    
    void recordMessageMetric(StellarMessage const& stellarMsg,
                            Peer::pointer peer) override;
    uint32_t getFlowControlBytesTotal() const override;
    bool checkScheduledAndCache(std::shared_ptr<CapacityTrackedMessage> tracker) override;
    SearchableSnapshotConstPtr& getOverlayThreadSnapshot() override;

    // Access to IPC (for Herder to set callbacks)
    OverlayIPC& getOverlayIPC() { return *mOverlayIPC; }
    
    // Override: Get TX set from Rust overlay for nomination
    std::optional<std::pair<TxSetXDRFrameConstPtr, Hash>> 
    getTxSetForNomination(uint32_t ledgerSeq, Hash const& prevLedgerHash) override;
    
    // Override: Forward transaction to Rust overlay
    void broadcastTransaction(TransactionEnvelope const& tx, int64_t fee,
                              uint32_t numOps) override;
    
    // Override: Notify overlay to clear externalized TXs from mempool
    void notifyTxSetExternalized(Hash const& txSetHash) override;

  private:
    Application& mApp;
    std::unique_ptr<OverlayIPC> mOverlayIPC;
    std::atomic<bool> mShuttingDown{false};
    
    // Required components (minimal)
    OverlayMetrics mOverlayMetrics;
    PeerManager mPeerManager;
    PeerAuth mAuth;
    std::shared_ptr<SurveyManager> mSurveyManager;
    std::shared_ptr<int> mLiveInboundPeersCounter;
    
    // Empty peer lists (peers are managed by Rust)
    std::vector<Peer::pointer> mEmptyPeerList;
    std::map<NodeID, Peer::pointer> mEmptyPeerMap;
    
    // Overlay thread snapshot
    SearchableSnapshotConstPtr mOverlayThreadSnapshot;
};

} // namespace stellar
