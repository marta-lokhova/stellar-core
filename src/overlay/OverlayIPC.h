// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "overlay/IPC.h"
#include "xdr/Stellar-overlay.h"
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace stellar
{

class Application;

/**
 * OverlayIPC manages communication with the external Rust overlay process.
 * 
 * This class:
 * 1. Spawns the overlay process on startup
 * 2. Sends SCP envelopes to be broadcast
 * 3. Receives SCP envelopes from the network
 * 4. Requests TX set hashes for nomination
 * 
 * The overlay process handles:
 * - Peer connections and authentication (Noise protocol)
 * - SCP message relay with deduplication
 * - TX flooding with push-k strategy
 * - Mempool with fee ordering
 */
class OverlayIPC
{
  public:
    /// Callback when SCP envelope received from network
    using SCPReceivedCallback = std::function<void(SCPEnvelope const&)>;
    
    /// Callback when peer connects
    using PeerConnectedCallback = std::function<void(uint64_t peerId, std::vector<uint8_t> const& publicKey)>;
    
    /// Callback when peer disconnects
    using PeerDisconnectedCallback = std::function<void(uint64_t peerId)>;
    
    /**
     * Create an OverlayIPC instance.
     * 
     * @param socketPath Path for Unix domain socket
     * @param overlayBinaryPath Path to the overlay binary (stellar-overlay)
     * @param peerPort Port for peer TCP connections (passed to overlay)
     */
    OverlayIPC(std::string socketPath, std::string overlayBinaryPath, uint16_t peerPort);
    
    ~OverlayIPC();
    
    /**
     * Start the overlay process and connect.
     * 
     * @return true if started successfully
     */
    bool start();
    
    /**
     * Stop the overlay process.
     */
    void shutdown();
    
    /**
     * Broadcast an SCP envelope to all peers.
     * 
     * @param envelope The SCP envelope to broadcast
     * @return true if sent successfully
     */
    bool broadcastSCP(SCPEnvelope const& envelope);
    
    /**
     * Notify overlay of ledger close.
     * 
     * @param ledgerSeq The closed ledger sequence number
     * @param ledgerHash The closed ledger hash
     */
    void notifyLedgerClosed(uint32_t ledgerSeq, Hash const& ledgerHash);
    
    /**
     * Notify overlay that a TX set was externalized.
     * 
     * The overlay should clear the corresponding TXs from its mempool.
     * 
     * @param txSetHash The hash of the externalized TX set
     */
    void notifyTxSetExternalized(Hash const& txSetHash);
    
    /**
     * Request a nomination hash from the overlay.
     * 
     * This asks the overlay to build a TX set from mempool and return its hash.
     * The TX set can later be requested via getTxSet().
     * 
     * @param ledgerSeq The ledger sequence we're building a TX set for
     * @param prevLedgerHash The previous ledger hash
     * @param timeoutMs Timeout in milliseconds
     * @return The TX set hash, or empty hash on timeout/error
     */
    Hash requestNominationHash(uint32_t ledgerSeq, Hash const& prevLedgerHash, int timeoutMs = 1000);
    
    /**
     * Request a TX set by hash from the overlay.
     * 
     * @param hash The TX set hash
     * @param timeoutMs Timeout in milliseconds
     * @return The TX set XDR, or empty on timeout/error
     */
    std::optional<GeneralizedTransactionSet> getTxSet(Hash const& hash, int timeoutMs = 1000);
    
    /**
     * Request top N transactions by fee for nomination.
     * 
     * This is a synchronous call that blocks until response received
     * or timeout expires.
     * 
     * @param count Number of transactions to request
     * @param timeoutMs Timeout in milliseconds
     * @return Vector of transaction XDR (may be less than count if mempool is small)
     */
    std::vector<TransactionEnvelope> getTopTransactions(size_t count, int timeoutMs = 1000);
    
    /**
     * Submit a transaction to the overlay for flooding.
     * 
     * @param tx The transaction envelope
     * @param fee Transaction fee
     * @param numOps Number of operations
     */
    void submitTransaction(TransactionEnvelope const& tx, int64_t fee, uint32_t numOps);
    
    /**
     * Configure peer addresses for the overlay.
     * 
     * @param knownPeers List of known peer addresses (host:port)
     * @param preferredPeers List of preferred peer addresses (host:port)
     * @param listenPort Local port to listen on
     */
    void setPeerConfig(std::vector<std::string> const& knownPeers,
                       std::vector<std::string> const& preferredPeers,
                       uint16_t listenPort);
    
    /**
     * Connect to a specific peer.
     * 
     * @param address Peer address (host:port)
     */
    void connectToPeer(std::string const& address);
    
    /// Set callback for received SCP envelopes
    void setOnSCPReceived(SCPReceivedCallback cb);
    
    /// Set callback for peer connections
    void setOnPeerConnected(PeerConnectedCallback cb);
    
    /// Set callback for peer disconnections
    void setOnPeerDisconnected(PeerDisconnectedCallback cb);
    
    /// Check if connected to overlay
    bool isConnected() const;
    
    /// Get the socket path
    std::string const& getSocketPath() const { return mSocketPath; }
    
  private:
    /// Spawn the overlay process
    bool spawnOverlay();
    
    /// Reader thread function
    void readerLoop();
    
    /// Handle a received IPC message
    void handleMessage(IPCMessage const& msg);
    
    std::string mSocketPath;
    std::string mOverlayBinaryPath;
    uint16_t mPeerPort;
    
    std::unique_ptr<IPCChannel> mChannel;
    std::thread mReaderThread;
    std::atomic<bool> mRunning{false};
    
    pid_t mOverlayPid{-1};
    
    SCPReceivedCallback mOnSCPReceived;
    PeerConnectedCallback mOnPeerConnected;
    PeerDisconnectedCallback mOnPeerDisconnected;
    
    // For synchronous request/response
    std::mutex mRequestMutex;
    std::condition_variable mRequestCv;
    std::optional<IPCMessage> mPendingResponse;
};

} // namespace stellar
