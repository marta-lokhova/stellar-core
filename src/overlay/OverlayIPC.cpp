// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "overlay/OverlayIPC.h"
#include "crypto/Hex.h"
#include "util/Logging.h"
#include "util/types.h"
#include "xdr/Stellar-ledger.h"
#include <xdrpp/marshal.h>

#include <chrono>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

namespace stellar
{

OverlayIPC::OverlayIPC(std::string socketPath, std::string overlayBinaryPath,
                       uint16_t peerPort)
    : mSocketPath(std::move(socketPath))
    , mOverlayBinaryPath(std::move(overlayBinaryPath))
    , mPeerPort(peerPort)
{
}

OverlayIPC::~OverlayIPC()
{
    shutdown();
}

bool
OverlayIPC::start()
{
    if (mRunning)
    {
        CLOG_WARNING(Overlay, "OverlayIPC already running");
        return false;
    }

    // Remove old socket file if exists
    unlink(mSocketPath.c_str());

    // Spawn overlay process
    if (!spawnOverlay())
    {
        CLOG_ERROR(Overlay, "Failed to spawn overlay process");
        return false;
    }

    // Retry connection with backoff - overlay may take time to start
    constexpr int MAX_RETRIES = 10;
    constexpr int RETRY_DELAY_MS = 100;

    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS));

        mChannel = IPCChannel::connect(mSocketPath);
        if (mChannel && mChannel->isConnected())
        {
            CLOG_INFO(Overlay, "Connected to overlay IPC at {} (attempt {})",
                      mSocketPath, attempt + 1);

            // Start reader thread
            mRunning = true;
            mReaderThread = std::thread(&OverlayIPC::readerLoop, this);
            return true;
        }

        CLOG_DEBUG(Overlay, "Connection attempt {} failed, retrying...",
                   attempt + 1);
    }

    CLOG_ERROR(Overlay, "Failed to connect to overlay at {} after {} attempts",
               mSocketPath, MAX_RETRIES);
    shutdown();
    return false;
}

void
OverlayIPC::shutdown()
{
    if (!mRunning)
    {
        return;
    }

    CLOG_INFO(Overlay, "Shutting down overlay IPC");

    mRunning = false;

    // Send shutdown message
    if (mChannel && mChannel->isConnected())
    {
        IPCMessage msg;
        msg.type = IPCMessageType::SHUTDOWN;
        mChannel->send(msg);
    }

    // Close channel (will unblock reader)
    mChannel.reset();

    // Wait for reader thread
    if (mReaderThread.joinable())
    {
        mReaderThread.join();
    }

    // Wait for overlay process
    if (mOverlayPid > 0)
    {
        int status;
        // Give it a moment to exit gracefully
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        pid_t result = waitpid(mOverlayPid, &status, WNOHANG);
        if (result == 0)
        {
            // Still running, send SIGTERM
            kill(mOverlayPid, SIGTERM);
            waitpid(mOverlayPid, &status, 0);
        }
        mOverlayPid = -1;
    }
}

bool
OverlayIPC::spawnOverlay()
{
    pid_t pid = fork();
    if (pid < 0)
    {
        CLOG_ERROR(Overlay, "fork() failed: {}", strerror(errno));
        return false;
    }

    if (pid == 0)
    {
        // Child process - exec overlay binary
        // Arguments: <binary> --listen <socket-path> --peer-port <port>
        std::string portStr = std::to_string(mPeerPort);
        execl(mOverlayBinaryPath.c_str(), mOverlayBinaryPath.c_str(),
              "--listen", mSocketPath.c_str(), "--peer-port", portStr.c_str(),
              nullptr);

        // exec failed
        _exit(1);
    }

    // Parent process
    mOverlayPid = pid;
    CLOG_INFO(Overlay, "Spawned overlay process (pid={})", pid);
    return true;
}

void
OverlayIPC::readerLoop()
{
    CLOG_DEBUG(Overlay, "OverlayIPC reader thread started");

    while (mRunning && mChannel && mChannel->isConnected())
    {
        auto msg = mChannel->receive();
        if (!msg)
        {
            if (mRunning)
            {
                CLOG_WARNING(Overlay, "Overlay IPC connection closed");
            }
            break;
        }

        handleMessage(*msg);
    }

    CLOG_DEBUG(Overlay, "OverlayIPC reader thread exiting");
}

void
OverlayIPC::handleMessage(IPCMessage const& msg)
{
    CLOG_TRACE(Overlay, "IPC handleMessage: type={}, payload_size={}",
               static_cast<uint32_t>(msg.type), msg.payload.size());
    switch (msg.type)
    {
    case IPCMessageType::SCP_RECEIVED:
    {
        CLOG_DEBUG(Overlay,
                   "Received SCP_RECEIVED IPC message ({} bytes payload)",
                   msg.payload.size());
        if (mOnSCPReceived)
        {
            try
            {
                SCPEnvelope envelope;
                xdr::xdr_from_opaque(msg.payload, envelope);
                CLOG_TRACE(Overlay, "Invoking SCP received callback");
                mOnSCPReceived(envelope);
            }
            catch (std::exception const& e)
            {
                CLOG_WARNING(Overlay, "Failed to parse SCP envelope: {}",
                             e.what());
            }
        }
        else
        {
            CLOG_WARNING(Overlay, "No SCP callback registered!");
        }
        break;
    }

    case IPCMessageType::NOMINATION_HASH:
    case IPCMessageType::TX_SET_AVAILABLE:
    {
        // Response to a request - wake up waiting thread
        std::lock_guard<std::mutex> lock(mRequestMutex);
        mPendingResponse = msg;
        mRequestCv.notify_one();
        break;
    }

    case IPCMessageType::PEER_REQUESTS_SCP_STATE:
    {
        // Peer is asking for our SCP state
        if (mOnScpStateRequest && msg.payload.size() >= 4)
        {
            uint32_t ledgerSeq;
            std::memcpy(&ledgerSeq, msg.payload.data(), 4);
            CLOG_DEBUG(Overlay, "Peer requesting SCP state for ledger >= {}",
                       ledgerSeq);

            auto envelopes = mOnScpStateRequest(ledgerSeq);
            sendScpStateResponse(envelopes);
        }
        break;
    }

    default:
        CLOG_DEBUG(Overlay, "Unhandled IPC message type: {}",
                   static_cast<uint32_t>(msg.type));
        break;
    }
}

bool
OverlayIPC::broadcastSCP(SCPEnvelope const& envelope)
{
    if (!mChannel || !mChannel->isConnected())
    {
        CLOG_WARNING(Overlay, "Cannot broadcast SCP: not connected to overlay");
        return false;
    }

    IPCMessage msg;
    msg.type = IPCMessageType::BROADCAST_SCP;
    msg.payload = xdr::xdr_to_opaque(envelope);

    return mChannel->send(msg);
}

void
OverlayIPC::notifyLedgerClosed(uint32_t ledgerSeq, Hash const& ledgerHash)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    IPCMessage msg;
    msg.type = IPCMessageType::LEDGER_CLOSED;

    // Payload: [ledgerSeq:4][ledgerHash:32]
    msg.payload.resize(4 + 32);
    std::memcpy(msg.payload.data(), &ledgerSeq, 4);
    std::memcpy(msg.payload.data() + 4, ledgerHash.data(), 32);

    mChannel->send(msg);
}

void
OverlayIPC::notifyTxSetExternalized(Hash const& txSetHash)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    IPCMessage msg;
    msg.type = IPCMessageType::TX_SET_EXTERNALIZED;

    // Payload: [txSetHash:32]
    msg.payload.resize(32);
    std::memcpy(msg.payload.data(), txSetHash.data(), 32);

    mChannel->send(msg);
}

Hash
OverlayIPC::requestNominationHash(uint32_t ledgerSeq,
                                  Hash const& prevLedgerHash, int timeoutMs)
{
    Hash result;
    std::memset(result.data(), 0, result.size());

    if (!mChannel || !mChannel->isConnected())
    {
        return result;
    }

    // Send request with payload: [ledgerSeq:4][prevLedgerHash:32]
    IPCMessage req;
    req.type = IPCMessageType::REQUEST_NOMINATION_HASH;
    req.payload.resize(4 + 32);
    std::memcpy(req.payload.data(), &ledgerSeq, 4);
    std::memcpy(req.payload.data() + 4, prevLedgerHash.data(), 32);

    if (!mChannel->send(req))
    {
        return result;
    }

    // Wait for response
    std::unique_lock<std::mutex> lock(mRequestMutex);
    mPendingResponse.reset();

    bool gotResponse =
        mRequestCv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this] { return mPendingResponse.has_value(); });

    if (!gotResponse)
    {
        CLOG_WARNING(Overlay, "Timeout waiting for nomination hash");
        return result;
    }

    auto& response = *mPendingResponse;
    if (response.type != IPCMessageType::NOMINATION_HASH ||
        response.payload.size() != 32)
    {
        CLOG_WARNING(
            Overlay, "Invalid nomination hash response: type={}, size={}",
            static_cast<uint32_t>(response.type), response.payload.size());
        return result;
    }

    std::memcpy(result.data(), response.payload.data(), 32);
    CLOG_DEBUG(Overlay, "Got nomination hash from overlay");
    return result;
}

std::optional<GeneralizedTransactionSet>
OverlayIPC::getTxSet(Hash const& hash, int timeoutMs)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return std::nullopt;
    }

    // Send request with payload: [hash:32]
    IPCMessage req;
    req.type = IPCMessageType::REQUEST_TX_SET;
    req.payload.resize(32);
    std::memcpy(req.payload.data(), hash.data(), 32);

    if (!mChannel->send(req))
    {
        return std::nullopt;
    }

    // Wait for response
    std::unique_lock<std::mutex> lock(mRequestMutex);
    mPendingResponse.reset();

    bool gotResponse =
        mRequestCv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this] { return mPendingResponse.has_value(); });

    if (!gotResponse)
    {
        CLOG_WARNING(Overlay, "Timeout waiting for TX set");
        return std::nullopt;
    }

    auto& response = *mPendingResponse;
    if (response.type != IPCMessageType::TX_SET_AVAILABLE ||
        response.payload.size() < 32)
    {
        CLOG_WARNING(Overlay, "Invalid TX set response");
        return std::nullopt;
    }

    // Payload: [hash:32][xdr...]
    // Skip the hash, parse XDR
    try
    {
        GeneralizedTransactionSet txSet;
        std::vector<uint8_t> xdrData(response.payload.begin() + 32,
                                     response.payload.end());
        xdr::xdr_from_opaque(xdrData, txSet);
        CLOG_DEBUG(Overlay, "Got TX set from overlay ({} bytes XDR)",
                   xdrData.size());
        return txSet;
    }
    catch (std::exception const& e)
    {
        CLOG_WARNING(Overlay, "Failed to parse TX set: {}", e.what());
        return std::nullopt;
    }
}

std::vector<TransactionEnvelope>
OverlayIPC::getTopTransactions(size_t count, int timeoutMs)
{
    std::vector<TransactionEnvelope> result;

    if (!mChannel || !mChannel->isConnected())
    {
        return result;
    }

    // Send request
    IPCMessage req;
    req.type = IPCMessageType::REQUEST_NOMINATION_HASH;
    uint32_t countU32 = static_cast<uint32_t>(count);
    req.payload.resize(4);
    std::memcpy(req.payload.data(), &countU32, 4);

    if (!mChannel->send(req))
    {
        return result;
    }

    // Wait for response
    std::unique_lock<std::mutex> lock(mRequestMutex);
    mPendingResponse.reset();

    bool gotResponse =
        mRequestCv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this] { return mPendingResponse.has_value(); });

    if (!gotResponse)
    {
        CLOG_WARNING(Overlay, "Timeout waiting for nomination transactions");
        return result;
    }

    auto& response = *mPendingResponse;

    // Parse response: list of XDR-encoded TransactionEnvelopes
    // Format: [count:4][len1:4][tx1:len1][len2:4][tx2:len2]...
    if (response.payload.size() < 4)
    {
        return result;
    }

    uint32_t txCount;
    std::memcpy(&txCount, response.payload.data(), 4);

    size_t offset = 4;
    for (uint32_t i = 0; i < txCount && offset + 4 <= response.payload.size();
         ++i)
    {
        uint32_t txLen;
        std::memcpy(&txLen, response.payload.data() + offset, 4);
        offset += 4;

        if (offset + txLen > response.payload.size())
        {
            break;
        }

        try
        {
            TransactionEnvelope tx;
            std::vector<uint8_t> txData(response.payload.begin() + offset,
                                        response.payload.begin() + offset +
                                            txLen);
            xdr::xdr_from_opaque(txData, tx);
            result.push_back(std::move(tx));
        }
        catch (std::exception const& e)
        {
            CLOG_WARNING(Overlay, "Failed to parse transaction: {}", e.what());
        }

        offset += txLen;
    }

    return result;
}

void
OverlayIPC::submitTransaction(TransactionEnvelope const& tx, int64_t fee,
                              uint32_t numOps)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    IPCMessage msg;
    msg.type = IPCMessageType::SUBMIT_TX;

    auto txData = xdr::xdr_to_opaque(tx);

    // Payload: [fee:8][numOps:4][txData...]
    msg.payload.resize(8 + 4 + txData.size());
    size_t offset = 0;

    std::memcpy(msg.payload.data() + offset, &fee, 8);
    offset += 8;

    std::memcpy(msg.payload.data() + offset, &numOps, 4);
    offset += 4;

    std::memcpy(msg.payload.data() + offset, txData.data(), txData.size());

    mChannel->send(msg);
}

void
OverlayIPC::requestTxSet(Hash const& hash)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    IPCMessage msg;
    msg.type = IPCMessageType::REQUEST_TX_SET;
    msg.payload.resize(32);
    std::memcpy(msg.payload.data(), hash.data(), 32);

    CLOG_DEBUG(Overlay, "Requesting TX set {}", hexAbbrev(hash));
    mChannel->send(msg);
}

void
OverlayIPC::setPeerConfig(std::vector<std::string> const& knownPeers,
                          std::vector<std::string> const& preferredPeers,
                          uint16_t listenPort)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    // Build JSON payload
    std::string json = "{\"known_peers\":[";
    for (size_t i = 0; i < knownPeers.size(); ++i)
    {
        if (i > 0)
            json += ",";
        json += "\"" + knownPeers[i] + "\"";
    }
    json += "],\"preferred_peers\":[";
    for (size_t i = 0; i < preferredPeers.size(); ++i)
    {
        if (i > 0)
            json += ",";
        json += "\"" + preferredPeers[i] + "\"";
    }
    json += "],\"listen_port\":" + std::to_string(listenPort) + "}";

    IPCMessage msg;
    msg.type = IPCMessageType::SET_PEER_CONFIG;
    msg.payload.assign(json.begin(), json.end());

    CLOG_DEBUG(Overlay, "Sending peer config: {}", json);
    mChannel->send(msg);
}

void
OverlayIPC::requestScpState(uint32_t ledgerSeq)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    IPCMessage msg;
    msg.type = IPCMessageType::REQUEST_SCP_STATE;
    msg.payload.resize(4);
    std::memcpy(msg.payload.data(), &ledgerSeq, 4);

    CLOG_DEBUG(Overlay, "Requesting SCP state from peers, ledger >= {}",
               ledgerSeq);
    mChannel->send(msg);
}

void
OverlayIPC::setOnSCPReceived(SCPReceivedCallback cb)
{
    mOnSCPReceived = std::move(cb);
}

void
OverlayIPC::setOnScpStateRequest(ScpStateRequestCallback cb)
{
    mOnScpStateRequest = std::move(cb);
}

void
OverlayIPC::sendScpStateResponse(std::vector<SCPEnvelope> const& envelopes)
{
    if (!mChannel || !mChannel->isConnected())
    {
        return;
    }

    // Serialize all envelopes into payload
    // Format: [count:u32][envelope1_len:u32][envelope1_xdr]...
    std::vector<uint8_t> payload;
    uint32_t count = static_cast<uint32_t>(envelopes.size());
    payload.resize(4);
    std::memcpy(payload.data(), &count, 4);

    for (auto const& env : envelopes)
    {
        auto xdr = xdr::xdr_to_opaque(env);
        uint32_t len = static_cast<uint32_t>(xdr.size());
        size_t offset = payload.size();
        payload.resize(offset + 4 + len);
        std::memcpy(payload.data() + offset, &len, 4);
        std::memcpy(payload.data() + offset + 4, xdr.data(), len);
    }

    IPCMessage msg;
    msg.type = IPCMessageType::SCP_STATE_RESPONSE;
    msg.payload = std::move(payload);

    CLOG_DEBUG(Overlay, "Sending SCP state response with {} envelopes", count);
    mChannel->send(msg);
}

bool
OverlayIPC::isConnected() const
{
    return mChannel && mChannel->isConnected();
}

} // namespace stellar
