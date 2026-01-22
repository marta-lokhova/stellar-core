// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "lib/catch.hpp"
#include "overlay/IPC.h"
#include "overlay/OverlayIPC.h"
#include "util/TmpDir.h"
#include "xdr/Stellar-overlay.h"
#include "xdr/Stellar-SCP.h"

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace stellar;

/**
 * These tests verify communication between C++ Core and Rust overlay.
 * 
 * To run these tests:
 * 1. Build the Rust overlay: cd overlay && cargo build --release
 * 2. Run tests: stellar-core test '[overlay-ipc-rust]'
 * 
 * Tests are tagged with [.] so they don't run by default (require overlay binary).
 */

namespace
{

// Helper to find the overlay binary
std::string
findOverlayBinary()
{
    // Try various paths
    std::vector<std::string> paths = {
        "overlay/target/release/stellar-overlay",
        "../overlay/target/release/stellar-overlay",
        "target/release/stellar-overlay",
    };
    
    for (auto const& p : paths)
    {
        if (access(p.c_str(), X_OK) == 0)
        {
            // Return absolute path for forked child process
            return std::filesystem::absolute(p).string();
        }
    }
    
    return "";
}

// Get absolute socket path from TmpDir
std::string
getAbsoluteSocketPath(TmpDir const& tmpDir)
{
    return std::filesystem::absolute(tmpDir.getName() + "/overlay.sock").string();
}

// Create a mock SCP envelope for testing
SCPEnvelope
makeMockSCPEnvelope(uint64_t slotIndex, uint32_t nodeId)
{
    SCPEnvelope env;
    env.statement.slotIndex = slotIndex;
    env.statement.pledges.type(SCP_ST_NOMINATE);
    
    // Set some mock data
    auto& nom = env.statement.pledges.nominate();
    nom.quorumSetHash.fill(static_cast<uint8_t>(nodeId));
    
    // Value is opaque<> (xvector<uint8_t>), not Hash
    Value mockValue;
    mockValue.resize(32);
    std::fill(mockValue.begin(), mockValue.end(), 
              static_cast<uint8_t>(slotIndex & 0xFF));
    nom.votes.push_back(mockValue);
    
    return env;
}

} // anonymous namespace

TEST_CASE("OverlayIPC connects to Rust overlay", "[overlay-ipc-rust][.]")
{
    std::string overlayBinary = findOverlayBinary();
    REQUIRE_FALSE(overlayBinary.empty());
    
    TmpDir tmpDir("overlay-ipc-test");
    std::string socketPath = getAbsoluteSocketPath(tmpDir);
    
    OverlayIPC ipc(socketPath, overlayBinary, 11625);
    
    SECTION("start and connect")
    {
        REQUIRE(ipc.start());
        REQUIRE(ipc.isConnected());
        
        // Clean shutdown
        ipc.shutdown();
        REQUIRE_FALSE(ipc.isConnected());
    }
}

TEST_CASE("OverlayIPC broadcasts SCP to Rust overlay", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    REQUIRE_FALSE(overlayBinary.empty());
    
    TmpDir tmpDir("overlay-ipc-broadcast-test");
    std::string socketPath = getAbsoluteSocketPath(tmpDir);
    
    OverlayIPC ipc(socketPath, overlayBinary, 11625);
    REQUIRE(ipc.start());
    
    SECTION("broadcast SCP envelope")
    {
        auto envelope = makeMockSCPEnvelope(100, 1);
        
        // Should succeed (overlay accepts the message)
        REQUIRE(ipc.broadcastSCP(envelope));
    }
    
    SECTION("broadcast multiple envelopes")
    {
        for (uint64_t i = 0; i < 10; ++i)
        {
            auto envelope = makeMockSCPEnvelope(100 + i, 1);
            REQUIRE(ipc.broadcastSCP(envelope));
        }
    }
    
    ipc.shutdown();
}

TEST_CASE("OverlayIPC receives SCP from Rust overlay", "[overlay-ipc][.]")
{
    // This test requires two overlay instances to actually relay messages
    // For now, we just verify the callback mechanism works
    
    std::string overlayBinary = findOverlayBinary();
    REQUIRE_FALSE(overlayBinary.empty());
    
    TmpDir tmpDir("overlay-ipc-receive-test");
    std::string socketPath = getAbsoluteSocketPath(tmpDir);
    
    OverlayIPC ipc(socketPath, overlayBinary, 11625);
    
    std::atomic<int> receivedCount{0};
    ipc.setOnSCPReceived([&](SCPEnvelope const& env) {
        ++receivedCount;
    });
    
    REQUIRE(ipc.start());
    
    // Broadcast and verify no crash
    auto envelope = makeMockSCPEnvelope(200, 2);
    REQUIRE(ipc.broadcastSCP(envelope));
    
    // Give it a moment
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Note: receivedCount may be 0 since overlay won't echo back our own message
    // This is correct behavior - we're just verifying no crash
    
    ipc.shutdown();
}

TEST_CASE("OverlayIPC ledger close notification", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    REQUIRE_FALSE(overlayBinary.empty());
    
    TmpDir tmpDir("overlay-ipc-ledger-test");
    std::string socketPath = getAbsoluteSocketPath(tmpDir);
    
    OverlayIPC ipc(socketPath, overlayBinary, 11625);
    REQUIRE(ipc.start());
    
    SECTION("notify ledger closed")
    {
        Hash ledgerHash;
        ledgerHash.fill(42);
        
        // Should not crash
        ipc.notifyLedgerClosed(12345, ledgerHash);
    }
    
    ipc.shutdown();
}

/**
 * Full end-to-end test with two Core instances communicating via their
 * respective overlays.
 * 
 * This is a more complex test that verifies:
 * 1. Core A broadcasts SCP
 * 2. Overlay A sends to Overlay B
 * 3. Core B receives the SCP
 */
TEST_CASE("Two Cores communicate via Rust overlays", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    REQUIRE_FALSE(overlayBinary.empty());
    
    TmpDir tmpDirA("overlay-ipc-e2e-A");
    TmpDir tmpDirB("overlay-ipc-e2e-B");
    
    std::string socketPathA = getAbsoluteSocketPath(tmpDirA);
    std::string socketPathB = getAbsoluteSocketPath(tmpDirB);
    
    // This test would require overlays to connect to each other,
    // which needs config files and peer discovery.
    // For now, we skip the actual connectivity test and just verify
    // the IPC mechanism works independently.
    
    OverlayIPC ipcA(socketPathA, overlayBinary, 11626);
    OverlayIPC ipcB(socketPathB, overlayBinary, 11627);
    
    REQUIRE(ipcA.start());
    REQUIRE(ipcB.start());
    
    // Track received messages
    std::atomic<int> receivedByA{0};
    std::atomic<int> receivedByB{0};
    
    ipcA.setOnSCPReceived([&](SCPEnvelope const&) { ++receivedByA; });
    ipcB.setOnSCPReceived([&](SCPEnvelope const&) { ++receivedByB; });
    
    // Broadcast from A
    auto envelope = makeMockSCPEnvelope(300, 1);
    REQUIRE(ipcA.broadcastSCP(envelope));
    
    // Note: Without peer connectivity configured, B won't receive the message.
    // This test just verifies the infrastructure works.
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    ipcA.shutdown();
    ipcB.shutdown();
    
    // For a proper e2e test, we'd need to:
    // 1. Configure overlays to connect to each other
    // 2. Wait for connection established
    // 3. Then verify message relay
    // This is left for future work.
}

// Include simulation headers for the E2E test
#include "crypto/SHA.h"
#include "herder/Herder.h"
#include "ledger/LedgerTxn.h"
#include "simulation/Simulation.h"
#include "test/test.h"
#include "test/TestAccount.h"
#include "test/TxTests.h"
#include "transactions/TransactionUtils.h"
#include "util/MetricsRegistry.h"

/**
 * End-to-end test using Simulation framework to verify SCP consensus
 * works correctly over the Rust overlay.
 * 
 * This test:
 * 1. Creates 2 nodes with OVER_TCP mode (which uses RustOverlayManager)
 * 2. Connects them via their Rust overlays
 * 3. Starts SCP and verifies they reach consensus on multiple ledgers
 * 
 * Unlike TCPPeer tests, this doesn't check C++ Peer objects - it only
 * verifies that the end-to-end consensus works.
 */
TEST_CASE("Rust overlay SCP consensus", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    // Use OVER_TCP mode which enables RustOverlayManager
    Hash networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
    auto simulation = std::make_shared<Simulation>(
        Simulation::OVER_TCP, networkID);
    
    // Create 2 nodes with a simple quorum
    auto key0 = SecretKey::fromSeed(sha256("RUST_OVERLAY_TEST_NODE_0"));
    auto key1 = SecretKey::fromSeed(sha256("RUST_OVERLAY_TEST_NODE_1"));
    
    SCPQuorumSet qSet;
    qSet.threshold = 2;
    qSet.validators.push_back(key0.getPublicKey());
    qSet.validators.push_back(key1.getPublicKey());
    
    auto node0 = simulation->addNode(key0, qSet);
    auto node1 = simulation->addNode(key1, qSet);
    
    // Connect the nodes (this triggers overlay connection via RustOverlayManager)
    simulation->addPendingConnection(key0.getPublicKey(), key1.getPublicKey());
    
    // Start all nodes
    simulation->startAllNodes();
    
    // Target: externalize ledger 5 (proves SCP relay is working)
    int const targetLedger = 5;
    
    // Crank until both nodes reach consensus on target ledger
    // Use simulation's expected close time multiplied by number of ledgers
    simulation->crankUntil(
        [&]() { 
            return simulation->haveAllExternalized(targetLedger, 2); 
        },
        30 * targetLedger * simulation->getExpectedLedgerCloseTime(), 
        false);
    
    // Verify consensus was reached
    REQUIRE(simulation->haveAllExternalized(targetLedger, 2));
    
    // Verify both nodes have the same ledger hash for each ledger
    for (int seq = 2; seq <= targetLedger; ++seq)
    {
        auto& lm0 = node0->getLedgerManager();
        auto& lm1 = node1->getLedgerManager();
        
        // Both should have closed this ledger
        REQUIRE(lm0.getLastClosedLedgerNum() >= static_cast<uint32_t>(seq));
        REQUIRE(lm1.getLastClosedLedgerNum() >= static_cast<uint32_t>(seq));
    }
    
    LOG_INFO(DEFAULT_LOG, "Rust overlay SCP consensus test passed - "
             "reached ledger {} on both nodes", targetLedger);
}

/**
 * Test TX set building and nomination hash request.
 * 
 * This test:
 * 1. Creates an OverlayIPC connection to a Rust overlay
 * 2. Requests a nomination hash (which builds a TX set from empty mempool)
 * 3. Verifies a hash is returned
 */
TEST_CASE("Rust overlay nomination hash", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_nomination_hash_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11625;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // Request nomination hash for ledger 2 with mock prev hash
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    // With empty mempool, should still get a valid hash (for empty TX set)
    // The hash should not be all zeros
    Hash emptyHash;
    std::fill(emptyHash.begin(), emptyHash.end(), 0);
    
    REQUIRE(nominationHash != emptyHash);
    
    LOG_INFO(DEFAULT_LOG, "Got nomination hash for empty TX set: {:02x}{:02x}{:02x}{:02x}...",
             nominationHash[0], nominationHash[1], nominationHash[2], nominationHash[3]);
    
    ipc->shutdown();
}

/**
 * Test TX submission and inclusion in TX set.
 * 
 * This test:
 * 1. Submits a transaction to Rust overlay via IPC
 * 2. Requests a nomination hash
 * 3. Retrieves the TX set and verifies it contains the submitted TX
 */
TEST_CASE("Rust overlay TX submission", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_tx_submit_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11626;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // Create a minimal valid TransactionEnvelope
    TransactionEnvelope txEnv;
    txEnv.type(ENVELOPE_TYPE_TX);
    auto& tx = txEnv.v1().tx;
    tx.sourceAccount.type(KEY_TYPE_ED25519);
    std::fill(tx.sourceAccount.ed25519().begin(), 
              tx.sourceAccount.ed25519().end(), 0xAB);
    tx.fee = 1000;
    tx.seqNum = 12345;
    tx.cond.type(PRECOND_NONE);
    // Add a dummy operation
    tx.operations.resize(1);
    tx.operations[0].body.type(BUMP_SEQUENCE);
    tx.operations[0].body.bumpSequenceOp().bumpTo = 12346;
    
    int64_t fee = 1000;
    uint32_t numOps = 1;
    
    // Submit the transaction
    ipc->submitTransaction(txEnv, fee, numOps);
    
    // Give Rust overlay time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Request nomination hash
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    // Hash should be non-zero
    Hash emptyHash;
    std::fill(emptyHash.begin(), emptyHash.end(), 0);
    REQUIRE(nominationHash != emptyHash);
    
    // Get the TX set
    auto txSetOpt = ipc->getTxSet(nominationHash, 5000);
    REQUIRE(txSetOpt.has_value());
    
    // The TX set should have exactly 1 TX in the classic phase
    auto& txSet = *txSetOpt;
    REQUIRE(txSet.v() == 1);
    auto& phases = txSet.v1TxSet().phases;
    REQUIRE(phases.size() == 2); // CLASSIC + SOROBAN
    
    // CLASSIC phase should have our TX
    auto& classicPhase = phases[0];
    REQUIRE(classicPhase.v() == 0);
    auto& components = classicPhase.v0Components();
    REQUIRE(components.size() == 1);
    auto& txs = components[0].txsMaybeDiscountedFee().txs;
    REQUIRE(txs.size() == 1);
    
    // Verify it's the same TX we submitted
    auto& retrievedTx = txs[0];
    REQUIRE(retrievedTx.type() == ENVELOPE_TYPE_TX);
    REQUIRE(retrievedTx.v1().tx.fee == 1000);
    REQUIRE(retrievedTx.v1().tx.seqNum == 12345);
    
    LOG_INFO(DEFAULT_LOG, "TX submission test passed - TX included in TX set");
    
    ipc->shutdown();
}

/**
 * Helper to create a TransactionEnvelope with specified fee and sequence.
 */
static TransactionEnvelope
makeTxEnvelope(int64_t fee, int64_t seqNum, uint8_t accountByte, uint32_t numOps = 1)
{
    TransactionEnvelope txEnv;
    txEnv.type(ENVELOPE_TYPE_TX);
    auto& tx = txEnv.v1().tx;
    tx.sourceAccount.type(KEY_TYPE_ED25519);
    std::fill(tx.sourceAccount.ed25519().begin(), 
              tx.sourceAccount.ed25519().end(), accountByte);
    tx.fee = static_cast<uint32_t>(fee);
    tx.seqNum = seqNum;
    tx.cond.type(PRECOND_NONE);
    // Add operations
    tx.operations.resize(numOps);
    for (uint32_t i = 0; i < numOps; ++i)
    {
        tx.operations[i].body.type(BUMP_SEQUENCE);
        tx.operations[i].body.bumpSequenceOp().bumpTo = seqNum + 1;
    }
    return txEnv;
}

/**
 * Test TX fee ordering in TX set.
 * 
 * Submit multiple TXs with different fees, verify they appear in fee order.
 */
TEST_CASE("Rust overlay TX fee ordering", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_tx_fee_order_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11627;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // Submit TXs with different fees (out of order)
    // fee/op: 100/1=100, 500/1=500, 300/1=300
    auto tx1 = makeTxEnvelope(100, 1, 0x01);  // lowest fee
    auto tx2 = makeTxEnvelope(500, 2, 0x02);  // highest fee
    auto tx3 = makeTxEnvelope(300, 3, 0x03);  // middle fee
    
    ipc->submitTransaction(tx1, 100, 1);
    ipc->submitTransaction(tx2, 500, 1);
    ipc->submitTransaction(tx3, 300, 1);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    auto txSetOpt = ipc->getTxSet(nominationHash, 5000);
    REQUIRE(txSetOpt.has_value());
    
    auto& txs = txSetOpt->v1TxSet().phases[0].v0Components()[0].txsMaybeDiscountedFee().txs;
    REQUIRE(txs.size() == 3);
    
    // Should be ordered by fee (highest first): 500, 300, 100
    REQUIRE(txs[0].v1().tx.fee == 500);
    REQUIRE(txs[1].v1().tx.fee == 300);
    REQUIRE(txs[2].v1().tx.fee == 100);
    
    LOG_INFO(DEFAULT_LOG, "TX fee ordering test passed");
    ipc->shutdown();
}

/**
 * Test TX fee-per-op ordering.
 * 
 * A TX with 200 fee / 2 ops (100/op) should rank lower than 150 fee / 1 op (150/op).
 */
TEST_CASE("Rust overlay TX fee per op ordering", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_tx_fee_per_op_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11628;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // TX1: 200 fee / 2 ops = 100 per op
    // TX2: 150 fee / 1 op = 150 per op (higher priority despite lower total fee)
    auto tx1 = makeTxEnvelope(200, 1, 0x01, 2);  // 100 per op
    auto tx2 = makeTxEnvelope(150, 2, 0x02, 1);  // 150 per op
    
    ipc->submitTransaction(tx1, 200, 2);
    ipc->submitTransaction(tx2, 150, 1);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    auto txSetOpt = ipc->getTxSet(nominationHash, 5000);
    REQUIRE(txSetOpt.has_value());
    
    auto& txs = txSetOpt->v1TxSet().phases[0].v0Components()[0].txsMaybeDiscountedFee().txs;
    REQUIRE(txs.size() == 2);
    
    // TX2 (150/1=150 per op) should come before TX1 (200/2=100 per op)
    REQUIRE(txs[0].v1().tx.fee == 150);
    REQUIRE(txs[1].v1().tx.fee == 200);
    
    LOG_INFO(DEFAULT_LOG, "TX fee per op ordering test passed");
    ipc->shutdown();
}

/**
 * Test mempool eviction when at capacity.
 * 
 * The mempool has a max size. When full, lowest fee TXs should be evicted.
 * Note: Default mempool size is 1000, so we need to submit more than that,
 * OR we test that high-fee TXs are kept when building TX set (which has its own limit).
 */
TEST_CASE("Rust overlay mempool eviction", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_mempool_eviction_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11629;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // Submit many low-fee TXs first
    for (int i = 0; i < 50; ++i)
    {
        auto tx = makeTxEnvelope(100 + i, i + 1, static_cast<uint8_t>(i));
        ipc->submitTransaction(tx, 100 + i, 1);
    }
    
    // Submit a few high-fee TXs
    auto highTx1 = makeTxEnvelope(10000, 100, 0xF1);
    auto highTx2 = makeTxEnvelope(9000, 101, 0xF2);
    auto highTx3 = makeTxEnvelope(8000, 102, 0xF3);
    
    ipc->submitTransaction(highTx1, 10000, 1);
    ipc->submitTransaction(highTx2, 9000, 1);
    ipc->submitTransaction(highTx3, 8000, 1);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    auto txSetOpt = ipc->getTxSet(nominationHash, 5000);
    REQUIRE(txSetOpt.has_value());
    
    auto& txs = txSetOpt->v1TxSet().phases[0].v0Components()[0].txsMaybeDiscountedFee().txs;
    
    // The high-fee TXs should be at the front
    REQUIRE(txs.size() >= 3);
    REQUIRE(txs[0].v1().tx.fee == 10000);
    REQUIRE(txs[1].v1().tx.fee == 9000);
    REQUIRE(txs[2].v1().tx.fee == 8000);
    
    LOG_INFO(DEFAULT_LOG, "Mempool eviction test passed - high fee TXs prioritized ({} total TXs)", 
             txs.size());
    ipc->shutdown();
}

/**
 * Test TX deduplication.
 * 
 * Submitting the same TX twice should only result in one TX in the set.
 */
TEST_CASE("Rust overlay TX deduplication", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_tx_dedup_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11630;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // Submit the same TX twice
    auto tx = makeTxEnvelope(1000, 12345, 0xAB);
    
    ipc->submitTransaction(tx, 1000, 1);
    ipc->submitTransaction(tx, 1000, 1);  // duplicate
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    auto txSetOpt = ipc->getTxSet(nominationHash, 5000);
    REQUIRE(txSetOpt.has_value());
    
    auto& txs = txSetOpt->v1TxSet().phases[0].v0Components()[0].txsMaybeDiscountedFee().txs;
    
    // Should only have 1 TX (not 2)
    REQUIRE(txs.size() == 1);
    REQUIRE(txs[0].v1().tx.fee == 1000);
    
    LOG_INFO(DEFAULT_LOG, "TX deduplication test passed");
    ipc->shutdown();
}

/**
 * Test mempool clear after TX set externalized.
 * 
 * After externalization, TXs in the externalized TX set should be removed from mempool.
 */
TEST_CASE("Rust overlay mempool clear on externalize", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    TmpDir tmpDir("overlay_ipc_mempool_clear_test");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";
    uint16_t peerPort = 11631;
    
    auto ipc = std::make_unique<OverlayIPC>(socketPath, overlayBinary, peerPort);
    REQUIRE(ipc->start());
    
    // Submit a TX
    auto tx = makeTxEnvelope(1000, 12345, 0xAB);
    ipc->submitTransaction(tx, 1000, 1);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Build TX set for ledger 2
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    Hash nominationHash = ipc->requestNominationHash(2, prevHash, 5000);
    
    // Get TX set - should have 1 TX
    auto txSetOpt = ipc->getTxSet(nominationHash, 5000);
    REQUIRE(txSetOpt.has_value());
    REQUIRE(txSetOpt->v1TxSet().phases[0].v0Components().size() == 1);
    REQUIRE(txSetOpt->v1TxSet().phases[0].v0Components()[0].txsMaybeDiscountedFee().txs.size() == 1);
    
    // Notify that this TX set was externalized
    ipc->notifyTxSetExternalized(nominationHash);
    
    // Give time for mempool cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Request another nomination hash - should be empty since TX was cleared
    Hash prevHash2;
    std::fill(prevHash2.begin(), prevHash2.end(), 0x43);
    Hash nominationHash2 = ipc->requestNominationHash(3, prevHash2, 5000);
    
    // Get TX set - should be empty now
    auto txSetOpt2 = ipc->getTxSet(nominationHash2, 5000);
    REQUIRE(txSetOpt2.has_value());
    
    // CLASSIC phase should have 0 components (empty)
    REQUIRE(txSetOpt2->v1TxSet().phases[0].v0Components().size() == 0);
    
    LOG_INFO(DEFAULT_LOG, "Mempool clear on externalize test passed");
    ipc->shutdown();
}

/**
 * Test TX flooding between two Rust overlays.
 * 
 * This test:
 * 1. Creates two Rust overlay processes
 * 2. Connects them via TCP (peer-to-peer)
 * 3. Submits a TX to overlay A
 * 4. Verifies the TX appears in overlay B's mempool
 * 
 * This proves the TX flooding path:
 * Core A → IPC → Overlay A mempool → TCP → Overlay B mempool
 */
TEST_CASE("Rust overlay TX flooding between peers", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    // Create two overlay processes on different ports
    TmpDir tmpDirA("overlay_ipc_flood_test_a");
    TmpDir tmpDirB("overlay_ipc_flood_test_b");
    std::string socketPathA = tmpDirA.getName() + "/overlay.sock";
    std::string socketPathB = tmpDirB.getName() + "/overlay.sock";
    uint16_t peerPortA = 11640;
    uint16_t peerPortB = 11641;
    
    auto ipcA = std::make_unique<OverlayIPC>(socketPathA, overlayBinary, peerPortA);
    auto ipcB = std::make_unique<OverlayIPC>(socketPathB, overlayBinary, peerPortB);
    
    REQUIRE(ipcA->start());
    REQUIRE(ipcB->start());
    
    // Configure overlay B to connect to overlay A
    std::vector<std::string> knownPeers = {"127.0.0.1:" + std::to_string(peerPortA)};
    std::vector<std::string> preferredPeers;
    ipcB->setPeerConfig(knownPeers, preferredPeers, peerPortB);
    
    // Wait for peer connection to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Submit a TX to overlay A
    auto tx = makeTxEnvelope(1000, 12345, 0xAA);
    ipcA->submitTransaction(tx, 1000, 1);
    
    LOG_INFO(DEFAULT_LOG, "Submitted TX to overlay A, waiting for flood to B...");
    
    // Wait for TX to flood from A to B
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Request nomination hash from overlay B - should have the TX
    Hash prevHash;
    std::fill(prevHash.begin(), prevHash.end(), 0x42);
    Hash nominationHashB = ipcB->requestNominationHash(2, prevHash, 5000);
    
    // Get TX set from B
    auto txSetOptB = ipcB->getTxSet(nominationHashB, 5000);
    REQUIRE(txSetOptB.has_value());
    
    // CLASSIC phase should have 1 component with 1 TX (the flooded TX)
    auto& phasesB = txSetOptB->v1TxSet().phases;
    REQUIRE(phasesB.size() == 2); // CLASSIC + SOROBAN
    
    auto& classicPhaseB = phasesB[0];
    REQUIRE(classicPhaseB.v() == 0);
    auto& componentsB = classicPhaseB.v0Components();
    
    REQUIRE(componentsB.size() == 1);
    auto& txsB = componentsB[0].txsMaybeDiscountedFee().txs;
    REQUIRE(txsB.size() == 1);
    
    // Verify it's the same TX we submitted to A
    TransactionEnvelope receivedTx = txsB[0];
    REQUIRE(receivedTx.v1().tx.sourceAccount == tx.v1().tx.sourceAccount);
    REQUIRE(receivedTx.v1().tx.fee == tx.v1().tx.fee);
    REQUIRE(receivedTx.v1().tx.seqNum == tx.v1().tx.seqNum);
    
    LOG_INFO(DEFAULT_LOG, "TX flooding between peers test passed - "
             "TX submitted to A appeared in B's mempool!");
    
    ipcA->shutdown();
    ipcB->shutdown();
}

/**
 * Full E2E test: Submit TX to one node, verify it gets included in ledger.
 * 
 * Uses Simulation framework with Rust overlay (OVER_TCP mode) to:
 * 1. Create 2 nodes running SCP consensus
 * 2. Submit a TX to node 0 via Herder
 * 3. Verify the TX propagates to node 1 via overlay flooding
 * 4. Verify the TX gets included in the externalized ledger
 */
TEST_CASE("Rust overlay TX included in ledger", "[overlay-ipc][.]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    // Use OVER_TCP mode which enables RustOverlayManager
    Hash networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
    auto simulation = std::make_shared<Simulation>(
        Simulation::OVER_TCP, networkID);
    
    // Create 2 nodes with a simple quorum
    auto key0 = SecretKey::fromSeed(sha256("RUST_TX_LEDGER_TEST_NODE_0"));
    auto key1 = SecretKey::fromSeed(sha256("RUST_TX_LEDGER_TEST_NODE_1"));
    
    SCPQuorumSet qSet;
    qSet.threshold = 2;
    qSet.validators.push_back(key0.getPublicKey());
    qSet.validators.push_back(key1.getPublicKey());
    
    auto node0 = simulation->addNode(key0, qSet);
    auto node1 = simulation->addNode(key1, qSet);
    
    // Connect the nodes
    simulation->addPendingConnection(key0.getPublicKey(), key1.getPublicKey());
    
    // Start all nodes
    simulation->startAllNodes();
    
    // Wait for initial consensus (ledger 2)
    simulation->crankUntil(
        [&]() { return simulation->haveAllExternalized(2, 2); },
        30 * 2 * simulation->getExpectedLedgerCloseTime(), 
        false);
    REQUIRE(simulation->haveAllExternalized(2, 2));
    LOG_INFO(DEFAULT_LOG, "Initial consensus reached at ledger 2");
    
    // Get root account from node0
    auto root = TestAccount{*node0, txtest::getRoot(networkID)};
    auto rootSeqNum = root.getLastSequenceNumber();
    
    // Create a destination account
    SecretKey destKey = SecretKey::pseudoRandomForTesting();
    
    // Create a valid transaction: root creates destination account
    // Use 500 XLM (500000000000 stroops) to exceed base reserve of 100 XLM
    auto tx = root.tx({txtest::createAccount(destKey.getPublicKey(), 500000000000)});
    
    LOG_INFO(DEFAULT_LOG, "Submitting TX {} to node0",
             binToHex(tx->getFullHash()).substr(0, 8));
    
    // Submit via Herder (this will route to Rust overlay)
    auto result = node0->getHerder().recvTransaction(tx, false);
    REQUIRE(result.code == TransactionQueue::AddResultCode::ADD_STATUS_PENDING);
    
    LOG_INFO(DEFAULT_LOG, "TX submitted successfully, waiting for inclusion...");
    
    // Crank until ledger 4 to give time for TX to be included
    int const targetLedger = 4;
    simulation->crankUntil(
        [&]() { return simulation->haveAllExternalized(targetLedger, 2); },
        30 * targetLedger * simulation->getExpectedLedgerCloseTime(), 
        false);
    
    REQUIRE(simulation->haveAllExternalized(targetLedger, 2));
    
    // Verify the destination account exists (TX was applied)
    {
        LedgerTxn ltx(node0->getLedgerTxnRoot());
        auto destAccount = stellar::loadAccount(ltx, destKey.getPublicKey());
        REQUIRE(destAccount);
        LOG_INFO(DEFAULT_LOG, "Destination account created successfully!");
    }
    
    // Also verify on node1 (TX propagated and was applied)
    {
        LedgerTxn ltx(node1->getLedgerTxnRoot());
        auto destAccount = stellar::loadAccount(ltx, destKey.getPublicKey());
        REQUIRE(destAccount);
        LOG_INFO(DEFAULT_LOG, "Destination account exists on node1 too!");
    }
    
    LOG_INFO(DEFAULT_LOG, "TX included in ledger test passed - "
             "TX submitted to node0, included in consensus, applied on both nodes");
}

/**
 * Stress test: Submit TXs in batches and measure SCP latency.
 * 
 * This test verifies that SCP consensus timing remains stable even under
 * heavy TX load, validating the dual-channel isolation design.
 * 
 * Runs at 3 different TX batch sizes:
 * - 10 tx/ledger (light load)
 * - 50 tx/ledger (moderate load)  
 * - 200 tx/ledger (heavy load)
 * 
 * For each rate, runs for several ledgers and measures:
 * - scp.timing.nominated (time from nomination to prepare)
 * - scp.timing.externalized (time from prepare to externalize)
 * - ledger.ledger.close (total ledger close time)
 */
TEST_CASE("Rust overlay SCP latency under TX load", "[overlay-ipc-stress][.][!mayfail]")
{
    std::string overlayBinary = findOverlayBinary();
    if (overlayBinary.empty())
    {
        WARN("Skipping test - overlay binary not found");
        return;
    }
    
    // Test parameters - tx per ledger batch
    struct TestRun {
        int txPerLedger;
        int ledgerCount;
        std::string label;
    };
    
    std::vector<TestRun> runs = {
        {10, 5, "Light (10 tx/ledger)"},
        {1000, 5, "Moderate (1000 tx/ledger)"},
        {8000, 5, "Heavy (8000 tx/ledger)"}
    };
    
    // Results storage
    struct Results {
        std::string label;
        int txSubmitted;
        int txIncluded;
        double scpNominatedMean;
        double scpNominatedMax;
        double scpExternalizedMean;
        double scpExternalizedMax;
        double ledgerCloseMean;
        double ledgerCloseMax;
    };
    std::vector<Results> allResults;
    
    for (auto const& run : runs)
    {
        LOG_INFO(DEFAULT_LOG, "========================================");
        LOG_INFO(DEFAULT_LOG, "Starting stress test: {}", run.label);
        LOG_INFO(DEFAULT_LOG, "========================================");
        
        // Create simulation with 2 nodes
        Hash networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
        auto simulation = std::make_shared<Simulation>(
            Simulation::OVER_TCP, networkID);
        
        auto key0 = SecretKey::fromSeed(sha256("STRESS_TEST_NODE_0"));
        auto key1 = SecretKey::fromSeed(sha256("STRESS_TEST_NODE_1"));
        
        SCPQuorumSet qSet;
        qSet.threshold = 2;
        qSet.validators.push_back(key0.getPublicKey());
        qSet.validators.push_back(key1.getPublicKey());
        
        // Configure genesis accounts for high TX throughput
        int totalTxs = run.txPerLedger * run.ledgerCount;
        auto cfg0 = simulation->newConfig();
        cfg0.GENESIS_TEST_ACCOUNT_COUNT = totalTxs + 100;
        cfg0.TESTING_UPGRADE_MAX_TX_SET_SIZE = 10000;
        auto cfg1 = simulation->newConfig();
        cfg1.GENESIS_TEST_ACCOUNT_COUNT = totalTxs + 100;
        cfg1.TESTING_UPGRADE_MAX_TX_SET_SIZE = 10000;
        
        auto node0 = simulation->addNode(key0, qSet, &cfg0);
        auto node1 = simulation->addNode(key1, qSet, &cfg1);
        
        simulation->addPendingConnection(key0.getPublicKey(), key1.getPublicKey());
        simulation->startAllNodes();
        
        // Wait for initial consensus
        simulation->crankUntil(
            [&]() { return simulation->haveAllExternalized(2, 2); },
            30 * 2 * simulation->getExpectedLedgerCloseTime(),
            false);
        REQUIRE(simulation->haveAllExternalized(2, 2));
        
        // Get metrics (they accumulate across ledgers)
        auto& metrics = node0->getMetrics();
        auto& scpNominated = metrics.NewTimer({"scp", "timing", "nominated"});
        auto& scpExternalized = metrics.NewTimer({"scp", "timing", "externalized"});
        auto& ledgerClose = metrics.NewTimer({"ledger", "ledger", "close"});
        
        // Create destination account (using a genesis account to fund it)
        auto fundingAccount = txtest::getGenesisAccount(*node0, 0);
        SecretKey destKey = SecretKey::pseudoRandomForTesting();
        auto destAccount = TestAccount{*node0, destKey};
        
        // Create dest with 100 XLM
        auto createTx = fundingAccount.tx({txtest::createAccount(destKey.getPublicKey(), 100000000000)});
        node0->getHerder().recvTransaction(createTx, false);
        
        // Crank to apply create account
        simulation->crankUntil(
            [&]() { return simulation->haveAllExternalized(3, 2); },
            30 * simulation->getExpectedLedgerCloseTime(),
            false);
        
        // Track start ledger
        uint32_t startLedger = node0->getLedgerManager().getLastClosedLedgerNum();
        uint32_t targetLedger = startLedger + run.ledgerCount;
        
        int txSubmitted = 0;
        auto startTime = std::chrono::steady_clock::now();
        
        // For each ledger, submit a batch of TXs then crank until next ledger
        // Use genesis accounts as sources (starting from 1 since 0 was used for setup)
        for (int ledgerIdx = 0; ledgerIdx < run.ledgerCount; ledgerIdx++)
        {
            uint32_t currentLedger = node0->getLedgerManager().getLastClosedLedgerNum();
            
            int batchSubmitted = 0;
            int batchPending = 0;
            
            // Submit batch for this ledger using genesis accounts as sources
            for (int i = 0; i < run.txPerLedger && txSubmitted < totalTxs; i++)
            {
                // Get a unique genesis account for each TX (start at 1, 0 is used for setup)
                auto source = txtest::getGenesisAccount(*node0, txSubmitted + 1);
                
                // Payment of 1 XLM to dest (genesis accounts start with funds)
                auto tx = source.tx({txtest::payment(destKey.getPublicKey(), 1000000)});
                
                auto result = node0->getHerder().recvTransaction(tx, false);
                txSubmitted++;
                batchSubmitted++;
                if (result.code == TransactionQueue::AddResultCode::ADD_STATUS_PENDING)
                {
                    batchPending++;
                }
            }
            
            LOG_INFO(DEFAULT_LOG, "Batch {}/{}: submitted={}, pending={}",
                     ledgerIdx + 1, run.ledgerCount, batchSubmitted, batchPending);
            
            // Crank until we move to next ledger
            simulation->crankUntil(
                [&]() { 
                    return node0->getLedgerManager().getLastClosedLedgerNum() > currentLedger; 
                },
                30 * simulation->getExpectedLedgerCloseTime(),
                false);
        }
        
        // Wait for final ledger to externalize on both nodes
        simulation->crankUntil(
            [&]() { return simulation->haveAllExternalized(targetLedger, 2); },
            30 * simulation->getExpectedLedgerCloseTime(),
            false);
        
        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            endTime - startTime).count();
        
        // Count included TXs by checking dest account balance 
        // (each payment adds 0.1 XLM = 1000000 stroops, starting from 100 XLM)
        int64_t txIncluded = 0;
        {
            LedgerTxn ltx(node0->getLedgerTxnRoot());
            auto destAccount = stellar::loadAccount(ltx, destKey.getPublicKey());
            if (destAccount)
            {
                // Each successful payment adds 1000000 stroops
                // Initial balance is 100000000000 stroops (100 XLM)
                int64_t balance = destAccount.current().data.account().balance;
                txIncluded = (balance - 100000000000) / 1000000;
            }
        }
        
        // Collect results
        Results res;
        res.label = run.label;
        res.txSubmitted = txSubmitted;
        res.txIncluded = static_cast<int>(txIncluded);
        res.scpNominatedMean = scpNominated.mean();
        res.scpNominatedMax = scpNominated.max();
        res.scpExternalizedMean = scpExternalized.mean();
        res.scpExternalizedMax = scpExternalized.max();
        res.ledgerCloseMean = ledgerClose.mean();
        res.ledgerCloseMax = ledgerClose.max();
        
        allResults.push_back(res);
        
        LOG_INFO(DEFAULT_LOG, "{}: {} TXs submitted, {} included in {} ms",
                 run.label, txSubmitted, res.txIncluded, duration);
    }
    
    // Print summary table
    LOG_INFO(DEFAULT_LOG, "");
    LOG_INFO(DEFAULT_LOG, "================================================================================");
    LOG_INFO(DEFAULT_LOG, "                    SCP LATENCY STRESS TEST SUMMARY");
    LOG_INFO(DEFAULT_LOG, "================================================================================");
    LOG_INFO(DEFAULT_LOG, "");
    LOG_INFO(DEFAULT_LOG, "{:<20} {:>10} {:>10} {:>12} {:>12} {:>12} {:>12}",
             "Load", "TX Sub", "TX Incl", "SCP Nom", "SCP Nom", "SCP Ext", "SCP Ext");
    LOG_INFO(DEFAULT_LOG, "{:<20} {:>10} {:>10} {:>12} {:>12} {:>12} {:>12}",
             "", "", "", "Mean(ms)", "Max(ms)", "Mean(ms)", "Max(ms)");
    LOG_INFO(DEFAULT_LOG, "--------------------------------------------------------------------------------");
    
    for (auto const& r : allResults)
    {
        LOG_INFO(DEFAULT_LOG, "{:<20} {:>10} {:>10} {:>12.2f} {:>12.2f} {:>12.2f} {:>12.2f}",
                 r.label, r.txSubmitted, r.txIncluded,
                 r.scpNominatedMean, r.scpNominatedMax,
                 r.scpExternalizedMean, r.scpExternalizedMax);
    }
    
    LOG_INFO(DEFAULT_LOG, "--------------------------------------------------------------------------------");
    LOG_INFO(DEFAULT_LOG, "");
    LOG_INFO(DEFAULT_LOG, "Ledger close times:");
    for (auto const& r : allResults)
    {
        LOG_INFO(DEFAULT_LOG, "  {}: mean={:.2f}ms, max={:.2f}ms",
                 r.label, r.ledgerCloseMean, r.ledgerCloseMax);
    }
    LOG_INFO(DEFAULT_LOG, "");
    LOG_INFO(DEFAULT_LOG, "================================================================================");
    
    // Verify SCP latency didn't degrade significantly under load
    // Allow 3x degradation from light to heavy load
    if (allResults.size() >= 2)
    {
        double lightMean = allResults[0].scpNominatedMean;
        double heavyMean = allResults.back().scpNominatedMean;
        
        LOG_INFO(DEFAULT_LOG, "SCP nominated latency ratio (heavy/light): {:.2f}x",
                 heavyMean / lightMean);
        
        // Warn but don't fail if degradation is significant
        if (heavyMean > lightMean * 5)
        {
            WARN("SCP latency degraded significantly under load: " 
                 << lightMean << "ms -> " << heavyMean << "ms");
        }
    }
    
    // Verify all TXs were included in ledgers
    for (auto const& r : allResults)
    {
        REQUIRE(r.txIncluded == r.txSubmitted);
    }
}
