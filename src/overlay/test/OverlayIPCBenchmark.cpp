// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "lib/catch.hpp"
#include "overlay/OverlayIPC.h"
#include "util/Logging.h"
#include "util/TmpDir.h"
#include "xdr/Stellar-transaction.h"
#include "xdrpp/marshal.h"

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace stellar;

namespace
{

std::string
requireOverlayBinary()
{
    auto overlayBinary = OverlayIPC::findOverlayBinaryPath();
    if (!overlayBinary)
    {
        FAIL("Skipping test - overlay binary not found");
    }
    return *overlayBinary;
}

struct BenchmarkResult
{
    size_t payloadSize;
    int iterations;
    double totalTimeMs;
    double avgLatencyMs;
    double throughputMBps;
    double minLatencyMs;
    double maxLatencyMs;
};

BenchmarkResult
benchmarkPayloadSize(OverlayIPC& ipc, size_t payloadSize, int iterations)
{
    BenchmarkResult result;
    result.payloadSize = payloadSize;
    result.iterations = iterations;

    std::vector<double> latencies;
    latencies.reserve(iterations);

    auto startTotal = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < iterations; i++)
    {
        auto start = std::chrono::high_resolution_clock::now();

        // Just call getTopTransactions to measure IPC latency
        // Payload size doesn't matter much here - mainly testing IPC overhead
        auto txs = ipc.getTopTransactions(payloadSize / 300);

        auto end = std::chrono::high_resolution_clock::now();
        double latencyMs =
            std::chrono::duration<double, std::milli>(end - start).count();
        latencies.push_back(latencyMs);
    }

    auto endTotal = std::chrono::high_resolution_clock::now();
    result.totalTimeMs =
        std::chrono::duration<double, std::milli>(endTotal - startTotal)
            .count();

    // Calculate stats
    double sum = 0;
    result.minLatencyMs = latencies[0];
    result.maxLatencyMs = latencies[0];

    for (double lat : latencies)
    {
        sum += lat;
        if (lat < result.minLatencyMs)
            result.minLatencyMs = lat;
        if (lat > result.maxLatencyMs)
            result.maxLatencyMs = lat;
    }

    result.avgLatencyMs = sum / iterations;

    // Calculate throughput (requests/sec)
    double totalSeconds = result.totalTimeMs / 1000.0;
    result.throughputMBps = iterations / totalSeconds;

    return result;
}

} // namespace

/**
 * Benchmark IPC performance with different payload sizes.
 *
 * This test measures the latency and throughput of the IPC channel
 * for various payload sizes to identify bottlenecks.
 *
 * Tagged with [.] and [benchmark] so it doesn't run by default.
 * Run with: stellar-core test '[ipc-benchmark]'
 */
TEST_CASE("IPC payload size benchmark", "[overlay-ipc-rust][.][benchmark]")
{
    std::string overlayBinary = requireOverlayBinary();

    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");
    CLOG_INFO(Overlay, "           IPC PAYLOAD SIZE BENCHMARK");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");
    CLOG_INFO(Overlay, "");

    TmpDir tmpDir("ipc-benchmark");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";

    // Start the Rust overlay process
    OverlayIPC ipc(socketPath, overlayBinary, 11625);
    ipc.start();

    // Wait for connection
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    REQUIRE(ipc.isConnected());

    CLOG_INFO(Overlay, "IPC connected, starting benchmarks...");
    CLOG_INFO(Overlay, "");

    // Test different request counts
    struct TestCase
    {
        size_t size;
        int iterations;
        std::string label;
    };

    std::vector<TestCase> testCases = {
        {1, 1000, "1 TX request"},     {10, 500, "10 TX request"},
        {100, 100, "100 TX request"},  {1000, 50, "1000 TX request"},
        {5000, 20, "5000 TX request"}, {10000, 10, "10000 TX request"},
    };

    std::vector<BenchmarkResult> results;

    for (auto const& tc : testCases)
    {
        CLOG_INFO(Overlay, "Benchmarking {} ({} iterations)...", tc.label,
                  tc.iterations);

        auto result = benchmarkPayloadSize(ipc, tc.size, tc.iterations);
        results.push_back(result);

        CLOG_INFO(Overlay, "  Avg latency: {:.3f} ms", result.avgLatencyMs);
        CLOG_INFO(Overlay, "  Throughput: {:.2f} MB/s", result.throughputMBps);
        CLOG_INFO(Overlay, "  Min/Max: {:.3f} / {:.3f} ms", result.minLatencyMs,
                  result.maxLatencyMs);
        CLOG_INFO(Overlay, "");
    }

    // Print summary table
    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");
    CLOG_INFO(Overlay, "                       SUMMARY");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");
    CLOG_INFO(Overlay, "");

    CLOG_INFO(Overlay, "{:<12} {:>10} {:>12} {:>12} {:>12} {:>12}", "Request",
              "Iterations", "Avg (ms)", "Min (ms)", "Max (ms)", "Throughput");
    CLOG_INFO(Overlay, "{:<12} {:>10} {:>12} {:>12} {:>12} {:>12}", "Size", "",
              "", "", "", "(req/s)");
    CLOG_INFO(Overlay, "--------------------------------------------"
                       "----------------------------------------");

    for (size_t i = 0; i < results.size(); i++)
    {
        auto const& r = results[i];
        CLOG_INFO(Overlay,
                  "{:<12} {:>10} {:>12.3f} {:>12.3f} {:>12.3f} {:>12.0f}",
                  testCases[i].label, r.iterations, r.avgLatencyMs,
                  r.minLatencyMs, r.maxLatencyMs, r.throughputMBps);
    }

    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");

    // Analysis: Check for performance cliffs
    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "Performance Analysis:");
    for (size_t i = 1; i < results.size(); i++)
    {
        double sizeRatio = static_cast<double>(results[i].payloadSize) /
                           results[i - 1].payloadSize;
        double latencyRatio =
            results[i].avgLatencyMs / results[i - 1].avgLatencyMs;

        if (latencyRatio > sizeRatio * 2)
        {
            CLOG_WARNING(Overlay,
                         "  Performance cliff at {}: latency increased {}x "
                         "while size increased {}x",
                         testCases[i].label, latencyRatio, sizeRatio);
        }
        else if (latencyRatio < sizeRatio * 0.5)
        {
            CLOG_INFO(Overlay,
                      "  Good scaling at {}: latency increased {}x while size "
                      "increased {}x",
                      testCases[i].label, latencyRatio, sizeRatio);
        }
    }

    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "Benchmark complete!");

    ipc.shutdown();
}

namespace
{
// Build a syntactically valid, realistically sized (~300 byte) signed payment
// envelope with a distinct source account per index. Signature is garbage --
// the Rust overlay trusts core-submitted txs and only hashes the bytes.
TransactionEnvelope
makeSyntheticPaymentTx(uint32_t index)
{
    TransactionEnvelope env(ENVELOPE_TYPE_TX);
    auto& tx = env.v1().tx;
    tx.sourceAccount.ed25519().at(0) = static_cast<uint8_t>(index);
    tx.sourceAccount.ed25519().at(1) = static_cast<uint8_t>(index >> 8);
    tx.sourceAccount.ed25519().at(2) = static_cast<uint8_t>(index >> 16);
    tx.fee = 100 + (index % 1000);
    tx.seqNum = 1;
    tx.memo.type(MEMO_TEXT);
    tx.memo.text() = "benchmark-synthetic-tx-pad";
    auto& op = tx.operations.emplace_back();
    op.body.type(PAYMENT);
    op.body.paymentOp().destination.ed25519().at(0) = 0x42;
    op.body.paymentOp().asset.type(ASSET_TYPE_NATIVE);
    op.body.paymentOp().amount = 1 + index;
    auto& sig = env.v1().signatures.emplace_back();
    sig.hint.at(0) = static_cast<uint8_t>(index);
    sig.signature.resize(64);
    return env;
}

// Build a syntactically valid, realistically sized (~700 B) signed Soroban SAC
// transfer envelope (InvokeHostFunction "transfer" with source-account auth,
// a 4-entry footprint and Soroban resources), distinct source per index.
// Signature is garbage -- the Rust overlay only hashes the bytes.
TransactionEnvelope
makeSyntheticSacTransferTx(uint32_t index)
{
    TransactionEnvelope env(ENVELOPE_TYPE_TX);
    auto& tx = env.v1().tx;
    tx.sourceAccount.ed25519().at(0) = static_cast<uint8_t>(index);
    tx.sourceAccount.ed25519().at(1) = static_cast<uint8_t>(index >> 8);
    tx.sourceAccount.ed25519().at(2) = static_cast<uint8_t>(index >> 16);
    tx.fee = 1'000'000;
    tx.seqNum = 1;

    SCAddress contractAddr(SC_ADDRESS_TYPE_CONTRACT);
    contractAddr.contractId().at(0) = 0x53; // arbitrary fixed SAC contract id
    contractAddr.contractId().at(1) = 0xac;

    SCVal from(SCV_ADDRESS);
    from.address().type(SC_ADDRESS_TYPE_ACCOUNT);
    from.address().accountId().ed25519() = tx.sourceAccount.ed25519();
    SCVal to(SCV_ADDRESS);
    to.address().type(SC_ADDRESS_TYPE_ACCOUNT);
    to.address().accountId().ed25519().at(0) = static_cast<uint8_t>(~index);
    SCVal amount(SCV_I128);
    amount.i128().hi = 0;
    amount.i128().lo = 1 + index;

    auto& op = tx.operations.emplace_back();
    op.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = op.body.invokeHostFunctionOp();
    ihf.hostFunction.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    auto& call = ihf.hostFunction.invokeContract();
    call.contractAddress = contractAddr;
    call.functionName = "transfer";
    call.args = {from, to, amount};

    auto& auth = ihf.auth.emplace_back();
    auth.credentials.type(SOROBAN_CREDENTIALS_SOURCE_ACCOUNT);
    auth.rootInvocation.function.type(
        SOROBAN_AUTHORIZED_FUNCTION_TYPE_CONTRACT_FN);
    auth.rootInvocation.function.contractFn() = call;

    tx.ext.v(1);
    auto& sorobanData = tx.ext.sorobanData();
    auto& resources = sorobanData.resources;
    resources.instructions = 2'000'000;
    resources.diskReadBytes = 2000;
    resources.writeBytes = 2000;

    LedgerKey instanceKey(CONTRACT_DATA);
    instanceKey.contractData().contract = contractAddr;
    instanceKey.contractData().key.type(SCV_LEDGER_KEY_CONTRACT_INSTANCE);
    instanceKey.contractData().durability = PERSISTENT;
    LedgerKey codeKey(CONTRACT_CODE);
    codeKey.contractCode().hash.at(0) = 0xc0;
    resources.footprint.readOnly = {instanceKey, codeKey};

    LedgerKey fromKey(ACCOUNT);
    fromKey.account().accountID.ed25519() = tx.sourceAccount.ed25519();
    LedgerKey toKey(ACCOUNT);
    toKey.account().accountID.ed25519() = to.address().accountId().ed25519();
    resources.footprint.readWrite = {fromKey, toKey};

    sorobanData.resourceFee = 900'000;

    auto& sig = env.v1().signatures.emplace_back();
    sig.hint.at(0) = static_cast<uint8_t>(index);
    sig.signature.resize(64);
    return env;
}
} // namespace

/**
 * Benchmark getTopTransactions against a POPULATED mempool: this measures the
 * real triggerNextLedger-path cost (Rust mempool walk + serialize + pipe
 * transfer + C++ XDR parse) rather than an empty round-trip.
 */
TEST_CASE("IPC getTopTransactions populated mempool benchmark",
          "[overlay-ipc-rust][.][benchmark]")
{
    std::string overlayBinary = requireOverlayBinary();

    struct TxKind
    {
        char const* name;
        TransactionEnvelope (*make)(uint32_t);
    };
    // Fresh overlay process + mempool per kind so fee-based eviction of one
    // shape by the other can't skew results.
    for (auto const& kind :
         {TxKind{"classic payment (~230 B)", makeSyntheticPaymentTx},
          TxKind{"Soroban SAC transfer (~700 B)", makeSyntheticSacTransferTx}})
    {
        TmpDir tmpDir("ipc-benchmark");
        std::string socketPath = tmpDir.getName() + "/overlay.sock";

        OverlayIPC ipc(socketPath, overlayBinary, 11625);
        ipc.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        REQUIRE(ipc.isConnected());

        size_t const TOTAL_TXS = 8000;
        size_t txBytes = xdr::xdr_to_opaque(kind.make(0)).size();
        printf("\n=== getTopTransactions populated mempool benchmark: %s "
               "(%zu B/tx) ===\n",
               kind.name, txBytes);
        printf("Submitting %zu synthetic txs to Rust mempool...\n", TOTAL_TXS);

        for (uint32_t i = 0; i < TOTAL_TXS; ++i)
        {
            auto env = kind.make(i);
            ipc.submitTransaction(env, 100 + (i % 1000), 1);
        }

        // Wait for async ingestion to finish.
        size_t inPool = 0;
        for (int attempt = 0; attempt < 100; ++attempt)
        {
            inPool = ipc.getTopTransactions(TOTAL_TXS).size();
            if (inPool == TOTAL_TXS)
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        printf("Mempool populated with %zu txs\n", inPool);
        REQUIRE(inPool == TOTAL_TXS);

        for (size_t count : {size_t(500), size_t(1000), size_t(2000),
                             size_t(4000), size_t(8000)})
        {
            int const iterations = 20;
            double totalMs = 0, minMs = 1e9, maxMs = 0;
            size_t got = 0;
            for (int i = 0; i < iterations; ++i)
            {
                auto start = std::chrono::steady_clock::now();
                auto txs = ipc.getTopTransactions(count);
                double ms = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - start)
                                .count();
                got = txs.size();
                totalMs += ms;
                minMs = std::min(minMs, ms);
                maxMs = std::max(maxMs, ms);
            }
            printf("getTopTransactions(%5zu) -> %5zu txs: avg %7.2f ms  min "
                   "%7.2f  max %7.2f\n",
                   count, got, totalMs / iterations, minMs, maxMs);
            fflush(stdout);
        }

        ipc.shutdown();
    }
}

/**
 * Benchmark concurrent IPC calls to measure contention.
 *
 * This test sends multiple requests in parallel to measure serialized IPC
 * throughput. IPC calls are serialized with a mutex since the channel is
 * not thread-safe (concurrent writes corrupt messages).
 */
TEST_CASE("IPC concurrent access benchmark", "[overlay-ipc-rust][.][benchmark]")
{
    std::string overlayBinary = requireOverlayBinary();

    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");
    CLOG_INFO(Overlay, "        IPC CONCURRENT ACCESS BENCHMARK");
    CLOG_INFO(Overlay, "============================================"
                       "========================================");

    TmpDir tmpDir("ipc-benchmark");
    std::string socketPath = tmpDir.getName() + "/overlay.sock";

    OverlayIPC ipc(socketPath, overlayBinary, 11625);
    ipc.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    REQUIRE(ipc.isConnected());

    // Test concurrent getTopTransactions calls
    size_t const numThreads = 4;
    size_t const callsPerThread = 100;
    size_t const payloadSize = 1024; // 1KB TXs

    CLOG_INFO(Overlay, "Testing {} threads, {} calls each, {} byte payloads",
              numThreads, callsPerThread, payloadSize);

    auto startTime = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    std::vector<double> threadTimes(numThreads);
    std::mutex ipcMutex; // Serialize IPC access (channel not thread-safe)

    for (size_t t = 0; t < numThreads; t++)
    {
        threads.emplace_back([&, t]() {
            auto threadStart = std::chrono::high_resolution_clock::now();

            for (size_t i = 0; i < callsPerThread; i++)
            {
                std::lock_guard<std::mutex> lock(ipcMutex);
                auto txs = ipc.getTopTransactions(10);
                // Just query, don't validate results
            }

            auto threadEnd = std::chrono::high_resolution_clock::now();
            threadTimes[t] = std::chrono::duration<double, std::milli>(
                                 threadEnd - threadStart)
                                 .count();
        });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    double totalTimeMs =
        std::chrono::duration<double, std::milli>(endTime - startTime).count();

    CLOG_INFO(Overlay, "");
    CLOG_INFO(Overlay, "Results:");
    CLOG_INFO(Overlay, "  Total wall time: {:.2f} ms", totalTimeMs);
    CLOG_INFO(Overlay, "  Total calls: {}", numThreads * callsPerThread);
    CLOG_INFO(Overlay, "  Calls/sec: {:.0f}",
              (numThreads * callsPerThread) / (totalTimeMs / 1000.0));

    for (size_t i = 0; i < numThreads; i++)
    {
        CLOG_INFO(Overlay, "  Thread {} time: {:.2f} ms", i, threadTimes[i]);
    }

    ipc.shutdown();
}
