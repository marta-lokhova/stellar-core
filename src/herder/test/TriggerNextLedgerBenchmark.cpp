// Copyright 2026 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

// Benchmark decomposing the latency-critical tx-set construction portion of
// HerderImpl::triggerNextLedger into its component stages, to identify where
// the time goes and quantify candidate optimizations. Runs the pipeline for
// two transaction shapes: minimal classic 1-op payments (~200 B) and real
// Soroban SAC transfers (~1 KB), since the per-byte stages (XDR encode/decode,
// hashing, frame build) scale with envelope size while signature verification
// does not.
//
// Run with: stellar-core test --ll error "[triggerbench]"

#include "crypto/Hex.h"
#include "crypto/SecretKey.h"
#include "herder/TxSetFrame.h"
#include "herder/TxSetUtils.h"
#include "ledger/LedgerManager.h"
#include "lib/catch.hpp"
#include "main/Application.h"
#include "main/Config.h"
#include "test/TestAccount.h"
#include "test/TestUtils.h"
#include "test/TxTests.h"
#include "test/test.h"
#include "transactions/TransactionFrameBase.h"
#include "transactions/test/SorobanTxTestUtils.h"
#include "util/Logging.h"
#include "xdr/Stellar-transaction.h"
#include "xdrpp/marshal.h"

#include <chrono>
#include <cstdio>
#include <numeric>
#include <thread>
#include <vector>

using namespace stellar;
using namespace stellar::txtest;

namespace
{

template <typename F>
double
timeMs(F&& f)
{
    auto start = std::chrono::steady_clock::now();
    f();
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - start)
        .count();
}

struct SigWork
{
    PublicKey key;
    Signature sig;
    Hash payload;
};

// Verify all signatures using `numThreads` threads; populates the global
// verify-sig cache exactly like checkValid would.
double
parallelPrewarm(std::vector<SigWork> const& work, size_t numThreads)
{
    return timeMs([&] {
        std::vector<std::thread> threads;
        std::atomic<size_t> next{0};
        for (size_t t = 0; t < numThreads; ++t)
        {
            threads.emplace_back([&] {
                for (size_t i = next.fetch_add(1); i < work.size();
                     i = next.fetch_add(1))
                {
                    PubKeyUtils::verifySig(work[i].key, work[i].sig,
                                           work[i].payload);
                }
            });
        }
        for (auto& t : threads)
        {
            t.join();
        }
    });
}

// Run the full triggerNextLedger tx-set pipeline decomposition over the given
// pre-built envelopes. `soroban` selects which phase the txs are placed in.
void
runPipelineBenchmark(Application& app, char const* label,
                     std::vector<TransactionEnvelope> const& allEnvs,
                     bool soroban)
{
    auto const& lclHeader = app.getLedgerManager().getLastClosedLedgerHeader();
    Hash const& networkID = app.getNetworkID();
    unsigned const hw = std::max(2u, std::thread::hardware_concurrency());

    printf("\n=== triggerNextLedger pipeline benchmark: %s ===\n", label);
    printf("hardware threads: %u\n", hw);

    for (size_t N : {size_t(1024), size_t(2048), allEnvs.size()})
    {
        std::vector<TransactionEnvelope> envs(allEnvs.begin(),
                                              allEnvs.begin() + N);

        auto makePhases = [&](TxFrameList const& list) {
            PerPhaseTransactionList phases;
            if (soroban)
            {
                phases.emplace_back(); // empty classic phase
                phases.emplace_back(list);
            }
            else
            {
                phases.emplace_back(list);
                phases.emplace_back(); // empty soroban phase
            }
            return phases;
        };

        // --- Stage A: XDR-encode all envelopes (Rust->IPC payload build,
        // also equals the C++ submit-side encode) ---
        std::vector<std::vector<uint8_t>> encoded;
        encoded.reserve(N);
        double tEncode = timeMs([&] {
            for (auto const& e : envs)
            {
                auto v = xdr::xdr_to_opaque(e);
                encoded.emplace_back(v.begin(), v.end());
            }
        });
        size_t totalBytes = 0;
        for (auto const& b : encoded)
        {
            totalBytes += b.size();
        }

        // --- Stage B: XDR-decode all envelopes, with the per-tx heap copy
        // exactly as OverlayIPC::getTopTransactions does ---
        std::vector<TransactionEnvelope> decoded;
        decoded.reserve(N);
        double tDecode = timeMs([&] {
            for (auto const& b : encoded)
            {
                std::vector<uint8_t> txData(b.begin(), b.end());
                TransactionEnvelope e;
                xdr::xdr_from_opaque(txData, e);
                decoded.push_back(std::move(e));
            }
        });

        // --- Stage C: build TransactionFrames ---
        TxFrameList txs;
        txs.reserve(N);
        double tFrames = timeMs([&] {
            for (auto const& e : decoded)
            {
                txs.push_back(
                    TransactionFrameBase::makeTransactionFromWire(networkID,
                                                                  e));
            }
        });

        // --- Stage C2: contents + full hash of every tx (serial, as the
        // main thread pays it today via prewarm collection and set sorting;
        // each is an XDR re-serialization plus SHA-256) ---
        double tContentsHash =
            timeMs([&] {
                for (auto const& tx : txs)
                {
                    tx->getContentsHash();
                }
            });
        double tFullHash = timeMs([&] {
            for (auto const& tx : txs)
            {
                tx->getFullHash();
            }
        });

        // Collect signature work for the prewarm experiment.
        std::vector<SigWork> sigWork;
        sigWork.reserve(N);
        for (size_t i = 0; i < N; ++i)
        {
            auto const& env = txs[i]->getEnvelope();
            REQUIRE(env.type() == ENVELOPE_TYPE_TX);
            REQUIRE(env.v1().signatures.size() == 1);
            SigWork w;
            w.key = txs[i]->getSourceID();
            w.sig = env.v1().signatures[0].signature;
            w.payload = txs[i]->getContentsHash();
            sigWork.push_back(std::move(w));
        }

        auto trim = [&](TxFrameList const& list) {
            UnorderedMap<AccountID, int64_t> accountFeeMap;
            TxFrameList invalid;
            auto valid =
                TxSetUtils::trimInvalid(list, app, accountFeeMap, 0, 0,
                                        invalid);
            REQUIRE(invalid.empty());
            return valid;
        };

        // --- Stage D: trimInvalid (per-tx checkValid) with cold sig cache ---
        PubKeyUtils::clearVerifySigCache();
        uint64_t hits = 0, misses = 0;
        PubKeyUtils::flushVerifySigCacheCounts(hits, misses);
        double tTrimCold = timeMs([&] { trim(txs); });
        PubKeyUtils::flushVerifySigCacheCounts(hits, misses);
        uint64_t coldMisses = misses;

        // --- Stage E: trimInvalid with warm sig cache ---
        double tTrimWarm = timeMs([&] { trim(txs); });

        // --- Stage F: full makeTxSetFromTransactions (warm cache) ---
        PerPhaseTransactionList txPhases = makePhases(txs);
        TxSetXDRFrameConstPtr proposedSet;
        ApplicableTxSetFrameConstPtr applicableSet;
        double tMakeWarm = timeMs([&] {
            std::tie(proposedSet, applicableSet) =
                makeTxSetFromTransactions(txPhases, app, 0, 0);
        });
        REQUIRE(applicableSet);
        REQUIRE(applicableSet->sizeTxTotal() == N);

        // --- Stage G: decompose the wire roundtrip ---
        // G1: re-encode the produced set (what cacheTxSet sends to Rust)
        GeneralizedTransactionSet gts;
        std::vector<uint8_t> setBytes;
        double tEncodeSet = timeMs([&] {
            proposedSet->toXDR(gts);
            auto v = xdr::xdr_to_opaque(gts);
            setBytes.assign(v.begin(), v.end());
        });
        // G2: hash the set (part of TxSetXDRFrame construction)
        double tHashSet = timeMs([&] {
            auto frame = TxSetXDRFrame::makeFromWire(gts);
            REQUIRE(frame->getContentsHash() == proposedSet->getContentsHash());
        });
        // G3: prepareForApply (second decode + frame build + sorting;
        // done inside makeTxSetFromTransactions as the "roundtrip")
        double tPrepare = timeMs([&] {
            auto prepared = proposedSet->prepareForApply(app, lclHeader.header);
            REQUIRE(prepared);
        });
        // G4: raw sha256 over the already-encoded bytes -- what the contents
        // hash would cost if we reused the encoded XDR instead of
        // re-serializing inside xdrSha256.
        double tRawHash = timeMs([&] {
            auto h = sha256(setBytes);
            REQUIRE(h == proposedSet->getContentsHash());
        });
        // G5: surge pricing + encode alone (validation skipped).
        double tSurgeOnly = timeMs([&] {
            auto [s, a] = makeTxSetFromTransactions(
                txPhases, app, 0, 0, /* skipValidation */ true);
            REQUIRE(s);
        });

        // --- Stage H: end-to-end simulated trigger tx-set portion, cold ---
        PubKeyUtils::clearVerifySigCache();
        double tTotalCold = timeMs([&] {
            std::vector<TransactionEnvelope> decoded2;
            decoded2.reserve(N);
            for (auto const& b : encoded)
            {
                std::vector<uint8_t> txData(b.begin(), b.end());
                TransactionEnvelope e;
                xdr::xdr_from_opaque(txData, e);
                decoded2.push_back(std::move(e));
            }
            TxFrameList txs2;
            txs2.reserve(N);
            for (auto const& e : decoded2)
            {
                txs2.push_back(TransactionFrameBase::makeTransactionFromWire(
                    networkID, e));
            }
            auto phases2 = makePhases(txs2);
            auto [set2, app2] = makeTxSetFromTransactions(phases2, app, 0, 0);
            REQUIRE(app2);
            GeneralizedTransactionSet g2;
            set2->toXDR(g2);
            auto v = xdr::xdr_to_opaque(g2);
            REQUIRE(!v.empty());
        });

        // --- Stage I: parallel signature prewarm + warm rebuild ---
        double tPrewarm1 = 0, tPrewarmHw = 0;
        {
            PubKeyUtils::clearVerifySigCache();
            tPrewarm1 = parallelPrewarm(sigWork, 1);
        }
        {
            // Production helper, as called from triggerNextLedger.
            PubKeyUtils::clearVerifySigCache();
            uint64_t h = 0, m = 0;
            PubKeyUtils::flushVerifySigCacheCounts(h, m); // reset counters
            tPrewarmHw =
                timeMs([&] { TxSetUtils::prewarmSignatureCache(txs, hw); });
            PubKeyUtils::flushVerifySigCacheCounts(h, m);
            REQUIRE(m == N); // all sigs verified by the prewarm
        }
        double tMakeAfterPrewarm = timeMs([&] {
            auto [s, a] = makeTxSetFromTransactions(txPhases, app, 0, 0);
            REQUIRE(a);
        });

        printf("\n--- %s: N = %zu txs (%zu KB total, %zu B avg/tx) ---\n",
               label, N, totalBytes / 1024, totalBytes / N);
        printf("A  encode envelopes (Rust-side sim)     : %8.2f ms\n",
               tEncode);
        printf("B  decode envelopes (IPC parse)         : %8.2f ms\n",
               tDecode);
        printf("C  makeTransactionFromWire              : %8.2f ms\n",
               tFrames);
        printf("C2 contents hash (re-encode + sha256)   : %8.2f ms\n",
               tContentsHash);
        printf("C3 full hash (re-encode + sha256)       : %8.2f ms\n",
               tFullHash);
        printf("D  trimInvalid, cold sig cache          : %8.2f ms  (%llu sig "
               "verifies)\n",
               tTrimCold, static_cast<unsigned long long>(coldMisses));
        printf("E  trimInvalid, warm sig cache          : %8.2f ms\n",
               tTrimWarm);
        printf("F  makeTxSetFromTransactions (warm)     : %8.2f ms\n",
               tMakeWarm);
        printf("G1 re-encode tx set (cacheTxSet payload): %8.2f ms  (%zu KB)\n",
               tEncodeSet, setBytes.size() / 1024);
        printf("G2 tx set contents hash                 : %8.2f ms\n",
               tHashSet);
        printf("G3 prepareForApply (roundtrip decode)   : %8.2f ms\n",
               tPrepare);
        printf("G4 sha256 over pre-encoded set bytes    : %8.2f ms\n",
               tRawHash);
        printf("G5 surge pricing + encode (no validate) : %8.2f ms\n",
               tSurgeOnly);
        printf("H  END-TO-END cold (decode..encode set) : %8.2f ms\n",
               tTotalCold);
        printf("I  sig prewarm 1 thread                 : %8.2f ms\n",
               tPrewarm1);
        printf("I  sig prewarm %2u threads               : %8.2f ms\n", hw,
               tPrewarmHw);
        printf("I  makeTxSetFromTransactions after warm : %8.2f ms\n",
               tMakeAfterPrewarm);
        printf("   => projected trigger with prewarmed sigs: %.2f ms "
               "(vs %.2f ms cold)\n",
               tDecode + tFrames + tMakeAfterPrewarm, tTotalCold);
        fflush(stdout);
    }
}

} // namespace

TEST_CASE("trigger next ledger pipeline benchmark", "[triggerbench][!hide]")
{
    size_t const MAX_N = 6000;

    Config cfg = getTestConfig(0, Config::TESTDB_BUCKET_DB_PERSISTENT);
    // SAC transfers use disjoint (source, destination) account pairs so the
    // parallel Soroban phase sees independent footprints.
    cfg.GENESIS_TEST_ACCOUNT_COUNT = static_cast<uint32_t>(2 * MAX_N);
    cfg.TESTING_UPGRADE_MAX_TX_SET_SIZE = static_cast<uint32_t>(MAX_N);

    // Raise ledger-wide Soroban limits so all MAX_N SAC transfers fit in one
    // tx set; per-tx limits come from the standard test limits.
    SorobanTest test(cfg, true, [&](SorobanNetworkConfig& sorobanCfg) {
        sorobanCfg.mLedgerMaxTxCount = static_cast<uint32_t>(MAX_N);
        sorobanCfg.mLedgerMaxInstructions =
            static_cast<int64_t>(MAX_N) * 4'000'000;
        sorobanCfg.mLedgerMaxDiskReadEntries =
            static_cast<uint32_t>(MAX_N) * 8;
        sorobanCfg.mLedgerMaxWriteLedgerEntries =
            static_cast<uint32_t>(MAX_N) * 4;
        sorobanCfg.mLedgerMaxDiskReadBytes =
            static_cast<uint32_t>(MAX_N) * 4000;
        sorobanCfg.mLedgerMaxWriteBytes = static_cast<uint32_t>(MAX_N) * 4000;
        sorobanCfg.mLedgerMaxTransactionsSizeBytes =
            static_cast<uint32_t>(MAX_N) * 4000;
        sorobanCfg.mLedgerMaxDependentTxClusters = 4;
    });
    Application& app = test.getApp();

    // Classic: one valid, signed 1-op payment per genesis account.
    std::vector<TransactionEnvelope> paymentEnvs;
    paymentEnvs.reserve(MAX_N);
    for (size_t i = 0; i < MAX_N; ++i)
    {
        auto acc = getGenesisAccount(app, static_cast<uint32_t>(i));
        auto tx = acc.tx({payment(acc.getPublicKey(), 1)});
        paymentEnvs.push_back(tx->getEnvelope());
    }

    // Soroban: one valid, signed native-asset SAC transfer per disjoint
    // (source, destination) genesis account pair.
    AssetContractTestClient sacClient(test, makeNativeAsset());
    std::vector<TransactionEnvelope> sacEnvs;
    sacEnvs.reserve(MAX_N);
    for (size_t i = 0; i < MAX_N; ++i)
    {
        auto from = getGenesisAccount(app, static_cast<uint32_t>(2 * i));
        auto to = getGenesisAccount(app, static_cast<uint32_t>(2 * i + 1));
        auto toAddr = makeAccountAddress(to.getPublicKey());
        auto tx = sacClient.getTransferTx(from, toAddr, 1);
        sacEnvs.push_back(tx->getEnvelope());
    }

    runPipelineBenchmark(app, "classic 1-op payment", paymentEnvs, false);
    runPipelineBenchmark(app, "Soroban SAC transfer", sacEnvs, true);
}

// Correctness: prewarming the signature cache must not change which
// transactions trimInvalid accepts or rejects, for valid txs, corrupted
// signatures, and signatures the prewarm heuristic skips.
TEST_CASE("prewarmSignatureCache does not change validation results",
          "[txset][triggerprewarm]")
{
    Config cfg = getTestConfig(0, Config::TESTDB_BUCKET_DB_PERSISTENT);
    cfg.GENESIS_TEST_ACCOUNT_COUNT = 64;

    VirtualClock clock;
    Application::pointer app = createTestApplication(clock, cfg, true);

    TxFrameList txs;
    for (uint32_t i = 0; i < 64; ++i)
    {
        auto acc = getGenesisAccount(*app, i);
        auto tx = acc.tx({payment(acc.getPublicKey(), 1)});
        auto env = tx->getEnvelope();
        if (i % 4 == 1)
        {
            // Corrupt the signature: must stay invalid with prewarm.
            env.v1().signatures[0].signature[5] ^= 0xff;
        }
        else if (i % 4 == 2)
        {
            // Corrupt the hint so the prewarm heuristic skips this tx and
            // validation takes the cold path. Signature itself is fine but
            // an unmatchable hint means the signature can't be used.
            env.v1().signatures[0].hint[0] ^= 0xff;
        }
        txs.push_back(TransactionFrameBase::makeTransactionFromWire(
            app->getNetworkID(), env));
    }

    auto runTrim = [&] {
        UnorderedMap<AccountID, int64_t> feeMap;
        TxFrameList invalid;
        auto valid = TxSetUtils::trimInvalid(txs, *app, feeMap, 0, 0, invalid);
        std::vector<Hash> validHashes, invalidHashes;
        for (auto const& t : valid)
        {
            validHashes.push_back(t->getFullHash());
        }
        for (auto const& t : invalid)
        {
            invalidHashes.push_back(t->getFullHash());
        }
        return std::make_pair(validHashes, invalidHashes);
    };

    PubKeyUtils::clearVerifySigCache();
    auto baseline = runTrim();

    PubKeyUtils::clearVerifySigCache();
    TxSetUtils::prewarmSignatureCache(txs, 4);
    auto withPrewarm = runTrim();

    REQUIRE(baseline.first == withPrewarm.first);
    REQUIRE(baseline.second == withPrewarm.second);
    // Sanity: some txs valid, some invalid.
    REQUIRE(!baseline.first.empty());
    REQUIRE(!baseline.second.empty());
}
