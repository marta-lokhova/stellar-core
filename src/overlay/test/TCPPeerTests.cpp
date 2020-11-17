// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "fmt/format.h"
#include "ledger/LedgerManager.h"
#include "lib/catch.hpp"
#include "main/Application.h"
#include "main/Config.h"
#include "overlay/OverlayManager.h"
#include "overlay/PeerBareAddress.h"
#include "overlay/PeerDoor.h"
#include "overlay/TCPPeer.h"
#include "simulation/Simulation.h"
#include "test/TestAccount.h"
#include "test/TxTests.h"
#include "test/test.h"
#include "util/Logging.h"
#include "util/Timer.h"

namespace stellar
{

TEST_CASE("TCPPeer can communicate", "[overlay][acceptance]")
{
    Hash networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
    Simulation::pointer s =
        std::make_shared<Simulation>(Simulation::OVER_TCP, networkID);

    auto v10SecretKey = SecretKey::fromSeed(sha256("v10"));
    auto v11SecretKey = SecretKey::fromSeed(sha256("v11"));

    SCPQuorumSet n0_qset;
    n0_qset.threshold = 1;
    n0_qset.validators.push_back(v10SecretKey.getPublicKey());
    auto n0 = s->addNode(v10SecretKey, n0_qset);

    SCPQuorumSet n1_qset;
    n1_qset.threshold = 1;
    n1_qset.validators.push_back(v11SecretKey.getPublicKey());
    auto n1 = s->addNode(v11SecretKey, n1_qset);

    s->addPendingConnection(v10SecretKey.getPublicKey(),
                            v11SecretKey.getPublicKey());
    s->startAllNodes();
    s->crankForAtLeast(std::chrono::seconds(1), false);

    auto p0 = n0->getOverlayManager().getConnectedPeer(
        PeerBareAddress{"127.0.0.1", n1->getConfig().PEER_PORT});

    auto p1 = n1->getOverlayManager().getConnectedPeer(
        PeerBareAddress{"127.0.0.1", n0->getConfig().PEER_PORT});

    REQUIRE(p0);
    REQUIRE(p1);
    REQUIRE(p0->isAuthenticated());
    REQUIRE(p1->isAuthenticated());
    s->stopAllNodes();
}

TEST_CASE("write queue benchmark", "[overlay][!hide]")
{
    auto test = [&](bool background, int batch) {
        Hash networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
        Simulation::pointer s =
            std::make_shared<Simulation>(Simulation::OVER_TCP, networkID);

        auto v10SecretKey = SecretKey::fromSeed(sha256("v10"));
        auto v11SecretKey = SecretKey::fromSeed(sha256("v11"));

        auto cfg1 = getTestConfig(1);
        auto cfg2 = getTestConfig(2);

        if (!background)
        {
            cfg1.HIGH_PRIORITY_WORKER_THREADS = 0;
            cfg2.HIGH_PRIORITY_WORKER_THREADS = 0;
        }

        SCPQuorumSet n0_qset;
        n0_qset.threshold = 1;
        n0_qset.validators.push_back(v10SecretKey.getPublicKey());
        auto n0 = s->addNode(v10SecretKey, n0_qset, &cfg1);

        SCPQuorumSet n1_qset;
        n1_qset.threshold = 1;
        n1_qset.validators.push_back(v11SecretKey.getPublicKey());
        auto n1 = s->addNode(v11SecretKey, n1_qset, &cfg2);

        s->addPendingConnection(v10SecretKey.getPublicKey(),
                                v11SecretKey.getPublicKey());
        s->startAllNodes();
        s->crankForAtLeast(std::chrono::seconds(1), false);

        auto p1 = n0->getOverlayManager().getConnectedPeer(
            PeerBareAddress{"127.0.0.1", n1->getConfig().PEER_PORT});

        auto p0 = n1->getOverlayManager().getConnectedPeer(
            PeerBareAddress{"127.0.0.1", n0->getConfig().PEER_PORT});

        REQUIRE(p0);
        REQUIRE(p1);
        REQUIRE(p0->isAuthenticated());
        REQUIRE(p1->isAuthenticated());

        auto lcl = n1->getLedgerManager().getLastClosedLedgerHeader();
        auto a1 = TestAccount{*n1, txtest::getAccount("A")};
        auto b1 = TestAccount{*n1, txtest::getAccount("B")};

        auto cur1 = b1.asset("CUR1");
        auto cur2 = b1.asset("CUR2");

        auto addTransactionsEx = [&](TxSetFramePtr txSet, int n,
                                     TestAccount& t) {
            txSet->mTransactions.resize(n);
            std::generate(std::begin(txSet->mTransactions),
                          std::end(txSet->mTransactions), [&]() {
                              return a1.tx({txtest::manageOffer(
                                  123, cur1, cur2, Price{3, 5}, 20)});
                          });
        };
        auto addTransactions =
            std::bind(addTransactionsEx, std::placeholders::_1,
                      std::placeholders::_2, a1);

        auto makeTransactions = [&](Hash hash, int n) {
            auto result = std::make_shared<TxSetFrame>(hash);
            addTransactions(result, n);
            return result;
        };

        auto transactions = makeTransactions(lcl.hash, 999);
        n1->getHerder().recvTxSet(transactions->getContentsHash(),
                                  *transactions);

        int numMesssages = 100000;
        StellarMessage newMsg;
        newMsg.type(GET_TX_SET);
        newMsg.txSetHash() = transactions->getContentsHash();

        while (numMesssages > 0)
        {
            n1->postOnMainThread(
                [&]() {
                    for (int i = 0; i < batch; i++)
                    {
                        p1->recvGetTxSet(newMsg);
                        if (--numMesssages <= 0)
                        {
                            break;
                        }
                    }
                },
                "message batch");
            s->crankForAtLeast(std::chrono::milliseconds(10), false);
        }

        auto& m1 =
            n0->getMetrics().NewMeter({"overlay", "async", "write"}, "call");
        auto& m2 = n0->getMetrics().NewMeter({"overlay", "message", "write"},
                                             "message");

        std::string type = background ? "BACKGROUND" : "MAIN";

        LOG(ERROR) << fmt::format(
            "{}: batch {}, total calls {}, rate (calls) {}", type, batch,
            m1.count(), m1.one_minute_rate());
        LOG(ERROR) << fmt::format(
            "{}: batch {}, total messages {}, rate (messages) {}", type, batch,
            m2.count(), m2.one_minute_rate());
    };

    SECTION("background")
    {
        SECTION("batch is 100")
        {
            test(true, 100);
        }
        SECTION("batch is 500")
        {
            test(true, 500);
        }
        SECTION("batch is 1000")
        {
            test(true, 1000);
        }
    }
    SECTION("main thread")
    {
        SECTION("batch is 100")
        {
            test(false, 100);
        }
        SECTION("batch is 500")
        {
            test(false, 500);
        }
        SECTION("batch is 1000")
        {
            test(false, 1000);
        }
    }
}
}
