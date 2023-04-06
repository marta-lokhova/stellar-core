// Copyright 2023 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "crypto/KeyUtils.h"
#include "crypto/SecretKey.h"
#include "lib/catch.hpp"
#include "main/Application.h"
#include "main/Config.h"
#include "overlay/BanManager.h"
#include "overlay/OverlayManagerImpl.h"
#include "overlay/PeerManager.h"
#include "overlay/TCPPeer.h"
#include "overlay/test/LoopbackPeer.h"
#include "overlay/test/OverlayTestUtils.h"
#include "simulation/Simulation.h"
#include "simulation/Topologies.h"
#include "test/TestUtils.h"
#include "test/test.h"
#include "util/Logging.h"
#include "util/ProtocolVersion.h"
#include "util/Timer.h"

#include "herder/HerderImpl.h"
#include "medida/meter.h"
#include "medida/metrics_registry.h"
#include "medida/timer.h"
#include <fmt/format.h>
#include <numeric>

using namespace stellar;
using namespace stellar::overlaytestutils;

namespace
{

void
dfs(Application& app, std::unordered_set<NodeID>& visited)
{
    visited.emplace(app.getConfig().NODE_SEED.getPublicKey());
    for (auto const& node : app.getOverlayManager().getAuthenticatedPeers())
    {
        if (visited.find(node.first) == visited.end())
        {
            auto& overlayApp = static_cast<ApplicationLoopbackOverlay&>(app);
            auto port = node.second->getAddress().getPort();
            auto otherApp = overlayApp.getSim().getAppFromPeerMap(port);
            dfs(*otherApp, visited);
        }
    }
}

bool
isConnected(int numNodes, int numWatchers, Simulation::pointer simulation)
{
    // Verify graph is connected
    std::unordered_set<NodeID> visited;
    dfs(*simulation->getNodes()[0], visited);
    return visited.size() == (numNodes + numWatchers);
}

void
logTopologyInfo(Simulation::pointer simulation)
{
    for (auto const& node : simulation->getNodes())
    {
        CLOG_INFO(
            Overlay,
            "Connections for node ({}) {} --> outbound {}/inbound "
            "{}, LCL={}",
            (node->getConfig().NODE_IS_VALIDATOR ? "validator" : "watcher"),
            node->getConfig().toShortString(
                node->getConfig().NODE_SEED.getPublicKey()),
            node->getOverlayManager().getOutboundAuthenticatedPeers().size(),
            node->getOverlayManager().getInboundAuthenticatedPeers().size(),
            node->getLedgerManager().getLastClosedLedgerNum());
    }
    CLOG_INFO(Overlay, "TOTAL CONNECTIONS {}",
              numberOfSimulationConnections(simulation));
}

void
getGraphMatrix(Application& app, std::unordered_set<NodeID>& visited,
               Json::Value& res)
{
    visited.emplace(app.getConfig().NODE_SEED.getPublicKey());
    auto id = KeyUtils::toStrKey(app.getConfig().NODE_SEED.getPublicKey());
    for (auto const& node : app.getOverlayManager().getAuthenticatedPeers())
    {
        auto& overlayApp = static_cast<ApplicationLoopbackOverlay&>(app);
        auto port = node.second->getAddress().getPort();
        auto otherApp = overlayApp.getSim().getAppFromPeerMap(port);
        Json::Value pp;
        pp[KeyUtils::toStrKey(otherApp->getConfig().NODE_SEED.getPublicKey())] =
            otherApp->getConfig().NODE_IS_VALIDATOR;
        res[id]["peers"].append(pp);
        if (visited.find(node.first) == visited.end())
        {
            getGraphMatrix(*otherApp, visited, res);
        }
    }
}

void
exportGraphJson(Json::Value graphJson, bool automaticPeers, int testID)
{
    std::string autoPeers = automaticPeers ? "auto-peers" : "no-auto-peers";
    std::string refJsonPath =
        fmt::format("src/testdata/test-{}-topology-{}-{}.json", testID,
                    autoPeers, std::to_string(rand_uniform(1, 100000)));

    std::ofstream outJson;
    outJson.exceptions(std::ios::failbit | std::ios::badbit);
    outJson.open(refJsonPath);

    outJson.write(graphJson.toStyledString().c_str(),
                  graphJson.toStyledString().size());
    outJson.close();
}

TEST_CASE("basic connectivity", "[overlay][connectivity][!hide]")
{
    auto test = [&](int maxOutbound, int maxInbound, int numNodes,
                    int numWatchers) {
        auto cfgs = std::vector<Config>{};
        auto peers = std::vector<std::string>{};

        for (int i = 1; i <= numNodes + numWatchers; ++i)
        {
            auto cfg = getTestConfig(i);
            cfgs.push_back(cfg);
            peers.push_back("127.0.0.1:" + std::to_string(cfg.PEER_PORT));
        }
        auto networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
        // Threshold 1 means everyone must agree, graph must be connected
        auto simulation =
            Topologies::separate(numNodes, 1, Simulation::OVER_LOOPBACK,
                                 networkID, numWatchers, [&](int i) {
                                     if (i == 0)
                                     {
                                         auto cfg = getTestConfig();
                                         cfg.TARGET_PEER_CONNECTIONS = 0;
                                         return cfg;
                                     }

                                     auto cfg = cfgs[i - 1];

                                     if (i > numNodes)
                                     {
                                         cfg.NODE_IS_VALIDATOR = false;
                                         cfg.FORCE_SCP = false;
                                     }
                                     cfg.TARGET_PEER_CONNECTIONS = maxOutbound;
                                     cfg.MAX_ADDITIONAL_PEER_CONNECTIONS =
                                         maxInbound;
                                     cfg.KNOWN_PEERS = peers;
                                     cfg.RUN_STANDALONE = false;
                                     return cfg;
                                 });

        simulation->startAllNodes();
        simulation->crankForAtLeast(std::chrono::seconds(10), false);
        for (auto const& node : simulation->getNodes())
        {
            CLOG_INFO(
                Overlay, "Connections for node ({}) {} --> {}",
                (node->getConfig().NODE_IS_VALIDATOR ? "validator" : "watcher"),
                node->getConfig().toShortString(
                    node->getConfig().NODE_SEED.getPublicKey()),
                node->getOverlayManager().getAuthenticatedPeersCount());
        }
        CLOG_INFO(Overlay, "TOTAL CONNECTIONS {}",
                  numberOfSimulationConnections(simulation));
        simulation->crankUntil(
            [&] { return simulation->haveAllExternalized(4, 1); },
            5 * Herder::EXP_LEDGER_TIMESPAN_SECONDS, false);
        return simulation;
    };

    SECTION("sparse")
    {
        // Test that test actually fails when things are too sparse
        test(1, 20, 23, 0);
    }
    SECTION("2 outbound per node")
    {
        // 1000 iterations, nodes can always connect and close ledgers
        test(2, 2, 23, 0);
    }
}

TEST_CASE("peer churn", "[overlay][connectivity][!hide]")
{
    auto cfgs = std::vector<Config>{};
    auto peers = std::vector<std::string>{};
    int numNodes = 23;
    int numWatchers = 77;

    int maxOutbound = 2;
    int maxInbound = 4;

    std::vector<SecretKey> keys;
    SCPQuorumSet qSet;
    qSet.threshold = numNodes;
    for (int i = 0; i < numNodes + numWatchers; i++)
    {
        auto key =
            SecretKey::fromSeed(sha256("NODE_SEED_" + std::to_string(i)));
        keys.push_back(key);
        if (i < numNodes)
        {
            qSet.validators.push_back(key.getPublicKey());
        }
        auto cfg = getTestConfig(i + 1);

        cfg.NODE_IS_VALIDATOR = i < numNodes;
        cfg.FORCE_SCP = cfg.NODE_IS_VALIDATOR;
        cfg.TARGET_PEER_CONNECTIONS = maxOutbound;
        cfg.MAX_ADDITIONAL_PEER_CONNECTIONS = maxInbound;
        cfg.KNOWN_PEERS = peers;
        cfg.RUN_STANDALONE = false;
        cfg.MODE_DOES_CATCHUP = false;
        cfg.MAX_SLOTS_TO_REMEMBER = 100;

        cfgs.push_back(cfg);
        peers.push_back("127.0.0.1:" + std::to_string(cfg.PEER_PORT));
    }

    auto networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
    auto simulation =
        std::make_shared<Simulation>(Simulation::OVER_LOOPBACK, networkID);

    SECTION("add peers one by one")
    {
        std::vector<int> randomIndexes;
        for (int i = 0; i < keys.size(); i++)
        {
            randomIndexes.push_back(i);
        }
        stellar::shuffle(std::begin(randomIndexes), std::end(randomIndexes),
                         gRandomEngine);
        // One-by-one add a node, ensure everyone can connect
        for (int i = 0; i < randomIndexes.size(); i++)
        {
            CLOG_INFO(Overlay, "NODE {}", i);
            auto index = randomIndexes[i];
            auto newNode = simulation->addNode(keys[index], qSet, &cfgs[index]);
            newNode->start();
            simulation->crankForAtLeast(
                std::chrono::seconds(rand_uniform(0, 3)), false);
        }

        simulation->crankForAtLeast(std::chrono::seconds(15), false);
        logTopologyInfo(simulation);

        // Verify graph is connected
        REQUIRE(simulation->getNodes().size() == (numNodes + numWatchers));
        REQUIRE(isConnected(numNodes, numWatchers, simulation));
    }

    SECTION("basic churn - remove and add peer")
    {
        for (int i = 0; i < keys.size(); i++)
        {
            simulation->addNode(keys[i], qSet, &cfgs[i]);
        }
        simulation->startAllNodes();
        simulation->crankUntil(
            [&] { return simulation->haveAllExternalized(2, 3); },
            5 * Herder::EXP_LEDGER_TIMESPAN_SECONDS, false);

        auto otherCfgs = std::vector<Config>{};

        // Add a bit of churn by stopping and starting two random (can be same)
        // peers
        for (int i = 0; i < 2; i++)
        {
            auto peerToChurn = rand_element(keys);
            auto cfg =
                simulation->getNode(peerToChurn.getPublicKey())->getConfig();
            simulation->removeNode(peerToChurn.getPublicKey());
            simulation->crankForAtLeast(std::chrono::seconds(30), false);
            auto node = simulation->addNode(cfg.NODE_SEED, qSet, &cfg);
            CLOG_INFO(Overlay, "Restart NODE {}",
                      node->getConfig().toShortString(
                          node->getConfig().NODE_SEED.getPublicKey()));
            node->start();
            simulation->crankForAtLeast(std::chrono::seconds(30), false);
            REQUIRE(isConnected(numNodes, numWatchers, simulation));
        }
    }
    SECTION("long-lasting churn - topology changes overtime")
    {
        auto test = [&](bool automaticPeers) {
            for (int i = 0; i < keys.size(); i++)
            {
                cfgs[i].AUTOMATIC_PREFERRED_PEERS = automaticPeers;
                simulation->addNode(keys[i], qSet, &cfgs[i]);
            }
            simulation->startAllNodes();
            simulation->crankUntil(
                [&] { return simulation->haveAllExternalized(5, 1); },
                5 * Herder::EXP_LEDGER_TIMESPAN_SECONDS, false);
            REQUIRE(isConnected(numNodes, numWatchers, simulation));
            // TODO: later _maybe_ also test that nodes can externalize, for now
            // just graph connectivity (assumption is that if graph is
            // connected, nodes can externalize)

            int iterations = 30;
            for (int i = 0; i < iterations; i++)
            {
                CLOG_INFO(Overlay, "ITERATION {}", i);

                // Crank for random amount of time
                simulation->crankForAtLeast(
                    std::chrono::seconds(rand_uniform(0, 20)), false);

                auto otherCfgs = std::vector<Config>{};
                std::vector<SecretKey> peersToChurn;
                while (peersToChurn.size() < 3)
                {
                    auto peerToChurn = rand_element(keys);
                    if (std::find(peersToChurn.begin(), peersToChurn.end(),
                                  peerToChurn) == peersToChurn.end())
                    {
                        peersToChurn.push_back(peerToChurn);
                    }
                }

                for (auto const& peerToChurn : peersToChurn)
                {
                    auto cfg = simulation->getNode(peerToChurn.getPublicKey())
                                   ->getConfig();
                    otherCfgs.push_back(cfg);
                    simulation->removeNode(peerToChurn.getPublicKey());
                }

                simulation->crankForAtLeast(std::chrono::seconds(60), false);
                for (int j = 0; j < peersToChurn.size(); j++)
                {
                    auto peerToChurn = peersToChurn[j];
                    auto node = simulation->addNode(otherCfgs[j].NODE_SEED,
                                                    qSet, &otherCfgs[j]);
                    CLOG_INFO(Overlay, "Restart NODE {}",
                              node->getConfig().toShortString(
                                  node->getConfig().NODE_SEED.getPublicKey()));
                    node->start();
                }

                // Allow nodes to reconnect
                simulation->crankForAtLeast(std::chrono::seconds(60), false);
                logTopologyInfo(simulation);
                REQUIRE(isConnected(numNodes, numWatchers, simulation));
            }

            // Export resulting graph to JSON file
            std::unordered_set<NodeID> visited;
            Json::Value res;
            getGraphMatrix(*(simulation->getNodes()[0]), visited, res);
            auto testID = rand_uniform(0, 100000);
            exportGraphJson(res, automaticPeers, testID);
        };
        SECTION("automatic peer selection")
        {
            test(true);
        }
        SECTION("no automatic peer selection")
        {
            test(false);
        }
    }
}

// overtime promote closer connection to
TEST_CASE("automatic peer selection", "[overlay][connectivity][!hide]")
{
    int iterations = 10;
    auto cfgs = std::vector<Config>{};
    int numNodes = 23;
    int numWatchers = 100;
    auto peers = std::vector<std::string>{};
    auto preferred = std::vector<std::string>();
    for (int i = 1; i <= numNodes + numWatchers; ++i)
    {
        auto cfg = getTestConfig(i);
        cfgs.push_back(cfg);
        peers.push_back("127.0.0.1:" + std::to_string(cfg.PEER_PORT));
        if (i <= numNodes)
        {
            preferred.push_back("127.0.0.1:" + std::to_string(cfg.PEER_PORT));
        }
    }
    int testId = rand_uniform(1, 1000000);

    auto test = [&](std::vector<std::string> known,
                    std::vector<std::string> preferred, int maxOutbound,
                    int maxInbound, bool automaticPreferredPeers) {
        auto networkID = sha256(getTestConfig().NETWORK_PASSPHRASE);
        auto simulation = Topologies::separate(
            // Threshold 1 means all validators must agree
            numNodes, 1, Simulation::OVER_LOOPBACK, networkID, numWatchers,
            [&](int i) {
                if (i == 0)
                {
                    auto cfg = getTestConfig();
                    cfg.TARGET_PEER_CONNECTIONS = 0;
                    return cfg;
                }

                auto cfg = cfgs[i - 1];
                if (i > numNodes)
                {
                    cfg.NODE_IS_VALIDATOR = false;
                    cfg.FORCE_SCP = false;
                }
                cfg.TARGET_PEER_CONNECTIONS = maxOutbound;
                cfg.MAX_ADDITIONAL_PEER_CONNECTIONS = maxInbound;
                cfg.PREFERRED_PEERS = preferred;
                cfg.KNOWN_PEERS = peers;
                cfg.RUN_STANDALONE = false;
                cfg.AUTOMATIC_PREFERRED_PEERS = automaticPreferredPeers;
                return cfg;
            });

        simulation->startAllNodes();

        // Allow all the connections to settle
        simulation->crankForAtLeast(std::chrono::seconds(60), false);

        auto firstNode = simulation->getNodes()[0];
        auto lcl = firstNode->getLedgerManager().getLastClosedLedgerNum();
        simulation->crankUntil(
            [&]() {
                return simulation->haveAllExternalized(
                    lcl + 3, 1, /* validatorsOnly */ true);
            },
            10 * Herder::EXP_LEDGER_TIMESPAN_SECONDS, false);

        // Verify graph is connected
        REQUIRE(simulation->getNodes().size() == (numNodes + numWatchers));
        std::unordered_set<NodeID> visited;
        Json::Value res;
        getGraphMatrix(*firstNode, visited, res);
        REQUIRE(visited.size() == (numNodes + numWatchers));

        for (auto const& node : simulation->getNodes())
        {
            CLOG_INFO(
                Overlay, "Connections for node ({}) {} --> {}",
                (node->getConfig().NODE_IS_VALIDATOR ? "validator" : "watcher"),
                node->getConfig().toShortString(
                    node->getConfig().NODE_SEED.getPublicKey()),
                node->getOverlayManager().getAuthenticatedPeersCount());
            if (node->getConfig().NODE_IS_VALIDATOR)
            {
                int knownAutomatic = 0;
                int connectedAutomatic = 0;
                for (auto const& otherNode : simulation->getNodes())
                {
                    if (otherNode->getConfig().NODE_IS_VALIDATOR &&
                        otherNode != node)
                    {
                        if (knowsAsAutomatic(*node, *otherNode))
                        {
                            ++knownAutomatic;
                        }
                        auto auth =
                            node->getOverlayManager().getAuthenticatedPeers();
                        if (auth.find(otherNode->getConfig()
                                          .NODE_SEED.getPublicKey()) !=
                            auth.end())
                        {
                            connectedAutomatic++;
                        }
                    }
                }
                CLOG_INFO(Overlay, "Automatic CONNECTED connections: {}",
                          connectedAutomatic);
                CLOG_INFO(Overlay, "KNOWN AUTOMATIC: {}", knownAutomatic);
            }
        }

        CLOG_INFO(Overlay, "TOTAL CONNECTIONS {}",
                  numberOfSimulationConnections(simulation));

        // Export resulting graph to JSON file
        exportGraphJson(res, automaticPreferredPeers, testId);

        return simulation;
    };

    int success = 0;
    int maxConn = 4;
    SECTION("insufficient capacity, automatic preferred")
    {
        for (int i = 0; i < iterations; i++)
        {
            try
            {
                test(peers, preferred, maxConn, maxConn, true);
                success++;
            }
            catch (std::exception const&)
            {
                CLOG_INFO(Overlay, "Iteration {} FAILED", i);
            }
        }
        CLOG_INFO(Perf, "Automatic PEERS - {}/{} successful iterations",
                  success, iterations);
    }
    SECTION("insufficient capacity, no automatic peer selection")
    {
        for (int i = 0; i < iterations; i++)
        {
            try
            {
                test(peers, {}, maxConn, maxConn, false);
                success++;
            }
            catch (std::exception const&)
            {
                CLOG_INFO(Overlay, "Iteration {} FAILED", i);
            }
        }
        CLOG_INFO(Perf, "Non-automatic PEERS - {}/{} successful iterations",
                  success, iterations);
    }
}

}