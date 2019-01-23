// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "overlay/OverlayManagerImpl.h"
#include "crypto/KeyUtils.h"
#include "crypto/SecretKey.h"
#include "database/Database.h"
#include "main/Application.h"
#include "main/Config.h"
#include "overlay/PeerBareAddress.h"
#include "overlay/PeerManager.h"
#include "overlay/RandomPeerSource.h"
#include "overlay/TCPPeer.h"
#include "util/Logging.h"
#include "util/Math.h"
#include "util/XDROperators.h"

#include "medida/counter.h"
#include "medida/meter.h"
#include "medida/metrics_registry.h"

#include <algorithm>
#include <random>

/*

Connection process:
A wants to connect to B
A initiates a tcp connection to B
connection is established
A sends HELLO(CertA,NonceA) to B
B now has IP and listening port of A, sends HELLO(CertB,NonceB) back
A sends AUTH(signed([0],keyAB))
B verifies and either:
    sends AUTH(signed([0],keyBA)) back or
    disconnects, if it's full, optionally sending a list of other peers to try
first

keyAB and keyBA are per-connection HMAC keys derived from non-interactive
ECDH on random curve25519 keys conveyed in CertA and CertB (certs signed by
Node Ed25519 keys) the result of which is then fed through HKDF with the
per-connection nonces. See PeerAuth.h.

If any verify step fails, the peer disconnects immediately.

*/

namespace stellar
{

using namespace soci;
using namespace std;

OverlayManagerImpl::PeersList::PeersList(
    OverlayManagerImpl& overlayManager,
    medida::MetricsRegistry& metricsRegistry, std::string directionString,
    std::string cancelledName, unsigned short maxAuthenticatedCount)
    : mConnectionsAttempted(metricsRegistry.NewMeter(
          {"overlay", directionString, "attempt"}, "connection"))
    , mConnectionsEstablished(metricsRegistry.NewMeter(
          {"overlay", directionString, "establish"}, "connection"))
    , mConnectionsDropped(metricsRegistry.NewMeter(
          {"overlay", directionString, "drop"}, "connection"))
    , mConnectionsCancelled(metricsRegistry.NewMeter(
          {"overlay", directionString, cancelledName}, "connection"))
    , mOverlayManager(overlayManager)
    , mMaxAuthenticatedCount(maxAuthenticatedCount)
{
}

Peer::pointer
OverlayManagerImpl::PeersList::byAddress(PeerBareAddress const& address) const
{
    auto pendingPeerIt = std::find_if(std::begin(mPending), std::end(mPending),
                                      [address](Peer::pointer const& peer) {
                                          return peer->getAddress() == address;
                                      });
    if (pendingPeerIt != std::end(mPending))
    {
        return *pendingPeerIt;
    }

    auto authenticatedPeerIt =
        std::find_if(std::begin(mAuthenticated), std::end(mAuthenticated),
                     [address](std::pair<NodeID, Peer::pointer> const& peer) {
                         return peer.second->getAddress() == address;
                     });
    if (authenticatedPeerIt != std::end(mAuthenticated))
    {
        return authenticatedPeerIt->second;
    }

    return {};
}

void
OverlayManagerImpl::PeersList::removePeer(Peer* peer)
{
    assert(peer->getState() == Peer::CLOSING);

    auto pendingIt =
        std::find_if(std::begin(mPending), std::end(mPending),
                     [&](Peer::pointer const& p) { return p.get() == peer; });
    if (pendingIt != std::end(mPending))
    {
        mPending.erase(pendingIt);
        mConnectionsDropped.Mark();
    }

    auto authentiatedIt = mAuthenticated.find(peer->getPeerID());
    if (authentiatedIt != std::end(mAuthenticated))
    {
        mAuthenticated.erase(authentiatedIt);
        mConnectionsDropped.Mark();
    }

    CLOG(WARNING, "Overlay") << "Dropping unlisted peer";
}

bool
OverlayManagerImpl::PeersList::moveToAuthenticated(Peer::pointer peer)
{
    auto pendingIt = std::find(std::begin(mPending), std::end(mPending), peer);
    if (pendingIt == std::end(mPending))
    {
        CLOG(WARNING, "Overlay")
            << "Trying to move non-pending peer " << peer->toString()
            << " to authenticated list";
        mConnectionsCancelled.Mark();
        return false;
    }

    auto authenticatedIt = mAuthenticated.find(peer->getPeerID());
    if (authenticatedIt != std::end(mAuthenticated))
    {
        CLOG(WARNING, "Overlay")
            << "Trying to move authenticated peer " << peer->toString()
            << " to authenticated list again";
        mConnectionsCancelled.Mark();
        return false;
    }

    mPending.erase(pendingIt);
    mAuthenticated[peer->getPeerID()] = peer;

    return true;
}

bool
OverlayManagerImpl::PeersList::acceptAuthenticatedPeer(Peer::pointer peer)
{
    if (mOverlayManager.isPreferred(peer.get()))
    {
        if (mAuthenticated.size() < mMaxAuthenticatedCount)
        {
            return moveToAuthenticated(peer);
        }

        for (auto victim : mAuthenticated)
        {
            if (!mOverlayManager.isPreferred(victim.second.get()))
            {
                CLOG(INFO, "Overlay")
                    << "Evicting non-preferred peer "
                    << victim.second->toString() << " for preferred peer "
                    << peer->toString();
                victim.second->drop(ERR_LOAD, "preferred peer selected instead",
                                    Peer::DropMode::IGNORE_WRITE_QUEUE);
                return moveToAuthenticated(peer);
            }
        }
    }

    if (!mOverlayManager.mApp.getConfig().PREFERRED_PEERS_ONLY &&
        mAuthenticated.size() < mMaxAuthenticatedCount)
    {
        return moveToAuthenticated(peer);
    }

    CLOG(WARNING, "Debug") << "Non preferred authenticated peer "
                           << peer->toString()
                           << " rejected because of lack of space";
    mConnectionsCancelled.Mark();
    return false;
}

void
OverlayManagerImpl::PeersList::shutdown()
{
    auto pendingPeersToStop = mPending;
    for (auto& p : pendingPeersToStop)
    {
        p->drop(ERR_MISC, "peer shutdown", Peer::DropMode::IGNORE_WRITE_QUEUE);
    }
    auto authenticatedPeersToStop = mAuthenticated;
    for (auto& p : authenticatedPeersToStop)
    {
        p.second->drop(ERR_MISC, "peer shutdown",
                       Peer::DropMode::IGNORE_WRITE_QUEUE);
    }
}

std::unique_ptr<OverlayManager>
OverlayManager::create(Application& app)
{
    return std::make_unique<OverlayManagerImpl>(app);
}

OverlayManagerImpl::OverlayManagerImpl(Application& app)
    : mApp(app)
    , mInboundPeers(*this, mApp.getMetrics(), "inbound", "reject",
                    mApp.getConfig().MAX_ADDITIONAL_PEER_CONNECTIONS)
    , mOutboundPeers(*this, mApp.getMetrics(), "outbound", "cancel",
                     mApp.getConfig().TARGET_PEER_CONNECTIONS)
    , mPeerManager(app)
    , mDoor(mApp)
    , mAuth(mApp)
    , mShuttingDown(false)
    , mMessagesBroadcast(app.getMetrics().NewMeter(
          {"overlay", "message", "broadcast"}, "message"))
    , mPendingPeersSize(
          app.getMetrics().NewCounter({"overlay", "connection", "pending"}))
    , mAuthenticatedPeersSize(app.getMetrics().NewCounter(
          {"overlay", "connection", "authenticated"}))
    , mTimer(app)
    , mFloodGate(app)
{
    mPeerSources[PeerType::INBOUND] = std::make_unique<RandomPeerSource>(
        mPeerManager, RandomPeerSource::nextAttemptCutoff(PeerType::INBOUND));
    mPeerSources[PeerType::OUTBOUND] = std::make_unique<RandomPeerSource>(
        mPeerManager, RandomPeerSource::nextAttemptCutoff(PeerType::OUTBOUND));
    mPeerSources[PeerType::PREFERRED] = std::make_unique<RandomPeerSource>(
        mPeerManager, RandomPeerSource::nextAttemptCutoff(PeerType::PREFERRED));
}

OverlayManagerImpl::~OverlayManagerImpl()
{
}

void
OverlayManagerImpl::start()
{
    mDoor.start();
    mTimer.expires_from_now(std::chrono::seconds(2));

    if (!mApp.getConfig().RUN_STANDALONE)
    {
        mTimer.async_wait(
            [this]() {
                storeConfigPeers();
                this->tick();
            },
            VirtualTimer::onFailureNoop);
    }
}

void
OverlayManagerImpl::connectTo(PeerBareAddress const& address)
{
    connectToImpl(address, false);
}

void
OverlayManagerImpl::connectToImpl(PeerBareAddress const& address,
                                  bool forceoutbound)
{
    auto currentConnection = getConnectedPeer(address);
    if (!currentConnection || (forceoutbound && currentConnection->getRole() ==
                                                    Peer::REMOTE_CALLED_US))
    {
        getPeerManager().update(address, PeerManager::BackOffUpdate::INCREASE);
        addOutboundConnection(TCPPeer::initiate(mApp, address));
    }
    else
    {
        CLOG(ERROR, "Overlay")
            << "trying to connect to a node we're already connected to "
            << address.toString();
    }
}

OverlayManagerImpl::PeersList&
OverlayManagerImpl::getPeersList(Peer* peer)
{
    switch (peer->getRole())
    {
    case Peer::WE_CALLED_REMOTE:
        return mOutboundPeers;
    case Peer::REMOTE_CALLED_US:
        return mInboundPeers;
    default:
        abort();
    }
}

void
OverlayManagerImpl::storePeerList(std::vector<std::string> const& list,
                                  bool setPreferred)
{
    for (auto const& peerStr : list)
    {
        try
        {
            auto address = PeerBareAddress::resolve(peerStr, mApp);
            auto typeUpgrade = setPreferred
                                   ? PeerManager::TypeUpdate::SET_PREFERRED
                                   : PeerManager::TypeUpdate::KEEP;
            getPeerManager().update(address, typeUpgrade,
                                    PeerManager::BackOffUpdate::RESET);
        }
        catch (std::runtime_error&)
        {
            CLOG(ERROR, "Overlay") << "Unable to add peer '" << peerStr << "'";
        }
    }
}

void
OverlayManagerImpl::storeConfigPeers()
{
    // compute normalized mConfigurationPreferredPeers
    std::vector<std::string> ppeers;
    for (auto const& s : mApp.getConfig().PREFERRED_PEERS)
    {
        try
        {
            auto pr = PeerBareAddress::resolve(s, mApp);
            auto r = mConfigurationPreferredPeers.insert(pr);
            if (r.second)
            {
                ppeers.push_back(r.first->toString());
            }
        }
        catch (std::runtime_error&)
        {
            CLOG(ERROR, "Overlay")
                << "Unable to add preferred peer '" << s << "'";
        }
    }

    storePeerList(mApp.getConfig().KNOWN_PEERS, false);
    storePeerList(ppeers, true);
}

std::vector<PeerBareAddress>
OverlayManagerImpl::getPeersToConnectTo(int maxNum, PeerType peerType)
{
    assert(maxNum >= 0);
    if (maxNum == 0)
    {
        return {};
    }

    auto keep = [&](PeerBareAddress const& address) {
        auto peer = getConnectedPeer(address);
        auto promote = peer && (peerType == PeerType::INBOUND) &&
                       (peer->getRole() == Peer::REMOTE_CALLED_US);
        return !peer || promote;
    };

    // don't connect to too many peers at once
    return mPeerSources[peerType]->getRandomPeers(std::min(maxNum, 50), keep);
}

void
OverlayManagerImpl::connectTo(int maxNum, PeerType peerType)
{
    connectTo(getPeersToConnectTo(maxNum, peerType),
              peerType == PeerType::INBOUND);
}

void
OverlayManagerImpl::connectTo(std::vector<PeerBareAddress> const& peers,
                              bool forceoutbound)
{
    for (auto& address : peers)
    {
        connectToImpl(address, forceoutbound);
    }
}

// called every 2 seconds
void
OverlayManagerImpl::tick()
{
    CLOG(TRACE, "Overlay") << "OverlayManagerImpl tick";

    mLoad.maybeShedExcessLoad(mApp);

    // try to replace all connections with preferred peers
    connectTo(mApp.getConfig().TARGET_PEER_CONNECTIONS, PeerType::PREFERRED);

    // connect to non-preferred candidates from the database
    // when PREFERRED_PEER_ONLY is set and we connect to a non preferred_peer we
    // just end up dropping & backing off it during handshake (this allows for
    // preferred_peers to work for both ip based and key based preferred mode)
    connectTo(missingAuthenticatedCount(), PeerType::OUTBOUND);
    connectTo(missingAuthenticatedCount(), PeerType::INBOUND);

    mTimer.expires_from_now(
        std::chrono::seconds(mApp.getConfig().PEER_AUTHENTICATION_TIMEOUT + 1));
    mTimer.async_wait([this]() { this->tick(); }, VirtualTimer::onFailureNoop);
}

int
OverlayManagerImpl::missingAuthenticatedCount() const
{
    if (mOutboundPeers.mAuthenticated.size() <
        mApp.getConfig().TARGET_PEER_CONNECTIONS)
    {
        return mApp.getConfig().TARGET_PEER_CONNECTIONS -
               mOutboundPeers.mAuthenticated.size();
    }
    else
    {
        return 0;
    }
}

Peer::pointer
OverlayManagerImpl::getConnectedPeer(PeerBareAddress const& address)
{
    auto outbound = mOutboundPeers.byAddress(address);
    return outbound ? outbound : mInboundPeers.byAddress(address);
}

void
OverlayManagerImpl::ledgerClosed(uint32_t lastClosedledgerSeq)
{
    mFloodGate.clearBelow(lastClosedledgerSeq);
}

void
OverlayManagerImpl::updateSizeCounters()
{
    mPendingPeersSize.set_count(getPendingPeersCount());
    mAuthenticatedPeersSize.set_count(getAuthenticatedPeersCount());
}

void
OverlayManagerImpl::addInboundConnection(Peer::pointer peer)
{
    assert(peer->getRole() == Peer::REMOTE_CALLED_US);
    mInboundPeers.mConnectionsAttempted.Mark();

    auto haveSpace = mInboundPeers.mPending.size() <
                     mApp.getConfig().MAX_INBOUND_PENDING_CONNECTIONS;
    if (!haveSpace && mInboundPeers.mPending.size() <
                          mApp.getConfig().MAX_INBOUND_PENDING_CONNECTIONS +
                              Config::POSSIBLY_PREFERRED_EXTRA)
    {
        // for peers that are possibly preferred (they have the same IP as some
        // preferred peer we enocuntered in past), we allow an extra
        // Config::POSSIBLY_PREFERRED_EXTRA incoming pending connections, that
        // are not available for non-preferred peers
        haveSpace = isPossiblyPreferred(peer->getIP());
    }

    if (mShuttingDown || !haveSpace)
    {
        if (!mShuttingDown)
        {
            CLOG(DEBUG, "Overlay")
                << "Peer rejected - all inbound connections taken: "
                << peer->toString();
        }

        mInboundPeers.mConnectionsCancelled.Mark();
        peer->drop(Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }
    CLOG(INFO, "Overlay") << "New connected peer " << peer->toString();
    mInboundPeers.mConnectionsEstablished.Mark();
    mInboundPeers.mPending.push_back(peer);
    updateSizeCounters();
}

bool
OverlayManagerImpl::isPossiblyPreferred(std::string const& ip)
{
    return std::any_of(
        std::begin(mConfigurationPreferredPeers),
        std::end(mConfigurationPreferredPeers),
        [&](PeerBareAddress const& address) { return address.getIP() == ip; });
}

void
OverlayManagerImpl::addOutboundConnection(Peer::pointer peer)
{
    assert(peer->getRole() == Peer::WE_CALLED_REMOTE);
    mOutboundPeers.mConnectionsAttempted.Mark();

    if (mShuttingDown || mOutboundPeers.mPending.size() >=
                             mApp.getConfig().MAX_OUTBOUND_PENDING_CONNECTIONS)
    {
        if (!mShuttingDown)
        {
            CLOG(DEBUG, "Overlay")
                << "Peer rejected - all outbound connections taken: "
                << peer->toString();
        }

        mOutboundPeers.mConnectionsCancelled.Mark();
        peer->drop(Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }
    CLOG(INFO, "Overlay") << "New connected peer " << peer->toString();
    mOutboundPeers.mConnectionsEstablished.Mark();
    mOutboundPeers.mPending.push_back(peer);
    updateSizeCounters();
}

void
OverlayManagerImpl::removePeer(Peer* peer)
{
    CLOG(INFO, "Overlay") << "Dropping peer "
                          << mApp.getConfig().toShortString(peer->getPeerID())
                          << "@" << peer->toString();

    getPeersList(peer).removePeer(peer);
    updateSizeCounters();
}

bool
OverlayManagerImpl::moveToAuthenticated(Peer::pointer peer)
{
    auto result = getPeersList(peer.get()).moveToAuthenticated(peer);
    updateSizeCounters();
    return result;
}

bool
OverlayManagerImpl::acceptAuthenticatedPeer(Peer::pointer peer)
{
    return getPeersList(peer.get()).acceptAuthenticatedPeer(peer);
}

std::vector<Peer::pointer>
OverlayManagerImpl::getPendingPeers() const
{
    auto result = mOutboundPeers.mPending;
    result.insert(std::end(result), std::begin(mInboundPeers.mPending),
                  std::end(mInboundPeers.mPending));
    return result;
}

std::map<NodeID, Peer::pointer>
OverlayManagerImpl::getAuthenticatedPeers() const
{
    auto result = mOutboundPeers.mAuthenticated;
    result.insert(std::begin(mInboundPeers.mAuthenticated),
                  std::end(mInboundPeers.mAuthenticated));
    return result;
}

int
OverlayManagerImpl::getPendingPeersCount() const
{
    return mInboundPeers.mPending.size() + mOutboundPeers.mPending.size();
}

int
OverlayManagerImpl::getAuthenticatedPeersCount() const
{
    return mInboundPeers.mAuthenticated.size() +
           mOutboundPeers.mAuthenticated.size();
}

bool
OverlayManagerImpl::isPreferred(Peer* peer)
{
    std::string pstr = peer->toString();

    if (mConfigurationPreferredPeers.find(peer->getAddress()) !=
        mConfigurationPreferredPeers.end())
    {
        CLOG(DEBUG, "Overlay") << "Peer " << pstr << " is preferred";
        return true;
    }

    if (peer->isAuthenticated())
    {
        std::string kstr = KeyUtils::toStrKey(peer->getPeerID());
        std::vector<std::string> const& pk =
            mApp.getConfig().PREFERRED_PEER_KEYS;
        if (std::find(pk.begin(), pk.end(), kstr) != pk.end())
        {
            CLOG(DEBUG, "Overlay")
                << "Peer key " << mApp.getConfig().toStrKey(peer->getPeerID())
                << " is preferred";
            return true;
        }
    }

    CLOG(DEBUG, "Overlay") << "Peer " << pstr << " is not preferred";
    return false;
}

std::vector<Peer::pointer>
OverlayManagerImpl::getRandomAuthenticatedPeers()
{
    auto goodPeers = std::vector<Peer::pointer>{};
    auto extractPeer = [](std::pair<NodeID, Peer::pointer> const& peer) {
        return peer.second;
    };
    std::transform(std::begin(mInboundPeers.mAuthenticated),
                   std::end(mInboundPeers.mAuthenticated),
                   std::back_inserter(goodPeers), extractPeer);
    std::transform(std::begin(mOutboundPeers.mAuthenticated),
                   std::end(mOutboundPeers.mAuthenticated),
                   std::back_inserter(goodPeers), extractPeer);
    std::shuffle(goodPeers.begin(), goodPeers.end(), gRandomEngine);
    return goodPeers;
}

void
OverlayManagerImpl::recvFloodedMsg(StellarMessage const& msg,
                                   Peer::pointer peer)
{
    mFloodGate.addRecord(msg, peer);
}

void
OverlayManagerImpl::broadcastMessage(StellarMessage const& msg, bool force)
{
    mMessagesBroadcast.Mark();
    mFloodGate.broadcast(msg, force);
}

void
OverlayManager::dropAll(Database& db)
{
    PeerManager::dropAll(db);
}

std::set<Peer::pointer>
OverlayManagerImpl::getPeersKnows(Hash const& h)
{
    return mFloodGate.getPeersKnows(h);
}

PeerAuth&
OverlayManagerImpl::getPeerAuth()
{
    return mAuth;
}

LoadManager&
OverlayManagerImpl::getLoadManager()
{
    return mLoad;
}

PeerManager&
OverlayManagerImpl::getPeerManager()
{
    return mPeerManager;
}

void
OverlayManagerImpl::shutdown()
{
    if (mShuttingDown)
    {
        return;
    }
    mShuttingDown = true;
    mDoor.close();
    mFloodGate.shutdown();
    mInboundPeers.shutdown();
    mOutboundPeers.shutdown();
}

bool
OverlayManagerImpl::isShuttingDown() const
{
    return mShuttingDown;
}
}
