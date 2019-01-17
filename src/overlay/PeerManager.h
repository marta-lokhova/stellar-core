#pragma once

// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "overlay/PeerBareAddress.h"
#include "util/Timer.h"

#include <functional>

namespace soci
{
class statement;
}

namespace stellar
{

class Database;

enum class PeerType
{
    INBOUND,
    OUTBOUND,
    PREFERRED,
    COUNT
};

/**
 * Raw database record of peer data. Its key is PeerBareAddress.
 */
struct PeerRecord
{
    std::tm mNextAttempt;
    int mNumFailures{0};
    int mType{0};
};

bool operator==(PeerRecord const& x, PeerRecord const& y);

PeerAddress toXdr(PeerBareAddress const& address);

/**
 * Maintain list of know peers in database.
 */
class PeerManager
{
  public:
    struct PeerQuery
    {
        bool mNextAttempt;
        int mMaxNumFailures;
        optional<PeerType> mRequireExactType;
        optional<bool> mRequireOutbound;
    };

    enum class TypeUpdate
    {
        KEEP,
        SET_INBOUND,
        SET_OUTBOUND,
        SET_PREFERRED
    };

    enum class BackOffUpdate
    {
        KEEP,
        RESET,
        INCREASE
    };

    static PeerQuery maxFailures(int maxFailures, bool outbound);
    static PeerQuery nextAttemptCutoff(PeerType peerType);

    static void dropAll(Database& db);
    static void renameFlagsToType(Database& db);

    explicit PeerManager(Application& app);

    std::vector<PeerBareAddress>
    getRandomPeers(PeerQuery const& query, size_t size,
                   std::function<bool(PeerBareAddress const&)> pred);

    /**
     * Ensure that given peer is stored in database.
     */
    void ensureExists(PeerBareAddress const& address);

    /**
     * Update type of peer associated with given address.
     */
    void update(PeerBareAddress const& address, TypeUpdate type);

    /**
     * Update "next try" of peer associated with given address - can reset
     * it to now or back off even further in future.
     */
    void update(PeerBareAddress const& address, BackOffUpdate backOff);

    /**
     * Update both type and "next try" of peer associated with given address.
     */
    void update(PeerBareAddress const& address, TypeUpdate type,
                BackOffUpdate backOff);

    /**
     * Load PeerRecord data for peer with given address. If not available in
     * database, create default one. Second value in pair is true when data
     * was loaded from database, false otherwise.
     */
    std::pair<PeerRecord, bool> load(PeerBareAddress const& address);

    /**
     * Store PeerRecord data into database. If inDatabase is true, uses UPDATE
     * query, uses INSERT otherwise.
     */
    void store(PeerBareAddress const& address, PeerRecord const& PeerRecord,
               bool inDatabase);

  private:
    static const char* kSQLCreateWithFlagsStatement;
    static const char* kSQLCreateWithTypeStatement;

    Application& mApp;
    size_t const mBatchSize;
    std::map<PeerQuery, std::vector<PeerBareAddress>> mPeerCache;

    std::vector<PeerBareAddress> loadRandomPeers(PeerQuery const& query,
                                                 size_t size);

    size_t countPeers(std::string const& where,
                      std::function<void(soci::statement&)> const& bind);
    std::vector<PeerBareAddress>
    loadPeers(int limit, int offset, std::string const& where,
              std::function<void(soci::statement&)> const& bind);

    void update(PeerRecord& peer, TypeUpdate type);
    void update(PeerRecord& peer, BackOffUpdate backOff, Application& app);
};

bool operator<(PeerManager::PeerQuery const& x,
               PeerManager::PeerQuery const& y);
}
