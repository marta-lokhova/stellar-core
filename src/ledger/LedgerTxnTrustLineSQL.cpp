// Copyright 2017 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "crypto/KeyUtils.h"
#include "crypto/SecretKey.h"
#include "database/Database.h"
#include "ledger/LedgerTxnImpl.h"
#include "ledger/SqliteUtils.h"
#include "util/XDROperators.h"
#include "util/types.h"

namespace stellar
{

std::shared_ptr<LedgerEntry const>
LedgerTxnRoot::Impl::loadTrustLine(LedgerKey const& key) const
{
    auto const& asset = key.trustLine().asset;
    if (asset.type() == ASSET_TYPE_NATIVE)
    {
        throw std::runtime_error("XLM TrustLine?");
    }
    else if (key.trustLine().accountID == getIssuer(asset))
    {
        throw std::runtime_error("TrustLine accountID is issuer");
    }

    std::string actIDStrKey = KeyUtils::toStrKey(key.trustLine().accountID);
    std::string issuerStr, assetStr;
    if (asset.type() == ASSET_TYPE_CREDIT_ALPHANUM4)
    {
        assetCodeToStr(asset.alphaNum4().assetCode, assetStr);
        issuerStr = KeyUtils::toStrKey(asset.alphaNum4().issuer);
    }
    else if (asset.type() == ASSET_TYPE_CREDIT_ALPHANUM12)
    {
        assetCodeToStr(asset.alphaNum12().assetCode, assetStr);
        issuerStr = KeyUtils::toStrKey(asset.alphaNum12().issuer);
    }

    Liabilities liabilities;
    soci::indicator buyingLiabilitiesInd, sellingLiabilitiesInd;

    LedgerEntry le;
    le.data.type(TRUSTLINE);
    TrustLineEntry& tl = le.data.trustLine();

    auto prep = mDatabase.getPreparedStatement(
        "SELECT tlimit, balance, flags, lastmodified, buyingliabilities, "
        "sellingliabilities FROM trustlines "
        "WHERE accountid= :id AND issuer= :issuer AND assetcode= :asset");
    auto& st = prep.statement();
    st.exchange(soci::into(tl.limit));
    st.exchange(soci::into(tl.balance));
    st.exchange(soci::into(tl.flags));
    st.exchange(soci::into(le.lastModifiedLedgerSeq));
    st.exchange(soci::into(liabilities.buying, buyingLiabilitiesInd));
    st.exchange(soci::into(liabilities.selling, sellingLiabilitiesInd));
    st.exchange(soci::use(actIDStrKey));
    st.exchange(soci::use(issuerStr));
    st.exchange(soci::use(assetStr));
    st.define_and_bind();
    {
        auto timer = mDatabase.getSelectTimer("trust");
        st.execute(true);
    }
    if (!st.got_data())
    {
        return nullptr;
    }

    tl.accountID = key.trustLine().accountID;
    tl.asset = key.trustLine().asset;

    assert(buyingLiabilitiesInd == sellingLiabilitiesInd);
    if (buyingLiabilitiesInd == soci::i_ok)
    {
        tl.ext.v(1);
        tl.ext.v1().liabilities = liabilities;
    }

    return std::make_shared<LedgerEntry>(std::move(le));
}

void
LedgerTxnRoot::Impl::insertOrUpdateTrustLine(LedgerEntry const& entry,
                                             bool isInsert)
{
    auto const& tl = entry.data.trustLine();

    std::string actIDStrKey = KeyUtils::toStrKey(tl.accountID);
    unsigned int assetType = tl.asset.type();
    std::string issuerStr, assetCode;
    if (tl.asset.type() == ASSET_TYPE_CREDIT_ALPHANUM4)
    {
        issuerStr = KeyUtils::toStrKey(tl.asset.alphaNum4().issuer);
        assetCodeToStr(tl.asset.alphaNum4().assetCode, assetCode);
    }
    else if (tl.asset.type() == ASSET_TYPE_CREDIT_ALPHANUM12)
    {
        issuerStr = KeyUtils::toStrKey(tl.asset.alphaNum12().issuer);
        assetCodeToStr(tl.asset.alphaNum12().assetCode, assetCode);
    }
    if (actIDStrKey == issuerStr)
    {
        throw std::runtime_error("Issuer's own trustline should not be used "
                                 "outside of OperationFrame");
    }

    Liabilities liabilities;
    soci::indicator liabilitiesInd = soci::i_null;
    if (tl.ext.v() == 1)
    {
        liabilities = tl.ext.v1().liabilities;
        liabilitiesInd = soci::i_ok;
    }

    std::string sql;
    if (isInsert)
    {
        sql = "INSERT INTO trustlines "
              "(accountid, assettype, issuer, assetcode, balance, tlimit, "
              "flags, lastmodified, buyingliabilities, sellingliabilities) "
              "VALUES (:id, :at, :iss, :ac, :b, :tl, :f, :lm, :bl, :sl)";
    }
    else
    {
        sql = "UPDATE trustlines "
              "SET balance=:b, tlimit=:tl, flags=:f, lastmodified=:lm, "
              "buyingliabilities=:bl, sellingliabilities=:sl "
              "WHERE accountid=:id AND issuer=:iss AND assetcode=:ac";
    }
    auto prep = mDatabase.getPreparedStatement(sql);
    auto& st = prep.statement();
    st.exchange(soci::use(actIDStrKey, "id"));
    if (isInsert)
    {
        st.exchange(soci::use(assetType, "at"));
    }
    st.exchange(soci::use(issuerStr, "iss"));
    st.exchange(soci::use(assetCode, "ac"));
    st.exchange(soci::use(tl.balance, "b"));
    st.exchange(soci::use(tl.limit, "tl"));
    st.exchange(soci::use(tl.flags, "f"));
    st.exchange(soci::use(entry.lastModifiedLedgerSeq, "lm"));
    st.exchange(soci::use(liabilities.buying, liabilitiesInd, "bl"));
    st.exchange(soci::use(liabilities.selling, liabilitiesInd, "sl"));
    st.define_and_bind();
    {
        auto timer = isInsert ? mDatabase.getInsertTimer("trust")
                              : mDatabase.getUpdateTimer("trust");
        st.execute(true);
    }
    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("Could not update data in SQL");
    }
}

void
LedgerTxnRoot::Impl::deleteTrustLine(LedgerKey const& key)
{
    auto const& tl = key.trustLine();

    std::string actIDStrKey = KeyUtils::toStrKey(tl.accountID);
    std::string issuerStr, assetCode;
    if (tl.asset.type() == ASSET_TYPE_CREDIT_ALPHANUM4)
    {
        issuerStr = KeyUtils::toStrKey(tl.asset.alphaNum4().issuer);
        assetCodeToStr(tl.asset.alphaNum4().assetCode, assetCode);
    }
    else if (tl.asset.type() == ASSET_TYPE_CREDIT_ALPHANUM12)
    {
        issuerStr = KeyUtils::toStrKey(tl.asset.alphaNum12().issuer);
        assetCodeToStr(tl.asset.alphaNum12().assetCode, assetCode);
    }
    if (actIDStrKey == issuerStr)
    {
        throw std::runtime_error("Issuer's own trustline should not be used "
                                 "outside of OperationFrame");
    }

    auto prep = mDatabase.getPreparedStatement(
        "DELETE FROM trustlines "
        "WHERE accountid=:v1 AND issuer=:v2 AND assetcode=:v3");
    auto& st = prep.statement();
    st.exchange(soci::use(actIDStrKey));
    st.exchange(soci::use(issuerStr));
    st.exchange(soci::use(assetCode));
    st.define_and_bind();
    {
        auto timer = mDatabase.getDeleteTimer("trust");
        st.execute(true);
    }
    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("Could not update data in SQL");
    }
}

void
LedgerTxnRoot::Impl::dropTrustLines()
{
    throwIfChild();
    mEntryCache.clear();
    mBestOffersCache.clear();

    mDatabase.getSession() << "DROP TABLE IF EXISTS trustlines;";
    mDatabase.getSession()
        << "CREATE TABLE trustlines"
           "("
           "accountid    VARCHAR(56)     NOT NULL,"
           "assettype    INT             NOT NULL,"
           "issuer       VARCHAR(56)     NOT NULL,"
           "assetcode    VARCHAR(12)     NOT NULL,"
           "tlimit       BIGINT          NOT NULL CHECK (tlimit > 0),"
           "balance      BIGINT          NOT NULL CHECK (balance >= 0),"
           "flags        INT             NOT NULL,"
           "lastmodified INT             NOT NULL,"
           "PRIMARY KEY  (accountid, issuer, assetcode)"
           ");";
}

static LedgerEntry
sqliteFetchTrustLine(sqlite3_stmt* st)
{
    LedgerEntry le;
    le.data.type(TRUSTLINE);
    auto& tl = le.data.trustLine();

    sqliteRead(st, tl.accountID, 0);
    sqliteReadAsset(st, tl.asset, 1, 2, 3);
    sqliteRead(st, tl.limit, 4);
    sqliteRead(st, tl.balance, 5);
    sqliteRead(st, tl.flags, 6);
    sqliteRead(st, le.lastModifiedLedgerSeq, 7);

    Liabilities liabilities;
    if (sqliteReadLiabilities(st, liabilities, 8, 9))
    {
        tl.ext.v(1);
        tl.ext.v1().liabilities = liabilities;
    }

    return le;
}

static std::vector<LedgerEntry>
sqliteSpecificBulkLoadTrustLines(
    Database& db, std::vector<std::string> const& accountIDs,
    std::vector<std::string> const& issuers,
    std::vector<std::string> const& assetCodes)
{
    assert(accountIDs.size() == issuers.size());
    assert(accountIDs.size() == assetCodes.size());

    std::vector<const char*> cstrAccountIDs;
    std::vector<const char*> cstrIssuers;
    std::vector<const char*> cstrAssetCodes;
    cstrAccountIDs.reserve(accountIDs.size());
    cstrIssuers.reserve(issuers.size());
    cstrAssetCodes.reserve(assetCodes.size());
    for (size_t i = 0; i < accountIDs.size(); ++i)
    {
        cstrAccountIDs.emplace_back(accountIDs[i].c_str());
        cstrIssuers.emplace_back(issuers[i].c_str());
        cstrAssetCodes.emplace_back(assetCodes[i].c_str());
    }

    std::string sqlJoin =
        "SELECT x.value, y.value, z.value FROM "
        "(SELECT rowid, value FROM carray(?, ?, 'char*') ORDER BY rowid) AS x "
        "INNER JOIN (SELECT rowid, value FROM carray(?, ?, 'char*') ORDER BY rowid) AS y ON x.rowid = y.rowid "
        "INNER JOIN (SELECT rowid, value FROM carray(?, ?, 'char*') ORDER BY rowid) AS z ON x.rowid = z.rowid";
    std::string sql = "WITH r AS (" + sqlJoin +
        ") SELECT accountid, assettype, assetcode, issuer, tlimit, balance, "
        "flags, lastmodified, buyingliabilities, sellingliabilities "
        "FROM trustlines WHERE (accountid, issuer, assetcode) IN r";

    auto prep = db.getPreparedStatement(sql);
    auto sqliteStatement = dynamic_cast<soci::sqlite3_statement_backend*>(prep.statement().get_backend());
    auto st = sqliteStatement->stmt_;

    sqlite3_reset(st);
    sqlite3_bind_pointer(st, 1, cstrAccountIDs.data(), "carray", 0);
    sqlite3_bind_int(st, 2, cstrAccountIDs.size());
    sqlite3_bind_pointer(st, 3, cstrIssuers.data(), "carray", 0);
    sqlite3_bind_int(st, 4, cstrIssuers.size());
    sqlite3_bind_pointer(st, 5, cstrAssetCodes.data(), "carray", 0);
    sqlite3_bind_int(st, 6, cstrAssetCodes.size());

    std::vector<LedgerEntry> res;
    while (true)
    {
        int stepRes = sqlite3_step(st);
        if (stepRes == SQLITE_DONE)
        {
            break;
        }
        else if (stepRes == SQLITE_ROW)
        {
            res.emplace_back(sqliteFetchTrustLine(st));
        }
        else
        {
            // TODO(jonjove): What to do?
            std::abort();
        }
    }
    return res;
}

#ifdef USE_POSTGRES
static std::vector<LedgerEntry>
postgresSpecificBulkLoadTrustLines(
    Database& db, std::vector<std::string> const& accountIDs,
    std::vector<std::string> const& issuers,
    std::vector<std::string> const& assetCodes)
{
    assert(accountIDs.size() == issuers.size());
    assert(accountIDs.size() == assetCodes.size());

    std::string accountID, assetCode, issuer;
    int64_t balance, limit;
    uint32_t assetType, flags, lastModified;
    Liabilities liabilities;
    soci::indicator buyingLiabilitiesInd, sellingLiabilitiesInd;

    std::string strAccountIDs;
    std::string strIssuers;
    std::string strAssetCodes;
    auto pg = dynamic_cast<soci::postgresql_session_backend*>(db.getSession().get_backend());
    marshalToPGArray(pg->conn_, strAccountIDs, accountIDs);
    marshalToPGArray(pg->conn_, strIssuers, issuers);
    marshalToPGArray(pg->conn_, strAssetCodes, assetCodes);

    auto prep = db.getPreparedStatement(
        "WITH r AS (SELECT unnest(:v1::TEXT[]), unnest(:v2::TEXT[]), "
        "unnest(:v3::TEXT[])) SELECT accountid, assettype, assetcode, issuer, "
        "tlimit, balance, flags, lastmodified, buyingliabilities, "
        "sellingliabilities FROM trustlines "
        "WHERE (accountid, issuer, assetcode) IN (SELECT * FROM r)");
    auto& st = prep.statement();
    st.exchange(soci::use(strAccountIDs));
    st.exchange(soci::use(strIssuers));
    st.exchange(soci::use(strAssetCodes));
    st.exchange(soci::into(accountID));
    st.exchange(soci::into(assetType));
    st.exchange(soci::into(assetCode));
    st.exchange(soci::into(issuer));
    st.exchange(soci::into(limit));
    st.exchange(soci::into(balance));
    st.exchange(soci::into(flags));
    st.exchange(soci::into(lastModified));
    st.exchange(soci::into(liabilities.buying, buyingLiabilitiesInd));
    st.exchange(soci::into(liabilities.selling, sellingLiabilitiesInd));
    st.define_and_bind();
    {
        auto timer = db.getSelectTimer("trust");
        st.execute(true);
    }

    std::vector<LedgerEntry> res;
    while (st.got_data())
    {
        res.emplace_back();
        auto& le = res.back();
        le.data.type(TRUSTLINE);
        auto& tl = le.data.trustLine();

        tl.accountID = KeyUtils::fromStrKey<PublicKey>(accountID);

        assert(assetType != ASSET_TYPE_NATIVE);
        tl.asset.type(static_cast<AssetType>(assetType));
        if (assetType == ASSET_TYPE_CREDIT_ALPHANUM4)
        {
            tl.asset.alphaNum4().issuer = KeyUtils::fromStrKey<PublicKey>(issuer);
            strToAssetCode(tl.asset.alphaNum4().assetCode, assetCode);
        }
        else
        {
            tl.asset.alphaNum12().issuer = KeyUtils::fromStrKey<PublicKey>(issuer);
            strToAssetCode(tl.asset.alphaNum12().assetCode, assetCode);
        }

        tl.limit = limit;
        tl.balance = balance;
        tl.flags = flags;
        le.lastModifiedLedgerSeq = lastModified;

        assert(buyingLiabilitiesInd == sellingLiabilitiesInd);
        if (buyingLiabilitiesInd == soci::i_ok)
        {
            tl.ext.v(1);
            tl.ext.v1().liabilities = liabilities;
        }

        st.fetch();
    }
    return res;
}
#endif

std::unordered_map<LedgerKey, std::shared_ptr<LedgerEntry const>>
LedgerTxnRoot::Impl::bulkLoadTrustLines(std::vector<LedgerKey> const& keys)
{
    std::vector<std::string> accountIDs;
    std::vector<std::string> issuers;
    std::vector<std::string> assetCodes;
    accountIDs.reserve(keys.size());
    issuers.reserve(keys.size());
    assetCodes.reserve(keys.size());
    for (auto const& k : keys)
    {
        assert(k.type() == TRUSTLINE);
        accountIDs.emplace_back(KeyUtils::toStrKey(k.trustLine().accountID));

        auto const& asset = k.trustLine().asset;
        assert(asset.type() != ASSET_TYPE_NATIVE);
        assetCodes.emplace_back();
        if (asset.type() == ASSET_TYPE_CREDIT_ALPHANUM4)
        {
            assetCodeToStr(asset.alphaNum4().assetCode, assetCodes.back());
            issuers.emplace_back(KeyUtils::toStrKey(asset.alphaNum4().issuer));
        }
        else if (asset.type() == ASSET_TYPE_CREDIT_ALPHANUM12)
        {
            assetCodeToStr(asset.alphaNum12().assetCode, assetCodes.back());
            issuers.emplace_back(KeyUtils::toStrKey(asset.alphaNum12().issuer));
        }
    }

    std::vector<LedgerEntry> entries;
    if (mDatabase.isSqlite())
    {
        entries = sqliteSpecificBulkLoadTrustLines(
            mDatabase, accountIDs, issuers, assetCodes);
    }
    else
    {
#ifdef USE_POSTGRES
        entries = postgresSpecificBulkLoadTrustLines(
            mDatabase, accountIDs, issuers, assetCodes);
#else
        std::abort();
#endif
    }

    std::unordered_map<LedgerKey, std::shared_ptr<LedgerEntry const>> res;
    for (auto const& le : entries)
    {
        res.emplace(LedgerEntryKey(le), std::make_shared<LedgerEntry const>(le));
    }
    for (auto const& key : keys)
    {
        if (res.find(key) == res.end())
        {
            res.emplace(key, nullptr);
        }
    }
    return res;
}
}
