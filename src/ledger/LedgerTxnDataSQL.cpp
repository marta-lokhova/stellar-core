// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "crypto/KeyUtils.h"
#include "crypto/SecretKey.h"
#include "database/Database.h"
#include "ledger/LedgerTxnImpl.h"
#include "ledger/SqliteUtils.h"
#include "util/Decoder.h"
#include "util/Logging.h"
#include "util/types.h"

namespace stellar
{

std::shared_ptr<LedgerEntry const>
LedgerTxnRoot::Impl::loadData(LedgerKey const& key) const
{
    std::string actIDStrKey = KeyUtils::toStrKey(key.data().accountID);
    std::string dataName = decoder::encode_b64(key.data().dataName);

    std::string dataValue;
    soci::indicator dataValueIndicator;

    LedgerEntry le;
    le.data.type(DATA);
    DataEntry& de = le.data.data();

    std::string sql = "SELECT datavalue, lastmodified "
                      "FROM accountdata "
                      "WHERE accountid= :id AND dataname= :dataname";
    auto prep = mDatabase.getPreparedStatement(sql);
    auto& st = prep.statement();
    st.exchange(soci::into(dataValue, dataValueIndicator));
    st.exchange(soci::into(le.lastModifiedLedgerSeq));
    st.exchange(soci::use(actIDStrKey));
    st.exchange(soci::use(dataName));
    st.define_and_bind();
    st.execute(true);
    if (!st.got_data())
    {
        return nullptr;
    }

    de.accountID = key.data().accountID;
    de.dataName = key.data().dataName;

    if (dataValueIndicator != soci::i_ok)
    {
        throw std::runtime_error("bad database state");
    }
    decoder::decode_b64(dataValue, de.dataValue);

    return std::make_shared<LedgerEntry const>(std::move(le));
}

void
LedgerTxnRoot::Impl::insertOrUpdateData(LedgerEntry const& entry, bool isInsert)
{
    auto const& data = entry.data.data();
    std::string actIDStrKey = KeyUtils::toStrKey(data.accountID);
    std::string dataName = decoder::encode_b64(data.dataName);
    std::string dataValue = decoder::encode_b64(data.dataValue);

    std::string sql;
    if (isInsert)
    {
        sql = "INSERT INTO accountdata "
              "(accountid,dataname,datavalue,lastmodified)"
              " VALUES (:aid,:dn,:dv,:lm)";
    }
    else
    {
        sql = "UPDATE accountdata SET datavalue=:dv,lastmodified=:lm "
              " WHERE accountid=:aid AND dataname=:dn";
    }

    auto prep = mDatabase.getPreparedStatement(sql);
    auto& st = prep.statement();
    st.exchange(soci::use(actIDStrKey, "aid"));
    st.exchange(soci::use(dataName, "dn"));
    st.exchange(soci::use(dataValue, "dv"));
    st.exchange(soci::use(entry.lastModifiedLedgerSeq, "lm"));
    st.define_and_bind();
    st.execute(true);
    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("could not update SQL");
    }
}

void
LedgerTxnRoot::Impl::deleteData(LedgerKey const& key)
{
    auto const& data = key.data();
    std::string actIDStrKey = KeyUtils::toStrKey(data.accountID);
    std::string dataName = decoder::encode_b64(data.dataName);

    auto prep = mDatabase.getPreparedStatement(
        "DELETE FROM accountdata WHERE accountid=:id AND dataname=:s");
    auto& st = prep.statement();
    st.exchange(soci::use(actIDStrKey));
    st.exchange(soci::use(dataName));
    st.define_and_bind();
    {
        auto timer = mDatabase.getDeleteTimer("data");
        st.execute(true);
    }
    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("Could not update data in SQL");
    }
}

void
LedgerTxnRoot::Impl::dropData()
{
    throwIfChild();
    mEntryCache.clear();
    mBestOffersCache.clear();

    mDatabase.getSession() << "DROP TABLE IF EXISTS accountdata;";
    mDatabase.getSession() << "CREATE TABLE accountdata"
                              "("
                              "accountid    VARCHAR(56)  NOT NULL,"
                              "dataname     VARCHAR(64)  NOT NULL,"
                              "datavalue    VARCHAR(112) NOT NULL,"
                              "lastmodified INT          NOT NULL,"
                              "PRIMARY KEY  (accountid, dataname)"
                              ");";
}

static std::vector<LedgerEntry>
loadDataToEncode(Database& db)
{
    std::string accountID, dataName, dataValue;
    uint32_t lastModified;

    std::string sql = "SELECT accountid, dataname, datavalue, lastmodified "
                      "FROM accountdata";

    auto prep = db.getPreparedStatement(sql);
    auto& st = prep.statement();
    st.exchange(soci::into(accountID));
    st.exchange(soci::into(dataName));
    st.exchange(soci::into(dataValue));
    st.exchange(soci::into(lastModified));
    st.define_and_bind();
    st.execute(true);

    std::vector<LedgerEntry> res;
    while (st.got_data())
    {
        res.emplace_back();
        auto& le = res.back();
        le.data.type(DATA);

        auto& de = le.data.data();
        de.accountID = KeyUtils::fromStrKey<PublicKey>(accountID);
        de.dataName = dataName;
        decoder::decode_b64(dataValue, de.dataValue);
        le.lastModifiedLedgerSeq = lastModified;

        st.fetch();
    }
    return res;
}

void
LedgerTxnRoot::Impl::encodeDataNamesBase64()
{
    throwIfChild();
    mEntryCache.clear();
    mBestOffersCache.clear();

    CLOG(INFO, "Ledger")
        << "Loading all data entries from the accountdata table";
    auto dataToEncode = loadDataToEncode(mDatabase);

    // Note: The table must be recreated since dataname is part of the primary
    // key, so there could be a collision when updating it
    dropData();
    if (!mDatabase.isSqlite())
    {
        auto& session = mDatabase.getSession();
        session << "ALTER TABLE accountdata ALTER COLUMN dataname "
                   "SET DATA TYPE VARCHAR(88)";
    }

    size_t numUpdated = 0;
    for (auto const& le : dataToEncode)
    {
        insertOrUpdateData(le, true);

        if ((++numUpdated & 0xfff) == 0xfff ||
            (numUpdated == dataToEncode.size()))
        {
            CLOG(INFO, "Ledger") << "Wrote " << numUpdated << " data entries";
        }
    }
}

static LedgerEntry
sqliteFetchData(sqlite3_stmt* st)
{
    LedgerEntry le;
    le.data.type(DATA);
    auto& de = le.data.data();

    sqliteRead(st, de.accountID, 0);

    std::string dataName;
    sqliteRead(st, dataName, 1);
    decoder::decode_b64(dataName, de.dataName);

    std::string dataValue;
    sqliteRead(st, dataValue, 2);
    decoder::decode_b64(dataValue, de.dataValue);

    sqliteRead(st, le.lastModifiedLedgerSeq, 3);

    return le;
}

static std::vector<LedgerEntry>
sqliteSpecificBulkLoadData(
    Database& db, std::vector<std::string> const& accountIDs,
    std::vector<std::string> const& dataNames)
{
    assert(accountIDs.size() == dataNames.size());

    std::vector<const char*> cstrAccountIDs;
    std::vector<const char*> cstrDataNames;
    cstrAccountIDs.reserve(accountIDs.size());
    cstrDataNames.reserve(dataNames.size());
    for (size_t i = 0; i < accountIDs.size(); ++i)
    {
        cstrAccountIDs.emplace_back(accountIDs[i].c_str());
        cstrDataNames.emplace_back(dataNames[i].c_str());
    }

    std::string sqlJoin =
        "SELECT x.value, y.value FROM "
        "(SELECT rowid, value FROM carray(?, ?, 'char*') ORDER BY rowid) AS x "
        "INNER JOIN (SELECT rowid, value FROM carray(?, ?, 'char*') ORDER BY rowid) AS y ON x.rowid = y.rowid";
    std::string sql = "WITH r AS (" + sqlJoin +
        ") SELECT accountid, dataname, datavalue, lastmodified "
        "FROM accountdata WHERE (accountid, dataname) IN r";

    auto prep = db.getPreparedStatement(sql);
    auto sqliteStatement = dynamic_cast<soci::sqlite3_statement_backend*>(prep.statement().get_backend());
    auto st = sqliteStatement->stmt_;

    sqlite3_reset(st);
    sqlite3_bind_pointer(st, 1, cstrAccountIDs.data(), "carray", 0);
    sqlite3_bind_int(st, 2, cstrAccountIDs.size());
    sqlite3_bind_pointer(st, 3, cstrDataNames.data(), "carray", 0);
    sqlite3_bind_int(st, 4, cstrDataNames.size());

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
            res.emplace_back(sqliteFetchData(st));
        }
        else
        {
            // TODO(jonjove): What to do?
            std::abort();
        }
    }
    return res;
}

std::unordered_map<LedgerKey, std::shared_ptr<LedgerEntry const>>
LedgerTxnRoot::Impl::bulkLoadData(std::vector<LedgerKey> const& keys)
{
    std::vector<std::string> accountIDs;
    std::vector<std::string> dataNames;
    accountIDs.reserve(keys.size());
    for (auto const& k : keys)
    {
        assert(k.type() == DATA);
        accountIDs.emplace_back(KeyUtils::toStrKey(k.data().accountID));
        dataNames.emplace_back(decoder::encode_b64(k.data().dataName));
    }

    std::vector<LedgerEntry> entries;
    if (mDatabase.isSqlite())
    {
        entries = sqliteSpecificBulkLoadData(mDatabase, accountIDs, dataNames);
    }
    else
    {
        std::abort();
    }

    std::unordered_map<LedgerKey, std::shared_ptr<LedgerEntry const>> res;
    for (auto const& le : entries)
    {
        res.emplace(LedgerEntryKey(le),
                    std::make_shared<LedgerEntry const>(le));
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
