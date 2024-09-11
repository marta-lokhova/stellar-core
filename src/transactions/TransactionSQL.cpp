// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/TransactionSQL.h"
#include "crypto/Hex.h"
#include "database/Database.h"
#include "database/DatabaseUtils.h"
#include "herder/TxSetFrame.h"
#include "history/HistoryManager.h"
#include "ledger/LedgerHeaderUtils.h"
#include "ledger/LedgerManager.h"
#include "main/Application.h"
#include "util/Decoder.h"
#include "util/GlobalChecks.h"
#include "util/XDRStream.h"
#include "xdrpp/marshal.h"
#include "xdrpp/message.h"
#include <Tracy.hpp>

namespace stellar
{
void
createTxSetHistoryTable(Database& db)
{
    db.getSession() << "DROP TABLE IF EXISTS txsethistory";
    db.getSession() << "CREATE TABLE txsethistory ("
                       "ledgerseq   INT NOT NULL CHECK (ledgerseq >= 0),"
                       "txset       TEXT NOT NULL,"
                       "PRIMARY KEY (ledgerseq)"
                       ")";
}

void
storeTransaction(Database& db, uint32_t ledgerSeq,
                 TransactionFrameBasePtr const& tx, TransactionMeta const& tm,
                 TransactionResultSet const& resultSet, Config const& cfg)
{
    ZoneScoped;
    std::string txBody =
        decoder::encode_b64(xdr::xdr_to_opaque(tx->getEnvelope()));
    std::string txResult =
        decoder::encode_b64(xdr::xdr_to_opaque(resultSet.results.back()));
    std::string meta = decoder::encode_b64(xdr::xdr_to_opaque(tm));

    std::string txIDString = binToHex(tx->getContentsHash());
    uint32_t txIndex = static_cast<uint32_t>(resultSet.results.size());

    std::string sqlStr;
    if (cfg.isUsingBucketListDB())
    {
        sqlStr = "INSERT INTO txhistory "
                 "( txid, ledgerseq, txindex,  txbody, txresult) VALUES "
                 "(:id,  :seq,      :txindex, :txb,   :txres)";
    }
    else
    {
        sqlStr =
            "INSERT INTO txhistory "
            "( txid, ledgerseq, txindex,  txbody, txresult, txmeta) VALUES "
            "(:id,  :seq,      :txindex, :txb,   :txres,   :meta)";
    }

    auto prep = db.getPreparedStatement(sqlStr);
    auto& st = prep.statement();
    st.exchange(soci::use(txIDString));
    st.exchange(soci::use(ledgerSeq));
    st.exchange(soci::use(txIndex));
    st.exchange(soci::use(txBody));
    st.exchange(soci::use(txResult));

    if (!cfg.isUsingBucketListDB())
    {
        st.exchange(soci::use(meta));
    }

    st.define_and_bind();
    {
        auto timer = db.getInsertTimer("txhistory");
        st.execute(true);
    }

    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("Could not update data in SQL");
    }
}

void
dropSupportTransactionFeeHistory(Database& db)
{
    ZoneScoped;
    db.getSession() << "DROP TABLE IF EXISTS txfeehistory";
}

void
dropSupportTxSetHistory(Database& db)
{
    ZoneScoped;
    db.getSession() << "DROP TABLE IF EXISTS txsethistory";
}

void
dropTransactionHistory(Database& db, Config const& cfg)
{
    ZoneScoped;
    db.getSession() << "DROP TABLE IF EXISTS txhistory";

    // txmeta only supported when BucketListDB is not enabled
    std::string txMetaColumn =
        cfg.isUsingBucketListDB() ? "" : "txmeta      TEXT NOT NULL,";

    db.getSession() << "CREATE TABLE txhistory ("
                       "txid        CHARACTER(64) NOT NULL,"
                       "ledgerseq   INT NOT NULL CHECK (ledgerseq >= 0),"
                       "txindex     INT NOT NULL,"
                       "txbody      TEXT NOT NULL,"
                       "txresult    TEXT NOT NULL," +
                           txMetaColumn +
                           "PRIMARY KEY (ledgerseq, txindex)"
                           ")";

    db.getSession() << "CREATE INDEX histbyseq ON txhistory (ledgerseq);";

    createTxSetHistoryTable(db);
}

void
deleteOldTransactionHistoryEntries(Database& db, uint32_t ledgerSeq,
                                   uint32_t count)
{
    ZoneScoped;
    DatabaseUtils::deleteOldEntriesHelper(db.getSession(), ledgerSeq, count,
                                          "txhistory", "ledgerseq");
}

void
deleteNewerTransactionHistoryEntries(Database& db, uint32_t ledgerSeq)
{
    ZoneScoped;
    DatabaseUtils::deleteNewerEntriesHelper(db.getSession(), ledgerSeq,
                                            "txhistory", "ledgerseq");
}

}
