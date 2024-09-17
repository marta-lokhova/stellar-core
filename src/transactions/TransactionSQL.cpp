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
    releaseAssert(threadIsMain());
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
deleteOldTransactionHistoryEntries(soci::session& sess, uint32_t ledgerSeq,
                                   uint32_t count)
{
    ZoneScoped;
    DatabaseUtils::deleteOldEntriesHelper(sess, ledgerSeq, count,
                                          "txhistory", "ledgerseq");
}

void
deleteNewerTransactionHistoryEntries(soci::session& sess, uint32_t ledgerSeq)
{
    ZoneScoped;
    DatabaseUtils::deleteNewerEntriesHelper(sess, ledgerSeq,
                                            "txhistory", "ledgerseq");
}

}
