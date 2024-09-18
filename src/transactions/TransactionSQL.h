// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "database/Database.h"
#include "herder/TxSetFrame.h"
#include "overlay/StellarXDR.h"
#include "transactions/TransactionFrameBase.h"

namespace stellar
{
class Application;
class XDROutputFileStream;

void createTxSetHistoryTable(Database& db);
void dropSupportTransactionFeeHistory(Database& db);
void dropSupportTxSetHistory(Database& db);

void dropTransactionHistory(Database& db, Config const& cfg);

void deleteOldTransactionHistoryEntries(soci::session& sess, uint32_t ledgerSeq,
                                        uint32_t count);

void deleteNewerTransactionHistoryEntries(soci::session& sess,
                                          uint32_t ledgerSeq);
}
