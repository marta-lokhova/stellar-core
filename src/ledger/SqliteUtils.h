// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "crypto/KeyUtils.h"
#include "crypto/SecretKey.h"
#include "crypto/SignerKey.h"
#include "xdr/Stellar-ledger-entries.h"
#include "xdrpp/types.h"

#include <soci-sqlite3.h>

namespace stellar
{

typedef sqlite_api::sqlite3_stmt sqlite3_stmt;

template <typename T>
bool
sqliteRead(sqlite3_stmt* st, T& res, int column, bool canBeNull);

template <>
bool
sqliteRead<std::string>(sqlite3_stmt* st, std::string& res, int column, bool canBeNull);

template <>
bool
sqliteRead<xdr::xstring<32>>(sqlite3_stmt* st, xdr::xstring<32>& res, int column, bool canBeNull);

template <>
bool
sqliteRead<PublicKey>(sqlite3_stmt* st, PublicKey& res, int column, bool canBeNull);

template <>
bool
sqliteRead<int32_t>(sqlite3_stmt* st, int32_t& res, int column, bool canBeNull);

template <>
bool
sqliteRead<int64_t>(sqlite3_stmt* st, int64_t& res, int column, bool canBeNull);

template <>
bool
sqliteRead<uint32_t>(sqlite3_stmt* st, uint32_t& res, int column, bool canBeNull);

template <>
bool
sqliteRead<uint64_t>(sqlite3_stmt* st, uint64_t& res, int column, bool canBeNull);

template <typename T>
void
sqliteRead(sqlite3_stmt* st, T& res, int column)
{
    sqliteRead(st, res, column, false);
}

template <typename T>
void
sqliteRead(sqlite3_stmt* st, xdr::pointer<T>& res, int column)
{
    T val;
    if (sqliteRead(st, val, column, true))
    {
        res.activate() = val;
    }
}

bool
sqliteReadLiabilities(sqlite3_stmt* st, Liabilities& res, int buyingColumn, int sellingColumn);

void
sqliteReadAsset(sqlite3_stmt* st, Asset& res, int assetTypeColumn, int assetCodeColumn, int issuerColumn);
}
