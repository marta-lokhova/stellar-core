// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/SqliteUtils.h"
#include "util/types.h"

namespace stellar
{

template <>
bool
sqliteRead<std::string>(sqlite3_stmt* st, std::string& res, int column, bool canBeNull)
{
    auto cstr = reinterpret_cast<char const*>(sqlite3_column_text(st, column));
    if (!canBeNull)
    {
        assert(cstr);
        res = cstr;
    }
    else if (cstr)
    {
        res = cstr;
    }
    return cstr;
}

template <>
bool
sqliteRead<PublicKey>(sqlite3_stmt* st, PublicKey& res, int column, bool canBeNull)
{
    std::string str;
    bool loaded = sqliteRead(st, str, column, canBeNull);
    if (loaded)
    {
        res = KeyUtils::fromStrKey<PublicKey>(str);
    }
    return loaded;
}

template <>
bool
sqliteRead<xdr::xstring<32>>(sqlite3_stmt* st, xdr::xstring<32>& res, int column, bool canBeNull)
{
    std::string str;
    bool loaded = sqliteRead(st, str, column, canBeNull);
    if (loaded)
    {
        res = str;
    }
    return loaded;
}

template <>
bool
sqliteRead<xdr::xstring<64>>(sqlite3_stmt* st, xdr::xstring<64>& res, int column, bool canBeNull)
{
    std::string str;
    bool loaded = sqliteRead(st, str, column, canBeNull);
    if (loaded)
    {
        res = str;
    }
    return loaded;
}

template <>
bool
sqliteRead<int32_t>(sqlite3_stmt* st, int32_t& res, int column, bool canBeNull)
{
    if (!canBeNull)
    {
        res = sqlite3_column_int(st, column);
        return true;
    }
    else if (sqlite3_column_bytes(st, column) > 0)
    {
        res = sqlite3_column_int(st, column);
        return true;
    }
    return false;
}

template <>
bool
sqliteRead<int64_t>(sqlite3_stmt* st, int64_t& res, int column, bool canBeNull)
{
    if (!canBeNull)
    {
        res = sqlite3_column_int64(st, column);
        return true;
    }
    else if (sqlite3_column_bytes(st, column) > 0)
    {
        res = sqlite3_column_int64(st, column);
        return true;
    }
    return false;
}

template <>
bool
sqliteRead<uint32_t>(sqlite3_stmt* st, uint32_t& res, int column, bool canBeNull)
{
    if (!canBeNull)
    {
        res = sqlite3_column_int(st, column);
        return true;
    }
    else if (sqlite3_column_bytes(st, column) > 0)
    {
        res = sqlite3_column_int(st, column);
        return true;
    }
    return false;
}

template <>
bool
sqliteRead<uint64_t>(sqlite3_stmt* st, uint64_t& res, int column, bool canBeNull)
{
    if (!canBeNull)
    {
        res = sqlite3_column_int64(st, column);
        return true;
    }
    else if (sqlite3_column_bytes(st, column) > 0)
    {
        res = sqlite3_column_int64(st, column);
        return true;
    }
    return false;
}

bool
sqliteReadLiabilities(sqlite3_stmt* st, Liabilities& res, int buyingColumn, int sellingColumn)
{
    bool hasBuyingLiab = sqliteRead(st, res.buying, buyingColumn, true);
    bool hasSellingLiab = sqliteRead(st, res.selling, sellingColumn, true);
    assert(hasBuyingLiab == hasSellingLiab);
    return hasBuyingLiab;
}

void
sqliteReadAsset(sqlite3_stmt* st, Asset& res, int assetTypeColumn, int assetCodeColumn, int issuerColumn)
{
    uint32_t assetType;
    sqliteRead(st, assetType, assetTypeColumn);

    res.type(static_cast<AssetType>(assetType));
    if (assetType == ASSET_TYPE_CREDIT_ALPHANUM4)
    {
        std::string assetCode;
        sqliteRead(st, assetCode, assetCodeColumn);
        strToAssetCode(res.alphaNum4().assetCode, assetCode);
        sqliteRead(st, res.alphaNum4().issuer, issuerColumn);
    }
    else if (assetType == ASSET_TYPE_CREDIT_ALPHANUM12)
    {
        std::string assetCode;
        sqliteRead(st, assetCode, assetCodeColumn);
        strToAssetCode(res.alphaNum12().assetCode, assetCode);
        sqliteRead(st, res.alphaNum12().issuer, issuerColumn);
    }
}
}
