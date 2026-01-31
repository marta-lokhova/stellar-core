# stellar-core Knowledge Base

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
├─────────────┬─────────────┬─────────────┬──────────────┤
│   Herder    │   Ledger    │   History   │   Overlay    │
│   (SCP)     │   Manager   │   Manager   │   Manager    │
├─────────────┴─────────────┴─────────────┴──────────────┤
│                    Database Layer                       │
│  ┌─────────────────┐       ┌─────────────────┐         │
│  │    Main DB      │       │    Misc DB      │         │
│  │  (ledger state) │       │ (consensus/peer)│         │
│  └─────────────────┘       └─────────────────┘         │
└─────────────────────────────────────────────────────────┘
```

---

## Database Organization

### Why Two Databases?

SQLite locks the entire database file during writes, preventing parallelism between ledger apply and consensus/mempool operations. The solution is to split into two database files that can handle independent writes:

- **Main DB**: Ledger state, touched on startup and during apply
- **Misc DB**: Consensus data, overlay data, upgrades

Postgres's concurrency model allows concurrent writes to different tables, so this split only applies to SQLite.

### Table Ownership

**Main DB** (touched during ledger apply):
| Table | Purpose |
|-------|---------|
| `storestate` | LCL hash, HAS, schema version, network passphrase, rebuild flags |
| `ledgerheaders` | Ledger header history |
| `offers` | Offer ledger entries |
| `accounts` | Account ledger entries |
| `trustlines` | Trustline ledger entries |
| `claimablebalance` | Claimable balance entries |
| `liquiditypool` | Liquidity pool entries |
| `contractdata` | Soroban contract data |
| `contractcode` | Soroban contract code |
| `configsettings` | Network config settings |
| `ttl` | Soroban TTL entries |

**Misc DB** (touched during consensus/networking):
| Table | Purpose |
|-------|---------|
| `peers` | Known peer addresses |
| `ban` | Banned node IDs |
| `scphistory` | SCP envelope history |
| `scpquorums` | Quorum set definitions |
| `quoruminfo` | Node to quorum set mapping |
| `slotstate` | SCP slot state, tx sets, upgrades, misc schema version |

---

## Session & Pool Management

### Session Types

| Method | SQLite (on-disk) | Postgres | In-memory SQLite |
|--------|------------------|----------|------------------|
| `getSession()` | main DB | main DB | main DB |
| `getMiscSession()` | misc DB | main DB (fallback) | main DB (fallback) |
| `getPool()` | main pool | main pool | throws |
| `getMiscPool()` | misc pool | throws | throws |

### Usage Rules

- **Main thread** uses `getSession()` / `getMiscSession()`
- **Background threads** use `getPool()` / `getMiscPool()` to get connections
- Always use the session/pool matching the table being accessed
- On Postgres, `getMiscSession()` correctly falls back to main session

### Prepared Statement Cache

- Statements cached per session name in `mCaches` map
- Must call `clearPreparedStatementCache()` before schema changes
- Cache cleared automatically on transaction commit in some paths

---

## Threading Model

### Thread Types

| Thread | Purpose | DB Access |
|--------|---------|-----------|
| Main | Consensus, networking, commands | `getSession()`, `getMiscSession()` |
| Apply | Parallel ledger application | Pool connections |
| Ledger Close | Background ledger ops | Pool connections |
| Background Workers | History archiving, maintenance | Pool connections |

### Thread Safety

- `releaseAssert(threadIsMain())` guards main-thread-only operations
- `mApp.threadIsType(Application::ThreadType::APPLY)` checks for apply thread
- Database pools provide thread-safe connection borrowing
- Prepared statement cache protected by `mStatementsMutex`

---

## Schema Migrations

### Version Constants

```cpp
static constexpr unsigned long MIN_SCHEMA_VERSION = 25;
static constexpr unsigned long SCHEMA_VERSION = 26;
static constexpr unsigned long FIRST_MAIN_VERSION_WITH_MISC = 26;
static constexpr unsigned long MIN_MISC_SCHEMA_VERSION = 0;
static constexpr unsigned long MISC_SCHEMA_VERSION = 1;
```

### Migration Flow

1. `upgradeToCurrentSchema()` called on startup
2. Misc DB migrated first (if `canUseMiscDB()`)
3. Main DB migrated second
4. Each version increment calls `applySchemaUpgrade()` or `applyMiscSchemaUpgrade()`
5. Version stored after successful migration

### Migration v25 → v26

1. Create tables in misc DB
2. Copy data from main to misc via `ATTACH DATABASE`
3. Drop migrated tables from main DB
4. `DETACH DATABASE` after commit

---

## History & Checkpointing

### Checkpoint Structure

- Checkpoints every 64 ledgers (configurable)
- Files per checkpoint:
  - Ledger headers (`.xdr.gz`)
  - Transactions (`.xdr.gz`)
  - Transaction results (`.xdr.gz`)
  - SCP messages (`.xdr.gz`)
  - Bucket files

### CheckpointBuilder

- Accumulates data during ledger close
- Writes XDR streams to temporary files
- Must fsync before publishing
- Atomic patterns: write temp → fsync → rename → fsync dir

### Archive Publishing

- Publishes to configured history archives
- Handles partial upload failures
- Can resume after interruption

---

## Key Components

### Database (`src/database/Database.cpp`)

- Manages SQLite/Postgres connections
- Session and pool management
- Schema versioning and migrations
- Prepared statement caching

### PersistentState (`src/main/PersistentState.cpp`)

- Stores critical node state
- Two tables: `storestate` (main), `slotstate` (misc)
- Entries: LCL, HAS, schema versions, SCP data, upgrades

### HerderPersistence (`src/herder/HerderPersistenceImpl.cpp`)

- Persists SCP messages and quorum sets
- Writes to `scphistory`, `scpquorums`, `quoruminfo`
- Used for crash recovery and history archiving

### LedgerManager (`src/ledger/LedgerManagerImpl.cpp`)

- Manages ledger state transitions
- Coordinates ledger close
- Stores LCL and HAS

### HistoryManager (`src/history/HistoryManagerImpl.cpp`)

- Manages history archiving
- Coordinates checkpoint publishing
- Handles archive verification

---

## Common Patterns

### Transaction Wrapping

```cpp
soci::transaction tx(session);
// ... operations ...
tx.commit();
```

### Prepared Statement Usage

```cpp
auto prep = db.getPreparedStatement(query, session);
auto& st = prep.statement();
st.exchange(soci::use(param));
st.exchange(soci::into(result));
st.define_and_bind();
st.execute(true);
```

### SOCI Row Iteration

```cpp
st.execute(true);
while (st.got_data()) {
    // process row
    st.fetch();  // Don't forget this!
}
```

### Conditional DB Access

```cpp
if (canUseMiscDB()) {
    // SQLite with split DB
    getMiscSession()...
} else {
    // Postgres or in-memory - use main
    getSession()...
}
```

---

## Debugging Tips

### Check Which DB a Table Is In

If you get "table not found", verify you're using the right session:
- Misc tables → `getMiscSession()`
- Main tables → `getSession()`

### Schema Version Issues

```sql
-- Check main schema version
SELECT state FROM storestate WHERE statename = 'databaseschema';

-- Check misc schema version (SQLite only)
SELECT state FROM slotstate WHERE statename = 'miscdatabaseschema';
```

### Connection Pool Issues

- Pool only available when `canUsePool()` returns true
- In-memory SQLite cannot use pools
- Misc pool only available when `canUseMiscDB()` returns true

---

## Rust Overlay Integration

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    stellar-core                          │
├─────────────┬─────────────┬─────────────┬──────────────┤
│   Herder    │   Ledger    │   History   │    Rust      │
│   (SCP)     │   Manager   │   Manager   │   Overlay    │
│             │             │             │   Manager    │
└──────┬──────┴─────────────┴─────────────┴──────┬───────┘
       │                                          │
       │ Unix Socket IPC                          │
       │                                          │
┌──────▼──────────────────────────────────────────▼───────┐
│                stellar-overlay (Rust)                    │
├─────────────────────────────────────────────────────────┤
│  Mempool │ Peer Connections │ SCP Relay │ TX Flooding   │
└─────────────────────────────────────────────────────────┘
```

### IPC Message Types

| Message | Direction | Purpose |
|---------|-----------|---------|
| BROADCAST_SCP | Core→Overlay | Broadcast SCP envelope to peers |
| SCP_RECEIVED | Overlay→Core | SCP envelope received from peer |
| REQUEST_NOMINATION_HASH | Core→Overlay | Build TX set, return hash |
| NOMINATION_HASH | Overlay→Core | TX set hash response |
| REQUEST_TX_SET | Core→Overlay | Request TX set by hash |
| TX_SET_AVAILABLE | Overlay→Core | TX set XDR response |
| SUBMIT_TX | Core→Overlay | Add TX to mempool |
| TX_SET_EXTERNALIZED | Core→Overlay | Clear TXs from mempool |
| LEDGER_CLOSED | Core→Overlay | Ledger close notification |

### GeneralizedTransactionSet XDR Format

**CRITICAL**: Protocol ≥23 requires parallel Soroban phase format.

```
GeneralizedTransactionSet {
  v: 1
  v1TxSet: {
    previousLedgerHash: Hash
    phases: [
      Phase 0 (CLASSIC): TransactionPhase v=0 (v0Components)
        - Empty phase = 0 components (valid)
        - Phase with empty component = 1 component with 0 txs (INVALID)
      
      Phase 1 (SOROBAN): TransactionPhase v=1 (parallelTxsComponent)  // MUST be v=1 for protocol ≥23!
        - Uses parallelTxsComponent with executionStages
        - Even empty Soroban phase must use v=1, not v=0
        - Empty = { baseFee: none, executionStages: [] }
    ]
  }
}
```

**Validation**:
- `validateTxSetXDRStructure()` in TxSetFrame.cpp
- `checkValidSoroban()` at TxSetFrame.cpp:1785-1795 validates parallel format
- Constant: `PARALLEL_SOROBAN_PHASE_PROTOCOL_VERSION = V_23` (ProtocolVersion.h:56)

### TX Flow

1. `Herder::recvTransaction()` validates and queues TX locally
2. `broadcastTransaction()` forwards to overlay via IPC
3. Overlay stores in mempool with fee/op ordering
4. `triggerNextLedger()` calls `getTxSetForNomination()`
5. Overlay builds TX set, caches it, returns hash
6. SCP nominates the hash
7. On externalize: `notifyTxSetExternalized()` clears mempool
