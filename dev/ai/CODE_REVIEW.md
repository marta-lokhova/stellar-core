# Code Review Instructions for stellar-core

## General Principles

When reviewing code changes, analyze them in this priority order:

1. **Crash Recovery & Durability** - Can data be corrupted or lost?
2. **Atomicity & Consistency** - What state is left after partial failure?
3. **Correct Usage of APIs** - Are new/modified interfaces used correctly everywhere?
4. **Minor Issues** - Inefficiencies, code style, non-critical bugs

---

## 1. Crash Recovery & Durability

### File I/O Operations

**Check fsync ordering:**
- Data must be fsynced BEFORE metadata that references it
- Parent directory must be fsynced after creating/renaming files
- Temporary files should be fsynced before atomic rename

```
BAD:  write metadata → write data → fsync
GOOD: write data → fsync data → write metadata → fsync metadata → fsync directory
```

**Atomic file patterns:**
- Look for write-to-temp + rename patterns
- Verify temp file is fsynced before rename
- Verify directory is fsynced after rename
- Check: what happens if crash occurs between steps?

**Questions to ask:**
- If power is lost mid-operation, what state is on disk?
- Can the operation be resumed/retried after crash?
- Are there ordering dependencies between file writes?

### Database Operations

**Transaction boundaries:**
- Identify what's inside vs outside transactions
- Check if related writes span multiple transactions (atomicity gap)
- Verify transaction commit happens AFTER all critical writes

**Questions to ask:**
- If crash occurs after commit but before subsequent operations, is state consistent?
- Are there operations that should be in the same transaction but aren't?

---

## 2. Atomicity & Consistency

### Multi-Phase Operations

**Identify phases that can fail independently:**
- Migration sequences (misc DB then main DB)
- File copies followed by deletes
- Any "prepare → commit" patterns

**For each phase boundary, ask:**
- If phase N succeeds but phase N+1 fails, what state are we in?
- Can we detect this partial state on restart?
- Can we recover/retry safely?

### Resource Cleanup on Failure

**Check exception paths:**
- Are resources released when exceptions occur?
- RAII preferred over manual cleanup
- Watch for operations that aren't rolled back by transaction abort (e.g., SQLite ATTACH)

**Non-transactional operations inside transactions:**
```cpp
// DANGER: ATTACH persists even if transaction rolls back
soci::transaction tx(session);
session << "ATTACH DATABASE...";  // Not rolled back on failure!
doWork();  // If this throws...
tx.commit();
session << "DETACH DATABASE...";  // Never reached!
```

---

## 3. Correct Usage of APIs

### When New Functions/Classes Are Added

**Find ALL call sites:**
- Use grep/search to find every usage
- Don't assume the diff shows all relevant changes
- Check test files too - they may use APIs incorrectly

**Check consistency:**
- If function A has fallback behavior, do related functions (A', A'') have matching behavior?
- If condition X guards operation Y, is X the right condition everywhere?

**Example pattern to watch for:**
```cpp
// Inconsistent fallback behavior
getMiscSession() {
    if (!canUseMiscDB()) return mSession;  // Falls back
    return mMiscSession;
}
getMiscPool() {
    if (!canUseMiscDB()) throw;  // Throws instead!
    return mMiscPool;
}
```

### Condition Mismatches

Look for cases where similar-but-different conditions are used:
- `canUsePool()` vs `canUseMiscDB()`
- `isSqlite()` vs `!isPostgres()`
- `hasFeature()` vs `featureEnabled()`

---

## 4. Database Class Specific

### Session Selection (getSession vs getMiscSession)

**Table ownership:**
```
Main DB:  storestate, ledgerheaders, offers, accounts, trustlines, ...
Misc DB:  peers, ban, quoruminfo, scpquorums, scphistory, slotstate
```

**Verification checklist:**
- [ ] SQL accessing misc tables uses `getMiscSession()` or `getRawMiscSession()`
- [ ] SQL accessing main tables uses `getSession()` or `getRawSession()`
- [ ] Pool usage: misc tables use `getMiscPool()`, main tables use `getPool()`

**Postgres behavior:**
- `getMiscSession()` falls back to main session (correct)
- `getMiscPool()` throws (inconsistent - potential bug)
- All tables are in one DB, so using misc session is fine

**SQLite behavior:**
- Misc session connects to separate `-misc.db` file
- Using wrong session = table not found error

### Schema Migrations

**Check migration ordering:**
1. Create/populate destination before removing source
2. Version numbers updated only after successful migration
3. Consider: what if crash between steps?

**Verify idempotency:**
- Can migration be re-run safely if interrupted?
- Do `DROP IF EXISTS` / `CREATE IF NOT EXISTS` patterns handle retry?

---

## 5. History/Checkpoint Specific

### CheckpointBuilder & File Operations

**Verify fsync discipline:**
- XDR files fsynced before being referenced
- Checkpoint files fsynced before publishing
- Directory fsynced after file creation

**Atomic checkpoint pattern:**
- Build checkpoint in temp location
- Fsync all files
- Rename/move to final location
- Fsync directories

### Archive Publishing

**Check failure scenarios:**
- What if upload fails mid-checkpoint?
- Are partial uploads cleaned up?
- Can publishing resume after failure?

---

## 6. Common Bug Patterns

### Loop Iteration
```cpp
// Missing advancement - infinite loop
while (st.got_data()) {
    process(data);
    // MISSING: st.fetch();
}
```

### Condition/Action Mismatch
```cpp
// Wrong condition for the action
if (canUsePool()) {  // Should be canUseMiscDB()
    getMiscPool();   // Throws when canUseMiscDB() is false
}
```

### Transaction Scope
```cpp
// Operations that should be atomic aren't
tx1.commit();  // Phase 1 done
// CRASH HERE = inconsistent state
tx2.begin();   // Phase 2 never starts
```

### Fallback Inconsistency
```cpp
// Related functions should have matching fallback behavior
getX()     { if (!supported()) return default; ... }  // Falls back
getXPool() { if (!supported()) throw; ... }           // Throws - inconsistent!
```

### Wire Format Mismatch (IPC/Networking)
```cpp
// Different layers may expect different formats
// IPC: raw XDR bytes (SCPEnvelope)
// Peer-to-peer: StellarMessage union (4-byte discriminant + payload)

// WRONG: Sending raw envelope to peer
ipc.broadcastSCP(envelope_bytes);  // Raw SCPEnvelope
peer.send(envelope_bytes);          // Peer expects StellarMessage!

// CORRECT: Wrap in StellarMessage format
auto stellar_msg = makeHeader(SCP_MESSAGE) + envelope_bytes;
peer.send(stellar_msg);
```

**Check when reviewing networking/IPC code:**
- What format does the sender produce?
- What format does the receiver expect?
- Are there encoding transformations at boundaries?
- Are the XDR discriminant values aligned between C++ and Rust?

---

## Review Checklist

- [ ] **Durability:** Are file writes properly ordered and fsynced?
- [ ] **Atomicity:** What happens if failure occurs mid-operation?
- [ ] **Recovery:** Can the system recover from crash at any point?
- [ ] **API consistency:** Do related functions have matching behavior?
- [ ] **Condition correctness:** Are the right conditions used for each operation?
- [ ] **Session/pool selection:** Are database tables accessed via correct session?
- [ ] **Resource cleanup:** Are resources released on all error paths?
- [ ] **Loop correctness:** Do loops properly advance and terminate?
- [ ] **Wire format:** Are encoding formats aligned at IPC/network boundaries?
- [ ] **Enum alignment:** Do C++/Rust enum values match exactly?

---

## 7. XDR Serialization

### Before Implementing XDR Serialization

**ALWAYS read the validation/parsing code first:**
- Find where the XDR is parsed/validated
- Understand exact format requirements
- Check for edge cases (empty arrays, optional fields)

### Common XDR Pitfalls

**Empty vs Zero-length:**
```cpp
// These are DIFFERENT in validation!
phases.push_back(empty_phase);           // 1 phase
phases[0].components.resize(0);          // 0 components - OK

phases.push_back(phase_with_empty_comp); // 1 phase
phases[0].components.resize(1);          // 1 component
phases[0].components[0].txs.resize(0);   // 0 txs - INVALID!
```

**Phase count requirements:**
- GeneralizedTransactionSet requires exactly 2 phases
- Even if SOROBAN is empty, the phase must exist

### Validation Checklist

- [ ] Read the `validate*` function for this XDR type
- [ ] Check array size requirements (min, max, exact)
- [ ] Check for "empty container" vs "no container" distinction
- [ ] Verify field ordering matches XDR definition
- [ ] Test with empty/minimal data first

---

## 8. IPC/RPC Boundary Testing

### Don't Hide Failures with Fallbacks

During development, remove fallback paths that could mask failures:

```cpp
// BAD during development - hides failures
if (overlayTxSet) {
    use(overlayTxSet);
} else {
    use(localTxSet);  // Silently falls back!
}

// GOOD during development - fails loudly with crash
if (overlayTxSet) {
    use(overlayTxSet);
} else {
    releaseAssert(false && "No overlay TX set");  // Crash immediately
}
```

Re-add fallbacks only after the primary path is tested.
