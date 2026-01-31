---
description: Review code changes for bugs, crash recovery issues, and correctness
argument-hint: <commit|file|commit-range>
---

# Code Review Task

First, read the code review guidelines from `dev/ai/CODE_REVIEW.md`.

Then examine the code changes specified: $ARGUMENTS

## Review Process

Analyze the changes following this priority order:

1. **Crash Recovery & Durability**
   - File I/O: Check fsync ordering (data before metadata)
   - Database: Check transaction boundaries
   - What happens if crash occurs mid-operation?

2. **Atomicity & Consistency**
   - Multi-phase operations: What if phase N succeeds but N+1 fails?
   - Resource cleanup on failure paths
   - Non-transactional operations inside transactions (e.g., SQLite ATTACH)

3. **Correct Usage of APIs**
   - Find ALL call sites of new/modified functions
   - Check for consistent fallback behavior
   - Verify condition/action matches (e.g., canUsePool vs canUseMiscDB)

4. **Database-specific** (if applicable)
   - Session selection: getSession() vs getMiscSession()
   - Main tables: storestate, ledgerheaders, offers, accounts, etc.
   - Misc tables: peers, ban, scphistory, scpquorums, quoruminfo, slotstate

5. **Minor Issues**
   - Loop correctness (iterator advancement)
   - Inefficiencies

## Output

Report findings organized by severity (Critical, High, Medium, Low).
