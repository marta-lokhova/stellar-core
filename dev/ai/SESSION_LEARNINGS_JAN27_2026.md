# Session Learnings - Rust Overlay Flooding Debug Session

**Date:** January 27, 2026  
**Focus:** Fixing 10-node consensus test failures

---

## What I Did Wrong (Anti-Patterns)

### 1. Jumped to Implementation Without Understanding
- **Mistake:** Started debugging rate limits and adding features before understanding the root cause
- **Should Have Done:** Read design docs first, trace message flow, understand what SHOULD happen vs what IS happening
- **Example:** Spent time hypothesizing about rate limits when the real issue was missing message forwarding

### 2. Made Assumptions Instead of Asking Questions
- **Mistake:** Assumed SCP messages should be broadcast to all peers individually
- **Should Have Done:** Asked: "How should message propagation work in a partially connected network?"
- **Example:** The design was pull-based flooding, not push broadcasting

### 3. Missed the Obvious by Looking Too Deep
- **Mistake:** Investigated complex scenarios (mempool sync, TX set fetching) when the issue was simpler
- **Should Have Done:** Start with basic message flow tracing - are messages reaching all nodes?
- **Example:** Could have found "no forwarding" bug immediately by checking if node B forwards messages from node A to node C

### 4. Added Complexity Before Verifying Simplicity
- **Mistake:** Implemented deduplication tracking before verifying basic flooding worked
- **Should Have Done:** Fix one thing at a time, verify it works, then optimize
- **Example:** Should have implemented basic flooding → test → then add smart deduplication → test

---

## What Worked Well

### 1. Following Test-Driven Development
- Write test expectations first
- Implement minimal fix
- Run test immediately
- Fixed the crankUntil bug quickly because we had a clear failing test

### 2. Incremental Debugging
- Used trace logs to understand message flow
- Counted SCP receives to quantify the problem (23 → 3780)
- Each hypothesis was testable and measurable

### 3. Questioning Assumptions When Evidence Contradicted Them
- **User said:** "One node is falling behind"
- **I initially believed it but then checked:** All 10 nodes showing "agree":10
- **Corrected understanding:** No node falling behind, test predicate is buggy

### 4. User's Sharp Questions Led to Root Causes
- **"Why does crankUntil keep cranking past ledger 5?"** → Found predicate bug immediately
- **"Are disconnects related to flooding?"** → Found stream reopening bug
- Sharp questions beat vague hypotheses

---

## Technical Insights Gained

### 1. Gossip Protocol Fundamentals
**Key Insight:** Gossip requires BOTH:
- Origin broadcast to directly connected peers
- **Forwarding received messages to other peers**

Many implementations only do the first, breaking partially connected networks.

### 2. Deduplication Strategy
**Key Insight:** Need deduplication at BOTH layers:
- **Receive-side:** Don't reprocess same message (correctness)
- **Send-side:** Don't send to peers who already have it (efficiency)

Different purposes, both necessary.

### 3. Connection Management Under Load
**Key Insight:** Stream lifecycle should be separate from message sending:
- **Stream management:** Persistent, proactive, handles reconnections
- **Message sending:** Opportunistic, uses existing streams, fails gracefully

Mixing these creates connection churn during high load.

### 4. Test Predicate Design
**Key Insight:** When checking if a system reached a state:
- Use `>=` not `==` for monotonically increasing values (ledger numbers, timestamps)
- Use `==` only for exact state matches (flags, enums)
- Consider check frequency vs state change rate

The `min == num` predicate created a race condition where fast-advancing nodes skipped the exact target.

### 5. Debugging Distributed Systems
**Process that worked:**
1. Define expected behavior (all nodes should receive all SCP messages)
2. Measure actual behavior (only 23 SCP receives, should be ~3000)
3. Identify the gap (messages not being forwarded)
4. Find simplest fix (add forwarding logic)
5. Verify fix quantitatively (receives jumped to 3780)
6. Optimize if needed (add smart deduplication)

---

## Patterns to Remember

### Debugging Pattern
```
1. Understand what SHOULD happen (read design docs)
2. Observe what IS happening (trace logs, metrics)
3. Find the divergence point (where expected != actual)
4. Hypothesize root cause
5. Verify hypothesis with minimal test
6. Fix, measure, repeat
```

### Flooding Pattern
```rust
// On receive message M from peer P:
1. Deduplicate (have I seen M before?)
2. Process (forward to Core)
3. Identify flood targets (all peers except P who don't have M)
4. Send opportunistically (only to peers with open streams)
5. Track sends (mark peers as "have M")
```

### Stream Management Pattern
```
SEPARATE concerns:
- Stream lifecycle: Open proactively, keep alive, reconnect on disconnect
- Message sending: Use existing streams, fail gracefully if not available
- Flooding: Never trigger stream opens, only opportunistic sends
```

---

## Mistakes That Cost Time

### 1. Not Reading Design Docs First
**Time Lost:** ~1 hour of wrong hypotheses  
**Lesson:** Always start with "what's the intended design?" not "what might be wrong?"

### 2. Implementing Features Before Understanding Bugs
**Time Lost:** ~30 minutes on SCP state sync before realizing flooding was broken  
**Lesson:** Fix existing bugs before adding new features

### 3. Trusting Initial Symptoms Without Verification
**User said:** "One node falling behind"  
**I believed it without checking**  
**Reality:** All nodes advancing together, test predicate was buggy  
**Time Lost:** ~20 minutes looking for non-existent lagging node  
**Lesson:** Verify every claim with data, even from the user

### 4. Not Using Quantitative Metrics Early
**Late realization:** Message count jumped from 23 to 3780  
**Should have measured:** Message counts per node FIRST, before hypothesizing  
**Lesson:** Start with metrics, not theories

---

## Questions I Should Have Asked Immediately

1. **"What's the message propagation model in the design doc?"**
   - Would have immediately seen: gossip-based flooding required

2. **"How many SCP messages should each node receive for one consensus round?"**
   - Would have immediately seen: 23 is WAY too low

3. **"In a 10-node network with ring topology, how should messages propagate?"**
   - Would have realized: forwarding is mandatory, not optional

4. **"What does the predicate `haveAllExternalized(5, 3)` actually check?"**
   - Would have found the `min == num` bug faster

5. **"Why are streams being opened during message flood?"**
   - Would have separated stream management from flooding immediately

---

## Root Cause Analysis Framework (What Worked)

### For "Consensus Timeout" Bug:
1. **Symptom:** Test times out at ledger 3
2. **Expected:** All nodes should reach ledger 5
3. **Observation:** Only 23 SCP message receives (way too low)
4. **Hypothesis:** Messages not propagating to all nodes
5. **Verification:** Trace logs show messages only reaching directly connected peers
6. **Root Cause:** Missing forwarding logic in receive handlers
7. **Fix:** Add flooding logic to forward received messages
8. **Verification:** Message count jumps to 3780, test advances past ledger 5

### For "Overshoot" Bug:
1. **Symptom:** Test fails with "min 9, expected 5"
2. **Expected:** Test should stop cranking at ledger 5
3. **Observation:** All nodes reached ledger 5, but kept advancing
4. **Hypothesis:** Predicate not detecting "reached target" correctly
5. **Verification:** Read predicate code, found `min == num` instead of `min >= num`
6. **Root Cause:** Exact equality check misses fast-advancing nodes
7. **Fix:** Change to `>=` for "at or past target" semantics
8. **Verification:** Test passes at ledger 5-6

---

## Collaboration Dynamics That Worked

1. **User kept me focused on symptoms, not speculation**
   - "Are these sockets wrong?" → specific observable issue
   - "Why does crankUntil keep cranking?" → sharp question, immediate answer

2. **User corrected wrong assumptions immediately**
   - When I said "one node falling behind," user checked logs and corrected me
   - Prevented wasting time on non-existent problems

3. **User let me explore but redirected when needed**
   - Let me investigate SCP state sync
   - But redirected: "Focus on flooding, not sync"

4. **Incremental verification at each step**
   - Build → Test → Observe → Fix → Repeat
   - Never went more than 10 minutes without running the test

---

## Best Practices Reinforced

1. ✅ **Read design docs BEFORE debugging**
2. ✅ **Start with metrics, not theories**
3. ✅ **Verify symptoms with data, don't trust assumptions**
4. ✅ **Fix one thing at a time, test immediately**
5. ✅ **Question sharp vs vague**: "Why does X happen?" beats "Something seems wrong"
6. ✅ **Separate concerns**: Stream management ≠ message sending
7. ✅ **Use >= for state progression checks, not ==**
8. ✅ **Flooding = receive + forward, not just receive**

---

## For Next Time

### Before Starting:
- [ ] Read relevant design docs in full
- [ ] Understand expected behavior quantitatively
- [ ] Set up metrics/logging to measure actual behavior
- [ ] Define success criteria clearly

### During Debugging:
- [ ] Ask "what SHOULD happen?" before "what IS happening?"
- [ ] Measure before hypothesizing
- [ ] Verify every assumption with data
- [ ] Fix smallest possible thing, test, repeat

### After Fixing:
- [ ] Document root cause, not just symptom
- [ ] Update design docs if implementation diverged
- [ ] Add test coverage for the bug
- [ ] Reflect on what questions would have found it faster

---

## Key Takeaway

**The fastest path to a fix is:**
1. Understand expected behavior (design docs)
2. Measure actual behavior (metrics, logs)
3. Find the gap (expected - actual)
4. Fix the simplest thing that could cause the gap
5. Verify quantitatively (metrics improved?)

**Avoid:**
- Speculating without data
- Implementing features before fixing bugs
- Trusting assumptions without verification
- Fixing multiple things simultaneously

**This session: Started with speculation (rate limits? mempool sync?), wasted time. Switched to systematic gap analysis (expected 3000 messages, got 23), found root cause immediately (no forwarding).**

The bugs were simple once we measured the gap. The complexity was in trusting assumptions instead of verifying them.
