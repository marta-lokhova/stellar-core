# Claude Code Instructions for stellar-core

## Canonical Knowledge base 

Use /Users/marta/Documents/dev/ai/ as the canonical knowledge base, always read it when prompted with "using your knowledge" or "knowing X", or "remembering how X works". Use that knowledge case to cross-check relevant assumptions.

## Custom Commands

### /code-review

When the user invokes `/code-review`, perform a code review following the guidelines in `dev/ai/CODE_REVIEW.md`.

**Usage:**
- `/code-review <commit>` - Review a specific commit
- `/code-review <commit1>..<commit2>` - Review a range of commits
- `/code-review <file>` - Review changes in a specific file

**Process:**
1. First, read `dev/ai/CODE_REVIEW.md` to load the review guidelines
2. Examine the code changes specified by the user
3. Analyze the changes following the priority order:
   - Crash Recovery & Durability
   - Atomicity & Consistency
   - Correct Usage of APIs
   - Minor Issues
4. For Database-related changes, pay special attention to session/pool selection
5. For file I/O changes, verify fsync ordering and atomic patterns
6. Report findings organized by severity

### /explain

When the user invokes `/explain`, use the deepwiki MCP to explain implementation details of stellar-core.

**Usage:**
- `/explain <topic>` - Explain a concept, component, or implementation detail
- `/explain how does <X> work` - Explain how a specific feature works
- `/explain <component>` - Explain a specific component (e.g., SCP, Herder, BucketList)

**Process:**
1. First, read `dev/ai/STELLAR_CORE.md` to check if local knowledge covers the topic
2. Use `mcp__deepwiki__ask_question` with repo "stellar/stellar-core" to get detailed explanation
3. If needed, use `mcp__deepwiki__read_wiki_structure` to find relevant documentation topics
4. Combine deepwiki knowledge with local context from `dev/ai/STELLAR_CORE.md`
5. Provide a clear, concise explanation with relevant code references if applicable

**Example queries:**
- `/explain SCP consensus protocol`
- `/explain how does ledger close work`
- `/explain bucket list merge`
- `/explain transaction apply flow`

---

## Design Session Guidelines

When working on large architectural designs or proposals:

### Interaction Style

1. **Ask clarifying questions upfront** - Don't guess at requirements. Ask about constraints, goals, and non-goals before diving in.

2. **Lead with bold proposals** - After clarifying, make ambitious proposals rather than conservative incremental ones. Let the user correct and refine.

3. **Present options with clear tradeoffs** - When there are multiple approaches, present them with a clear recommendation rather than asking "which do you prefer?"

4. **Keep diagrams minimal until convergence** - Simple ASCII diagrams iterate faster. Add detail only after the high-level design is agreed.

5. **Ask "what am I missing?" not "which option?"** - This invites correction rather than decision fatigue.

### Challenge Assumptions (With Data)

Don't just accept what the user says - push back when you have evidence:

- **Research and propose alternatives** backed by data, benchmarks, or industry practice
- **Cite specifics**: "QUIC adds 20ms handshake but 0-RTT mitigates this" not "QUIC might be slower"
- **Question stated requirements**: "You said X is critical - but looking at the code, it's only used in Y which happens rarely. Is it actually critical?"
- **Don't disagree just to disagree** - only push back when you have a concrete reason

Example good pushback: "You mentioned Unix sockets add latency, but benchmarks show ~5μs which is 0.005% of network RTT. Is this actually a concern?"

### When User Says "Think Bigger"

This is a signal to:
- Question fundamental assumptions, not just implementation details
- Explore the full design space (e.g., process isolation, not just FFI)
- Ask "what's the ideal outcome?" rather than cataloging current constraints
- Consider whether current complexity is actually necessary

### Capturing Design Progress

For multi-session design work:
- Dump all learnings, diagrams, and decisions into a markdown doc in `dev/ai/`
- Include resolved questions and their answers
- Document the "why" behind decisions, not just the "what"
- Note next steps clearly so the next session can pick up immediately

---

## What Works Well in Collaboration

Based on implementation sessions:

1. **Test-driven development** - Writing tests before/alongside code catches bugs early
2. **Incremental building** - Compiling frequently catches issues fast
3. **User catching AI oversights** - Direct feedback like "Remove fallback to avoid misleading yourself" or "you're not submitting TXs yet" prevents wasted effort
4. **Parallel tool calls** - Running multiple searches/views simultaneously speeds things up

Keep doing these. Call out when AI is going down wrong path.

---

## Critical: Read Design Docs FIRST

**Before debugging or implementing, ALWAYS:**
1. Read the design doc the user points you to (e.g., `RUST_OVERLAY_DESIGN.md`)
2. Summarize your understanding back to the user
3. Ask clarifying questions about anything unclear

**Specific failures from TX set fetching session (Jan 2026):**

| What I Did Wrong | What I Should Have Done |
|------------------|-------------------------|
| Jumped into debugging rate limits | Read design doc first - TXs aren't forwarded individually, only as blocks |
| Asked "is it push/pull/eager flooding?" | Design doc already specified pull-based flooding |
| Proposed broadcast GET_TX_SET to all peers | Asked: "Should I request from one peer or all?" (answer: ONE peer, retry on timeout) |
| Worried about mempool disagreement between nodes | Asked: "Is mempool sync required?" (answer: No, SCP reconciles) |
| Worried about exact TX counts per ledger | Asked: "How precise must TX distribution be?" (answer: Slight variations OK) |
| Forgot to write unit tests until reminded | Follow TDD from start - write tests alongside implementation |

**The pattern:** I made assumptions instead of asking questions, then had to backtrack when corrected.

**Better approach:**
```
1. "Let me read the design doc and summarize my understanding..."
2. "Before implementing, I want to confirm: [specific question about flow/behavior]"
3. "I'll write a unit test for this behavior first, then implement"
```

---

## Build Mental Model Before Debugging

When a test fails, don't jump to hypotheses. First understand:

1. **What is the expected flow?** (Read design docs)
2. **What actually happened?** (Trace through logs/code)
3. **Where does expected diverge from actual?** (Pinpoint the gap)

**Example from TX set session:**
- I hypothesized rate limiting was the issue
- User redirected: "Why do some TXs make it but not others?"
- Real issue: TX set fetching wasn't implemented - nodes couldn't get each other's nominated TX sets
- I would have found this faster by tracing the SCP message flow in the design doc

---

## Ask Questions, Don't Assume

**Questions I should have asked upfront:**
- "Are TX sets latency-critical? Which channel should they use?"
- "If a node doesn't have a TX set, how should it fetch it?"
- "What's the timeout for TX set requests?"
- "Should GET_TX_SET go to one peer or broadcast?"

**The user had to correct me multiple times because I assumed instead of asked.**

Rule: If you're making a design decision, ASK. If the design doc doesn't cover it, ASK.

---

## Incremental Development: Stop, Compile, Test, Repeat

**Don't write large chunks of code without verification.**

After every small change:
1. **Compile** - catch syntax/type errors immediately
2. **Run existing tests** - make sure you didn't break anything
3. **Write a new test** - verify the new behavior works

**From TX set session:**
- I wrote GET_TX_SET/TX_SET handling, FetchTxSet command, CacheTxSet command all at once
- Then had multiple compilation errors to fix
- Then tests failed because I forgot to spawn advert flush tasks
- User had to remind me: "TDD remember??"

**Better approach:**
```
1. Add GET_TX_SET message type → compile → tests pass
2. Add handler for GET_TX_SET → compile → write test for it → tests pass
3. Add TX_SET message type → compile → tests pass
4. Add handler for TX_SET → compile → write test for it → tests pass
5. Add FetchTxSet command → compile → write test for timeout → tests pass
6. Integration test → tests pass
```

Each step is small, verified, and has a test. Bugs are caught immediately, not after 200 lines of changes. Do NOT swallow errors - prefer crashing/asserting over silently proceeding.

---

## Build and Test Commands

**Rust overlay (in `overlay/` directory):**
```bash
# Build
cargo build

# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

**C++ stellar-core (in repo root):**
```bash
# Build (after configure)
make -j$(nproc)

# Run all tests
make check

# Run specific test suite
./src/stellar-core test [testname]

# Run stress tests (includes Rust overlay integration)
./src/stellar-core test "Rust overlay*"
```

**Typical workflow during development:**
```bash
# 1. After Rust changes
cd overlay && cargo build && cargo test && cd ..

# 2. After C++ changes  
make -j$(nproc)

# 3. Integration test (both sides)
./src/stellar-core test "Rust overlay SCP latency under TX load"
```

**Quick validation loop:**
```bash
# Rust: compile check only (faster than full build)
cd overlay && cargo check

# C++: build just the binary
make -j$(nproc) stellar-core
```

Always run `cargo test` after Rust changes and `make check` after C++ changes before moving on.
