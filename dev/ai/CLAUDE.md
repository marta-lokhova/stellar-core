# Claude Code Instructions for stellar-core

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
