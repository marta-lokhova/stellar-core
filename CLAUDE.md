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

### /reflect

When the user invokes `/reflect`, analyze the conversation to distill learnings and propose improvements to the AI knowledge base.

**Usage:**
- `/reflect` - Analyze the full conversation
- `/reflect <focus area>` - Focus on a specific topic (e.g., "database patterns", "crash recovery")

**Process:**
1. Review the conversation and extract:
   - **New Code Learnings**: Architecture patterns, component interactions, invariants
   - **Discovered Bugs**: Actual bugs found, root causes, which techniques found them
   - **Missing Test Coverage**: Areas where bugs slipped through, edge cases not covered
   - **Fundamental Invariants**: Properties that must always hold, consistency requirements
   - **Review Process Improvements**: What questions led to findings, wrong assumptions

2. Produce a structured summary:
   - Learnings for `dev/ai/STELLAR_CORE.md`
   - New review patterns for `dev/ai/CODE_REVIEW.md`
   - Table of bugs found with severity and discovery pattern
   - Specific test suggestions
   - Proposed file changes

3. Ask user before making any changes to:
   - `dev/ai/STELLAR_CORE.md` - new architectural knowledge
   - `dev/ai/CODE_REVIEW.md` - new review patterns
   - `CLAUDE.md` - improved instructions
   - `dev/ai/commands/*.md` - refined commands
