---
description: Explain stellar-core implementation details using deepwiki and local knowledge
argument-hint: <topic or question>
---

# Explain stellar-core Implementation

Topic/Question: $ARGUMENTS

## Process

1. First, check local knowledge in `dev/ai/STELLAR_CORE.md` for relevant context

2. Use the deepwiki MCP to get detailed explanation:
   - Call `mcp__deepwiki__ask_question` with repo "stellar/stellar-core" and the question
   - If needed, use `mcp__deepwiki__read_wiki_structure` to find relevant documentation topics

3. Combine deepwiki knowledge with local context from `dev/ai/STELLAR_CORE.md`

4. Provide a clear, concise explanation with:
   - High-level overview
   - Key components involved
   - Code references (file:line) where applicable
   - Any relevant gotchas or common pitfalls

## Example Topics

- SCP consensus protocol
- Ledger close flow
- Bucket list and merging
- Transaction apply pipeline
- History archiving
- Database session management
