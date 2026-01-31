---
description: Analyze conversation, distill learnings, and propose improvements to AI knowledge base
argument-hint: [optional focus area]
---

# Reflect on Conversation

Optional focus: $ARGUMENTS

## Analysis Process

Review our conversation and extract:

### 1. New Code Learnings
- Architecture patterns discovered
- Component interactions understood
- Invariants identified
- Update `dev/ai/STELLAR_CORE.md` with new knowledge

### 2. Discovered Bugs
- Actual bugs found (not theoretical)
- Root cause patterns
- Which review techniques found them
- Update `dev/ai/CODE_REVIEW.md` if new patterns emerged

### 3. Missing Test Coverage
- Areas where bugs slipped through
- Edge cases not covered
- Suggest specific test cases

### 4. Fundamental Invariants
- Properties that must always hold
- Consistency requirements across components
- Atomicity boundaries
- Document in appropriate files

### 5. Review Process Improvements
- What questions led to finding bugs?
- What assumptions were wrong initially?
- What should be checked automatically?

## Output Structure

```markdown
## Learnings Summary

### For STELLAR_CORE.md
- [New knowledge to add]

### For CODE_REVIEW.md
- [New review patterns]

### Bugs Found
| Bug | Severity | How Found | Pattern |
|-----|----------|-----------|---------|

### Missing Tests
- [Specific test suggestions]

### Proposed File Changes
- [Specific edits to dev/ai/ files]
```

## Action

After analysis, propose specific edits to:
- `dev/ai/STELLAR_CORE.md` - new architectural knowledge
- `dev/ai/CODE_REVIEW.md` - new review patterns
- `dev/ai/CLAUDE.md` - improved instructions
- `.claude/commands/*.md` - refined commands

Ask user before making changes.
