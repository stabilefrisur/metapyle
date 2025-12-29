---
name: making-git-commits
description: Use when making a new git commit, amending an existing commit, or when user asks to commit changes - enforces conventional commit format with type, scope, and description
---

# Making Git Commits

## Overview

Git commits follow the **Conventional Commits** specification for consistent, parseable history.

**Core principle:** Every commit message is structured as `type(scope): description` with optional body and footer.

## When to Use

- Making a new commit
- Amending an existing commit
- User says "commit", "save changes", "commit this"
- After completing a task that should be committed
- Squashing or rewriting commits

## Commit Message Format

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Type (Required)

| Type | When to Use |
|------|-------------|
| `feat` | New feature for the user |
| `fix` | Bug fix for the user |
| `docs` | Documentation only changes |
| `style` | Formatting, missing semicolons (no code change) |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `test` | Adding or correcting tests |
| `build` | Changes to build system or dependencies |
| `ci` | Changes to CI configuration files/scripts |
| `chore` | Other changes that don't modify src or test files |
| `revert` | Reverts a previous commit |

### Scope (Optional but Recommended)

The scope provides context about what part of the codebase is affected:
- Module name: `feat(auth): add login`
- File/component: `fix(header): correct alignment`
- Area: `docs(readme): update installation`

### Description (Required)

- Use imperative mood: "add" not "added" or "adds"
- Lowercase first letter
- No period at the end
- Max 50 characters (hard limit: 72)
- Complete the sentence: "This commit will..."

### Body (Optional)

- Separated from description by blank line
- Wrap at 72 characters
- Explain WHAT and WHY, not HOW
- Use when description alone isn't sufficient

### Footer (Optional)

- Breaking changes: `BREAKING CHANGE: description`
- Issue references: `Closes #123`, `Fixes #456`
- Co-authors: `Co-authored-by: Name <email>`

## Quick Reference

```bash
# Feature
git commit -m "feat(users): add password reset flow"

# Bug fix
git commit -m "fix(api): handle null response gracefully"

# Breaking change
git commit -m "feat(api)!: change response format" -m "" -m "BREAKING CHANGE: response now returns array instead of object"

# With body
git commit -m "refactor(core): extract validation logic" -m "" -m "Moved validation from controller to dedicated service for reuse across endpoints."

# Amend last commit
git commit --amend -m "fix(auth): correct token expiry check"
```

## The Gate Function

```
BEFORE executing any git commit:

1. STAGE: Verify correct files staged (`git status`)
2. TYPE: Select appropriate commit type from table
3. SCOPE: Identify affected area (if applicable)
4. DESCRIPTION: Write imperative, lowercase, <50 chars
5. REVIEW: Read full message - does it explain the change?
6. COMMIT: Execute the commit command
7. VERIFY: Run `git log -1` to confirm message
```

## Common Mistakes

| Mistake | Correct |
|---------|---------|
| `Fixed bug` | `fix(module): handle edge case` |
| `feat: Added new feature` | `feat: add new feature` |
| `update code` | `refactor(auth): simplify token logic` |
| `WIP` | Don't commit WIP (use branches/stash) |
| `misc changes` | Split into focused commits |
| `feat(scope): Add feature.` | `feat(scope): add feature` |

## Red Flags - STOP

- About to commit with generic message ("update", "fix", "changes")
- Committing multiple unrelated changes together
- Using past tense in description
- Starting description with capital letter
- Message exceeds 72 characters
- No type prefix
- Committing without reviewing staged files

## Examples by Scenario

**New feature:**
```bash
git commit -m "feat(search): add fuzzy matching support"
```

**Bug fix with issue reference:**
```bash
git commit -m "fix(parser): handle empty input correctly" -m "" -m "Fixes #42"
```

**Documentation update:**
```bash
git commit -m "docs(api): add authentication examples"
```

**Refactoring:**
```bash
git commit -m "refactor(utils): consolidate date formatting functions"
```

**Breaking change:**
```bash
git commit -m "feat(config)!: require explicit environment variable" -m "" -m "BREAKING CHANGE: CONFIG_PATH environment variable is now required. Previously defaulted to ./config.json"
```

**Multiple files, single purpose:**
```bash
git add src/auth.py tests/test_auth.py
git commit -m "feat(auth): implement JWT validation"
```

## Amending Commits

When modifying existing commits:

```bash
# Amend message only (last commit)
git commit --amend -m "fix(api): correct type annotation"

# Amend with staged changes (last commit)
git add forgotten_file.py
git commit --amend --no-edit

# Interactive rebase for older commits
git rebase -i HEAD~3
# Change 'pick' to 'reword' for message changes
# Change 'pick' to 'edit' for content changes
```

## The Bottom Line

**Every commit tells a story.**

Format: `type(scope): imperative description`

The message should complete: "This commit will [description]"

When in doubt, ask: Would someone understand this commit 6 months from now?
