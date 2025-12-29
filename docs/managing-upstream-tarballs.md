# Managing Upstream Tarballs: Maintaining a Python Package Fork Without GitHub Access

## Overview

This guide covers the best practice workflow for maintaining a fork of a Python package when you **only have access to PyPI** (not GitHub or the original repository). This is common in enterprise environments where GitHub may be blocked but PyPI is permitted.

### The Challenge

- PyPI tarballs are **source snapshots** without git history
- You need to track upstream changes while maintaining your own modifications
- Merging upstream updates must be clean and auditable

### The Solution: Vendor Branch Strategy

Use a dedicated **vendor/upstream branch** that contains one commit per upstream release. Your development happens on `main`, and upstream updates are merged in.

```
main          ───●───●───●───────●───────●───●───●
                 │               │               │
                 │    (merge)    │    (merge)    │
                 │       ↑       │       ↑       │
upstream      ───●───────●───────●───────●───────●
              v1.0.0  v1.1.0  v1.2.0  v1.3.0  v1.4.0
```

---

## Initial Setup

### 1. Create Your Fork Repository

First, create an empty repository for your fork (on your internal GitLab, Azure DevOps, or local bare repo):

```powershell
# Create project directory
mkdir my-package-fork
cd my-package-fork
git init

# Configure (use your details)
git config user.name "Your Name"
git config user.email "your.email@company.com"
```

### 2. Download the Initial Upstream Tarball

```powershell
# Download the source distribution from PyPI
# Option A: Use pip download
pip download --no-deps --no-binary :all: package-name==1.0.0

# Option B: Direct URL (find on pypi.org)
Invoke-WebRequest -Uri "https://files.pythonhosted.org/packages/.../package-name-1.0.0.tar.gz" -OutFile "package-name-1.0.0.tar.gz"
```

> **Note**: Always prefer `.tar.gz` (sdist) over `.whl` (wheel) because sdists contain the full source code, tests, and build files.

### 3. Create an Orphan Upstream Branch

Use an **orphan branch** for upstream—it has no parent history, which is semantically correct for external vendor code:

```powershell
# Create orphan branch (no parent commits)
git checkout --orphan upstream

# Clear the staging area (orphan branches start with staged files from previous HEAD)
git rm -rf . 2>$null
git clean -fd
```

### 4. Extract and Commit Upstream Code

```powershell
# Extract directly to current directory, stripping the top-level folder
tar -xzf package-name-1.0.0.tar.gz --strip-components=1

# Add all files
git add -A

# Commit with version tag for traceability
git commit -m "upstream: import package-name v1.0.0 from PyPI"

# Tag the upstream version
git tag upstream/v1.0.0
```

> **Note**: The `--strip-components=1` flag removes the top-level directory that tarballs typically contain (e.g., `package-name-1.0.0/`), extracting files directly to the current directory.

> **Windows Note**: The `tar` command is included in Windows 10 (build 17063+). For older Windows, use Git Bash, WSL, or extract with 7-Zip and copy files manually.

### 5. Set Up Main Branch for Development

```powershell
# Create main branch from current upstream state
git checkout -b main

# Create your first fork commit (even if no changes yet)
git commit --allow-empty -m "fork: initialize fork from upstream v1.0.0"
```

> **Note**: This guide assumes a local-only repository. If you have a remote (GitLab, Azure DevOps), push both branches and tags after setup: `git push -u origin main && git push origin upstream --tags`

---

## Making Your Fork Installable

After importing upstream code, you need to make your fork installable under a new name. This section covers the minimal changes needed.

### Strategy: Re-Export Layer

Rather than renaming upstream's source directory (which causes merge conflicts on every upgrade), create a thin re-export layer:

```
src/
  original_pkg/        # Upstream code (keep their structure)
    __init__.py
    client.py
    utils.py
  myfork/              # Your re-export layer (NEW)
    __init__.py        # Re-exports public API + your extensions
    extensions.py      # Your custom code
```

This approach:
- **Minimizes merge conflicts** - upstream's directory structure stays intact
- **Isolates your code** - extensions live in `myfork/`, separate from upstream
- **Clean imports** - users do `from myfork import Client`

### 1. Update pyproject.toml

Edit the package metadata to use your fork name:

```toml
[project]
name = "myfork"  # Changed from "original-pkg"
version = "1.2.3+fork.1"  # Upstream version + fork revision
description = "Internal fork of original-pkg with custom extensions"
# ... rest of metadata

[project.scripts]
# Rename CLI entry points if the package has any
myfork-cli = "myfork:main"  # Changed from "original-cli"
```

**Version numbering convention:**
- `1.2.3+fork.1` = Based on upstream 1.2.3, first fork revision
- `1.2.3+fork.2` = Same upstream base, second fork revision (your changes)
- `1.2.4+fork.1` = Upgraded to upstream 1.2.4, reset fork revision

### 2. Create the Re-Export Layer

Create `src/myfork/__init__.py`:

```python
"""
myfork - Internal fork of original-pkg.

This module re-exports the public API from original_pkg,
plus fork-specific extensions.
"""

# Re-export the upstream public API
from original_pkg import (
    Client,
    Config,
    SomeClass,
    some_function,
    # ... all public symbols
)

# Export your extensions
from myfork.extensions import (
    CustomFeature,
    enhanced_function,
)

__all__ = [
    # Upstream API
    "Client",
    "Config", 
    "SomeClass",
    "some_function",
    # Fork extensions
    "CustomFeature",
    "enhanced_function",
]
```

### 3. Update README

Add a section explaining this is a fork:

```markdown
# myfork

Internal fork of [original-pkg](https://pypi.org/project/original-pkg/).

## Why a Fork?

This fork adds:
- Custom caching layer for internal infrastructure
- Authentication support for internal APIs
- [Other reasons]

## Installation

```bash
pip install -e /path/to/myfork
```

## Usage

```python
# Import from myfork, not original_pkg
from myfork import Client, CustomFeature

client = Client()
```

## Upstream Version

Currently based on original-pkg v1.2.3.
```

### 4. Verify Installation

```powershell
# Install in development mode
pip install -e .

# Verify import works
python -c "from myfork import Client; print('OK')"
```

---

## Alternative: Subdirectory Layout (Monorepo)

If you're adding a vendored package to an existing monorepo, extract to a subdirectory instead:

```powershell
# On upstream branch, use a dedicated subdirectory
git checkout upstream
git rm -rf third_party/pkg 2>$null
mkdir -p third_party/pkg
tar -xzf package-name-1.0.0.tar.gz -C third_party/pkg --strip-components=1
git add third_party/pkg
git commit -m "upstream: import package-name v1.0.0"
```

This isolates the vendored code and avoids polluting your repo root.

---

## Day-to-Day Development

Work on `main` branch as you normally would:

```powershell
# Create feature branches for significant work
git checkout -b feature/my-enhancement

# Make your changes...
# Commit using conventional commits
git commit -m "feat: add custom caching layer"

# Merge back to main
git checkout main
git merge feature/my-enhancement
git branch -d feature/my-enhancement
```

### Recommended Commit Prefixes

| Prefix | Use For |
|--------|---------|
| `upstream:` | Importing new upstream versions |
| `fork:` | Fork-specific infrastructure (CI, configs) |
| `feat:` | New features you're adding |
| `fix:` | Bug fixes (yours or upstream backports) |
| `docs:` | Documentation changes |

---

## Updating from Upstream (The Critical Workflow)

When a new upstream version is released on PyPI, follow these steps **exactly**:

### 1. Download the New Upstream Version

```powershell
# Check PyPI for new versions
pip index versions package-name

# Download the new version
pip download --no-deps --no-binary :all: package-name==1.1.0
```

### 2. Switch to Upstream Branch and Clear It

```powershell
git checkout upstream

# Remove all tracked files (but keep .git)
git rm -rf .

# Also remove untracked files for a clean slate
git clean -fd
```

> **Why clear everything?** This ensures deleted files from upstream are also deleted in your fork. Without this, you'd accumulate stale files.

### 3. Extract New Version

```powershell
# Extract directly, stripping the top-level directory
tar -xzf package-name-1.1.0.tar.gz --strip-components=1
```

### 4. Commit the Upstream Update

```powershell
# Stage all changes (additions, modifications, deletions)
git add -A

# Commit with clear provenance
git commit -m "upstream: import package-name v1.1.0 from PyPI"

# Tag for reference
git tag upstream/v1.1.0
```

### 5. Merge Upstream into Main

```powershell
git checkout main

# Merge upstream changes
git merge upstream -m "merge: upstream v1.1.0 into main"
```

### 6. Resolve Conflicts (If Any)

If you have conflicts:

```powershell
# See which files conflict
git status

# List just the conflicted file names
git diff --name-only --diff-filter=U
```

See the **Conflict Resolution Workflow** section below for detailed guidance.

### 7. Test

```powershell
# Run your test suite
pytest
```

---

## Conflict Resolution Workflow

When a merge produces conflicts, work through them systematically. This workflow is designed for human-agent collaboration: the agent suggests resolutions, you approve, then validate with tests.

### Step 1: Identify All Conflicts

```powershell
# List conflicted files
git diff --name-only --diff-filter=U
```

### Step 2: For Each Conflict, Suggest Resolution with Rationale

For each conflicted file, the agent (or you) should:

1. **Show the conflict** with surrounding context
2. **Categorize it**:
   - Upstream bugfix vs. your feature code
   - Both sides changed the same logic
   - Upstream refactored, your patch is now stale
3. **Propose a resolution** with clear rationale

**Example agent output:**

```
CONFLICT: src/original_pkg/client.py

Context: Lines 45-60, the retry logic

<<<<<<< HEAD (your fork)
    for attempt in range(self.max_retries):
        try:
            return self._send(request)
        except ConnectionError:
            if attempt == self.max_retries - 1:
                raise
            time.sleep(self.retry_delay)
=======
    for attempt in range(max_retries):
        try:
            return self._send(request)
        except (ConnectionError, TimeoutError):  # NEW: also catch TimeoutError
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # NEW: exponential backoff
>>>>>>> upstream

ANALYSIS:
- Your change: Added configurable max_retries and retry_delay as instance attributes
- Upstream change: Added TimeoutError handling and exponential backoff

SUGGESTED RESOLUTION:
Keep both improvements - use your configurable attributes with upstream's 
TimeoutError handling and backoff pattern:

    for attempt in range(self.max_retries):
        try:
            return self._send(request)
        except (ConnectionError, TimeoutError):
            if attempt == self.max_retries - 1:
                raise
            time.sleep(self.retry_delay * (2 ** attempt))

RATIONALE: Upstream's TimeoutError catch is a bugfix we want. Their exponential 
backoff is better than fixed delay. We keep our configurability by using 
self.retry_delay as the base.
```

### Step 3: Apply Resolution and Test

After you approve a resolution:

```powershell
# Edit the file to apply the resolution
# Then mark as resolved
git add src/original_pkg/client.py

# Run tests immediately to validate
pytest tests/ -x  # -x stops on first failure

# If tests pass, continue to next conflict
# If tests fail, revisit the resolution
```

### Step 4: Document Significant Decisions

For non-trivial resolutions, update `FORK_CHANGES.md`:

```markdown
## Conflict Resolution Log

### v1.2.4 Merge (2025-01-20)

**client.py retry logic**: Merged upstream's TimeoutError handling and 
exponential backoff with our configurable retry settings. Using 
`self.retry_delay * (2 ** attempt)` for exponential backoff with 
configurable base.
```

### Step 5: Complete the Merge

Once all conflicts are resolved and tests pass:

```powershell
# Commit the merge
git commit -m "merge: upstream v1.2.4 into main (resolved conflicts)"

# Final test run
pytest tests/
```

---

## Rollback

If an upstream merge breaks things and you need to revert:

### Revert the Merge Commit

```powershell
# Find the merge commit hash
git log --oneline -5

# Output example:
# a1b2c3d merge: upstream v1.2.4 into main
# ...

# Revert the merge (specify -m 1 to keep your main branch as the parent)
git revert -m 1 a1b2c3d

# This creates a new commit that undoes the merge
```

> **Note**: `-m 1` tells git to keep the first parent (your main branch) and undo changes from the second parent (upstream). This is essential for merge commits.

### After Reverting

Your main branch is now back to pre-merge state. The upstream branch still has the new version. When you're ready to try the merge again (after fixing issues):

```powershell
# Option A: Re-attempt the same merge
git merge upstream

# Option B: Wait for next upstream release and try that instead
```

---

## Handling Edge Cases

### Upstream Renames a File You Modified

Git may not detect this as a rename. You'll see:
- Deletion of old file (from upstream)
- Addition of new file (from upstream)
- Your modifications "orphaned"

**Solution:**

```powershell
# After merge with conflicts, check if it's a rename
git diff upstream~1 upstream --name-status | Select-String "^R"

# If so, manually port your changes to the new filename
```

### Upstream Restructures Directories

Major refactors can cause painful merges. Options:

1. **Accept upstream structure**: Rebase your changes onto new structure
2. **Keep your structure**: Ignore upstream refactor (risky for future merges)
3. **Hybrid**: Accept refactor, re-apply your logical changes

### You Need to Cherry-Pick a Specific Upstream Fix

If upstream fixes a critical bug but you can't update fully:

```powershell
# Download both versions
pip download --no-deps --no-binary :all: package-name==1.0.0
pip download --no-deps --no-binary :all: package-name==1.0.1

# Extract both
tar -xzf package-name-1.0.0.tar.gz
tar -xzf package-name-1.0.1.tar.gz

# Diff to find the fix
diff -rq package-name-1.0.0 package-name-1.0.1

# Manually apply the specific changes to your main branch
```

### Verifying Tarball Integrity

```powershell
# PyPI provides hashes - verify before extracting
pip hash package-name-1.1.0.tar.gz

# Compare with hash shown on PyPI package page
```

### Handling pyproject.toml Conflicts

Since you modify `pyproject.toml` (package name, version), it will conflict on **every** upstream merge. This is expected and easy to resolve:

```powershell
# After merge conflict in pyproject.toml:
# 1. Accept upstream's changes for dependencies, metadata they updated
# 2. Keep YOUR values for: name, version, scripts (entry points)
# 3. Merge both sides for any new fields
```

**Strategy**: Keep a mental model of "your sections" vs "upstream sections":
- **Your sections** (always keep yours): `name`, `version`, `description`, `[project.scripts]`
- **Upstream sections** (usually accept theirs): `dependencies`, `[build-system]`, `python_requires`
- **Merge carefully**: `[project.optional-dependencies]`, classifiers

### Version Bumping After Merge

After successfully merging an upstream update:

```powershell
# Update version in pyproject.toml
# Old: version = "1.2.3+fork.2"
# New: version = "1.3.0+fork.1"  (reset fork revision when upstream changes)

# Commit the version bump
git add pyproject.toml
git commit -m "chore: bump version to 1.3.0+fork.1"
```

---

## Best Practices Summary

### Do ✅

- **Always tag upstream imports**: `upstream/vX.Y.Z` makes history navigable
- **Clear upstream branch completely** before extracting new version
- **Test after every merge**: Run full test suite before pushing
- **Document your fork changes**: Keep a `FORK_CHANGES.md` file
- **Use merge commits** (not rebase) to preserve upstream/fork boundary
- **Keep upstream branch pure**: Never commit your changes there

### Don't ❌

- **Don't rebase main onto upstream**: Loses your merge history
- **Don't extract tarballs on top of existing files**: Stale files accumulate
- **Don't skip versions**: Import each upstream version sequentially for clean history
- **Don't modify files in upstream branch**: It's a vendor mirror only

---

## Tracking Your Fork Changes

Create a `FORK_CHANGES.md` in your repo:

```markdown
# Fork Changes

This is a fork of [original-pkg](https://pypi.org/project/original-pkg/).

## Current Base Version
- Upstream: v1.2.0
- Fork version: 1.2.0+fork.3

## Fork Architecture

```
src/
  original_pkg/     # Upstream code (untouched except for patches below)
  myfork/           # Re-export layer + extensions
    __init__.py     # Re-exports public API
    extensions.py   # Custom features
```

## Fork-Specific Changes

### Added (in myfork/)
- `extensions.py`: Custom caching layer
- `auth.py`: Internal authentication support

### Modified (in original_pkg/)
- `client.py`: Added retry logic for internal network (lines 45-60)
- `config.py`: Extended Config class with internal defaults

### Configuration
- `pyproject.toml`: Package renamed to `myfork`, version scheme updated
- `.env.example`: Template for internal environment variables

## Upstream Sync History
| Date | Upstream Version | Fork Version | Notes |
|------|------------------|--------------|-------|
| 2025-01-15 | v1.0.0 | 1.0.0+fork.1 | Initial import |
| 2025-03-20 | v1.1.0 | 1.1.0+fork.1 | Clean merge |
| 2025-06-10 | v1.2.0 | 1.2.0+fork.1 | Conflicts in client.py |

## Conflict Resolution Log

### v1.2.0 Merge (2025-06-10)
**client.py retry logic**: Merged upstream's TimeoutError handling with 
our configurable retry settings. See commit ghi9012.
```

---

## Quick Reference Card

```powershell
# === INITIAL SETUP ===
git init && git checkout --orphan upstream
pip download --no-deps --no-binary :all: pkg==1.0.0
tar -xzf pkg-1.0.0.tar.gz --strip-components=1
git add -A && git commit -m "upstream: import pkg v1.0.0"
git tag upstream/v1.0.0
git checkout -b main  # create main from upstream

# === MAKE INSTALLABLE (after setup) ===
# Edit pyproject.toml: name = "myfork", version = "1.0.0+fork.1"
mkdir src/myfork
# Create src/myfork/__init__.py with re-exports
pip install -e .

# === UPDATE FROM UPSTREAM ===
git checkout upstream
git rm -rf . && git clean -fd
pip download --no-deps --no-binary :all: pkg==1.1.0
tar -xzf pkg-1.1.0.tar.gz --strip-components=1
git add -A && git commit -m "upstream: import pkg v1.1.0 from PyPI"
git tag upstream/v1.1.0
git checkout main
git merge upstream -m "merge: upstream v1.1.0 into main"
# resolve conflicts if any, then: git add <file> && git commit
pytest tests/
# Update version in pyproject.toml: "1.1.0+fork.1"
git commit -am "chore: bump version to 1.1.0+fork.1"

# === CONFLICT RESOLUTION ===
git diff --name-only --diff-filter=U   # List conflicted files
# For each: review, resolve, test
git add <resolved-file>
pytest tests/ -x                        # Test after each resolution
git commit                              # Complete merge when all resolved

# === ROLLBACK ===
git log --oneline -5                    # Find merge commit
git revert -m 1 <merge-commit-hash>     # Undo the merge

# === USEFUL COMMANDS ===
pip index versions pkg              # Check available versions
git log --oneline upstream          # View upstream history  
git log --oneline main ^upstream    # View your fork-only commits
git diff upstream main              # See all fork differences
```

---

## Appendix: Why Not Just Patch Files?

An alternative approach is maintaining `.patch` files against upstream. While this works, it has drawbacks:

| Approach | Pros | Cons |
|----------|------|------|
| **Vendor Branch** (this guide) | Full git history, easy merges, IDE support | More git knowledge required |
| **Patch Files** | Simple, portable patches | Manual rebase, no merge tooling, patch rot |

The vendor branch strategy scales better for ongoing maintenance and is the industry standard for forking without upstream repo access.
