# Smoke Test & Test Distribution Design

**Date:** 2025-12-29  
**Status:** Approved

## Problem

Users who install metapyle via pip/uv cannot run integration tests because:
1. Tests are not included in the distributed package
2. Documentation assumes GitHub access for cloning, but some users can't access GitHub

## Solution

Provide two testing options for users:

1. **Smoke test script** (`docs/smoke-test.md`) - Quick verification without pytest
2. **Tests in sdist** - Full test suite available via PyPI source distribution

## Design

### 1. `docs/smoke-test.md`

A markdown file containing:

**Sections:**
- Overview - What the script does and when to use it
- Prerequisites - Python 3.12+, metapyle installed, source-specific requirements
- The Script - Single Python code block with all sources, each in commented sections
- Understanding Results - What success/failure looks like
- Troubleshooting - Common errors and fixes

**Test Symbols:**

| Source | Symbol | Field | Rationale |
|--------|--------|-------|-----------|
| Bloomberg | `SPX Index` | `PX_LAST` | Ubiquitous, always available |
| Macrobond | `usgdp` | (none) | Public macro series |
| GS Quant | `SPX` | `SWAPTION_VOL::atmVol` | Requires authenticated session |
| LocalFile | User-provided | User-provided | User supplies their own file |

**Script Pattern:**
```python
# === BLOOMBERG ===
# Uncomment to test Bloomberg connection
# try:
#     df = client.get_raw(source="bloomberg", symbol="SPX Index", field="PX_LAST", start=start)
#     assert not df.empty and len(df.columns) == 1
#     print(f"✓ Bloomberg: {len(df)} rows, column '{df.columns[0]}'")
# except Exception as e:
#     print(f"✗ Bloomberg: {e}")
```

**Verification Level:** Standard
- Connection works
- Basic fetch succeeds
- DataFrame shape is correct (non-empty, expected columns)

### 2. Include Tests in Source Distribution

Update `pyproject.toml`:

```toml
[tool.hatch.build.targets.sdist]
include = [
    "src/metapyle/**/*.py",
    "src/metapyle/**/*.json",
    "src/metapyle/**/*.yaml",
    "src/metapyle/**/*.md",
    "src/metapyle/py.typed",
    "tests/**/*.py",           # ADD
    "tests/**/*.yaml",         # ADD
]
```

### 3. User Guide Updates

Update "Running Integration Tests" section:

1. **Quick verification** → Link to `smoke-test.md`
2. **Full test suite (primary)** → Clone from GitHub
3. **Full test suite (alternative)** → Download sdist from PyPI (for restricted environments)

## Files Changed

| File | Change |
|------|--------|
| `docs/smoke-test.md` | Create new file |
| `pyproject.toml` | Add tests to sdist includes |
| `docs/user-guide.md` | Update testing section |

## Out of Scope

- `python -m metapyle.verify` module (decided against shipping in package)
- Comprehensive testing (caching, metadata) - keep smoke test simple
