# User Testing Fixes - Design Document

> Organized issues from user testing, categorized and prioritized.

---

## Summary

22 items organized into 7 sections, prioritized bugs/correctness first.

---

## Section 1: Critical Bug Fixes

| # | Issue | Impact |
|---|-------|--------|
| 1.1 | Verify column lookup fallback is complete | Data retrieval |
| 1.2 | Bloomberg field enforcement in catalog validation | Silent API errors |
| 1.3 | Move `pytest_addoption` to root conftest.py | Test infrastructure |

## Section 2: Source Adapter Gaps

| # | Issue | Description |
|---|-------|-------------|
| 2.1 | Implement Macrobond `get_unified_series()` | User guide mentions `unified=True` but not implemented |
| 2.2 | Simplify GsSession detection + update all examples | Use `GsSession.use()` pattern everywhere |

## Section 3: DataFrame Output Consistency

| # | Issue | Description |
|---|-------|-------------|
| 3.1 | Normalize date index naming to `'date'` | Currently inconsistent across sources |
| 3.2 | Standardize timezone awareness | Some naive, some UTC |
| 3.3 | Preserve column order matching input symbols list | Currently sorted by source |

## Section 4: Catalog & CSV/YAML Improvements

| # | Issue | Description |
|---|-------|-------------|
| 4.1 | Handle `params` in `csv_template()` | JSON in CSV cell |
| 4.2 | Improve `to_yaml()` output | Spacing, comments, source file info |
| 4.3 | CSV/YAML value sanitization | Validate/escape values |

## Section 5: Testing Infrastructure

| # | Issue | Description |
|---|-------|-------------|
| 5.1 | Update integration test date range | 2023-12-31 → 2024-12-31 |
| 5.2 | Update `test_frequency_alignment` assertion | Match new date range |

## Section 6: Documentation & Developer Experience

| # | Issue | Description |
|---|-------|-------------|
| 6.1 | Cache path guidance | PS style, multiple options |
| 6.2 | Cache DB inspection helpers | Helper functions |
| 6.3 | Fix `my_name` convention violations | Examples use wrong format |
| 6.4 | Clarify/fix absolute vs relative imports | copilot-instructions says relative |
| 6.5 | Export `BloombergSource` from `sources/__init__.py` | Missing from `__all__` |

## Section 7: Output Format Options

| # | Issue | Description |
|---|-------|-------------|
| 7.1 | Optional tall/melted output format | `index(date, name)` with `col(fields)` |

---

## Implementation Approach

Work through sections 1-7 in order. Each section can be implemented independently with its own tests.
