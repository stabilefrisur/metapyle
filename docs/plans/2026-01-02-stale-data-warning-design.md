# Stale Data Warning Design

## Overview

Add a warning log when fetched data ends significantly before the requested end date, helping users detect stale or delayed data without manual inspection.

## Problem

When requesting data with `end=None` (defaults to today):
- Some symbols have data as of T-1 (normal for most sources)
- Others may be T-x due to publication delays (e.g., quarterly GDP)
- Currently no visibility into this gap — stale data gets forward-filled and cached with `end=today`

## Solution

After fetching data from a source (not from cache), inspect `df.index.max()` for each symbol. If the actual end date is more than 1 business day before the requested end date, emit a WARNING log.

## Behavior

**Warning format:**
```
WARNING metapyle.client: stale_data: symbol=gdp_us, actual_end=2024-09-30, requested_end=2025-01-02, gap_bdays=67
```

**When it fires:**
- Only on fresh fetches (not cache hits — cache has no way to know actual end)
- Only when gap > 1 business day (T+1 delay is normal)

**Threshold calculation:**
- Use pandas `BDay()` offset for business day calculation
- `threshold = pd.Timestamp(requested_end) - pd.offsets.BDay(1)`
- Warning if `actual_end < threshold`

**Examples:**
- Request on Monday 2025-01-06 with `end=None` (today)
  - Data ends Friday 2025-01-03 → no warning (1 business day gap)
  - Data ends Thursday 2025-01-02 → warning (2 business day gap)
- Request on Sunday 2025-01-05
  - Data ends Friday 2025-01-03 → no warning (Sunday threshold resolves correctly)

## Implementation

**Location:** `client.py`, inside `get()` method, after splitting batch results and before caching.

```python
# Inside the "Split result and cache each column" loop
if col_name in result_df.columns:
    col_df = result_df[[col_name]]
    
    # Check for stale data
    actual_end = col_df.index.max()
    threshold = pd.Timestamp(end) - pd.offsets.BDay(1)
    if actual_end < threshold:
        gap_bdays = len(pd.bdate_range(actual_end, end)) - 1
        logger.warning(
            "stale_data: symbol=%s, actual_end=%s, requested_end=%s, gap_bdays=%d",
            entry.my_name,
            actual_end.date().isoformat(),
            end,
            gap_bdays,
        )
    
    # Cache the column (existing logic unchanged)
    ...
```

**No changes to:**
- `cache.py` — schema and logic unchanged
- `processing.py` — not involved
- Source adapters — not involved

## Testing

**Test file:** `tests/unit/test_client_stale_data.py`

**Test cases:**

1. **No warning when data is current** — request `end=2025-01-02` (Thursday), data ends `2025-01-02` → no warning

2. **No warning for 1 business day gap** — request `end=2025-01-06` (Monday), data ends `2025-01-03` (Friday) → no warning

3. **Warning for 2+ business day gap** — request `end=2025-01-06`, data ends `2025-01-02` (Thursday) → warning with `gap_bdays=2`

4. **Weekend handling** — request `end=2025-01-05` (Sunday), data ends `2025-01-03` (Friday) → no warning

5. **Multiple symbols, mixed freshness** — one fresh, one stale → warning only for stale one

**Test approach:**
- Mock the source adapter to return controlled data with specific end dates
- Use `caplog` pytest fixture to capture and assert warning messages

## Out of Scope (YAGNI)

- Storing `actual_end` in cache schema
- `df.attrs["freshness"]` metadata on returned DataFrame
- Automatic refresh/re-fetch behavior
- Configurable threshold per symbol or frequency
- `check_freshness()` or `cache_status()` methods
