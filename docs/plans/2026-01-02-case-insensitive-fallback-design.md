# Case-Insensitive Column Fallback Design

## Problem

Some data sources (e.g., Macrobond) normalize symbol case in their API responses. When a catalog entry has `ih:bl:SPX Index`, Macrobond returns the column as `ih:bl:spx index`. The current column lookup in `Client.get()` does exact string matching and fails to find the column.

The existing integration test didn't catch this because `sp500_mb` uses `uspricstk` which is already lowercase.

## Solution

Add a case-insensitive fallback in `Client.get()` column lookup logic.

### Implementation

In `Client.get()`, after batch fetching from a source, the column lookup currently tries:
1. `symbol::field` (exact match)
2. `symbol` only (exact match, for sources that ignore field)

Add a third fallback:
3. Case-insensitive match using a lowercase lookup map

```python
# Build case-insensitive lookup map (once per source batch)
lower_to_actual = {col.lower(): col for col in result_df.columns}

for entry in group_entries:
    col_name = make_column_name(entry.symbol, entry.field)
    
    # Fallback 1: symbol-only (e.g., Macrobond ignores field)
    if col_name not in result_df.columns:
        col_name = make_column_name(entry.symbol, None)
    
    # Fallback 2: case-insensitive (e.g., Macrobond normalizes case)
    if col_name not in result_df.columns:
        col_name = lower_to_actual.get(col_name.lower())
    
    if col_name is not None and col_name in result_df.columns:
        # ... existing column extraction logic
```

### Testing

Add integration test with mixed-case symbol:

1. New catalog entry in `macrobond.yaml`:
   - `my_name: sp500_mb_mixed_case`
   - `symbol: ih:bl:SPX Index` (mixed case)

2. New test class `TestMacrobondCaseFallback` that fetches this entry and verifies success.

## Files Changed

- `src/metapyle/client.py` - Add case-insensitive fallback
- `tests/integration/fixtures/macrobond.yaml` - Add test catalog entry
- `tests/integration/test_macrobond.py` - Add test class
