# DataFrame Normalization Design

## Problem

When concatenating DataFrames from different sources in `_assemble_dataframe`, pandas raises "Cannot concat tz-aware and tz-naive datetimeindex" if sources return inconsistent timezone handling.

Currently, each source (bloomberg, macrobond, localfile, gsquant) independently normalizes:
- Index name → `"date"`
- Timezone → UTC (localize if naive, convert if aware)

This duplication is error-prone and any new source that forgets normalization breaks the concat.

## Solution

Create a shared `normalize_dataframe()` utility function that sources call, plus a defensive safety net in `_assemble_dataframe`.

## Design

### 1. `normalize_dataframe()` function

**Location:** `src/metapyle/sources/base.py`

**Signature:**
```python
def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame for consistent source output.

    Ensures:
    - Index is DatetimeIndex (converts if needed)
    - Index timezone is UTC (localizes naive, converts aware)
    - Index name is "date"

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with datetime-like index.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame (may be same object if already normalized).

    Raises
    ------
    ValueError
        If index cannot be converted to DatetimeIndex.
    """
```

**Behavior:**
1. If index is not `DatetimeIndex`, convert via `pd.to_datetime(df.index)`
2. If index is tz-naive, localize to UTC
3. If index is tz-aware but not UTC, convert to UTC
4. Set `index.name = "date"`
5. Return the DataFrame (mutates in place for efficiency, returns for chaining)

### 2. Source adapter changes

Each source replaces inline normalization with `normalize_dataframe()` call.

**Before (example):**
```python
# Normalize index name
df.index.name = "date"

# Ensure UTC timezone
if df.index.tz is None:
    df.index = df.index.tz_localize("UTC")
else:
    df.index = df.index.tz_convert("UTC")

return df
```

**After:**
```python
return normalize_dataframe(df)
```

**Files to update:**
- `sources/bloomberg.py` — 1 location
- `sources/macrobond.py` — 2 locations (`_fetch_regular`, `_fetch_unified`)
- `sources/localfile.py` — 1 location
- `sources/gsquant.py` — 1 location

### 3. Safety net in `_assemble_dataframe`

In `client.py`, add defensive normalization before concat:

```python
def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame], names: list[str]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()

    renamed: list[pd.DataFrame] = []
    for my_name, df in dfs.items():
        col = df.columns[0]
        df_renamed = df[[col]].rename(columns={col: my_name})
        # Defensive normalization (sources should already normalize,
        # but this ensures safe concat even with misbehaving sources)
        renamed.append(normalize_dataframe(df_renamed))

    combined = pd.concat(renamed, axis=1)
    # ... rest unchanged
```

**Key points:**
- Import `normalize_dataframe` in client.py
- Idempotent — if already normalized, very cheap no-op
- Comment explains this is a safety net

### 4. Testing

**New file:** `tests/unit/test_sources_normalize.py`

Unit tests for `normalize_dataframe()`:
1. Tz-naive → UTC localized
2. Tz-aware non-UTC → UTC converted
3. Already UTC → unchanged (idempotent)
4. Index name set to "date"
5. Non-DatetimeIndex → converted
6. Invalid index → ValueError

**Integration-style test:**
- Two DataFrames: one tz-naive, one tz-aware with different timezone
- Pass through `_assemble_dataframe`
- Verify concat succeeds

## Benefits

- **DRY:** Single normalization implementation
- **Safe:** Defensive check in client prevents concat failures
- **Explicit contract:** Sources return normalized DataFrames
- **Testable:** Normalization logic is isolated and unit-testable
- **Cache stores normalized data:** Consistent, predictable cached DataFrames
