# Frequency Handling Redesign

**Date:** 2025-12-26  
**Status:** Approved

## Summary

Remove declared `frequency` from catalog entries. Use pandas frequency strings directly for alignment. Detect misaligned series from actual data and warn instead of error.

## Problem

The current design has a `frequency` field in `CatalogEntry` that is:
1. Never validated against actual returned data
2. Limited to 5 frequencies (daily, weekly, monthly, quarterly, annual)
3. A source of silent data quality issues when declared frequency doesn't match reality

## Design

### 1. Catalog Changes

**Remove `frequency` from catalog entirely.**

`CatalogEntry` loses the `frequency` field. The `Frequency` enum is deleted. YAML catalog files no longer require or accept a `frequency` key.

**Before:**
```yaml
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CURY Index
  frequency: quarterly
  field: PX_LAST
```

**After:**
```yaml
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CURY Index
  field: PX_LAST
```

Catalog parsing (`_parse_entry`) removes frequency validation. The `required_fields` list drops from 4 to 3.

### 2. Frequency Parameter & Alignment

**`client.get()` accepts pandas frequency strings directly.**

The `frequency` parameter accepts any valid pandas frequency alias: `"D"`, `"B"`, `"W"`, `"ME"`, `"BME"`, `"QE"`, `"YE"`, etc. No mapping layer — passed straight to `df.resample()`.

```python
# Examples
client.get(["GDP_US", "SPX"], start, end, frequency="ME")  # month-end
client.get(["GDP_US", "SPX"], start, end, frequency="BME") # business month-end
client.get(["GDP_US", "SPX"], start, end, frequency="QE")  # quarter-end
```

**Processing module simplification:**

- Delete `Frequency` enum import
- Delete `FREQUENCY_MAP` 
- Delete `get_pandas_frequency()`
- Simplify `align_to_frequency(df, target_frequency: str)` to just call `df.resample(target_frequency).last().ffill()`

Invalid frequency strings cause pandas to raise `ValueError` — let that propagate.

### 3. Mismatch Detection & Warning

**When `frequency` is not specified, detect misaligned series and warn.**

After fetching all series but before merging, check if indexes align:

```python
def _check_index_alignment(self, dfs: dict[str, pd.DataFrame]) -> None:
    """Warn if series have misaligned indexes."""
    if len(dfs) <= 1:
        return
    
    # Infer frequency for each series
    freqs = {name: pd.infer_freq(df.index) for name, df in dfs.items()}
    
    # Check for mismatches
    unique_freqs = set(freqs.values())
    
    if len(unique_freqs) > 1:
        # Different frequencies (including None for irregular)
        freq_summary = ", ".join(f"{name}={freq or 'irregular'}" for name, freq in freqs.items())
        logger.warning(
            "index_mismatch: Series have different frequencies: %s. "
            "Outer join may produce NaN values. Consider specifying frequency parameter.",
            freq_summary,
        )
    elif unique_freqs == {None}:
        # All irregular — check if indexes actually match
        indexes = list(dfs.values())
        first_idx = indexes[0].index
        if not all(df.index.equals(first_idx) for df in indexes[1:]):
            logger.warning(
                "index_mismatch: Irregular series have different dates. "
                "Outer join may produce NaN values. Consider specifying frequency parameter.",
            )
```

**No error raised** — data is merged as-is. User gets a warning in logs if there's potential data quality issues.

### 4. Metadata Changes

**`get_metadata()` infers frequency from data:**

```python
def get_metadata(self, symbol: str) -> dict[str, Any]:
    # ... fetch data or use cache ...
    
    inferred_freq = pd.infer_freq(df.index)
    
    return {
        **source_meta,
        "my_name": entry.my_name,
        "source": entry.source,
        "symbol": entry.symbol,
        "frequency": inferred_freq,  # str or None if irregular
        "field": entry.field,
        "description": entry.description,
        "unit": entry.unit,
    }
```

### 5. Exception Changes

**Remove `FrequencyMismatchError`:**

- Delete from `exceptions.py`
- Remove from `__init__.py` exports
- Delete `_check_frequency_compatibility()` from `client.py`

## Files Changed

| File | Changes |
|------|---------|
| `catalog.py` | Remove `Frequency` enum, remove `frequency` from `CatalogEntry` |
| `processing.py` | Delete `FREQUENCY_MAP`, `get_pandas_frequency()`; simplify `align_to_frequency()` |
| `client.py` | Delete `_check_frequency_compatibility()`, add `_check_index_alignment()`, update `get_metadata()` |
| `exceptions.py` | Delete `FrequencyMismatchError` |
| `__init__.py` | Remove `Frequency`, `FrequencyMismatchError` from exports |
| Tests | Update to reflect new behavior |

## Rationale

- **Truth from data, not declarations:** Declared frequency was never validated, creating false confidence
- **Pandas does it better:** Full frequency support without maintaining our own enum
- **User flexibility:** Warn but don't block — users may legitimately want misaligned data
- **Less code:** Removes enum, mapping layer, validation logic
