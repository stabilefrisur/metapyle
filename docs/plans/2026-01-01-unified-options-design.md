# Unified Options Design

## Problem

The `client.get()` method currently uses `**kwargs` to pass Macrobond-specific options to `get_unified_series()`. One of these options is `frequency` (a `SeriesFrequency` enum), which conflicts with the existing `frequency` parameter (a pandas frequency string for client-side resampling).

When a user writes:
```python
df = client.get(
    ["us_gdp"],
    start="2020-01-01",
    unified=True,
    frequency=SeriesFrequency.MONTHLY,
)
```

The `frequency` parameter is correctly passed to Macrobond's `get_unified_series()`, but then the client also tries to use it for pandas resampling, causing an error.

## Requirements

1. Allow Macrobond unified series options (including `frequency`) without conflicting with client-side `frequency`
2. Support mixed-source queries where Macrobond uses `unified=True` and client-side `frequency` aligns all sources
3. Support post-processing: even pure Macrobond unified queries may need additional client-side resampling

## Solution

Replace `**kwargs` with an explicit `unified_options: dict[str, Any] | None` parameter.

### New API Signature

```python
def get(
    self,
    names: list[str],
    start: str,
    end: str | None = None,
    *,
    frequency: str | None = None,           # pandas freq string (client-side)
    output_format: str = "wide",
    use_cache: bool = True,
    unified: bool = False,
    unified_options: dict[str, Any] | None = None,  # Macrobond options
) -> pd.DataFrame:
```

### Usage Examples

**Pure Macrobond with server-side alignment:**
```python
from macrobond_data_api.common.enums import SeriesFrequency

df = client.get(
    ["us_gdp", "eu_gdp"],
    start="2020-01-01",
    unified=True,
    unified_options={"frequency": SeriesFrequency.MONTHLY, "currency": "EUR"},
)
```

**Mixed sources with both alignments:**
```python
df = client.get(
    ["us_gdp", "sp500_close"],  # macrobond + bloomberg
    start="2020-01-01",
    unified=True,
    unified_options={"frequency": SeriesFrequency.MONTHLY},
    frequency="ME",  # client-side alignment for final merge
)
```

**Macrobond unified with additional client-side resampling:**
```python
df = client.get(
    ["us_gdp", "eu_gdp"],
    start="2020-01-01",
    unified=True,
    unified_options={"frequency": SeriesFrequency.QUARTERLY},
    frequency="ME",  # additional client-side resampling
)
```

## Implementation Changes

### `client.py`

1. Replace `**kwargs: Any` with `unified_options: dict[str, Any] | None = None` in `get()` signature
2. Pass `unified_options or {}` to `_fetch_from_source()` instead of `**kwargs`
3. Update `_fetch_from_source()` to accept `unified_options: dict[str, Any]` instead of `**kwargs`
4. Client-side `frequency` alignment runs independently after fetch (no conflict)

### `sources/macrobond.py`

1. Update `fetch()` to accept `unified_options: dict[str, Any]` instead of `**kwargs`
2. Extract options from dict and pass to `get_unified_series()`
3. Keep `unified` as a separate boolean parameter

### Validation

- If `unified_options` provided but `unified=False`: log warning (options ignored)
- If `unified=True` but no macrobond sources in request: log warning

### Edge Cases

- **Mixed sources**: Macrobond fetched with unified mode, other sources fetched normally, then client-side `frequency` merges them
- **Non-macrobond with unified_options**: Options silently ignored (no error)

## Testing Strategy

### Unit Tests

- `test_client.py`: Test `unified_options` parameter parsing
- `test_client.py`: Test `frequency` (pandas string) still works independently
- `test_client.py`: Test warning when `unified_options` provided but `unified=False`

### Integration Tests

- `test_macrobond.py`: Update existing unified tests to use `unified_options={"frequency": ...}`
- `test_cross_source.py`: Add test for mixed sources with both `unified_options` and `frequency`

### Documentation Updates

- `user-guide.md`: Update "Macrobond Unified Series" section with new syntax
- Docstrings in `client.py` and `macrobond.py`

## Breaking Change

This is a breaking change for users who currently pass Macrobond options via `**kwargs`. Migration:

**Before:**
```python
df = client.get(
    ["us_gdp"],
    unified=True,
    frequency=SeriesFrequency.MONTHLY,
    currency="EUR",
)
```

**After:**
```python
df = client.get(
    ["us_gdp"],
    unified=True,
    unified_options={
        "frequency": SeriesFrequency.MONTHLY,
        "currency": "EUR",
    },
)
```
