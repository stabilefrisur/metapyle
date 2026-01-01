# Macrobond Unified Series Design

## Overview

Add support for Macrobond's `get_unified_series()` function which converts multiple series to a common frequency and calendar. This enables server-side alignment of Macrobond data with currency conversion, frequency conversion, and calendar merging.

## User Interface

### `Client.get()` Signature Change

```python
def get(
    self,
    names: list[str],
    start: str,
    end: str | None = None,
    *,
    frequency: str | None = None,
    output_format: str = "wide",
    use_cache: bool = True,
    unified: bool = False,    # NEW
    **kwargs: Any,            # NEW - passed to sources
) -> pd.DataFrame:
```

### Usage Examples

```python
# Simple case - uses hardcoded defaults (most users)
client.get(["gdp_us", "cpi_eu"], start, end, unified=True)

# Power user - imports macrobond enums directly
from macrobond_data_api.common.enums import SeriesFrequency

client.get(["gdp_us"], start, end, unified=True, 
           frequency=SeriesFrequency.WEEKLY,
           currency="EUR")
```

## Hardcoded Defaults

When `unified=True` without overrides:

- `frequency`: `SeriesFrequency.DAILY`
- `weekdays`: `SeriesWeekdays.MONDAY_TO_FRIDAY`
- `calendar_merge_mode`: `CalendarMergeMode.AVAILABLE_IN_ALL`
- `currency`: `"USD"`
- `start_point`: `StartOrEndPoint(start)` from Client
- `end_point`: `StartOrEndPoint(end)` from Client

Power users can override any setting by passing macrobond enums directly via `**kwargs`. No string-to-enum mapping — kwargs flow straight to `get_unified_series()`.

## Source Interface Changes

### `BaseSource.fetch()` Signature

```python
@abstractmethod
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,  # NEW - sources accept and handle as appropriate
) -> pd.DataFrame:
```

### Source Behavior

| Source | `**kwargs` handling |
|--------|---------------------|
| BloombergSource | Ignores `**kwargs` |
| GSQuantSource | Ignores `**kwargs` (uses `FetchRequest.params`) |
| LocalFileSource | Ignores `**kwargs` |
| MacrobondSource | Uses `unified` kwarg to switch APIs; passes remaining kwargs to `get_unified_series()` |

### Client Passes Through Blindly

```python
def _fetch_from_source(self, source_name, requests, start, end, **kwargs):
    source = self._registry.get(source_name)
    return source.fetch(requests, start, end, **kwargs)
```

This keeps Client source-agnostic — no `if source == "macrobond"` logic.

## MacrobondSource Implementation

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
) -> pd.DataFrame:
    unified = kwargs.pop("unified", False)
    
    if unified:
        return self._fetch_unified(requests, start, end, **kwargs)
    else:
        return self._fetch_regular(requests, start, end)

def _fetch_unified(self, requests, start, end, **kwargs) -> pd.DataFrame:
    """Use get_unified_series() with hardcoded defaults, kwargs override."""
    from macrobond_data_api.common.enums import (
        SeriesFrequency, SeriesWeekdays, CalendarMergeMode
    )
    from macrobond_data_api.common.types import StartOrEndPoint
    
    symbols = [req.symbol for req in requests]
    
    # Hardcoded defaults
    unified_kwargs = {
        "frequency": SeriesFrequency.DAILY,
        "weekdays": SeriesWeekdays.MONDAY_TO_FRIDAY,
        "calendar_merge_mode": CalendarMergeMode.AVAILABLE_IN_ALL,
        "currency": "USD",
        "start_point": StartOrEndPoint(start),
        "end_point": StartOrEndPoint(end),
    }
    # Power user overrides
    unified_kwargs.update(kwargs)
    
    result = mda.get_unified_series(*symbols, **unified_kwargs)
    # Convert to DataFrame...

def _fetch_regular(self, requests, start, end) -> pd.DataFrame:
    """Existing get_series() logic (current implementation)."""
    ...
```

## Caching Behavior

When `unified=True`:
- **Cache is bypassed for macrobond entries** — unified data always fetched fresh
- Other sources (bloomberg, localfile) still use cache normally in mixed-source calls

This is because unified transformation is server-side and depends on all symbols together.

## Mixed Source Behavior

When `unified=True` and request includes mixed sources (macrobond + bloomberg + localfile):
- Macrobond entries use `get_unified_series()` (no cache)
- Other sources fetch normally with caching
- Results are joined together
- Client-side `frequency` alignment can still be applied after

## Testing Strategy

**Unit tests for MacrobondSource:**
- Test `fetch()` with `unified=False` (existing behavior)
- Test `fetch()` with `unified=True` calls `get_unified_series()` with defaults
- Test `fetch()` with `unified=True` and kwargs override defaults
- Mock `macrobond_data_api` module

**Unit tests for Client:**
- Test `get()` passes `**kwargs` through to `_fetch_from_source()`
- Test `unified=True` skips cache for macrobond entries
- Test mixed sources (macrobond unified + bloomberg cached) work together

**Integration tests:**
- Test against real Macrobond API with `unified=True` (marked `@pytest.mark.integration`)

## Documentation

**Docstring for `Client.get()`:**
- Document `unified` parameter
- Note that `**kwargs` are passed to source adapters
- Reference user guide for unified series details

**User guide section:**
- List default unified settings
- Show basic usage example
- Show power user override example with enum imports
- Link to Macrobond API documentation
- Note that caching is bypassed when `unified=True`

## Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| kwargs pass-through | Macrobond enums, no string mapping | Power users know the API; no translation/documentation burden |
| Override scope | Call-level only, not catalog | Unified kwargs apply to batch, not per-symbol |
| Caching | Bypass for unified | Server-side transformation, can't cache per-symbol |
| Mixed sources | Unified applies to macrobond only | Flexible, pragmatic |
| Source coupling | All sources accept `**kwargs` | Client stays source-agnostic |
