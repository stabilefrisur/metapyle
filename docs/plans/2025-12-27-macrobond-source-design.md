# Macrobond Source Design

## Overview

Add a new source adapter for Macrobond (`macrobond_data_api`) to metapyle, enabling users to fetch macroeconomic time series data from the Macrobond database.

## Decisions

### Client Type Handling
- **Decision:** Let Macrobond handle client auto-detection internally
- **Rationale:** Macrobond's `_get_api()` tries ComClient first (uses desktop app auth), falls back to WebClient (keyring credentials). No need to expose client_type parameter.

### Unified Series Support
- **Decision:** Support both `get_one_series` and `get_unified_series` via `unified` kwarg
- **Rationale:** Users familiar with Macrobond expect `get_unified_series` for server-side alignment. Default to raw fetch for consistency with other sources.

### Unified Parameters
- **Decision:** Pass-through kwargs to `get_unified_series`
- **Rationale:** Users who want unified mode already know Macrobond's API. No translation layer needed.

### Date Filtering
- **Decision:** Fetch all data, filter locally to start:end range
- **Rationale:** Macrobond series are typically macro data (not huge). Simple implementation. Caching optimizes repeated fetches.

## Architecture

### File Location
`src/metapyle/sources/macrobond.py`

### Dependencies
- `macrobond-data-api` (optional dependency)
- Lazy import pattern (consistent with Bloomberg source)

### Registration
```python
@register_source("macrobond")
class MacrobondSource(BaseSource):
    ...
```

## Implementation

### `fetch()` Method

```python
def fetch(
    self,
    symbol: str,
    start: str,
    end: str,
    *,
    unified: bool = False,
    **kwargs: Any,
) -> pd.DataFrame:
```

**Default behavior (`unified=False`):**
1. Call `mda.get_one_series(symbol)`
2. Convert to DataFrame with DatetimeIndex
3. Filter to `start:end` date range
4. Return single-column DataFrame with column named by `symbol`

**Unified behavior (`unified=True`):**
1. Call `mda.get_unified_series(symbol, **kwargs)`
2. All kwargs passed through (frequency, currency, calendar_merge_mode, etc.)
3. Convert to DataFrame with DatetimeIndex
4. Filter to `start:end` date range
5. Return single-column DataFrame

### `get_metadata()` Method

```python
def get_metadata(self, symbol: str) -> dict[str, Any]:
```

1. Call `mda.get_one_entity(symbol)`
2. Access `.metadata` property
3. Convert to plain dict and return

### Error Handling

- `FetchError` if `macrobond_data_api` not installed
- `NoDataError` if series returns empty or no data in date range
- `FetchError` wrapping any Macrobond API exceptions

## Testing Strategy

### Unit Tests (`tests/unit/test_sources_macrobond.py`)
- Mock `macrobond_data_api` module
- Test `fetch()` with `unified=False` - DataFrame construction, date filtering
- Test `fetch()` with `unified=True` - kwargs passed through
- Test `get_metadata()` - dict conversion
- Test error handling - various failure modes
- Test lazy import - error when package not installed

### Integration Tests (optional)
- Mark with `@pytest.mark.integration`
- Skip if Macrobond not available
- Test real fetch against known series

## Usage Examples

### Basic fetch
```python
source = MacrobondSource()
df = source.fetch("usgdp", "2020-01-01", "2024-12-31")
```

### Unified fetch with alignment
```python
from macrobond_data_api.common.enums import SeriesFrequency

source = MacrobondSource()
df = source.fetch(
    "usgdp",
    "2020-01-01",
    "2024-12-31",
    unified=True,
    frequency=SeriesFrequency.ANNUAL,
    currency="USD",
)
```

### Via catalog
```yaml
# catalog.yaml
GDP_US:
  source: macrobond
  symbol: usgdp
  frequency: quarterly
```

```python
client = Client(catalog="catalog.yaml")
df = client.get(["GDP_US"], start="2020-01-01", end="2024-12-31")
```
