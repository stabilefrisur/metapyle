# gs-quant Source Design

## Overview

Add gs-quant as a data source for fetching financial data from Goldman Sachs Marquee platform.

## Data Model

### Mapping

| Metapyle Concept | gs-quant Concept | Example |
|------------------|------------------|---------|
| `symbol` | Bloomberg ID (bbid) filter | `EURUSD`, `SPX`, `AAPL UW` |
| `field` | `{dataset_id}::{value_column}` | `FXIMPLIEDVOL::impliedVolatility` |
| `params` (new) | Additional query filters | `{"tenor": "3m", "deltaStrike": "DN"}` |

### API Pattern

```python
# Internal implementation
data = Dataset(dataset_id).get_data(start, end, bbid=[symbols], **params)
result = pd.pivot_table(data, values=value_column, index=['date'], columns=['bbid'])
```

### Catalog Entry Example

```yaml
- my_name: eurusd_3m_vol
  source: gsquant
  symbol: EURUSD
  field: FXIMPLIEDVOL::impliedVolatility
  params:
    tenor: 3m
    deltaStrike: DN
    location: NYC
```

## Design Decisions

### Authentication

**Decision**: External initialization (Option A)

User must call `GsSession.use()` before using metapyle with gsquant source. Source validates session exists but does not manage credentials.

**Rationale**: Matches Bloomberg/Macrobond pattern where connection setup is external. Keeps source adapter simple.

### Extra Filter Parameters

**Decision**: Add `params` field to `CatalogEntry` and `FetchRequest` (Option B)

Some gs-quant datasets require additional filters (tenor, deltaStrike, location). These are passed via a new `params: dict[str, Any] | None` field.

**Rationale**: Clean, extensible design. Useful for other sources in future.

### Output Column Naming

**Decision**: Symbol only (Option A)

Output columns are named by symbol (e.g., `EURUSD`), not by dataset or value column.

**Rationale**: Matches Macrobond pattern. The `field` info is in the catalog entry; column names match `my_name`'s symbol.

### Metadata Implementation

**Decision**: Minimal (Option A)

`get_metadata()` returns basic info: `{"source": "gsquant", "symbol": symbol, "gsquant_available": bool}`.

**Rationale**: Method signature only takes `symbol`, not `field`, so we can't determine which dataset to query. Keep simple for v1.

## Implementation

### Schema Changes

#### CatalogEntry

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    my_name: str
    source: str
    symbol: str
    field: str | None = None
    path: str | None = None
    description: str | None = None
    unit: str | None = None
    params: dict[str, Any] | None = None  # NEW
```

#### FetchRequest

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class FetchRequest:
    symbol: str
    field: str | None = None
    path: str | None = None
    params: dict[str, Any] | None = None  # NEW
```

### Files to Modify

| File | Change |
|------|--------|
| `catalog.py` | Add `params` to `CatalogEntry`, update parsing |
| `sources/base.py` | Add `params` to `FetchRequest` |
| `client.py` | Pass `params` from entry to request |
| `sources/__init__.py` | Import `GSQuantSource` |

### New Files

| File | Purpose |
|------|---------|
| `sources/gsquant.py` | gs-quant source adapter |
| `tests/unit/test_sources_gsquant.py` | Unit tests with mocked API |

### GSQuantSource Implementation

```python
"""gs-quant source adapter using gs_quant library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["GSQuantSource"]

logger = logging.getLogger(__name__)

_GSQUANT_AVAILABLE: bool | None = None
_gsquant_modules: dict[str, Any] = {}


def _get_gsquant() -> dict[str, Any]:
    """Lazy import of gs_quant modules."""
    global _GSQUANT_AVAILABLE, _gsquant_modules
    
    if _GSQUANT_AVAILABLE is None:
        try:
            from gs_quant.data import Dataset
            from gs_quant.session import GsSession
            _gsquant_modules = {"Dataset": Dataset, "GsSession": GsSession}
            _GSQUANT_AVAILABLE = True
        except (ImportError, Exception):
            _gsquant_modules = {}
            _GSQUANT_AVAILABLE = False
    
    return _gsquant_modules


def _parse_field(field: str) -> tuple[str, str]:
    """Parse field into dataset_id and value_column."""
    if "::" not in field:
        raise ValueError(f"Invalid field format: {field}. Expected 'dataset_id::value_column'")
    parts = field.split("::", 1)
    return parts[0], parts[1]


@register_source("gsquant")
class GSQuantSource(BaseSource):
    """Source adapter for Goldman Sachs Marquee data via gs-quant."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        if not requests:
            return pd.DataFrame()

        gs = _get_gsquant()
        if not gs:
            raise FetchError("gs-quant package is not installed. Install with: pip install gs-quant")

        Dataset = gs["Dataset"]

        # Group requests by dataset_id
        groups: dict[str, list[FetchRequest]] = {}
        value_columns: dict[str, str] = {}
        
        for req in requests:
            if not req.field:
                raise FetchError(f"gsquant source requires field in format 'dataset_id::value_column'")
            
            dataset_id, value_column = _parse_field(req.field)
            
            if dataset_id not in groups:
                groups[dataset_id] = []
                value_columns[dataset_id] = value_column
            elif value_columns[dataset_id] != value_column:
                raise FetchError(
                    f"Cannot batch requests with different value columns for same dataset: "
                    f"{value_columns[dataset_id]} vs {value_column}"
                )
            
            groups[dataset_id].append(req)

        # Fetch each dataset group
        result_dfs: list[pd.DataFrame] = []
        
        for dataset_id, group_requests in groups.items():
            symbols = [req.symbol for req in group_requests]
            value_column = value_columns[dataset_id]
            
            # Merge params (all requests in group should have compatible params)
            merged_params: dict[str, Any] = {}
            for req in group_requests:
                if req.params:
                    merged_params.update(req.params)

            logger.debug(
                "fetch_start: dataset=%s, symbols=%s, params=%s",
                dataset_id, symbols, merged_params
            )

            try:
                ds = Dataset(dataset_id)
                data = ds.get_data(start, end, bbid=symbols, **merged_params)
            except Exception as e:
                logger.error("fetch_failed: dataset=%s, error=%s", dataset_id, str(e))
                raise FetchError(f"gs-quant API error for {dataset_id}: {e}") from e

            if data.empty:
                raise NoDataError(f"No data returned for {symbols} from {dataset_id}")

            # Pivot to wide format
            pivoted = pd.pivot_table(
                data, 
                values=value_column, 
                index=['date'], 
                columns=['bbid']
            )
            
            # Ensure DatetimeIndex
            if not isinstance(pivoted.index, pd.DatetimeIndex):
                pivoted.index = pd.to_datetime(pivoted.index)
            
            result_dfs.append(pivoted)

        # Merge all results
        if not result_dfs:
            return pd.DataFrame()
        
        result = result_dfs[0]
        for df in result_dfs[1:]:
            result = result.join(df, how='outer')

        logger.info("fetch_complete: columns=%s, rows=%d", list(result.columns), len(result))
        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        _get_gsquant()
        return {
            "source": "gsquant",
            "symbol": symbol,
            "gsquant_available": _GSQUANT_AVAILABLE or False,
        }
```

### pyproject.toml Updates

```toml
[project.optional-dependencies]
gsquant = ["gs-quant>=1.0.0"]

[[tool.mypy.overrides]]
module = ["gs_quant", "gs_quant.*"]
ignore_missing_imports = true
```

## Testing Strategy

### Unit Tests

- Mock `gs_quant.data.Dataset` and `gs_quant.session.GsSession`
- Test field parsing (`dataset_id::value_column`)
- Test batch grouping by dataset
- Test params merging
- Test error handling (missing session, API errors, no data)

### Integration Tests (Manual)

- Requires valid GS credentials
- Test real API calls against known datasets

## Usage Example

```python
from gs_quant.session import GsSession, Environment

# User initializes session externally
GsSession.use(Environment.PROD, client_id='...', client_secret='...')

# Then use metapyle normally
from metapyle import Client

client = Client(catalog="gsquant_catalog.yaml")
df = client.get(["eurusd_3m_vol", "usdjpy_3m_vol"], start="2024-01-01", end="2024-12-31")
```

## Limitations

1. **Session management**: User must initialize `GsSession` before fetching
2. **Batch constraints**: Requests with same dataset must have same value_column
3. **Params merging**: All requests in a batch share merged params (may cause conflicts)
4. **Metadata**: Limited metadata available without dataset context
