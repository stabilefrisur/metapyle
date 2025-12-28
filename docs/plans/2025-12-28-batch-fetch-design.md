# Batch Fetch Design

## Summary

Replace single-symbol `fetch()` with batch-capable `fetch(requests, start, end)` across all source adapters. This reduces API round-trips and improves performance when fetching multiple series.

## Goals

- **Performance**: Reduce API calls by batching requests to sources that support it
- **Developer ergonomics**: Keep source adapter interface simple and uniform

## Non-Goals

- Backward compatibility with old single-symbol `fetch()` signature
- Partial failure handling (future enhancement)

## Design

### FetchRequest Dataclass

New dataclass in `sources/base.py`:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class FetchRequest:
    """Single request within a batch fetch."""
    symbol: str
    field: str | None = None
    path: str | None = None
```

### BaseSource Interface

Updated abstract method:

```python
@abstractmethod
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Fetch time-series data for one or more symbols.

    Parameters
    ----------
    requests : Sequence[FetchRequest]
        One or more fetch requests.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and one column per request.
        Column naming: "symbol::field" if field present, otherwise "symbol".

    Raises
    ------
    NoDataError
        If no data is returned for any symbol.
    FetchError
        If data retrieval fails.
    """
```

Single-symbol fetch is `requests=[FetchRequest(symbol="...")]` — no special case.

`get_metadata()` remains unchanged (inherently single-symbol).

### Column Naming Convention

Utility function in `sources/base.py`:

```python
def make_column_name(symbol: str, field: str | None) -> str:
    """Generate consistent column name for source output."""
    return f"{symbol}::{field}" if field else symbol
```

- **`get()`**: Client renames columns to `my_name` from catalog
- **`get_raw()`**: Returns columns as `"symbol::field"` or `"symbol"`

### Source Implementations

#### Bloomberg

```python
def fetch(self, requests, start, end):
    # Collect unique symbols and fields
    # Single bdh() call: blp.bdh(tickers, fields, start, end)
    # Parse MultiIndex result
    # Rename columns to "symbol::field" format
```

#### Macrobond

```python
def fetch(self, requests, start, end):
    # Extract symbol list from requests
    # Single get_series() call
    # Rename columns to symbol names
    # Filter by date range
```

#### Localfile

```python
def fetch(self, requests, start, end):
    # Validate all requests have same path
    # Read file once
    # Extract requested columns (symbol = column name in file)
    # Filter by date range
```

### Client Changes

#### `get()` Flow

1. Resolve catalog entries for all requested symbols
2. Check cache per-entry, separate cached vs uncached
3. Group uncached entries by source
4. For each source group:
   - Build `FetchRequest` list from entries
   - Call `source.fetch(requests, start, end)`
   - Split result DataFrame into single-column DataFrames
   - Cache each individually
5. Apply frequency alignment if requested
6. Rename all columns to `my_name`
7. Assemble final wide DataFrame

#### `get_raw()` Flow

1. Default `end` to today if None
2. Check cache
3. If miss: create `FetchRequest`, call `source.fetch([request], start, end)`
4. Cache result
5. Return DataFrame with source column naming

### Cache Strategy

- Cache keys remain: `(source, symbol, field, path, start_date, end_date)`
- Check cache per-symbol before batching
- Only fetch uncached symbols
- After batch fetch, split and cache each column individually

### Error Handling

- Source raises on any failure in batch
- No partial success in v1
- Future: Client could retry failed symbols individually

## Files Changed

- `src/metapyle/sources/base.py` — Add `FetchRequest`, update `BaseSource.fetch()`, add `make_column_name()`
- `src/metapyle/sources/bloomberg.py` — Implement batch fetch
- `src/metapyle/sources/macrobond.py` — Implement batch fetch
- `src/metapyle/sources/localfile.py` — Implement batch fetch
- `src/metapyle/client.py` — Update `get()` and `get_raw()` for batching
- `tests/unit/test_sources_*.py` — Update all source tests
- `tests/unit/test_client.py` — Update client tests

## Public API Impact

- `Client.get()` — No signature change, behavior unchanged
- `Client.get_raw()` — No signature change, column naming now consistent
- `BaseSource.fetch()` — Breaking change (internal API)
