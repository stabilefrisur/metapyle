# Metapyle Design Document

**Date:** December 25, 2025  
**Status:** Approved  
**Use Case:** Team library for standardized financial data access

## Overview

Metapyle provides a unified interface for querying financial time-series data from multiple sources (Bloomberg, local files, custom APIs) using human-readable catalog names. Built for team use with version-controlled catalogs and fail-fast error handling.

**Key Principles:**
- Explicit over implicit (no silent data transformations)
- Fail-fast error handling (no partial results)
- Strict validation at initialization
- Clean extension points for custom sources

## Core Architecture

Layered architecture with four components:

```
Client
  ├── Query orchestration, frequency validation
  ├── DataFrame assembly
  │
Catalog
  ├── YAML loading, validation
  ├── Name → source mapping
  │
SourceRegistry
  ├── Decorator-based registration
  ├── BaseSource lookup
  │
Adapters
  ├── BloombergSource (xbbg)
  ├── LocalFileSource (CSV/Parquet)
  └── User-defined sources
```

## Client Interface

Main entry point for all data operations:

```python
from metapyle import Client

# Initialize with catalog(s)
client = Client(catalog="catalogs/financial.yaml")
client = Client(catalog=["catalogs/equities.yaml", "catalogs/macro.yaml"])

# With custom cache path
client = Client(catalog="catalogs/financial.yaml", cache_path="//server/shared/cache")

# Disable caching entirely
client = Client(catalog="catalogs/financial.yaml", cache_enabled=False)

# Query by catalog names
df = client.get(["GDP_US", "CPI_EU"], start="2020-01-01", end="2024-12-31")

# With explicit frequency alignment
df = client.get(["GDP_US", "SPX_CLOSE"], start="2020-01-01", frequency="daily")

# Bypass catalog for ad-hoc queries
df = client.get_raw(source="bloomberg", symbol="SPX Index", field="PX_LAST", 
                     start="2020-01-01", end="2024-12-31")

# Force fresh fetch (bypass cache)
df = client.get(["GDP_US"], start="2020-01-01", end="2024-12-31", use_cache=False)

# Retrieve metadata
meta = client.get_metadata("GDP_US")

# Cache management
client.clear_cache()                    # Clear entire cache
client.clear_cache(symbol="GDP_US")     # Clear specific symbol
```

**Behaviors:**
- **Fail-fast:** Any fetch failure or missing symbol raises exception immediately
- **Explicit alignment:** Frequency mismatch raises error unless `frequency` parameter specified
- **Wide format output:** DataFrame with datetime index, columns named by catalog `my_name`

## Catalog System

YAML-based catalog maps user-defined names to source/symbol pairs.

**Schema:**
```yaml
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  description: US Gross Domestic Product
  unit: USD billions
  
- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  frequency: daily
```

**Fields:**
- `my_name` (required) - Unique identifier across all loaded catalogs
- `source` (required) - Must match registered source adapter
- `symbol` (required) - Source-specific identifier
- `frequency` (required) - One of: `daily`, `weekly`, `monthly`, `quarterly`, `annual`
- `field` (optional) - For multi-field sources (e.g., Bloomberg)
- `description`, `unit` (optional) - Metadata only

**Validation (at Client initialization):**
1. Parse all YAML files
2. Validate against dataclass schema (missing required fields → error)
3. Check for duplicate `my_name` across all catalogs → raise `DuplicateNameError`
4. Verify each `source` exists in SourceRegistry → raise `UnknownSourceError`
5. Validate `frequency` values → raise `ValueError` if invalid

**Multi-source handling:**
Same security from different sources requires distinct names:
```yaml
# bloomberg_catalog.yaml
- my_name: SPX
  source: bloomberg
  symbol: SPX Index

# local_catalog.yaml - must use different name
- my_name: SPX_LOCAL
  source: localfile
  symbol: /data/spx.parquet
```

Duplicate base names across catalogs raise `DuplicateNameError`. Team must coordinate naming.

**Implementation:**
- Python dataclasses for validation (no Pydantic)
- Store as dict: `my_name` → `CatalogEntry` dataclass
- O(1) lookup performance

## Source Registry & Adapters

**Base Interface:**
```python
from abc import ABC, abstractmethod
import pandas as pd

class BaseSource(ABC):
    @abstractmethod
    def fetch(self, symbol: str, start: str, end: str, **kwargs) -> pd.DataFrame:
        """
        Fetch data, return DataFrame with:
        - DatetimeIndex
        - Single column named 'value'
        """
        pass
    
    @abstractmethod
    def get_metadata(self, symbol: str) -> dict:
        """Return metadata dict (description, unit, frequency, etc.)"""
        pass
```

**Registration Pattern:**
```python
from metapyle.sources import register_source, BaseSource

@register_source("bloomberg")
class BloombergSource(BaseSource):
    def fetch(self, symbol, start, end, field="PX_LAST", **kwargs):
        from xbbg import blp
        df = blp.bdh(symbol, field, start, end)
        return df.rename(columns={field: 'value'})
    
    def get_metadata(self, symbol):
        # Could call Bloomberg reference data if needed
        return {}

@register_source("localfile")
class LocalFileSource(BaseSource):
    def fetch(self, symbol, start, end, **kwargs):
        # symbol is file path
        if symbol.endswith('.parquet'):
            df = pd.read_parquet(symbol)
        else:
            df = pd.read_csv(symbol, index_col=0, parse_dates=True)
        
        # Expect datetime index, single value column
        return df.loc[start:end].rename(columns={df.columns[0]: 'value'})
    
    def get_metadata(self, symbol):
        return {}
```

**Built-in Adapters:**
- `bloomberg` - Uses xbbg for Desktop Terminal COM access
- `localfile` - Reads CSV/Parquet with datetime index

**User Extension:**
Team members can add custom adapters for internal APIs by:
1. Subclassing `BaseSource`
2. Implementing `fetch()` and `get_metadata()`
3. Decorating with `@register_source("name")`
4. Importing module before creating Client

Documentation and examples provided. No helper utilities (auth, retries, rate limiting) - keeping it minimal.

**Authentication:** Out of scope. Assumes Bloomberg Terminal logged in, API keys configured externally.

## Data Processing & Alignment

**Frequency Handling:**
1. Client checks all requested symbols have same `frequency` from catalog
2. If mismatch detected → raise `FrequencyMismatchError` with clear guidance
3. If user specifies `frequency='daily'` parameter → apply alignment

**Alignment Rules (when `frequency` specified):**
- **Upsampling** (monthly → daily): Forward-fill values using `asfreq()`
- **Downsampling** (daily → monthly): Take last value of period using `resample()`

**DataFrame Assembly:**
1. Fetch each symbol via appropriate adapter (fail immediately on error)
2. Check frequency compatibility
3. Apply alignment if `frequency` parameter provided
4. Merge on datetime index (outer join)
5. Rename columns to catalog `my_name` values
6. Return wide DataFrame

**Date Handling:**
- `start` and `end` passed directly to adapters as strings
- Adapters handle parsing and source-specific formatting
- Client doesn't validate dates

## Cache System

Optional SQLite-based caching for fetched data. Reduces API calls for repeated queries.

**Configuration:**

Cache path resolution order:
1. Constructor `cache_path` parameter (if provided)
2. Environment variable `METAPYLE_CACHE_PATH` (if set)
3. Default: `./cache/data_cache.db` (relative to current working directory)

**Cache Behavior:**
- Cache keys are `(source, symbol, field, start_date, end_date)`
- Cache hit: Exact match or requested range is subset of cached range
- Cache miss: Any other case → fresh API call, result stored in cache
- No time-series stitching - superset-or-miss logic only
- `use_cache=False`: Bypasses cache lookup, fetches fresh, overwrites existing cache entry
- Both `get()` and `get_raw()` use the cache

**Cache Invalidation:**
- No automatic expiration (cache never auto-expires)
- Manual clearing via `clear_cache()` or `clear_cache(symbol="...")`
- Per-call bypass with `use_cache=False`

**Storage Schema:**
```sql
CREATE TABLE cache_entries (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    field TEXT,                    -- NULL for sources without fields
    start_date TEXT NOT NULL,      -- ISO format: 2020-01-01
    end_date TEXT NOT NULL,
    cached_at TEXT NOT NULL,       -- When it was cached
    UNIQUE(source, symbol, field, start_date, end_date)
);

CREATE TABLE cache_data (
    entry_id INTEGER NOT NULL,
    date TEXT NOT NULL,            -- ISO format
    value REAL NOT NULL,
    FOREIGN KEY (entry_id) REFERENCES cache_entries(id) ON DELETE CASCADE,
    PRIMARY KEY (entry_id, date)
);

CREATE INDEX idx_cache_lookup ON cache_entries(source, symbol, field);
```

**Cache keys use source identifiers, not `my_name`:**
- Allows catalog renames without invalidating cache
- `get_raw()` queries share cache with equivalent `get()` queries
- Multiple catalog names pointing to same source data share cache

**Failure Handling:**
- Cache failures (corrupt DB, permissions, disk full) log a warning and proceed without cache
- User gets their data; cache just doesn't help
- Exception to fail-fast: cache is an optimization layer, not core functionality

**Concurrency:**
- SQLite default file locking
- Reads are concurrent, writes serialized
- Safe for shared network drives

## Error Handling

Strict fail-fast approach throughout.

**Exception Hierarchy:**
```python
class MetapyleError(Exception):
    """Base exception"""
    pass

class CatalogError(MetapyleError):
    """Catalog validation/lookup errors"""
    pass

class FetchError(MetapyleError):
    """Data fetching errors"""
    pass

class FrequencyMismatchError(MetapyleError):
    """Frequency incompatibility"""
    pass
```

**Specific Errors:**
- `CatalogValidationError` - Malformed YAML or missing required fields
- `DuplicateNameError` - Same `my_name` in multiple catalogs
- `UnknownSourceError` - Catalog references unregistered source
- `SymbolNotFoundError` - Queried name not in catalog
- `NoDataError` - Adapter returned empty result
- `FrequencyMismatchError` - Mixed frequencies without alignment parameter

**Error Messages:**
Clear, actionable guidance. Example:
```
FrequencyMismatchError: Cannot mix frequencies in single query.
  - GDP_US: quarterly
  - SPX_CLOSE: daily
Solution: Specify frequency parameter: client.get([...], frequency='daily')
```

**No retries, no fallbacks** - errors propagate immediately. Team must handle at application layer.

## Testing Strategy

Hybrid approach: comprehensive unit tests + optional integration tests.

**Unit Tests (always run):**
- Catalog: YAML parsing, validation, duplicate detection, merging
- Client: Query logic, frequency checking, DataFrame assembly (mocked sources)
- SourceRegistry: Registration, lookup
- Processing: Alignment functions with synthetic data
- Cache: Put/get operations, subset matching, clear operations, failure fallback
- All external dependencies mocked
- Target: 90%+ coverage

**Integration Tests (optional, credential-gated):**
- Bloomberg adapter: Fetch real data from SPX Index
- LocalFile adapter: Read test fixtures
- Marked with `@pytest.mark.integration`
- Skip by default, run with `pytest -m integration`
- CI runs in secure environment with credentials

**Structure:**
```
tests/
  unit/
    test_catalog.py
    test_client.py
    test_sources.py
    test_processing.py
    test_cache.py
  integration/
    test_bloomberg_adapter.py
    test_localfile_adapter.py
    test_cache_integration.py
    fixtures/
      test_data.csv
      test_data.parquet
```

## Package Structure

```
src/metapyle/
  __init__.py              # Exports: Client, BaseSource, register_source, exceptions
  client.py                # Client class
  catalog.py               # Catalog, CatalogEntry dataclass
  cache.py                 # Cache class, SQLite operations
  processing.py            # Alignment functions
  exceptions.py            # Exception hierarchy
  sources/
    __init__.py            # Exports: BaseSource, register_source
    base.py                # BaseSource ABC, SourceRegistry
    bloomberg.py           # BloombergSource
    localfile.py           # LocalFileSource
```

**Dependencies:**
```toml
[project]
name = "metapyle"
requires-python = ">=3.12"
dependencies = [
    "pandas>=2.0.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
bloomberg = ["xbbg>=0.6.0"]
dev = ["pytest>=7.0", "pytest-mock>=3.0"]
```

**Installation:**
```bash
pip install metapyle              # Core only
pip install metapyle[bloomberg]   # With Bloomberg support
```

**Public API:**
```python
from metapyle import (
    Client,
    BaseSource,
    register_source,
    MetapyleError,
    CatalogError,
    FetchError,
)
```

## Out of Scope

- Authentication management (assumed configured externally)
- Automatic retries or fallbacks
- Workflow orchestration
- Real-time/streaming data
- Data validation beyond outlier warnings
- General-purpose caching (metapyle's cache is domain-specific for time-series data)

## Implementation Phases

**Phase 1: Core Foundation**
- Exception hierarchy
- BaseSource ABC and SourceRegistry
- Catalog dataclass and YAML loading
- Cache class with SQLite storage
- Basic Client with `get()` method (single frequency only)

**Phase 2: Adapters**
- LocalFileSource implementation
- BloombergSource implementation
- Integration tests

**Phase 3: Processing**
- Frequency alignment logic
- Wide DataFrame assembly
- `get_metadata()` implementation

**Phase 4: Polish**
- `get_raw()` for ad-hoc queries
- Comprehensive error messages
- Documentation and examples
- Team onboarding guide

## Success Criteria

- Team members can query data without knowing source APIs
- Catalog maintained in git with clear change history
- Failures are immediate and actionable
- Custom adapters can be added in <50 lines of code
- 90%+ unit test coverage
