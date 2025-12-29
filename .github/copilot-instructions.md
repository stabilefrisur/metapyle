# Copilot Instructions for Metapyle

## Project Overview

Metapyle is a Python 3.12+ library providing a unified interface for querying financial time-series data from multiple sources. Follow these standards strictly.

## Python Version

**Target: Python 3.12+**

Use modern Python features exclusively. Never write code compatible with older versions.

---

## Type Hints

### Use Modern Syntax (PEP 604, PEP 585)

```python
# ✅ Correct - Python 3.12 style
def process(data: list[dict[str, int]] | None = None) -> tuple[str, int]:
    ...

# ❌ Wrong - Legacy typing
from typing import List, Dict, Optional, Tuple
def process(data: Optional[List[Dict[str, int]]] = None) -> Tuple[str, int]:
    ...
```

### Self Type

```python
from typing import Self

class Client:
    def with_cache(self, enabled: bool) -> Self:
        ...
```

### Type Aliases with `type` Statement (PEP 695)

```python
# ✅ Python 3.12 type alias syntax
type DataFrame = pd.DataFrame
type CatalogMapping = dict[str, CatalogEntry]
type SourceName = str

# ❌ Legacy aliases
from typing import TypeAlias
CatalogMapping: TypeAlias = dict[str, CatalogEntry]
```

### Generics with New Syntax (PEP 695)

```python
# ✅ Python 3.12 generic class syntax
class Registry[T]:
    def __init__(self) -> None:
        self._items: dict[str, T] = {}
    
    def get(self, name: str) -> T | None:
        return self._items.get(name)

# ✅ Generic functions
def first[T](items: list[T]) -> T | None:
    return items[0] if items else None

# ❌ Legacy generics
from typing import TypeVar, Generic
T = TypeVar('T')
class Registry(Generic[T]):
    ...
```

---

## Dataclasses

### Use `@dataclass` with Slots and Keyword-Only

```python
from dataclasses import dataclass, field

@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    my_name: str
    source: str
    symbol: str
    frequency: str
    field: str | None = None
    description: str | None = None
    unit: str | None = None
```

### Prefer `field()` for Mutable Defaults

```python
@dataclass(slots=True)
class Config:
    paths: list[str] = field(default_factory=list)
    options: dict[str, str] = field(default_factory=dict)
```

---

## Exception Handling

### Exception Chaining

```python
try:
    df = adapter.fetch(symbol, start, end)
except ConnectionError as e:
    raise FetchError(f"Failed to fetch {symbol}") from e
```

---

## Imports

Use `collections.abc` for abstract types (`Callable`, `Iterator`, `Mapping`, `Sequence`).

### Relative Imports Within Package

```python
# ✅ Inside src/metapyle/client.py
from .catalog import Catalog, CatalogEntry
from .exceptions import FetchError
from .sources import BaseSource

# ❌ Absolute imports within same package
from metapyle.catalog import Catalog
```

### Dependency Philosophy

Prefer stdlib over third-party when equivalent. Keep dependencies sparse.

---

## Enums

### Use `StrEnum` for String Enums

```python
from enum import StrEnum, auto

class Frequency(StrEnum):
    DAILY = auto()
    WEEKLY = auto()
    MONTHLY = auto()
    QUARTERLY = auto()
    ANNUAL = auto()
```

---

## Testing

Use pytest with type hints. Prefer `tmp_path` fixture for file tests, `@pytest.mark.parametrize` for variants.

---

## Docstrings

### NumPy Style

```python
def fetch(
    self,
    symbols: list[str],
    start: str,
    end: str,
    frequency: str | None = None,
) -> pd.DataFrame:
    """
    Fetch time-series data for multiple symbols.

    Parameters
    ----------
    symbols : list[str]
        List of catalog names to fetch.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).
    frequency : str | None, optional
        Alignment frequency. If omitted, all symbols must have the same
        native frequency.

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with datetime index and columns named by catalog names.

    Raises
    ------
    SymbolNotFoundError
        If any symbol is not in the catalog.
    FrequencyMismatchError
        If symbols have different frequencies and no alignment frequency
        specified.
    FetchError
        If data retrieval fails for any symbol.

    Examples
    --------
    >>> client = Client(catalog="financial.yaml")
    >>> df = client.fetch(["GDP_US", "CPI_EU"], start="2020-01-01", end="2024-12-31")
    """
```

---

## Logging

### Use `logging` Module with Lazy Formatting

```python
import logging

logger = logging.getLogger(__name__)

# ✅ Lazy formatting (deferred evaluation)
logger.debug("Fetching %s from %s", symbol, source)
logger.info("Loaded %d entries from catalog", len(entries))
logger.warning("Cache miss for %s, fetching from source", symbol)
logger.error("Failed to fetch %s: %s", symbol, error)

# ❌ Never use f-strings in logging (evaluated even if level disabled)
logger.debug(f"Fetching {symbol} from {source}")
```

### Logger Per Module

```python
# At top of each module
import logging

logger = logging.getLogger(__name__)
```

### Structured Log Messages

```python
# ✅ Consistent, parseable format
logger.info("fetch_complete: symbol=%s, rows=%d, duration=%.2fs", symbol, len(df), elapsed)
logger.error("fetch_failed: symbol=%s, source=%s, error=%s", symbol, source, str(e))

# ❌ Inconsistent prose
logger.info(f"Successfully fetched {len(df)} rows for {symbol}")
```

### Log Levels

| Level | Use For |
|-------|--------|
| `DEBUG` | Detailed diagnostic info (cache hits, query params, internal state) |
| `INFO` | Normal operations (fetches, cache operations, catalog loading) |
| `WARNING` | Recoverable issues (cache fallback, deprecated usage) |
| `ERROR` | Failures that don't crash (fetch errors, validation failures) |
| `CRITICAL` | Never use in library code (let application decide) |

### Exception Logging

```python
# ✅ Include exception info with exc_info
try:
    df = adapter.fetch(symbol, start, end)
except ConnectionError:
    logger.exception("fetch_failed: symbol=%s", symbol)  # Includes traceback
    raise

# For non-exception error paths
logger.error("validation_failed: symbol=%s, reason=%s", symbol, reason)
```

### No Print Statements

```python
# ❌ Never use print for diagnostics
print(f"Debug: {value}")

# ✅ Use logger
logger.debug("value=%s", value)
```

---

## Project-Specific Patterns

### Source Adapter Registration

Use the `@register_source` decorator to add new data sources:

```python
from collections.abc import Sequence
from metapyle.sources.base import BaseSource, FetchRequest, make_column_name, register_source

@register_source("custom")
class CustomSource(BaseSource):
    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch data for one or more requests.
        
        Returns DataFrame with DatetimeIndex and columns named using
        make_column_name(request.symbol, request.field).
        """
        # Example: batch fetch all symbols in one API call
        symbols = [req.symbol for req in requests]
        data = api.fetch_batch(symbols, start, end)
        
        # Rename columns using make_column_name
        result = pd.DataFrame()
        for req in requests:
            col_name = make_column_name(req.symbol, req.field)
            result[col_name] = data[req.symbol]
        return result
    
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return metadata dict (description, unit, frequency, etc.)."""
        ...
```

**FetchRequest** dataclass bundles request parameters:
```python
@dataclass(frozen=True, slots=True, kw_only=True)
class FetchRequest:
    symbol: str              # required
    field: str | None = None # optional (e.g., Bloomberg field)
    path: str | None = None  # optional (e.g., localfile path)
    params: dict[str, Any] | None = None  # optional (e.g., gsquant API params)
```

**make_column_name()** ensures consistent naming:
```python
make_column_name("SPX Index", "PX_LAST")  # → "SPX Index::PX_LAST"
make_column_name("usgdp", None)           # → "usgdp"
```

### Exception Hierarchy

```
MetapyleError (base)
├── CatalogError (catalog-related)
│   ├── CatalogValidationError (malformed YAML, missing fields)
│   ├── DuplicateNameError (same my_name twice)
│   ├── UnknownSourceError (source not registered)
│   └── SymbolNotFoundError (name not in catalog)
└── FetchError (data retrieval)
    └── NoDataError (adapter returned empty)
```

### Catalog Entry Structure

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    my_name: str                    # required
    source: str                     # required
    symbol: str                     # required
    field: str | None = None        # optional (e.g., Bloomberg field, gsquant dataset::column)
    path: str | None = None         # optional (e.g., localfile path)
    description: str | None = None  # optional
    unit: str | None = None         # optional
    params: dict[str, Any] | None = None  # optional (e.g., gsquant API params)
```

### Catalog CSV Import/Export

```python
# Generate CSV template (generic or source-specific)
Catalog.csv_template(source="bloomberg", path="template.csv")

# Load from CSV (validates all rows, reports all errors at once)
catalog = Catalog.from_csv("catalog.csv")  # or list of paths

# Export to CSV/YAML
catalog.to_csv("output.csv")
catalog.to_yaml("catalog.yaml")
```

`from_yaml()` and `Client()` accept `str | Path | list[str | Path]`.

### Cache Key Components

Cache stores data **per-symbol** (not per-batch). Each cache entry uses:
`(source, symbol, field, path, start_date, end_date)`.

All are strings; `field` and `path` can be `None`.

**Batch fetch behavior**: When fetching multiple symbols from the same source, metapyle:
1. Checks cache for each symbol individually
2. Groups uncached symbols by source
3. Batch fetches per source (single API call)
4. Splits result and caches each symbol separately

---

## Anti-Patterns to Avoid

### Never Do These

```python
# ❌ String type annotations (forward references unnecessary in 3.12)
def get_client(self) -> "Client": ...

# ❌ assert for validation (use explicit exceptions)
assert symbol in catalog, "Symbol not found"

# ❌ __future__ annotations import (unnecessary in 3.12)
from __future__ import annotations
```

---

## Tooling Compliance

This project uses:
- **ruff** for linting and formatting (see pyproject.toml)
- **mypy** with strict settings
- **pytest** for testing

All code must pass:
```bash
ruff check .
ruff format --check .
mypy src/
pytest
```
