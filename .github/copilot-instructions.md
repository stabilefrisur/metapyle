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

### Adapter Registration

```python
from metapyle.sources import register_source, BaseSource

@register_source("custom")
class CustomSource(BaseSource):
    def fetch(self, symbol: str, start: str, end: str, **kwargs: Any) -> pd.DataFrame:
        ...
    
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        ...
```

### Exception Hierarchy

```python
class MetapyleError(Exception):
    """Base exception for all metapyle errors."""

class CatalogError(MetapyleError):
    """Catalog-related errors."""

class FetchError(MetapyleError):
    """Data fetching errors."""
```

### Cache Key Construction

```python
@dataclass(frozen=True, slots=True)
class CacheKey:
    source: str
    symbol: str
    field: str | None
    start_date: str
    end_date: str
```

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
