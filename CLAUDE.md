# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Metapyle is a Python 3.12+ library providing a unified interface for querying financial time-series data from multiple sources (Bloomberg, Macrobond, GS Quant, local files). It uses a YAML/CSV catalog to map human-readable names to source-specific details.

For API usage, features, and end-user documentation, see [docs/user-guide.md](docs/user-guide.md).

## Build & Development Commands

```bash
# Install dependencies (uses uv)
uv sync

# Run all tests
pytest

# Run single test file
pytest tests/unit/test_client.py

# Run single test function
pytest tests/unit/test_client.py::test_function_name

# Run with markers (integration tests require external credentials)
pytest -m "not integration"        # Skip integration tests
pytest -m bloomberg                # Only Bloomberg tests
pytest -m macrobond                # Only Macrobond tests
pytest -m gsquant                  # Only GS Quant tests
pytest -m integration --run-private  # Include private/in-house series tests

# Linting and formatting
ruff check .
ruff format --check .
ruff format .                      # Apply formatting

# Type checking (strict mode)
mypy src/

# Coverage
pytest --cov=src --cov-report=term-missing
```

## Architecture

### Core Flow
`Client` → `Catalog` → `SourceRegistry` → `BaseSource` implementations

1. **Client** (`src/metapyle/client.py`): Main entry point. Loads catalog, resolves names to entries, batches requests by source, manages caching.

2. **Catalog** (`src/metapyle/catalog.py`): Maps `my_name` to `CatalogEntry` dataclass. Loads from YAML or CSV. Validates source-specific attribute rules.

3. **SourceRegistry** (`src/metapyle/sources/base.py`): Global registry of source adapters. Use `@register_source("name")` decorator to register new sources.

4. **BaseSource** (`src/metapyle/sources/base.py`): Abstract base class. Subclasses implement `fetch(requests, start, end)` and `get_metadata(symbol)`.

### Source Adapters
Each adapter in `src/metapyle/sources/`:
- `bloomberg.py` - via xbbg
- `macrobond.py` - via macrobond-data-api, supports unified series
- `gsquant.py` - via gs-quant
- `localfile.py` - CSV/Parquet files

### Data Flow
1. `Client.get(names, start, end)` resolves names via catalog
2. Checks cache for each symbol
3. Groups uncached entries by source
4. Batch fetches per source (single API call)
5. Splits results, caches each symbol separately
6. Applies frequency alignment if requested
7. Returns DataFrame

### Source-Specific Attribute Rules

| Source | `field` | `path` | `params` |
|--------|---------|--------|----------|
| `bloomberg` | Required | Forbidden | — |
| `gsquant` | Required | Forbidden | Optional |
| `macrobond` | Forbidden | Forbidden | — |
| `localfile` | Forbidden | Required | — |

### Column Naming Convention
Sources return columns named `symbol::field` (if field present) or just `symbol`. Use `make_column_name(symbol, field)` helper.

### Cache Key
`(source, symbol, field, path, start_date, end_date)` - stored per-symbol, not per-batch.

## Code Conventions

**Dataclasses**: Always use `@dataclass(frozen=True, slots=True, kw_only=True)` for immutable data structures.

**Logging**: Use structured, lazy-formatted logging: `logger.debug("fetch: symbol=%s", symbol)` with key=value pairs for structured parsing.

**Imports**: Use relative imports within the package (`from .catalog import Catalog`).

**Docstrings**: NumPy style with Parameters, Returns, Raises sections.

**Python Version**: Project requires Python 3.12+ and uses modern syntax (PEP 695 type aliases, native generics). Ruff and mypy enforce style rules.

## Exception Hierarchy

```
MetapyleError (base)
├── CatalogError
│   ├── CatalogValidationError
│   ├── DuplicateNameError
│   ├── UnknownSourceError
│   └── NameNotFoundError
└── FetchError
    └── NoDataError
```

## Adding a New Source

1. Create adapter in `src/metapyle/sources/`:

```python
from metapyle.sources.base import BaseSource, FetchRequest, make_column_name, register_source

@register_source("newsource")
class NewSource(BaseSource):
    def fetch(self, requests: Sequence[FetchRequest], start: str, end: str, **kwargs) -> pd.DataFrame:
        # Return DataFrame with DatetimeIndex, columns named via make_column_name()
        ...

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        ...
```

2. Add validation rules in `catalog.py`:
   - `_SOURCE_COLUMNS` for CSV template columns
   - `_SOURCE_VALIDATION` for attribute rules (required/forbidden/allowed)

3. Import the new source in `src/metapyle/sources/__init__.py` to trigger registration.

## Test Organization

- `tests/unit/` - Unit tests (mocked external dependencies)
- `tests/integration/` - Integration tests requiring live credentials
- `tests/integration/fixtures/` - YAML catalog files for integration tests

Test markers: `integration`, `bloomberg`, `macrobond`, `gsquant`, `private`
