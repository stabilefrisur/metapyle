# Metapyle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a unified interface for querying financial time-series data from multiple sources using human-readable catalog names.

**Architecture:** Layered design with Client (orchestration) → Catalog (name mapping) → SourceRegistry (adapter lookup) → Adapters (data fetch). SQLite cache for repeated queries. Fail-fast error handling throughout.

**Tech Stack:** Python 3.12+, pandas, PyYAML, SQLite (stdlib), xbbg (optional Bloomberg)

---

## Phase 1: Core Foundation

### Task 1.1: Exception Hierarchy

**Files:**
- Create: `src/metapyle/exceptions.py`
- Test: `tests/unit/test_exceptions.py`

**Step 1: Write the failing test**

```python
"""Unit tests for exception hierarchy."""

import pytest


def test_metapyle_error_is_exception() -> None:
    """MetapyleError should be a base exception."""
    from metapyle.exceptions import MetapyleError

    error = MetapyleError("test message")
    assert isinstance(error, Exception)
    assert str(error) == "test message"


def test_catalog_error_inherits_from_metapyle_error() -> None:
    """CatalogError should inherit from MetapyleError."""
    from metapyle.exceptions import CatalogError, MetapyleError

    error = CatalogError("catalog issue")
    assert isinstance(error, MetapyleError)
    assert isinstance(error, Exception)


def test_fetch_error_inherits_from_metapyle_error() -> None:
    """FetchError should inherit from MetapyleError."""
    from metapyle.exceptions import FetchError, MetapyleError

    error = FetchError("fetch failed")
    assert isinstance(error, MetapyleError)


def test_frequency_mismatch_error_inherits_from_metapyle_error() -> None:
    """FrequencyMismatchError should inherit from MetapyleError."""
    from metapyle.exceptions import FrequencyMismatchError, MetapyleError

    error = FrequencyMismatchError("frequency issue")
    assert isinstance(error, MetapyleError)


def test_catalog_validation_error_inherits_from_catalog_error() -> None:
    """CatalogValidationError should inherit from CatalogError."""
    from metapyle.exceptions import CatalogError, CatalogValidationError

    error = CatalogValidationError("invalid yaml")
    assert isinstance(error, CatalogError)


def test_duplicate_name_error_inherits_from_catalog_error() -> None:
    """DuplicateNameError should inherit from CatalogError."""
    from metapyle.exceptions import CatalogError, DuplicateNameError

    error = DuplicateNameError("duplicate found")
    assert isinstance(error, CatalogError)


def test_unknown_source_error_inherits_from_catalog_error() -> None:
    """UnknownSourceError should inherit from CatalogError."""
    from metapyle.exceptions import CatalogError, UnknownSourceError

    error = UnknownSourceError("unknown source")
    assert isinstance(error, CatalogError)


def test_symbol_not_found_error_inherits_from_catalog_error() -> None:
    """SymbolNotFoundError should inherit from CatalogError."""
    from metapyle.exceptions import CatalogError, SymbolNotFoundError

    error = SymbolNotFoundError("symbol not found")
    assert isinstance(error, CatalogError)


def test_no_data_error_inherits_from_fetch_error() -> None:
    """NoDataError should inherit from FetchError."""
    from metapyle.exceptions import FetchError, NoDataError

    error = NoDataError("no data returned")
    assert isinstance(error, FetchError)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_exceptions.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.exceptions'"

**Step 3: Write minimal implementation**

```python
"""Exception hierarchy for metapyle.

All metapyle exceptions inherit from MetapyleError for easy catching.
"""


class MetapyleError(Exception):
    """Base exception for all metapyle errors."""


class CatalogError(MetapyleError):
    """Catalog-related errors (validation, lookup, duplicates)."""


class FetchError(MetapyleError):
    """Data fetching errors."""


class FrequencyMismatchError(MetapyleError):
    """Raised when symbols have incompatible frequencies without alignment."""


class CatalogValidationError(CatalogError):
    """Raised when catalog YAML is malformed or missing required fields."""


class DuplicateNameError(CatalogError):
    """Raised when the same my_name appears in multiple catalog entries."""


class UnknownSourceError(CatalogError):
    """Raised when a catalog references a source that is not registered."""


class SymbolNotFoundError(CatalogError):
    """Raised when a queried name is not found in the catalog."""


class NoDataError(FetchError):
    """Raised when an adapter returns empty data."""
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_exceptions.py -v`
Expected: PASS (all 10 tests)

**Step 5: Commit**

```bash
git add src/metapyle/exceptions.py tests/unit/test_exceptions.py
git commit -m "feat(exceptions): add exception hierarchy"
```

---

### Task 1.2: BaseSource ABC and SourceRegistry

**Files:**
- Create: `src/metapyle/sources/__init__.py`
- Create: `src/metapyle/sources/base.py`
- Test: `tests/unit/test_sources_base.py`

**Step 1: Write the failing test**

```python
"""Unit tests for BaseSource ABC and SourceRegistry."""

import pandas as pd
import pytest


def test_base_source_is_abstract() -> None:
    """BaseSource cannot be instantiated directly."""
    from metapyle.sources.base import BaseSource

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        BaseSource()  # type: ignore[abstract]


def test_base_source_requires_fetch_method() -> None:
    """Subclass must implement fetch method."""
    from metapyle.sources.base import BaseSource

    class IncompleteSource(BaseSource):
        def get_metadata(self, symbol: str) -> dict:
            return {}

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        IncompleteSource()  # type: ignore[abstract]


def test_base_source_requires_get_metadata_method() -> None:
    """Subclass must implement get_metadata method."""
    from metapyle.sources.base import BaseSource

    class IncompleteSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame()

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        IncompleteSource()  # type: ignore[abstract]


def test_concrete_source_can_be_instantiated() -> None:
    """Concrete subclass with both methods can be instantiated."""
    from metapyle.sources.base import BaseSource

    class ConcreteSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame({"value": [1, 2, 3]})

        def get_metadata(self, symbol: str) -> dict:
            return {"description": "test"}

    source = ConcreteSource()
    assert source is not None


def test_source_registry_starts_empty() -> None:
    """SourceRegistry should start with no registered sources."""
    from metapyle.sources.base import SourceRegistry

    registry = SourceRegistry()
    assert registry.list_sources() == []


def test_source_registry_register_and_get() -> None:
    """SourceRegistry can register and retrieve sources."""
    from metapyle.sources.base import BaseSource, SourceRegistry

    class TestSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    registry = SourceRegistry()
    registry.register("test", TestSource)

    source = registry.get("test")
    assert isinstance(source, TestSource)


def test_source_registry_get_unknown_raises() -> None:
    """SourceRegistry raises UnknownSourceError for unknown source."""
    from metapyle.exceptions import UnknownSourceError
    from metapyle.sources.base import SourceRegistry

    registry = SourceRegistry()

    with pytest.raises(UnknownSourceError, match="unknown_source"):
        registry.get("unknown_source")


def test_source_registry_list_sources() -> None:
    """SourceRegistry can list all registered sources."""
    from metapyle.sources.base import BaseSource, SourceRegistry

    class TestSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    registry = SourceRegistry()
    registry.register("alpha", TestSource)
    registry.register("beta", TestSource)

    sources = registry.list_sources()
    assert sorted(sources) == ["alpha", "beta"]


def test_register_source_decorator() -> None:
    """register_source decorator adds source to global registry."""
    from metapyle.sources import BaseSource, register_source
    from metapyle.sources.base import _global_registry

    @register_source("decorated_test")
    class DecoratedSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    # Verify it's in the global registry
    source = _global_registry.get("decorated_test")
    assert isinstance(source, DecoratedSource)


def test_source_registry_caches_instances() -> None:
    """SourceRegistry returns same instance on repeated get calls."""
    from metapyle.sources.base import BaseSource, SourceRegistry

    class TestSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    registry = SourceRegistry()
    registry.register("test", TestSource)

    source1 = registry.get("test")
    source2 = registry.get("test")
    assert source1 is source2
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_base.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.sources'"

**Step 3: Write minimal implementation**

Create `src/metapyle/sources/__init__.py`:

```python
"""Source adapters for metapyle.

This module provides the base interface for data sources and a registry
for managing source adapters.
"""

from metapyle.sources.base import BaseSource, SourceRegistry, register_source

__all__ = ["BaseSource", "SourceRegistry", "register_source"]
```

Create `src/metapyle/sources/base.py`:

```python
"""Base source interface and registry."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from metapyle.exceptions import UnknownSourceError


class BaseSource(ABC):
    """Abstract base class for all data source adapters.

    Subclasses must implement `fetch` and `get_metadata` methods.
    """

    @abstractmethod
    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data for a symbol.

        Parameters
        ----------
        symbol : str
            Source-specific identifier (e.g., "SPX Index" for Bloomberg).
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        **kwargs : Any
            Source-specific parameters (e.g., field for Bloomberg).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named 'value'.

        Raises
        ------
        NoDataError
            If no data is returned for the symbol.
        FetchError
            If data retrieval fails.
        """
        pass

    @abstractmethod
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a symbol.

        Parameters
        ----------
        symbol : str
            Source-specific identifier.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary (description, unit, frequency, etc.).
        """
        pass


class SourceRegistry:
    """Registry for source adapters.

    Manages registration and retrieval of source adapters by name.
    Caches instantiated adapters for reuse.
    """

    def __init__(self) -> None:
        self._sources: dict[str, type[BaseSource]] = {}
        self._instances: dict[str, BaseSource] = {}

    def register(self, name: str, source_class: type[BaseSource]) -> None:
        """
        Register a source adapter class.

        Parameters
        ----------
        name : str
            Unique name for the source (e.g., "bloomberg", "localfile").
        source_class : type[BaseSource]
            The source adapter class to register.
        """
        self._sources[name] = source_class

    def get(self, name: str) -> BaseSource:
        """
        Get a source adapter instance by name.

        Parameters
        ----------
        name : str
            Name of the registered source.

        Returns
        -------
        BaseSource
            Instantiated source adapter.

        Raises
        ------
        UnknownSourceError
            If no source is registered with the given name.
        """
        if name not in self._sources:
            raise UnknownSourceError(
                f"Unknown source: {name}. "
                f"Available sources: {', '.join(self._sources.keys()) or 'none'}"
            )

        if name not in self._instances:
            self._instances[name] = self._sources[name]()

        return self._instances[name]

    def list_sources(self) -> list[str]:
        """
        List all registered source names.

        Returns
        -------
        list[str]
            List of registered source names.
        """
        return list(self._sources.keys())


# Global registry instance
_global_registry = SourceRegistry()


def register_source(name: str):
    """
    Decorator to register a source adapter class.

    Parameters
    ----------
    name : str
        Unique name for the source.

    Returns
    -------
    Callable
        Decorator function.

    Examples
    --------
    >>> @register_source("custom")
    ... class CustomSource(BaseSource):
    ...     def fetch(self, symbol, start, end, **kwargs):
    ...         ...
    ...     def get_metadata(self, symbol):
    ...         ...
    """

    def decorator(cls: type[BaseSource]) -> type[BaseSource]:
        _global_registry.register(name, cls)
        return cls

    return decorator
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_base.py -v`
Expected: PASS (all 10 tests)

**Step 5: Commit**

```bash
git add src/metapyle/sources/__init__.py src/metapyle/sources/base.py tests/unit/test_sources_base.py
git commit -m "feat(sources): add BaseSource ABC and SourceRegistry"
```

---

### Task 1.3: CatalogEntry Dataclass and Frequency Enum

**Files:**
- Create: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

```python
"""Unit tests for Catalog and CatalogEntry."""

import pytest


def test_frequency_enum_values() -> None:
    """Frequency enum should have expected values."""
    from metapyle.catalog import Frequency

    assert Frequency.DAILY == "daily"
    assert Frequency.WEEKLY == "weekly"
    assert Frequency.MONTHLY == "monthly"
    assert Frequency.QUARTERLY == "quarterly"
    assert Frequency.ANNUAL == "annual"


def test_frequency_is_str_enum() -> None:
    """Frequency values should be usable as strings."""
    from metapyle.catalog import Frequency

    assert f"frequency is {Frequency.DAILY}" == "frequency is daily"


def test_catalog_entry_required_fields() -> None:
    """CatalogEntry requires my_name, source, symbol, frequency."""
    from metapyle.catalog import CatalogEntry, Frequency

    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
        frequency=Frequency.QUARTERLY,
    )

    assert entry.my_name == "GDP_US"
    assert entry.source == "bloomberg"
    assert entry.symbol == "GDP CUR$ Index"
    assert entry.frequency == Frequency.QUARTERLY


def test_catalog_entry_optional_fields_default_none() -> None:
    """CatalogEntry optional fields default to None."""
    from metapyle.catalog import CatalogEntry, Frequency

    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
        frequency=Frequency.QUARTERLY,
    )

    assert entry.field is None
    assert entry.description is None
    assert entry.unit is None


def test_catalog_entry_with_optional_fields() -> None:
    """CatalogEntry can have optional fields set."""
    from metapyle.catalog import CatalogEntry, Frequency

    entry = CatalogEntry(
        my_name="SPX_CLOSE",
        source="bloomberg",
        symbol="SPX Index",
        frequency=Frequency.DAILY,
        field="PX_LAST",
        description="S&P 500 closing price",
        unit="points",
    )

    assert entry.field == "PX_LAST"
    assert entry.description == "S&P 500 closing price"
    assert entry.unit == "points"


def test_catalog_entry_is_frozen() -> None:
    """CatalogEntry should be immutable."""
    from metapyle.catalog import CatalogEntry, Frequency

    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
        frequency=Frequency.QUARTERLY,
    )

    with pytest.raises(AttributeError):
        entry.my_name = "changed"  # type: ignore[misc]


def test_catalog_entry_is_keyword_only() -> None:
    """CatalogEntry must use keyword arguments."""
    from metapyle.catalog import CatalogEntry, Frequency

    # This should work (keyword arguments)
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
        frequency=Frequency.QUARTERLY,
    )
    assert entry.my_name == "GDP_US"

    # Positional arguments should fail
    with pytest.raises(TypeError):
        CatalogEntry(  # type: ignore[misc]
            "GDP_US",
            "bloomberg",
            "GDP CUR$ Index",
            Frequency.QUARTERLY,
        )


def test_catalog_entry_uses_slots() -> None:
    """CatalogEntry should use slots for memory efficiency."""
    from metapyle.catalog import CatalogEntry, Frequency

    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
        frequency=Frequency.QUARTERLY,
    )

    # Slots-based classes don't have __dict__
    assert not hasattr(entry, "__dict__")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.catalog'"

**Step 3: Write minimal implementation**

```python
"""Catalog system for mapping human-readable names to data sources."""

from dataclasses import dataclass
from enum import StrEnum, auto


class Frequency(StrEnum):
    """Supported data frequencies."""

    DAILY = auto()
    WEEKLY = auto()
    MONTHLY = auto()
    QUARTERLY = auto()
    ANNUAL = auto()


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    """
    A single catalog entry mapping a name to a data source.

    Parameters
    ----------
    my_name : str
        Unique human-readable identifier for this data series.
    source : str
        Name of the registered source adapter (e.g., "bloomberg").
    symbol : str
        Source-specific identifier (e.g., "SPX Index").
    frequency : Frequency
        Data frequency (daily, weekly, monthly, quarterly, annual).
    field : str | None, optional
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    description : str | None, optional
        Human-readable description of the data series.
    unit : str | None, optional
        Unit of measurement (e.g., "USD billions", "points").
    """

    my_name: str
    source: str
    symbol: str
    frequency: Frequency
    field: str | None = None
    description: str | None = None
    unit: str | None = None
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: PASS (all 9 tests)

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add CatalogEntry dataclass and Frequency enum"
```

---

### Task 1.4: Catalog YAML Loading

**Files:**
- Modify: `src/metapyle/catalog.py`
- Modify: `tests/unit/test_catalog.py`
- Create: `tests/unit/fixtures/valid_catalog.yaml`
- Create: `tests/unit/fixtures/invalid_catalog.yaml`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
def test_catalog_load_from_yaml(tmp_path) -> None:
    """Catalog can load entries from a YAML file."""
    from metapyle.catalog import Catalog, Frequency

    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  description: US GDP

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  frequency: daily
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))

    assert len(catalog) == 2
    assert "GDP_US" in catalog
    assert "SPX_CLOSE" in catalog

    gdp = catalog.get("GDP_US")
    assert gdp.source == "bloomberg"
    assert gdp.frequency == Frequency.QUARTERLY
    assert gdp.description == "US GDP"

    spx = catalog.get("SPX_CLOSE")
    assert spx.field == "PX_LAST"
    assert spx.frequency == Frequency.DAILY


def test_catalog_load_missing_required_field(tmp_path) -> None:
    """Catalog raises CatalogValidationError for missing required fields."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import CatalogValidationError

    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  # missing symbol and frequency
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="symbol"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_invalid_frequency(tmp_path) -> None:
    """Catalog raises CatalogValidationError for invalid frequency."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import CatalogValidationError

    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: biweekly
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="frequency"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_duplicate_names(tmp_path) -> None:
    """Catalog raises DuplicateNameError for duplicate my_name."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import DuplicateNameError

    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly

- my_name: GDP_US
  source: localfile
  symbol: /data/gdp.csv
  frequency: quarterly
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(DuplicateNameError, match="GDP_US"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_multiple_files(tmp_path) -> None:
    """Catalog can load and merge multiple YAML files."""
    from metapyle.catalog import Catalog

    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
"""
    yaml2 = """
- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  frequency: daily
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    catalog = Catalog.from_yaml([str(file1), str(file2)])

    assert len(catalog) == 2
    assert "GDP_US" in catalog
    assert "SPX_CLOSE" in catalog


def test_catalog_load_duplicate_across_files(tmp_path) -> None:
    """Catalog raises DuplicateNameError for duplicates across files."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import DuplicateNameError

    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
"""
    yaml2 = """
- my_name: GDP_US
  source: localfile
  symbol: /data/gdp.csv
  frequency: quarterly
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    with pytest.raises(DuplicateNameError, match="GDP_US"):
        Catalog.from_yaml([str(file1), str(file2)])


def test_catalog_get_unknown_symbol() -> None:
    """Catalog raises SymbolNotFoundError for unknown symbol."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import SymbolNotFoundError

    catalog = Catalog({})

    with pytest.raises(SymbolNotFoundError, match="UNKNOWN"):
        catalog.get("UNKNOWN")


def test_catalog_list_names(tmp_path) -> None:
    """Catalog can list all entry names."""
    from metapyle.catalog import Catalog

    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  frequency: daily
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))
    names = catalog.list_names()

    assert sorted(names) == ["GDP_US", "SPX_CLOSE"]


def test_catalog_file_not_found() -> None:
    """Catalog raises CatalogValidationError for missing file."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import CatalogValidationError

    with pytest.raises(CatalogValidationError, match="not found"):
        Catalog.from_yaml("/nonexistent/path/catalog.yaml")


def test_catalog_malformed_yaml(tmp_path) -> None:
    """Catalog raises CatalogValidationError for malformed YAML."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import CatalogValidationError

    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  extra_indent:
    - this is wrong
      bad indentation
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="YAML"):
        Catalog.from_yaml(str(yaml_file))
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_catalog_load_from_yaml -v`
Expected: FAIL with "AttributeError: type object 'Catalog' has no attribute 'from_yaml'"

**Step 3: Write minimal implementation**

Add to `src/metapyle/catalog.py`:

```python
"""Catalog system for mapping human-readable names to data sources."""

import logging
from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, Self

import yaml

from metapyle.exceptions import (
    CatalogValidationError,
    DuplicateNameError,
    SymbolNotFoundError,
)

logger = logging.getLogger(__name__)


class Frequency(StrEnum):
    """Supported data frequencies."""

    DAILY = auto()
    WEEKLY = auto()
    MONTHLY = auto()
    QUARTERLY = auto()
    ANNUAL = auto()


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    """
    A single catalog entry mapping a name to a data source.

    Parameters
    ----------
    my_name : str
        Unique human-readable identifier for this data series.
    source : str
        Name of the registered source adapter (e.g., "bloomberg").
    symbol : str
        Source-specific identifier (e.g., "SPX Index").
    frequency : Frequency
        Data frequency (daily, weekly, monthly, quarterly, annual).
    field : str | None, optional
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    description : str | None, optional
        Human-readable description of the data series.
    unit : str | None, optional
        Unit of measurement (e.g., "USD billions", "points").
    """

    my_name: str
    source: str
    symbol: str
    frequency: Frequency
    field: str | None = None
    description: str | None = None
    unit: str | None = None


class Catalog:
    """
    Collection of catalog entries with name-based lookup.

    Parameters
    ----------
    entries : dict[str, CatalogEntry]
        Dictionary mapping my_name to CatalogEntry.
    """

    def __init__(self, entries: dict[str, CatalogEntry]) -> None:
        self._entries = entries

    @classmethod
    def from_yaml(cls, paths: str | list[str]) -> Self:
        """
        Load catalog entries from one or more YAML files.

        Parameters
        ----------
        paths : str | list[str]
            Path or list of paths to YAML catalog files.

        Returns
        -------
        Catalog
            Catalog instance with loaded entries.

        Raises
        ------
        CatalogValidationError
            If file not found, YAML malformed, or entries invalid.
        DuplicateNameError
            If the same my_name appears in multiple entries.
        """
        if isinstance(paths, str):
            paths = [paths]

        entries: dict[str, CatalogEntry] = {}

        for path in paths:
            file_path = Path(path)

            if not file_path.exists():
                raise CatalogValidationError(f"Catalog file not found: {path}")

            logger.info("loading_catalog: path=%s", path)

            try:
                with open(file_path) as f:
                    raw_entries = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise CatalogValidationError(
                    f"Malformed YAML in {path}: {e}"
                ) from e

            if not isinstance(raw_entries, list):
                raise CatalogValidationError(
                    f"Catalog file {path} must contain a list of entries"
                )

            for raw in raw_entries:
                entry = cls._parse_entry(raw, path)

                if entry.my_name in entries:
                    raise DuplicateNameError(
                        f"Duplicate catalog name: {entry.my_name}"
                    )

                entries[entry.my_name] = entry

        logger.info("catalog_loaded: entries=%d", len(entries))
        return cls(entries)

    @staticmethod
    def _parse_entry(raw: dict[str, Any], source_file: str) -> CatalogEntry:
        """
        Parse a raw dictionary into a CatalogEntry.

        Parameters
        ----------
        raw : dict[str, Any]
            Raw dictionary from YAML.
        source_file : str
            Path to source file (for error messages).

        Returns
        -------
        CatalogEntry
            Validated catalog entry.

        Raises
        ------
        CatalogValidationError
            If required fields missing or values invalid.
        """
        required_fields = ["my_name", "source", "symbol", "frequency"]

        for field in required_fields:
            if field not in raw:
                raise CatalogValidationError(
                    f"Missing required field '{field}' in {source_file}"
                )

        freq_str = raw["frequency"].lower()
        try:
            frequency = Frequency(freq_str)
        except ValueError as e:
            valid = ", ".join(f.value for f in Frequency)
            raise CatalogValidationError(
                f"Invalid frequency '{freq_str}' in {source_file}. "
                f"Valid values: {valid}"
            ) from e

        return CatalogEntry(
            my_name=raw["my_name"],
            source=raw["source"],
            symbol=raw["symbol"],
            frequency=frequency,
            field=raw.get("field"),
            description=raw.get("description"),
            unit=raw.get("unit"),
        )

    def get(self, name: str) -> CatalogEntry:
        """
        Get a catalog entry by name.

        Parameters
        ----------
        name : str
            The my_name of the catalog entry.

        Returns
        -------
        CatalogEntry
            The catalog entry.

        Raises
        ------
        SymbolNotFoundError
            If no entry with the given name exists.
        """
        if name not in self._entries:
            raise SymbolNotFoundError(
                f"Symbol not found in catalog: {name}. "
                f"Available: {', '.join(sorted(self._entries.keys())[:5])}"
                + ("..." if len(self._entries) > 5 else "")
            )
        return self._entries[name]

    def list_names(self) -> list[str]:
        """
        List all catalog entry names.

        Returns
        -------
        list[str]
            List of all my_name values in the catalog.
        """
        return list(self._entries.keys())

    def __len__(self) -> int:
        """Return the number of entries in the catalog."""
        return len(self._entries)

    def __contains__(self, name: str) -> bool:
        """Check if a name exists in the catalog."""
        return name in self._entries
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: PASS (all 19 tests)

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add Catalog class with YAML loading"
```

---

### Task 1.5: Cache Class with SQLite Storage

**Files:**
- Create: `src/metapyle/cache.py`
- Test: `tests/unit/test_cache.py`

**Step 1: Write the failing test**

```python
"""Unit tests for Cache with SQLite storage."""

import pandas as pd
import pytest


@pytest.fixture
def cache(tmp_path):
    """Create a cache instance with temporary database."""
    from metapyle.cache import Cache

    db_path = tmp_path / "test_cache.db"
    return Cache(str(db_path))


def test_cache_initializes_database(tmp_path) -> None:
    """Cache creates database file on initialization."""
    from metapyle.cache import Cache

    db_path = tmp_path / "cache.db"
    assert not db_path.exists()

    Cache(str(db_path))

    assert db_path.exists()


def test_cache_put_and_get(cache) -> None:
    """Cache can store and retrieve data."""
    df = pd.DataFrame(
        {"value": [100.0, 101.0, 102.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    )

    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-03",
        data=df,
    )

    result = cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert result is not None
    pd.testing.assert_frame_equal(result, df)


def test_cache_get_returns_none_for_miss(cache) -> None:
    """Cache returns None when data not found."""
    result = cache.get(
        source="bloomberg",
        symbol="UNKNOWN",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert result is None


def test_cache_get_subset_of_cached_range(cache) -> None:
    """Cache returns data when requested range is subset of cached range."""
    df = pd.DataFrame(
        {"value": [100.0, 101.0, 102.0, 103.0, 104.0]},
        index=pd.to_datetime([
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ]),
    )

    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-05",
        data=df,
    )

    # Request subset of cached range
    result = cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-02",
        end_date="2024-01-04",
    )

    assert result is not None
    expected = df.loc["2024-01-02":"2024-01-04"]
    pd.testing.assert_frame_equal(result, expected)


def test_cache_miss_when_range_exceeds_cached(cache) -> None:
    """Cache returns None when requested range exceeds cached range."""
    df = pd.DataFrame(
        {"value": [100.0, 101.0, 102.0]},
        index=pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
    )

    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-02",
        end_date="2024-01-04",
        data=df,
    )

    # Request starts before cached range
    result = cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert result is None


def test_cache_null_field(cache) -> None:
    """Cache handles null field correctly."""
    df = pd.DataFrame(
        {"value": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    cache.put(
        source="localfile",
        symbol="/data/file.csv",
        field=None,
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )

    result = cache.get(
        source="localfile",
        symbol="/data/file.csv",
        field=None,
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert result is not None
    pd.testing.assert_frame_equal(result, df)


def test_cache_clear_all(cache) -> None:
    """Cache can clear all entries."""
    df = pd.DataFrame(
        {"value": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )
    cache.put(
        source="bloomberg",
        symbol="VIX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )

    cache.clear()

    assert cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
    ) is None
    assert cache.get(
        source="bloomberg",
        symbol="VIX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
    ) is None


def test_cache_clear_specific_symbol(cache) -> None:
    """Cache can clear entries for a specific symbol."""
    df = pd.DataFrame(
        {"value": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )
    cache.put(
        source="bloomberg",
        symbol="VIX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )

    cache.clear(source="bloomberg", symbol="SPX Index")

    # SPX should be cleared
    assert cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
    ) is None

    # VIX should still exist
    assert cache.get(
        source="bloomberg",
        symbol="VIX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
    ) is not None


def test_cache_put_overwrites_existing(cache) -> None:
    """Cache put overwrites existing entry for same key."""
    df1 = pd.DataFrame(
        {"value": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )
    df2 = pd.DataFrame(
        {"value": [200.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df1,
    )
    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df2,
    )

    result = cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    pd.testing.assert_frame_equal(result, df2)


def test_cache_disabled() -> None:
    """Cache with enabled=False does nothing."""
    from metapyle.cache import Cache

    cache = Cache(enabled=False)

    df = pd.DataFrame(
        {"value": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    # Put should not raise
    cache.put(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )

    # Get should return None
    result = cache.get(
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert result is None


def test_cache_default_path_from_env(tmp_path, monkeypatch) -> None:
    """Cache uses METAPYLE_CACHE_PATH environment variable."""
    from metapyle.cache import Cache

    env_path = tmp_path / "env_cache.db"
    monkeypatch.setenv("METAPYLE_CACHE_PATH", str(env_path))

    cache = Cache()

    # Should create database at env path
    df = pd.DataFrame(
        {"value": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )
    cache.put(
        source="test",
        symbol="test",
        field=None,
        start_date="2024-01-01",
        end_date="2024-01-01",
        data=df,
    )

    assert env_path.exists()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cache.py::test_cache_initializes_database -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.cache'"

**Step 3: Write minimal implementation**

```python
"""SQLite-based cache for time-series data."""

import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CACHE_PATH = "./cache/data_cache.db"


class Cache:
    """
    SQLite-based cache for time-series data.

    Caches fetched data to reduce API calls. Cache keys are
    (source, symbol, field, start_date, end_date). Cache hit occurs
    when requested range is subset of cached range.

    Parameters
    ----------
    path : str | None, optional
        Path to SQLite database file. If None, uses METAPYLE_CACHE_PATH
        environment variable or default path.
    enabled : bool, optional
        If False, cache operations are no-ops. Default True.
    """

    def __init__(
        self,
        path: str | None = None,
        *,
        enabled: bool = True,
    ) -> None:
        self._enabled = enabled

        if not enabled:
            self._conn: sqlite3.Connection | None = None
            return

        if path is None:
            path = os.environ.get("METAPYLE_CACHE_PATH", DEFAULT_CACHE_PATH)

        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        try:
            self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
            self._init_schema()
            logger.info("cache_initialized: path=%s", self._path)
        except sqlite3.Error as e:
            logger.warning("cache_init_failed: error=%s", e)
            self._conn = None
            self._enabled = False

    def _init_schema(self) -> None:
        """Create database schema if not exists."""
        if self._conn is None:
            return

        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS cache_entries (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                symbol TEXT NOT NULL,
                field TEXT,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                cached_at TEXT NOT NULL,
                UNIQUE(source, symbol, field, start_date, end_date)
            );

            CREATE TABLE IF NOT EXISTS cache_data (
                entry_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                value REAL NOT NULL,
                FOREIGN KEY (entry_id) REFERENCES cache_entries(id) ON DELETE CASCADE,
                PRIMARY KEY (entry_id, date)
            );

            CREATE INDEX IF NOT EXISTS idx_cache_lookup
            ON cache_entries(source, symbol, field);
        """)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.commit()

    def put(
        self,
        source: str,
        symbol: str,
        field: str | None,
        start_date: str,
        end_date: str,
        data: pd.DataFrame,
    ) -> None:
        """
        Store data in cache.

        Parameters
        ----------
        source : str
            Source adapter name.
        symbol : str
            Source-specific symbol.
        field : str | None
            Source-specific field (None for sources without fields).
        start_date : str
            Start date in ISO format.
        end_date : str
            End date in ISO format.
        data : pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.
        """
        if not self._enabled or self._conn is None:
            return

        try:
            cursor = self._conn.cursor()

            # Delete existing entry with same key
            cursor.execute(
                """
                DELETE FROM cache_entries
                WHERE source = ? AND symbol = ? AND field IS ? 
                    AND start_date = ? AND end_date = ?
                """,
                (source, symbol, field, start_date, end_date),
            )

            # Insert new entry
            cursor.execute(
                """
                INSERT INTO cache_entries (source, symbol, field, start_date, end_date, cached_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source, symbol, field, start_date, end_date, datetime.now().isoformat()),
            )
            entry_id = cursor.lastrowid

            # Insert data rows
            for date_idx, row in data.iterrows():
                cursor.execute(
                    """
                    INSERT INTO cache_data (entry_id, date, value)
                    VALUES (?, ?, ?)
                    """,
                    (entry_id, date_idx.strftime("%Y-%m-%d"), float(row["value"])),
                )

            self._conn.commit()
            logger.debug(
                "cache_put: source=%s, symbol=%s, rows=%d",
                source,
                symbol,
                len(data),
            )

        except sqlite3.Error as e:
            logger.warning("cache_put_failed: error=%s", e)

    def get(
        self,
        source: str,
        symbol: str,
        field: str | None,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame | None:
        """
        Retrieve data from cache.

        Returns data if requested range is subset of a cached range.

        Parameters
        ----------
        source : str
            Source adapter name.
        symbol : str
            Source-specific symbol.
        field : str | None
            Source-specific field.
        start_date : str
            Start date in ISO format.
        end_date : str
            End date in ISO format.

        Returns
        -------
        pd.DataFrame | None
            Cached data if found, None otherwise.
        """
        if not self._enabled or self._conn is None:
            return None

        try:
            cursor = self._conn.cursor()

            # Find cache entry that covers the requested range
            cursor.execute(
                """
                SELECT id, start_date, end_date
                FROM cache_entries
                WHERE source = ? AND symbol = ? AND field IS ?
                    AND start_date <= ? AND end_date >= ?
                """,
                (source, symbol, field, start_date, end_date),
            )
            row = cursor.fetchone()

            if row is None:
                logger.debug(
                    "cache_miss: source=%s, symbol=%s, start=%s, end=%s",
                    source,
                    symbol,
                    start_date,
                    end_date,
                )
                return None

            entry_id = row[0]

            # Fetch data within requested range
            cursor.execute(
                """
                SELECT date, value
                FROM cache_data
                WHERE entry_id = ? AND date >= ? AND date <= ?
                ORDER BY date
                """,
                (entry_id, start_date, end_date),
            )
            rows = cursor.fetchall()

            if not rows:
                return None

            dates = [pd.to_datetime(r[0]) for r in rows]
            values = [r[1] for r in rows]

            df = pd.DataFrame({"value": values}, index=pd.DatetimeIndex(dates))
            logger.debug(
                "cache_hit: source=%s, symbol=%s, rows=%d",
                source,
                symbol,
                len(df),
            )
            return df

        except sqlite3.Error as e:
            logger.warning("cache_get_failed: error=%s", e)
            return None

    def clear(
        self,
        source: str | None = None,
        symbol: str | None = None,
    ) -> None:
        """
        Clear cache entries.

        Parameters
        ----------
        source : str | None, optional
            If provided with symbol, clear only that source/symbol combination.
        symbol : str | None, optional
            If provided with source, clear only that source/symbol combination.
            If both are None, clear entire cache.
        """
        if not self._enabled or self._conn is None:
            return

        try:
            if source is not None and symbol is not None:
                self._conn.execute(
                    "DELETE FROM cache_entries WHERE source = ? AND symbol = ?",
                    (source, symbol),
                )
                logger.info("cache_cleared: source=%s, symbol=%s", source, symbol)
            else:
                self._conn.execute("DELETE FROM cache_entries")
                logger.info("cache_cleared: all entries")

            self._conn.commit()

        except sqlite3.Error as e:
            logger.warning("cache_clear_failed: error=%s", e)

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_cache.py -v`
Expected: PASS (all 12 tests)

**Step 5: Commit**

```bash
git add src/metapyle/cache.py tests/unit/test_cache.py
git commit -m "feat(cache): add Cache class with SQLite storage"
```

---

### Task 1.6: Basic Client with get() Method

**Files:**
- Create: `src/metapyle/client.py`
- Test: `tests/unit/test_client.py`

**Step 1: Write the failing test**

```python
"""Unit tests for Client."""

import pandas as pd
import pytest


@pytest.fixture
def mock_catalog(tmp_path):
    """Create a test catalog YAML file."""
    yaml_content = """
- my_name: GDP_US
  source: mock_source
  symbol: GDP_SYMBOL
  frequency: quarterly
  description: US GDP

- my_name: SPX_CLOSE
  source: mock_source
  symbol: SPX_SYMBOL
  field: PX_LAST
  frequency: daily
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)
    return str(yaml_file)


@pytest.fixture
def mock_source():
    """Register a mock source adapter."""
    from metapyle.sources import BaseSource, register_source

    @register_source("mock_source")
    class MockSource(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            # Return simple data based on symbol
            if symbol == "GDP_SYMBOL":
                return pd.DataFrame(
                    {"value": [100.0, 101.0]},
                    index=pd.to_datetime(["2024-01-01", "2024-04-01"]),
                )
            elif symbol == "SPX_SYMBOL":
                return pd.DataFrame(
                    {"value": [5000.0, 5001.0, 5002.0]},
                    index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                )
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {"symbol": symbol}

    return MockSource


def test_client_initialization(mock_catalog, mock_source) -> None:
    """Client can be initialized with a catalog path."""
    from metapyle import Client

    client = Client(catalog=mock_catalog)
    assert client is not None


def test_client_get_single_symbol(mock_catalog, mock_source) -> None:
    """Client can fetch a single symbol."""
    from metapyle import Client

    client = Client(catalog=mock_catalog, cache_enabled=False)
    df = client.get(["GDP_US"], start="2024-01-01", end="2024-12-31")

    assert isinstance(df, pd.DataFrame)
    assert "GDP_US" in df.columns
    assert len(df) == 2


def test_client_get_multiple_symbols_same_frequency(mock_catalog, mock_source) -> None:
    """Client can fetch multiple symbols with same frequency."""
    from metapyle.sources import BaseSource, register_source
    from metapyle.sources.base import _global_registry

    # Register second mock source for same frequency test
    @register_source("mock_source2")
    class MockSource2(BaseSource):
        def fetch(
            self, symbol: str, start: str, end: str, **kwargs
        ) -> pd.DataFrame:
            return pd.DataFrame(
                {"value": [200.0, 201.0]},
                index=pd.to_datetime(["2024-01-01", "2024-04-01"]),
            )

        def get_metadata(self, symbol: str) -> dict:
            return {}

    # Create catalog with two quarterly series
    yaml_content = """
- my_name: GDP_US
  source: mock_source
  symbol: GDP_SYMBOL
  frequency: quarterly

- my_name: GDP_EU
  source: mock_source2
  symbol: GDP_EU_SYMBOL
  frequency: quarterly
"""
    from pathlib import Path
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        catalog_path = f.name

    from metapyle import Client

    client = Client(catalog=catalog_path, cache_enabled=False)
    df = client.get(["GDP_US", "GDP_EU"], start="2024-01-01", end="2024-12-31")

    assert "GDP_US" in df.columns
    assert "GDP_EU" in df.columns

    Path(catalog_path).unlink()


def test_client_get_frequency_mismatch_raises(mock_catalog, mock_source) -> None:
    """Client raises FrequencyMismatchError for mixed frequencies."""
    from metapyle import Client
    from metapyle.exceptions import FrequencyMismatchError

    client = Client(catalog=mock_catalog, cache_enabled=False)

    with pytest.raises(FrequencyMismatchError, match="frequency"):
        client.get(["GDP_US", "SPX_CLOSE"], start="2024-01-01", end="2024-12-31")


def test_client_get_unknown_symbol_raises(mock_catalog, mock_source) -> None:
    """Client raises SymbolNotFoundError for unknown symbol."""
    from metapyle import Client
    from metapyle.exceptions import SymbolNotFoundError

    client = Client(catalog=mock_catalog, cache_enabled=False)

    with pytest.raises(SymbolNotFoundError, match="UNKNOWN"):
        client.get(["UNKNOWN"], start="2024-01-01", end="2024-12-31")


def test_client_uses_cache(mock_catalog, mock_source, tmp_path) -> None:
    """Client uses cache for repeated queries."""
    from metapyle import Client

    cache_path = tmp_path / "cache.db"
    client = Client(catalog=mock_catalog, cache_path=str(cache_path))

    # First fetch - cache miss
    df1 = client.get(["GDP_US"], start="2024-01-01", end="2024-12-31")

    # Second fetch - should hit cache
    df2 = client.get(["GDP_US"], start="2024-01-01", end="2024-12-31")

    pd.testing.assert_frame_equal(df1, df2)
    assert cache_path.exists()


def test_client_bypass_cache(mock_catalog, mock_source, tmp_path) -> None:
    """Client can bypass cache with use_cache=False."""
    from metapyle import Client

    cache_path = tmp_path / "cache.db"
    client = Client(catalog=mock_catalog, cache_path=str(cache_path))

    # First fetch - populates cache
    client.get(["GDP_US"], start="2024-01-01", end="2024-12-31")

    # Second fetch with use_cache=False - bypasses cache
    df = client.get(
        ["GDP_US"], start="2024-01-01", end="2024-12-31", use_cache=False
    )

    assert isinstance(df, pd.DataFrame)


def test_client_clear_cache(mock_catalog, mock_source, tmp_path) -> None:
    """Client can clear cache."""
    from metapyle import Client

    cache_path = tmp_path / "cache.db"
    client = Client(catalog=mock_catalog, cache_path=str(cache_path))

    # Populate cache
    client.get(["GDP_US"], start="2024-01-01", end="2024-12-31")

    # Clear cache
    client.clear_cache()

    # Cache should be empty but database file still exists
    assert cache_path.exists()


def test_client_multiple_catalog_files(tmp_path, mock_source) -> None:
    """Client can load multiple catalog files."""
    from metapyle import Client

    yaml1 = """
- my_name: GDP_US
  source: mock_source
  symbol: GDP_SYMBOL
  frequency: quarterly
"""
    yaml2 = """
- my_name: CPI_US
  source: mock_source
  symbol: CPI_SYMBOL
  frequency: monthly
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    client = Client(catalog=[str(file1), str(file2)], cache_enabled=False)

    # Should be able to access symbols from both catalogs
    df = client.get(["GDP_US"], start="2024-01-01", end="2024-12-31")
    assert "GDP_US" in df.columns
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::test_client_initialization -v`
Expected: FAIL with "ImportError: cannot import name 'Client' from 'metapyle'"

**Step 3: Write minimal implementation**

```python
"""Client for querying financial time-series data."""

import logging
from typing import Any

import pandas as pd

from metapyle.cache import Cache
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.exceptions import FrequencyMismatchError
from metapyle.sources.base import _global_registry

logger = logging.getLogger(__name__)


class Client:
    """
    Main client for querying financial time-series data.

    Parameters
    ----------
    catalog : str | list[str]
        Path or list of paths to YAML catalog files.
    cache_path : str | None, optional
        Path to SQLite cache database. If None, uses default or env var.
    cache_enabled : bool, optional
        If False, disables caching entirely. Default True.
    """

    def __init__(
        self,
        catalog: str | list[str],
        *,
        cache_path: str | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self._catalog = Catalog.from_yaml(catalog)
        self._cache = Cache(path=cache_path, enabled=cache_enabled)
        self._registry = _global_registry

        logger.info(
            "client_initialized: catalog_entries=%d, cache_enabled=%s",
            len(self._catalog),
            cache_enabled,
        )

    def get(
        self,
        symbols: list[str],
        start: str,
        end: str,
        *,
        frequency: str | None = None,
        use_cache: bool = True,
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
        use_cache : bool, optional
            If False, bypass cache and fetch fresh data. Default True.

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
        """
        # Resolve catalog entries
        entries = [self._catalog.get(name) for name in symbols]

        # Check frequency compatibility (if no alignment requested)
        if frequency is None:
            self._check_frequency_compatibility(entries)

        # Fetch each symbol
        dfs: dict[str, pd.DataFrame] = {}
        for entry in entries:
            df = self._fetch_symbol(entry, start, end, use_cache)
            dfs[entry.my_name] = df

        # Assemble into wide DataFrame
        result = self._assemble_dataframe(dfs)

        logger.info(
            "get_complete: symbols=%d, rows=%d, start=%s, end=%s",
            len(symbols),
            len(result),
            start,
            end,
        )

        return result

    def _check_frequency_compatibility(self, entries: list[CatalogEntry]) -> None:
        """
        Check that all entries have the same frequency.

        Raises
        ------
        FrequencyMismatchError
            If entries have different frequencies.
        """
        if len(entries) <= 1:
            return

        frequencies = {entry.frequency for entry in entries}
        if len(frequencies) > 1:
            freq_details = "\n".join(
                f"  - {entry.my_name}: {entry.frequency}" for entry in entries
            )
            raise FrequencyMismatchError(
                f"Cannot mix frequencies in single query.\n{freq_details}\n"
                f"Solution: Specify frequency parameter: "
                f"client.get([...], frequency='daily')"
            )

    def _fetch_symbol(
        self,
        entry: CatalogEntry,
        start: str,
        end: str,
        use_cache: bool,
    ) -> pd.DataFrame:
        """
        Fetch data for a single catalog entry.

        Parameters
        ----------
        entry : CatalogEntry
            Catalog entry to fetch.
        start : str
            Start date in ISO format.
        end : str
            End date in ISO format.
        use_cache : bool
            Whether to use cache.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.
        """
        # Try cache first
        if use_cache:
            cached = self._cache.get(
                source=entry.source,
                symbol=entry.symbol,
                field=entry.field,
                start_date=start,
                end_date=end,
            )
            if cached is not None:
                logger.debug("fetch_from_cache: symbol=%s", entry.my_name)
                return cached

        # Fetch from source
        source = self._registry.get(entry.source)
        kwargs: dict[str, Any] = {}
        if entry.field is not None:
            kwargs["field"] = entry.field

        logger.debug("fetch_from_source: symbol=%s, source=%s", entry.my_name, entry.source)
        df = source.fetch(entry.symbol, start, end, **kwargs)

        # Store in cache
        self._cache.put(
            source=entry.source,
            symbol=entry.symbol,
            field=entry.field,
            start_date=start,
            end_date=end,
            data=df,
        )

        return df

    def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Assemble multiple single-column DataFrames into wide format.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping names to DataFrames with 'value' column.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by dictionary keys.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename 'value' column to symbol name
        renamed = {name: df.rename(columns={"value": name}) for name, df in dfs.items()}

        # Merge all DataFrames on index (outer join)
        result = pd.concat(renamed.values(), axis=1, join="outer")

        return result

    def clear_cache(self, *, symbol: str | None = None) -> None:
        """
        Clear cache entries.

        Parameters
        ----------
        symbol : str | None, optional
            If provided, clear only entries for this catalog symbol.
            If None, clear entire cache.
        """
        if symbol is not None:
            entry = self._catalog.get(symbol)
            self._cache.clear(source=entry.source, symbol=entry.symbol)
        else:
            self._cache.clear()
```

**Step 4: Update `__init__.py` to export Client**

Update `src/metapyle/__init__.py`:

```python
"""Metapyle - Unified interface for financial time-series data."""

from metapyle.client import Client
from metapyle.exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
    FrequencyMismatchError,
    MetapyleError,
    NoDataError,
    SymbolNotFoundError,
    UnknownSourceError,
)
from metapyle.sources import BaseSource, register_source

__all__ = [
    "Client",
    "BaseSource",
    "register_source",
    "MetapyleError",
    "CatalogError",
    "CatalogValidationError",
    "DuplicateNameError",
    "FetchError",
    "FrequencyMismatchError",
    "NoDataError",
    "SymbolNotFoundError",
    "UnknownSourceError",
]
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py -v`
Expected: PASS (all 10 tests)

**Step 6: Commit**

```bash
git add src/metapyle/client.py src/metapyle/__init__.py tests/unit/test_client.py
git commit -m "feat(client): add Client class with get() method"
```

---

## Phase 1 Complete

At this point, the core foundation is complete:
- ✅ Exception hierarchy
- ✅ BaseSource ABC and SourceRegistry
- ✅ CatalogEntry dataclass and Frequency enum
- ✅ Catalog YAML loading
- ✅ Cache with SQLite storage
- ✅ Basic Client with `get()` method

---

## Phase 2: Adapters

### Task 2.1: LocalFileSource Implementation

**Files:**
- Create: `src/metapyle/sources/localfile.py`
- Modify: `src/metapyle/sources/__init__.py`
- Test: `tests/unit/test_sources_localfile.py`
- Create: `tests/integration/fixtures/test_data.csv`
- Create: `tests/integration/fixtures/test_data.parquet`

**Step 1: Create test fixtures**

Create `tests/integration/fixtures/test_data.csv`:
```csv
date,value
2024-01-01,100.0
2024-01-02,101.5
2024-01-03,102.0
2024-01-04,103.5
2024-01-05,104.0
```

Create `tests/integration/fixtures/test_data.parquet` (via Python):
```python
import pandas as pd
df = pd.DataFrame({
    "value": [100.0, 101.5, 102.0, 103.5, 104.0]
}, index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]))
df.to_parquet("tests/integration/fixtures/test_data.parquet")
```

**Step 2: Write the failing test**

```python
"""Unit tests for LocalFileSource adapter."""

import pandas as pd
import pytest


@pytest.fixture
def csv_file(tmp_path):
    """Create a test CSV file."""
    csv_content = """date,value
2024-01-01,100.0
2024-01-02,101.5
2024-01-03,102.0
"""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(csv_content)
    return str(csv_path)


@pytest.fixture
def parquet_file(tmp_path):
    """Create a test Parquet file."""
    df = pd.DataFrame(
        {"value": [100.0, 101.5, 102.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    )
    parquet_path = tmp_path / "test.parquet"
    df.to_parquet(parquet_path)
    return str(parquet_path)


def test_localfile_source_fetch_csv(csv_file) -> None:
    """LocalFileSource can fetch from CSV files."""
    from metapyle.sources.localfile import LocalFileSource

    source = LocalFileSource()
    df = source.fetch(csv_file, start="2024-01-01", end="2024-01-03")

    assert isinstance(df, pd.DataFrame)
    assert "value" in df.columns
    assert len(df) == 3
    assert df.iloc[0]["value"] == 100.0


def test_localfile_source_fetch_parquet(parquet_file) -> None:
    """LocalFileSource can fetch from Parquet files."""
    from metapyle.sources.localfile import LocalFileSource

    source = LocalFileSource()
    df = source.fetch(parquet_file, start="2024-01-01", end="2024-01-03")

    assert isinstance(df, pd.DataFrame)
    assert "value" in df.columns
    assert len(df) == 3


def test_localfile_source_date_filtering(csv_file) -> None:
    """LocalFileSource filters by date range."""
    from metapyle.sources.localfile import LocalFileSource

    source = LocalFileSource()
    df = source.fetch(csv_file, start="2024-01-02", end="2024-01-02")

    assert len(df) == 1
    assert df.iloc[0]["value"] == 101.5


def test_localfile_source_file_not_found() -> None:
    """LocalFileSource raises FetchError for missing file."""
    from metapyle.exceptions import FetchError
    from metapyle.sources.localfile import LocalFileSource

    source = LocalFileSource()

    with pytest.raises(FetchError, match="not found"):
        source.fetch("/nonexistent/file.csv", start="2024-01-01", end="2024-01-03")


def test_localfile_source_empty_file(tmp_path) -> None:
    """LocalFileSource raises NoDataError for empty file."""
    from metapyle.exceptions import NoDataError
    from metapyle.sources.localfile import LocalFileSource

    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("date,value\n")

    source = LocalFileSource()

    with pytest.raises(NoDataError):
        source.fetch(str(csv_path), start="2024-01-01", end="2024-01-03")


def test_localfile_source_get_metadata(csv_file) -> None:
    """LocalFileSource returns basic metadata."""
    from metapyle.sources.localfile import LocalFileSource

    source = LocalFileSource()
    meta = source.get_metadata(csv_file)

    assert isinstance(meta, dict)
    assert "path" in meta


def test_localfile_source_is_registered() -> None:
    """LocalFileSource is registered as 'localfile'."""
    from metapyle.sources.base import _global_registry

    source = _global_registry.get("localfile")
    assert source is not None
```

**Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_localfile.py::test_localfile_source_fetch_csv -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.sources.localfile'"

**Step 4: Write minimal implementation**

```python
"""LocalFile source adapter for CSV and Parquet files."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)


@register_source("localfile")
class LocalFileSource(BaseSource):
    """
    Source adapter for local CSV and Parquet files.

    The symbol parameter is interpreted as the file path.
    Files must have a datetime index and a 'value' column
    (or a single data column that will be renamed to 'value').
    """

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch data from a local file.

        Parameters
        ----------
        symbol : str
            Path to the CSV or Parquet file.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        **kwargs : Any
            Ignored.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.

        Raises
        ------
        FetchError
            If file not found or cannot be read.
        NoDataError
            If file is empty or no data in date range.
        """
        path = Path(symbol)

        if not path.exists():
            raise FetchError(f"File not found: {symbol}")

        logger.debug("fetch_localfile: path=%s, start=%s, end=%s", symbol, start, end)

        try:
            if path.suffix.lower() == ".parquet":
                df = pd.read_parquet(path)
            else:
                # Assume CSV
                df = pd.read_csv(path, index_col=0, parse_dates=True)
        except Exception as e:
            raise FetchError(f"Failed to read file {symbol}: {e}") from e

        if df.empty:
            raise NoDataError(f"No data in file: {symbol}")

        # Ensure we have a 'value' column
        if "value" not in df.columns:
            if len(df.columns) == 1:
                df = df.rename(columns={df.columns[0]: "value"})
            else:
                raise FetchError(
                    f"File {symbol} has multiple columns but no 'value' column. "
                    f"Columns: {list(df.columns)}"
                )

        # Filter by date range
        df = df.loc[start:end]

        if df.empty:
            raise NoDataError(
                f"No data in date range {start} to {end} for file: {symbol}"
            )

        # Ensure single column output
        result = df[["value"]]

        logger.debug("fetch_localfile_complete: path=%s, rows=%d", symbol, len(result))

        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Get metadata for a local file.

        Parameters
        ----------
        symbol : str
            Path to the file.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary with path info.
        """
        path = Path(symbol)
        return {
            "path": str(path.absolute()),
            "exists": path.exists(),
            "suffix": path.suffix,
        }
```

**Step 5: Update sources __init__.py**

```python
"""Source adapters for metapyle.

This module provides the base interface for data sources and a registry
for managing source adapters.
"""

from metapyle.sources.base import BaseSource, SourceRegistry, register_source

# Import adapters to register them
from metapyle.sources import localfile  # noqa: F401

__all__ = ["BaseSource", "SourceRegistry", "register_source"]
```

**Step 6: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_localfile.py -v`
Expected: PASS (all 7 tests)

**Step 7: Commit**

```bash
git add src/metapyle/sources/localfile.py src/metapyle/sources/__init__.py tests/unit/test_sources_localfile.py
git commit -m "feat(sources): add LocalFileSource adapter"
```

---

### Task 2.2: BloombergSource Implementation

**Files:**
- Create: `src/metapyle/sources/bloomberg.py`
- Modify: `src/metapyle/sources/__init__.py`
- Test: `tests/unit/test_sources_bloomberg.py`
- Test: `tests/integration/test_bloomberg_adapter.py`

**Step 1: Write the failing unit test (mocked)**

```python
"""Unit tests for BloombergSource adapter (mocked)."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


def test_bloomberg_source_fetch_single_ticker() -> None:
    """BloombergSource can fetch data for a single ticker."""
    from metapyle.sources.bloomberg import BloombergSource

    # Mock xbbg.blp.bdh response
    mock_df = pd.DataFrame(
        {("SPX Index", "PX_LAST"): [5000.0, 5001.0, 5002.0]},
        index=pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
    )
    mock_df.columns = pd.MultiIndex.from_tuples([("SPX Index", "PX_LAST")])

    with patch("metapyle.sources.bloomberg.blp") as mock_blp:
        mock_blp.bdh.return_value = mock_df

        source = BloombergSource()
        df = source.fetch("SPX Index", start="2024-01-02", end="2024-01-04")

        assert isinstance(df, pd.DataFrame)
        assert "value" in df.columns
        assert len(df) == 3
        assert df.iloc[0]["value"] == 5000.0

        mock_blp.bdh.assert_called_once_with(
            "SPX Index", "PX_LAST", "2024-01-02", "2024-01-04"
        )


def test_bloomberg_source_fetch_custom_field() -> None:
    """BloombergSource can fetch with custom field."""
    from metapyle.sources.bloomberg import BloombergSource

    mock_df = pd.DataFrame(
        {("SPX Index", "PX_OPEN"): [4999.0, 5000.0]},
        index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
    )
    mock_df.columns = pd.MultiIndex.from_tuples([("SPX Index", "PX_OPEN")])

    with patch("metapyle.sources.bloomberg.blp") as mock_blp:
        mock_blp.bdh.return_value = mock_df

        source = BloombergSource()
        df = source.fetch(
            "SPX Index", start="2024-01-02", end="2024-01-03", field="PX_OPEN"
        )

        assert df.iloc[0]["value"] == 4999.0
        mock_blp.bdh.assert_called_once_with(
            "SPX Index", "PX_OPEN", "2024-01-02", "2024-01-03"
        )


def test_bloomberg_source_empty_response_raises() -> None:
    """BloombergSource raises NoDataError for empty response."""
    from metapyle.exceptions import NoDataError
    from metapyle.sources.bloomberg import BloombergSource

    with patch("metapyle.sources.bloomberg.blp") as mock_blp:
        mock_blp.bdh.return_value = pd.DataFrame()

        source = BloombergSource()

        with pytest.raises(NoDataError, match="No data"):
            source.fetch("INVALID Index", start="2024-01-02", end="2024-01-04")


def test_bloomberg_source_api_error_raises() -> None:
    """BloombergSource raises FetchError for API errors."""
    from metapyle.exceptions import FetchError
    from metapyle.sources.bloomberg import BloombergSource

    with patch("metapyle.sources.bloomberg.blp") as mock_blp:
        mock_blp.bdh.side_effect = Exception("Bloomberg API error")

        source = BloombergSource()

        with pytest.raises(FetchError, match="Bloomberg"):
            source.fetch("SPX Index", start="2024-01-02", end="2024-01-04")


def test_bloomberg_source_get_metadata() -> None:
    """BloombergSource returns metadata dict."""
    from metapyle.sources.bloomberg import BloombergSource

    source = BloombergSource()
    meta = source.get_metadata("SPX Index")

    assert isinstance(meta, dict)
    assert "symbol" in meta


def test_bloomberg_source_is_registered() -> None:
    """BloombergSource is registered as 'bloomberg'."""
    from metapyle.sources.base import _global_registry

    # Note: May fail if xbbg not installed
    try:
        source = _global_registry.get("bloomberg")
        assert source is not None
    except Exception:
        pytest.skip("Bloomberg source not registered (xbbg not installed)")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_bloomberg.py::test_bloomberg_source_fetch_single_ticker -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.sources.bloomberg'"

**Step 3: Write minimal implementation**

```python
"""Bloomberg source adapter using xbbg."""

import logging
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)

try:
    from xbbg import blp

    _XBBG_AVAILABLE = True
except ImportError:
    _XBBG_AVAILABLE = False
    blp = None  # type: ignore[assignment]


@register_source("bloomberg")
class BloombergSource(BaseSource):
    """
    Source adapter for Bloomberg Terminal via xbbg.

    Requires Bloomberg Terminal to be running and logged in.
    Uses xbbg's blp.bdh() for historical data retrieval.
    """

    def __init__(self) -> None:
        if not _XBBG_AVAILABLE:
            logger.warning(
                "xbbg not installed. Install with: pip install metapyle[bloomberg]"
            )

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        field: str = "PX_LAST",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch historical data from Bloomberg.

        Parameters
        ----------
        symbol : str
            Bloomberg ticker (e.g., "SPX Index").
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        field : str, optional
            Bloomberg field to fetch. Default "PX_LAST".
        **kwargs : Any
            Additional arguments (ignored).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.

        Raises
        ------
        FetchError
            If Bloomberg API call fails.
        NoDataError
            If no data returned for symbol/date range.
        """
        if not _XBBG_AVAILABLE:
            raise FetchError(
                "Bloomberg source requires xbbg. "
                "Install with: pip install metapyle[bloomberg]"
            )

        logger.debug(
            "fetch_bloomberg: symbol=%s, field=%s, start=%s, end=%s",
            symbol,
            field,
            start,
            end,
        )

        try:
            df = blp.bdh(symbol, field, start, end)
        except Exception as e:
            raise FetchError(
                f"Bloomberg API error fetching {symbol}: {e}"
            ) from e

        if df.empty:
            raise NoDataError(
                f"No data returned from Bloomberg for {symbol} "
                f"({field}) from {start} to {end}"
            )

        # blp.bdh returns MultiIndex columns: (ticker, field)
        # Extract the single series and rename to 'value'
        try:
            result = df[(symbol, field)].to_frame(name="value")
        except KeyError as e:
            raise FetchError(
                f"Unexpected Bloomberg response format for {symbol}: {e}"
            ) from e

        logger.debug(
            "fetch_bloomberg_complete: symbol=%s, rows=%d",
            symbol,
            len(result),
        )

        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Get metadata for a Bloomberg symbol.

        Parameters
        ----------
        symbol : str
            Bloomberg ticker.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary.
        """
        return {
            "symbol": symbol,
            "source": "bloomberg",
        }
```

**Step 4: Update sources __init__.py**

```python
"""Source adapters for metapyle.

This module provides the base interface for data sources and a registry
for managing source adapters.
"""

from metapyle.sources.base import BaseSource, SourceRegistry, register_source

# Import adapters to register them
from metapyle.sources import localfile  # noqa: F401
from metapyle.sources import bloomberg  # noqa: F401

__all__ = ["BaseSource", "SourceRegistry", "register_source"]
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_bloomberg.py -v`
Expected: PASS (all 6 tests)

**Step 6: Create integration test (credential-gated)**

Create `tests/integration/test_bloomberg_adapter.py`:

```python
"""Integration tests for Bloomberg adapter.

These tests require Bloomberg Terminal to be running and logged in.
Run with: pytest -m integration
"""

import pandas as pd
import pytest


@pytest.mark.integration
def test_bloomberg_fetch_spx_index() -> None:
    """Fetch real SPX Index data from Bloomberg."""
    from metapyle.sources.bloomberg import BloombergSource

    source = BloombergSource()
    df = source.fetch("SPX Index", start="2024-01-02", end="2024-01-05")

    assert isinstance(df, pd.DataFrame)
    assert "value" in df.columns
    assert len(df) > 0
    assert df.index[0] >= pd.Timestamp("2024-01-02")


@pytest.mark.integration
def test_bloomberg_fetch_with_field() -> None:
    """Fetch SPX with specific field."""
    from metapyle.sources.bloomberg import BloombergSource

    source = BloombergSource()
    df = source.fetch(
        "SPX Index", start="2024-01-02", end="2024-01-05", field="PX_OPEN"
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
```

**Step 7: Commit**

```bash
git add src/metapyle/sources/bloomberg.py src/metapyle/sources/__init__.py tests/unit/test_sources_bloomberg.py tests/integration/test_bloomberg_adapter.py
git commit -m "feat(sources): add BloombergSource adapter"
```

---

## Phase 2 Complete

- ✅ LocalFileSource (CSV/Parquet)
- ✅ BloombergSource (xbbg)
- ✅ Integration tests (credential-gated)

---

## Phase 3: Processing

### Task 3.1: Frequency Alignment Functions

**Files:**
- Create: `src/metapyle/processing.py`
- Test: `tests/unit/test_processing.py`

**Step 1: Write the failing test**

```python
"""Unit tests for frequency alignment and processing functions."""

import pandas as pd
import pytest


def test_align_to_frequency_downsample() -> None:
    """Downsample daily data to monthly (last value)."""
    from metapyle.processing import align_to_frequency

    # Daily data for January
    dates = pd.date_range("2024-01-01", "2024-01-31", freq="D")
    df = pd.DataFrame({"value": range(1, 32)}, index=dates)

    result = align_to_frequency(df, "monthly")

    # Should have 1 row (January)
    assert len(result) == 1
    # Should take last value of month (31)
    assert result.iloc[0]["value"] == 31


def test_align_to_frequency_upsample() -> None:
    """Upsample monthly data to daily (forward fill)."""
    from metapyle.processing import align_to_frequency

    # Monthly data
    dates = pd.to_datetime(["2024-01-31", "2024-02-29"])
    df = pd.DataFrame({"value": [100.0, 200.0]}, index=dates)

    result = align_to_frequency(df, "daily")

    # Should have daily frequency
    assert len(result) > 2
    # First value should be forward-filled
    assert result.iloc[0]["value"] == 100.0


def test_align_to_frequency_no_change() -> None:
    """No change when frequency matches."""
    from metapyle.processing import align_to_frequency

    dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")
    df = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}, index=dates)

    result = align_to_frequency(df, "daily")

    pd.testing.assert_frame_equal(result, df)


def test_align_to_frequency_quarterly_to_daily() -> None:
    """Upsample quarterly to daily."""
    from metapyle.processing import align_to_frequency

    dates = pd.to_datetime(["2024-03-31", "2024-06-30"])
    df = pd.DataFrame({"value": [100.0, 200.0]}, index=dates)

    result = align_to_frequency(df, "daily")

    # Should have many more rows
    assert len(result) > 2


def test_get_pandas_frequency_mapping() -> None:
    """Frequency enum maps to pandas frequency strings."""
    from metapyle.catalog import Frequency
    from metapyle.processing import get_pandas_frequency

    assert get_pandas_frequency(Frequency.DAILY) == "D"
    assert get_pandas_frequency(Frequency.WEEKLY) == "W"
    assert get_pandas_frequency(Frequency.MONTHLY) == "ME"
    assert get_pandas_frequency(Frequency.QUARTERLY) == "QE"
    assert get_pandas_frequency(Frequency.ANNUAL) == "YE"


def test_get_pandas_frequency_from_string() -> None:
    """Can get pandas frequency from string."""
    from metapyle.processing import get_pandas_frequency

    assert get_pandas_frequency("daily") == "D"
    assert get_pandas_frequency("monthly") == "ME"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_processing.py::test_align_to_frequency_downsample -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'metapyle.processing'"

**Step 3: Write minimal implementation**

```python
"""Data processing and frequency alignment functions."""

import logging

import pandas as pd

from metapyle.catalog import Frequency

logger = logging.getLogger(__name__)

# Mapping from Frequency enum to pandas frequency strings
FREQUENCY_MAP: dict[Frequency | str, str] = {
    Frequency.DAILY: "D",
    Frequency.WEEKLY: "W",
    Frequency.MONTHLY: "ME",
    Frequency.QUARTERLY: "QE",
    Frequency.ANNUAL: "YE",
    "daily": "D",
    "weekly": "W",
    "monthly": "ME",
    "quarterly": "QE",
    "annual": "YE",
}


def get_pandas_frequency(frequency: Frequency | str) -> str:
    """
    Convert frequency to pandas frequency string.

    Parameters
    ----------
    frequency : Frequency | str
        Frequency enum value or string.

    Returns
    -------
    str
        Pandas frequency string (e.g., "D", "ME", "QE").
    """
    if isinstance(frequency, str):
        frequency = frequency.lower()
    return FREQUENCY_MAP[frequency]


def align_to_frequency(
    df: pd.DataFrame,
    target_frequency: Frequency | str,
) -> pd.DataFrame:
    """
    Align DataFrame to target frequency.

    - Upsampling (e.g., monthly → daily): Forward-fill values
    - Downsampling (e.g., daily → monthly): Take last value of period

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with DatetimeIndex and 'value' column.
    target_frequency : Frequency | str
        Target frequency to align to.

    Returns
    -------
    pd.DataFrame
        Aligned DataFrame with same structure.
    """
    pandas_freq = get_pandas_frequency(target_frequency)

    # Infer current frequency
    if len(df) < 2:
        return df

    # Check if we need to resample
    current_freq = pd.infer_freq(df.index)

    logger.debug(
        "align_frequency: current=%s, target=%s",
        current_freq,
        pandas_freq,
    )

    # Resample to target frequency
    # For downsampling: take last value
    # For upsampling: forward-fill
    resampled = df.resample(pandas_freq).last()

    # Forward fill any NaN values (for upsampling)
    result = resampled.ffill()

    # Drop any remaining NaN rows at the start
    result = result.dropna()

    logger.debug("align_frequency_complete: rows=%d", len(result))

    return result
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_processing.py -v`
Expected: PASS (all 6 tests)

**Step 5: Commit**

```bash
git add src/metapyle/processing.py tests/unit/test_processing.py
git commit -m "feat(processing): add frequency alignment functions"
```

---

### Task 3.2: Update Client with Frequency Alignment

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `tests/unit/test_client.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_client.py`:

```python
def test_client_get_with_frequency_alignment(tmp_path) -> None:
    """Client aligns mixed frequencies when frequency parameter provided."""
    from metapyle.sources import BaseSource, register_source
    from metapyle import Client

    @register_source("mock_quarterly")
    class MockQuarterlySource(BaseSource):
        def fetch(self, symbol, start, end, **kwargs):
            return pd.DataFrame(
                {"value": [100.0, 101.0]},
                index=pd.to_datetime(["2024-03-31", "2024-06-30"]),
            )

        def get_metadata(self, symbol):
            return {}

    @register_source("mock_daily")
    class MockDailySource(BaseSource):
        def fetch(self, symbol, start, end, **kwargs):
            return pd.DataFrame(
                {"value": [5000.0, 5001.0, 5002.0]},
                index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            )

        def get_metadata(self, symbol):
            return {}

    yaml_content = """
- my_name: GDP
  source: mock_quarterly
  symbol: GDP_SYM
  frequency: quarterly

- my_name: SPX
  source: mock_daily
  symbol: SPX_SYM
  frequency: daily
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    client = Client(catalog=str(yaml_file), cache_enabled=False)

    # Should succeed with frequency parameter
    df = client.get(
        ["GDP", "SPX"],
        start="2024-01-01",
        end="2024-06-30",
        frequency="daily",
    )

    assert "GDP" in df.columns
    assert "SPX" in df.columns
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::test_client_get_with_frequency_alignment -v`
Expected: FAIL (currently ignores frequency parameter)

**Step 3: Update Client implementation**

Update `_fetch_symbol` and `get` methods in `src/metapyle/client.py`:

```python
    def get(
        self,
        symbols: list[str],
        start: str,
        end: str,
        *,
        frequency: str | None = None,
        use_cache: bool = True,
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
        use_cache : bool, optional
            If False, bypass cache and fetch fresh data. Default True.

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
        """
        from metapyle.processing import align_to_frequency

        # Resolve catalog entries
        entries = [self._catalog.get(name) for name in symbols]

        # Check frequency compatibility (if no alignment requested)
        if frequency is None:
            self._check_frequency_compatibility(entries)

        # Fetch each symbol
        dfs: dict[str, pd.DataFrame] = {}
        for entry in entries:
            df = self._fetch_symbol(entry, start, end, use_cache)

            # Apply frequency alignment if requested
            if frequency is not None:
                df = align_to_frequency(df, frequency)

            dfs[entry.my_name] = df

        # Assemble into wide DataFrame
        result = self._assemble_dataframe(dfs)

        logger.info(
            "get_complete: symbols=%d, rows=%d, start=%s, end=%s",
            len(symbols),
            len(result),
            start,
            end,
        )

        return result
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): add frequency alignment support"
```

---

### Task 3.3: Add get_metadata() to Client

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `tests/unit/test_client.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_client.py`:

```python
def test_client_get_metadata(mock_catalog, mock_source) -> None:
    """Client can retrieve metadata for a symbol."""
    from metapyle import Client

    client = Client(catalog=mock_catalog, cache_enabled=False)
    meta = client.get_metadata("GDP_US")

    assert isinstance(meta, dict)
    # Should include catalog info
    assert "my_name" in meta
    assert meta["my_name"] == "GDP_US"
    assert "source" in meta
    assert "frequency" in meta
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::test_client_get_metadata -v`
Expected: FAIL with "AttributeError: 'Client' object has no attribute 'get_metadata'"

**Step 3: Add get_metadata method**

Add to `src/metapyle/client.py`:

```python
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a catalog symbol.

        Parameters
        ----------
        symbol : str
            Catalog name.

        Returns
        -------
        dict[str, Any]
            Combined metadata from catalog entry and source adapter.

        Raises
        ------
        SymbolNotFoundError
            If symbol not in catalog.
        """
        entry = self._catalog.get(symbol)
        source = self._registry.get(entry.source)

        # Get source-specific metadata
        source_meta = source.get_metadata(entry.symbol)

        # Combine with catalog info
        return {
            "my_name": entry.my_name,
            "source": entry.source,
            "symbol": entry.symbol,
            "frequency": entry.frequency.value,
            "field": entry.field,
            "description": entry.description,
            "unit": entry.unit,
            **source_meta,
        }
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py::test_client_get_metadata -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): add get_metadata() method"
```

---

## Phase 3 Complete

- ✅ Frequency alignment functions
- ✅ Client frequency alignment support
- ✅ get_metadata() method

---

## Phase 4: Polish

### Task 4.1: Add get_raw() for Ad-hoc Queries

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `tests/unit/test_client.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_client.py`:

```python
def test_client_get_raw(mock_source) -> None:
    """Client can fetch raw data bypassing catalog."""
    from metapyle import Client
    import tempfile

    # Need a minimal catalog for Client init
    yaml_content = """
- my_name: DUMMY
  source: mock_source
  symbol: DUMMY
  frequency: daily
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        catalog_path = f.name

    from pathlib import Path

    client = Client(catalog=catalog_path, cache_enabled=False)

    df = client.get_raw(
        source="mock_source",
        symbol="SPX_SYMBOL",
        field="PX_LAST",
        start="2024-01-01",
        end="2024-12-31",
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

    Path(catalog_path).unlink()


def test_client_get_raw_uses_cache(mock_source, tmp_path) -> None:
    """get_raw() uses the cache."""
    from metapyle import Client
    import tempfile

    yaml_content = """
- my_name: DUMMY
  source: mock_source
  symbol: DUMMY
  frequency: daily
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        catalog_path = f.name

    from pathlib import Path

    cache_path = tmp_path / "cache.db"
    client = Client(catalog=catalog_path, cache_path=str(cache_path))

    # First fetch
    df1 = client.get_raw(
        source="mock_source",
        symbol="SPX_SYMBOL",
        start="2024-01-01",
        end="2024-12-31",
    )

    # Second fetch - should hit cache
    df2 = client.get_raw(
        source="mock_source",
        symbol="SPX_SYMBOL",
        start="2024-01-01",
        end="2024-12-31",
    )

    pd.testing.assert_frame_equal(df1, df2)

    Path(catalog_path).unlink()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::test_client_get_raw -v`
Expected: FAIL with "AttributeError: 'Client' object has no attribute 'get_raw'"

**Step 3: Add get_raw method**

Add to `src/metapyle/client.py`:

```python
    def get_raw(
        self,
        source: str,
        symbol: str,
        start: str,
        end: str,
        *,
        field: str | None = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch data directly from a source, bypassing the catalog.

        Useful for ad-hoc queries or testing new data series.

        Parameters
        ----------
        source : str
            Name of registered source adapter.
        symbol : str
            Source-specific symbol identifier.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        field : str | None, optional
            Source-specific field (e.g., "PX_LAST" for Bloomberg).
        use_cache : bool, optional
            If False, bypass cache. Default True.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.

        Raises
        ------
        UnknownSourceError
            If source is not registered.
        FetchError
            If data retrieval fails.
        """
        # Try cache first
        if use_cache:
            cached = self._cache.get(
                source=source,
                symbol=symbol,
                field=field,
                start_date=start,
                end_date=end,
            )
            if cached is not None:
                logger.debug("get_raw_from_cache: source=%s, symbol=%s", source, symbol)
                return cached

        # Fetch from source
        source_adapter = self._registry.get(source)
        kwargs: dict[str, Any] = {}
        if field is not None:
            kwargs["field"] = field

        logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
        df = source_adapter.fetch(symbol, start, end, **kwargs)

        # Store in cache
        self._cache.put(
            source=source,
            symbol=symbol,
            field=field,
            start_date=start,
            end_date=end,
            data=df,
        )

        return df
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): add get_raw() for ad-hoc queries"
```

---

### Task 4.2: Catalog Source Validation

**Files:**
- Modify: `src/metapyle/catalog.py`
- Modify: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
def test_catalog_validate_sources(tmp_path) -> None:
    """Catalog can validate that all sources are registered."""
    from metapyle.catalog import Catalog
    from metapyle.exceptions import UnknownSourceError

    yaml_content = """
- my_name: GDP_US
  source: nonexistent_source
  symbol: GDP CUR$ Index
  frequency: quarterly
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))

    # Import registry after sources are loaded
    from metapyle.sources.base import _global_registry

    with pytest.raises(UnknownSourceError, match="nonexistent_source"):
        catalog.validate_sources(_global_registry)


def test_catalog_validate_sources_success(tmp_path) -> None:
    """Catalog validation passes when all sources exist."""
    from metapyle.catalog import Catalog
    from metapyle.sources import BaseSource, register_source

    @register_source("valid_source")
    class ValidSource(BaseSource):
        def fetch(self, symbol, start, end, **kwargs):
            import pandas as pd
            return pd.DataFrame()

        def get_metadata(self, symbol):
            return {}

    yaml_content = """
- my_name: GDP_US
  source: valid_source
  symbol: GDP CUR$ Index
  frequency: quarterly
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))

    from metapyle.sources.base import _global_registry

    # Should not raise
    catalog.validate_sources(_global_registry)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_catalog_validate_sources -v`
Expected: FAIL with "AttributeError: 'Catalog' object has no attribute 'validate_sources'"

**Step 3: Add validate_sources method**

Add to `src/metapyle/catalog.py`:

```python
    def validate_sources(self, registry: "SourceRegistry") -> None:
        """
        Validate that all catalog sources are registered.

        Parameters
        ----------
        registry : SourceRegistry
            Source registry to validate against.

        Raises
        ------
        UnknownSourceError
            If any catalog entry references an unregistered source.
        """
        from metapyle.exceptions import UnknownSourceError

        registered = set(registry.list_sources())
        catalog_sources = {entry.source for entry in self._entries.values()}

        unknown = catalog_sources - registered
        if unknown:
            raise UnknownSourceError(
                f"Unknown source(s) in catalog: {', '.join(sorted(unknown))}. "
                f"Registered sources: {', '.join(sorted(registered))}"
            )
```

Add import at top:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from metapyle.sources.base import SourceRegistry
```

**Step 4: Update Client to validate sources**

In `src/metapyle/client.py` `__init__`:

```python
    def __init__(
        self,
        catalog: str | list[str],
        *,
        cache_path: str | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self._catalog = Catalog.from_yaml(catalog)
        self._cache = Cache(path=cache_path, enabled=cache_enabled)
        self._registry = _global_registry

        # Validate all catalog sources are registered
        self._catalog.validate_sources(self._registry)

        logger.info(
            "client_initialized: catalog_entries=%d, cache_enabled=%s",
            len(self._catalog),
            cache_enabled,
        )
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/metapyle/catalog.py src/metapyle/client.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add source validation at client init"
```

---

### Task 4.3: Final Package Exports and Documentation

**Files:**
- Modify: `src/metapyle/__init__.py`
- Create: `tests/unit/test_public_api.py`

**Step 1: Write test for public API**

```python
"""Test that public API is correctly exported."""


def test_public_api_exports() -> None:
    """All public symbols should be importable from metapyle."""
    from metapyle import (
        Client,
        BaseSource,
        register_source,
        MetapyleError,
        CatalogError,
        CatalogValidationError,
        DuplicateNameError,
        FetchError,
        FrequencyMismatchError,
        NoDataError,
        SymbolNotFoundError,
        UnknownSourceError,
    )

    # All should be importable
    assert Client is not None
    assert BaseSource is not None
    assert register_source is not None
    assert MetapyleError is not None
    assert CatalogError is not None
    assert CatalogValidationError is not None
    assert DuplicateNameError is not None
    assert FetchError is not None
    assert FrequencyMismatchError is not None
    assert NoDataError is not None
    assert SymbolNotFoundError is not None
    assert UnknownSourceError is not None


def test_version_available() -> None:
    """Package version should be available."""
    import metapyle

    assert hasattr(metapyle, "__version__")
```

**Step 2: Run test to verify current state**

Run: `pytest tests/unit/test_public_api.py -v`

**Step 3: Update __init__.py with version**

```python
"""Metapyle - Unified interface for financial time-series data."""

__version__ = "0.1.0"

from metapyle.client import Client
from metapyle.exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
    FrequencyMismatchError,
    MetapyleError,
    NoDataError,
    SymbolNotFoundError,
    UnknownSourceError,
)
from metapyle.sources import BaseSource, register_source

__all__ = [
    "__version__",
    "Client",
    "BaseSource",
    "register_source",
    "MetapyleError",
    "CatalogError",
    "CatalogValidationError",
    "DuplicateNameError",
    "FetchError",
    "FrequencyMismatchError",
    "NoDataError",
    "SymbolNotFoundError",
    "UnknownSourceError",
]
```

**Step 4: Run all tests**

Run: `pytest tests/unit/ -v`
Expected: All PASS

**Step 5: Run type checker and linter**

Run: `ruff check src/ tests/`
Run: `mypy src/`

**Step 6: Commit**

```bash
git add src/metapyle/__init__.py tests/unit/test_public_api.py
git commit -m "feat: finalize public API exports"
```

---

## Phase 4 Complete

- ✅ get_raw() for ad-hoc queries
- ✅ Catalog source validation
- ✅ Final package exports

---

## Final Verification

### Run Full Test Suite

```bash
# Unit tests
pytest tests/unit/ -v --cov=metapyle --cov-report=term-missing

# Type checking
mypy src/

# Linting
ruff check src/ tests/
ruff format --check src/ tests/
```

### Expected Coverage Target

- Aim for 90%+ unit test coverage
- All public API methods tested
- All exception paths tested

---

## Summary

| Phase | Tasks | Status |
|-------|-------|--------|
| Phase 1: Core Foundation | 6 tasks | Ready |
| Phase 2: Adapters | 2 tasks | Ready |
| Phase 3: Processing | 3 tasks | Ready |
| Phase 4: Polish | 3 tasks | Ready |

**Total: 14 tasks**

Each task follows TDD: write failing test → run test → implement → run test → commit.

