# Frequency Handling Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove declared `frequency` from catalog entries, use pandas frequency strings directly for alignment, and warn (not error) on misaligned series.

**Architecture:** Delete the `Frequency` enum and `FrequencyMismatchError`. Simplify `processing.py` to pass frequency strings directly to pandas. Add index alignment detection that warns via logging instead of raising exceptions.

**Tech Stack:** Python 3.12+, pandas, pytest

---

## Task 1: Remove FrequencyMismatchError from Exceptions

**Files:**
- Modify: `src/metapyle/exceptions.py`
- Modify: `tests/unit/test_exceptions.py`

**Step 1: Write the updated test file (remove FrequencyMismatchError tests)**

```python
"""Unit tests for exception hierarchy."""

import pytest

from metapyle.exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
    MetapyleError,
    NoDataError,
    SymbolNotFoundError,
    UnknownSourceError,
)


def test_metapyle_error_is_exception() -> None:
    """MetapyleError should be a base exception."""
    error = MetapyleError("test message")
    assert isinstance(error, Exception)
    assert str(error) == "test message"


def test_catalog_error_inherits_from_metapyle_error() -> None:
    """CatalogError should inherit from MetapyleError."""
    error = CatalogError("catalog issue")
    assert isinstance(error, MetapyleError)
    assert isinstance(error, Exception)


def test_fetch_error_inherits_from_metapyle_error() -> None:
    """FetchError should inherit from MetapyleError."""
    error = FetchError("fetch failed")
    assert isinstance(error, MetapyleError)


def test_catalog_validation_error_inherits_from_catalog_error() -> None:
    """CatalogValidationError should inherit from CatalogError."""
    error = CatalogValidationError("invalid yaml")
    assert isinstance(error, CatalogError)


def test_duplicate_name_error_inherits_from_catalog_error() -> None:
    """DuplicateNameError should inherit from CatalogError."""
    error = DuplicateNameError("duplicate found")
    assert isinstance(error, CatalogError)


def test_unknown_source_error_inherits_from_catalog_error() -> None:
    """UnknownSourceError should inherit from CatalogError."""
    error = UnknownSourceError("unknown source")
    assert isinstance(error, CatalogError)


def test_symbol_not_found_error_inherits_from_catalog_error() -> None:
    """SymbolNotFoundError should inherit from CatalogError."""
    error = SymbolNotFoundError("symbol not found")
    assert isinstance(error, CatalogError)


def test_no_data_error_inherits_from_fetch_error() -> None:
    """NoDataError should inherit from FetchError."""
    error = NoDataError("no data returned")
    assert isinstance(error, FetchError)


@pytest.mark.parametrize(
    "exception_class",
    [
        MetapyleError,
        CatalogError,
        FetchError,
        CatalogValidationError,
        DuplicateNameError,
        UnknownSourceError,
        SymbolNotFoundError,
        NoDataError,
    ],
)
def test_all_exceptions_catchable_via_metapyle_error(exception_class: type) -> None:
    """All exceptions should be catchable via MetapyleError."""
    with pytest.raises(MetapyleError):
        raise exception_class("test")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_exceptions.py -v`
Expected: FAIL - import error for `FrequencyMismatchError`

**Step 3: Update exceptions.py to remove FrequencyMismatchError**

```python
"""Exception hierarchy for metapyle.

All metapyle exceptions inherit from MetapyleError for easy catching.
"""

__all__ = [
    "MetapyleError",
    "CatalogError",
    "FetchError",
    "CatalogValidationError",
    "DuplicateNameError",
    "UnknownSourceError",
    "SymbolNotFoundError",
    "NoDataError",
]


class MetapyleError(Exception):
    """Base exception for all metapyle errors."""


class CatalogError(MetapyleError):
    """Catalog-related errors (validation, lookup, duplicates)."""


class FetchError(MetapyleError):
    """Data fetching errors."""


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
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/exceptions.py tests/unit/test_exceptions.py
git commit -m "refactor(exceptions): remove FrequencyMismatchError"
```

---

## Task 2: Remove Frequency Enum and Update CatalogEntry

**Files:**
- Modify: `src/metapyle/catalog.py`
- Modify: `tests/unit/test_catalog.py`

**Step 1: Write the updated test file (remove Frequency tests, update CatalogEntry tests)**

Replace the entire `tests/unit/test_catalog.py` with:

```python
"""Unit tests for Catalog and CatalogEntry."""

from pathlib import Path

import pytest

from metapyle.catalog import Catalog, CatalogEntry
from metapyle.exceptions import (
    CatalogValidationError,
    DuplicateNameError,
    SymbolNotFoundError,
    UnknownSourceError,
)
from metapyle.sources.base import SourceRegistry


def test_catalog_entry_required_fields() -> None:
    """CatalogEntry requires my_name, source, symbol."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    assert entry.my_name == "GDP_US"
    assert entry.source == "bloomberg"
    assert entry.symbol == "GDP CUR$ Index"


def test_catalog_entry_optional_fields_default_none() -> None:
    """CatalogEntry optional fields default to None."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    assert entry.field is None
    assert entry.description is None
    assert entry.unit is None


def test_catalog_entry_with_optional_fields() -> None:
    """CatalogEntry can have optional fields set."""
    entry = CatalogEntry(
        my_name="SPX_CLOSE",
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        description="S&P 500 closing price",
        unit="points",
    )

    assert entry.field == "PX_LAST"
    assert entry.description == "S&P 500 closing price"
    assert entry.unit == "points"


def test_catalog_entry_is_frozen() -> None:
    """CatalogEntry should be immutable."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    with pytest.raises(AttributeError):
        entry.my_name = "changed"  # type: ignore[misc]


def test_catalog_entry_is_keyword_only() -> None:
    """CatalogEntry must use keyword arguments."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )
    assert entry.my_name == "GDP_US"

    with pytest.raises(TypeError):
        CatalogEntry(  # type: ignore[misc]
            "GDP_US",
            "bloomberg",
            "GDP CUR$ Index",
        )


def test_catalog_entry_uses_slots() -> None:
    """CatalogEntry should use slots for memory efficiency."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    assert not hasattr(entry, "__dict__")


# ============================================================================
# Catalog Tests
# ============================================================================


def test_catalog_load_from_yaml(tmp_path: Path) -> None:
    """Catalog can load entries from a YAML file."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  description: US GDP

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))

    assert len(catalog) == 2
    assert "GDP_US" in catalog
    assert "SPX_CLOSE" in catalog

    gdp = catalog.get("GDP_US")
    assert gdp.source == "bloomberg"
    assert gdp.description == "US GDP"

    spx = catalog.get("SPX_CLOSE")
    assert spx.field == "PX_LAST"


def test_catalog_load_missing_required_field(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for missing required fields."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  # missing symbol
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="symbol"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_duplicate_names(tmp_path: Path) -> None:
    """Catalog raises DuplicateNameError for duplicate my_name."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index

- my_name: GDP_US
  source: localfile
  symbol: /data/gdp.csv
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(DuplicateNameError, match="GDP_US"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_multiple_files(tmp_path: Path) -> None:
    """Catalog can load and merge multiple YAML files."""
    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
"""
    yaml2 = """
- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    catalog = Catalog.from_yaml([str(file1), str(file2)])

    assert len(catalog) == 2
    assert "GDP_US" in catalog
    assert "SPX_CLOSE" in catalog


def test_catalog_load_duplicate_across_files(tmp_path: Path) -> None:
    """Catalog raises DuplicateNameError for duplicates across files."""
    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
"""
    yaml2 = """
- my_name: GDP_US
  source: localfile
  symbol: /data/gdp.csv
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    with pytest.raises(DuplicateNameError, match="GDP_US"):
        Catalog.from_yaml([str(file1), str(file2)])


def test_catalog_get_unknown_symbol() -> None:
    """Catalog raises SymbolNotFoundError for unknown symbol."""
    catalog = Catalog({})

    with pytest.raises(SymbolNotFoundError, match="UNKNOWN"):
        catalog.get("UNKNOWN")


def test_catalog_list_names(tmp_path: Path) -> None:
    """Catalog can list all entry names."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))
    names = catalog.list_names()

    assert sorted(names) == ["GDP_US", "SPX_CLOSE"]


def test_catalog_file_not_found() -> None:
    """Catalog raises CatalogValidationError for missing file."""
    with pytest.raises(CatalogValidationError, match="not found"):
        Catalog.from_yaml("/nonexistent/path/catalog.yaml")


def test_catalog_malformed_yaml(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for malformed YAML."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  bad_key: [unclosed bracket
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="YAML"):
        Catalog.from_yaml(str(yaml_file))


# ============================================================================
# Catalog Source Validation Tests
# ============================================================================


def test_catalog_validate_sources() -> None:
    """Catalog raises UnknownSourceError for unregistered sources."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="unknown_source",
        symbol="GDP CUR$ Index",
    )
    catalog = Catalog({"GDP_US": entry})

    registry = SourceRegistry()
    registry.register("bloomberg", type)

    with pytest.raises(UnknownSourceError, match="unknown_source"):
        catalog.validate_sources(registry)


def test_catalog_validate_sources_success() -> None:
    """Catalog validation passes when all sources are registered."""
    entry1 = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )
    entry2 = CatalogEntry(
        my_name="LOCAL_DATA",
        source="localfile",
        symbol="/data/local.csv",
    )
    catalog = Catalog({"GDP_US": entry1, "LOCAL_DATA": entry2})

    registry = SourceRegistry()
    registry.register("bloomberg", type)
    registry.register("localfile", type)

    catalog.validate_sources(registry)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: FAIL - `Frequency` still imported, `CatalogEntry` still requires `frequency`

**Step 3: Update catalog.py to remove Frequency enum and frequency field**

```python
"""Catalog system for mapping human-readable names to data sources."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from metapyle.sources.base import SourceRegistry

import yaml

from metapyle.exceptions import (
    CatalogValidationError,
    DuplicateNameError,
    SymbolNotFoundError,
)

__all__ = ["Catalog", "CatalogEntry"]

logger = logging.getLogger(__name__)


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
                raise CatalogValidationError(f"Malformed YAML in {path}: {e}") from e

            if not isinstance(raw_entries, list):
                raise CatalogValidationError(f"Catalog file {path} must contain a list of entries")

            for raw in raw_entries:
                entry = cls._parse_entry(raw, path)

                if entry.my_name in entries:
                    raise DuplicateNameError(f"Duplicate catalog name: {entry.my_name}")

                entries[entry.my_name] = entry

        logger.info("catalog_loaded: entries=%d", len(entries))
        return cls(entries)

    @staticmethod
    def _parse_entry(raw: dict[str, Any], source_file: str) -> CatalogEntry:
        """Parse a raw dictionary into a CatalogEntry."""
        required_fields = ["my_name", "source", "symbol"]

        for field in required_fields:
            if field not in raw:
                raise CatalogValidationError(f"Missing required field '{field}' in {source_file}")

        return CatalogEntry(
            my_name=raw["my_name"],
            source=raw["source"],
            symbol=raw["symbol"],
            field=raw.get("field"),
            description=raw.get("description"),
            unit=raw.get("unit"),
        )

    def get(self, name: str) -> CatalogEntry:
        """Get a catalog entry by name."""
        if name not in self._entries:
            raise SymbolNotFoundError(
                f"Symbol not found in catalog: {name}. "
                f"Available: {', '.join(sorted(self._entries.keys())[:5])}"
                + ("..." if len(self._entries) > 5 else "")
            )
        return self._entries[name]

    def list_names(self) -> list[str]:
        """List all catalog entry names."""
        return list(self._entries.keys())

    def __len__(self) -> int:
        """Return the number of entries in the catalog."""
        return len(self._entries)

    def __contains__(self, name: str) -> bool:
        """Check if a name exists in the catalog."""
        return name in self._entries

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

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "refactor(catalog): remove Frequency enum and frequency field from CatalogEntry"
```

---

## Task 3: Simplify Processing Module

**Files:**
- Modify: `src/metapyle/processing.py`
- Modify: `tests/unit/test_processing.py`

**Step 1: Write the updated test file (use pandas frequency strings directly)**

Replace the entire `tests/unit/test_processing.py` with:

```python
"""Unit tests for processing module."""

import pandas as pd
import pytest

from metapyle.processing import align_to_frequency


def test_align_to_frequency_downsample_to_monthly() -> None:
    """align_to_frequency downsamples daily data to monthly using last value."""
    dates = pd.date_range("2024-01-01", periods=90, freq="D")
    df = pd.DataFrame({"value": range(90)}, index=dates)

    result = align_to_frequency(df, "ME")

    # Should have 3 months: Jan, Feb, Mar
    assert len(result) == 3

    # Last value of January (index 30, value 30 for Jan 31)
    assert result.iloc[0]["value"] == 30

    # Last value of February (index 59 for Feb 29 in 2024, value 59)
    assert result.iloc[1]["value"] == 59

    # Last value of March (index 89 for Mar 30, value 89)
    assert result.iloc[2]["value"] == 89


def test_align_to_frequency_downsample_to_quarterly() -> None:
    """align_to_frequency downsamples daily data to quarterly."""
    dates = pd.date_range("2024-01-01", periods=180, freq="D")
    df = pd.DataFrame({"value": range(180)}, index=dates)

    result = align_to_frequency(df, "QE")

    # Should have 2 quarters: Q1 (ends Mar 31), Q2 (ends Jun 28)
    assert len(result) == 2


def test_align_to_frequency_upsample_to_daily() -> None:
    """align_to_frequency upsamples monthly data to daily using forward-fill."""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    df = pd.DataFrame({"value": [100, 200, 300]}, index=dates)

    result = align_to_frequency(df, "D")

    # First day should be Jan 31 with value 100
    assert result.iloc[0]["value"] == 100

    # February values should be forward-filled from 100 until Feb 29
    feb_values = result.loc["2024-02"]
    assert (feb_values["value"].iloc[:-1] == 100).all()
    assert feb_values.iloc[-1]["value"] == 200

    # March values should have 200 forward-filled until Mar 31
    mar_values = result.loc["2024-03"]
    assert (mar_values["value"].iloc[:-1] == 200).all()
    assert mar_values.iloc[-1]["value"] == 300


def test_align_to_frequency_business_day() -> None:
    """align_to_frequency supports business day frequency."""
    dates = pd.date_range("2024-01-01", periods=31, freq="D")
    df = pd.DataFrame({"value": range(31)}, index=dates)

    result = align_to_frequency(df, "B")

    # Business days only (excludes weekends)
    assert len(result) < len(df)
    # All result dates should be weekdays
    assert all(d.dayofweek < 5 for d in result.index)


def test_align_to_frequency_week_end() -> None:
    """align_to_frequency supports weekly frequency."""
    dates = pd.date_range("2024-01-01", periods=28, freq="D")
    df = pd.DataFrame({"value": range(28)}, index=dates)

    result = align_to_frequency(df, "W")

    # 4 weeks in 28 days
    assert len(result) == 4


def test_align_to_frequency_business_month_end() -> None:
    """align_to_frequency supports business month-end frequency."""
    dates = pd.date_range("2024-01-01", periods=90, freq="D")
    df = pd.DataFrame({"value": range(90)}, index=dates)

    result = align_to_frequency(df, "BME")

    # 3 business month-ends
    assert len(result) == 3
    # All should be weekdays
    assert all(d.dayofweek < 5 for d in result.index)


def test_align_to_frequency_year_end() -> None:
    """align_to_frequency supports year-end frequency."""
    dates = pd.date_range("2023-01-01", periods=730, freq="D")
    df = pd.DataFrame({"value": range(730)}, index=dates)

    result = align_to_frequency(df, "YE")

    # 2 years
    assert len(result) == 2


def test_align_to_frequency_invalid_frequency_raises() -> None:
    """align_to_frequency raises ValueError for invalid pandas frequency."""
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    df = pd.DataFrame({"value": range(10)}, index=dates)

    with pytest.raises(ValueError):
        align_to_frequency(df, "INVALID_FREQ")


def test_align_to_frequency_same_frequency() -> None:
    """align_to_frequency with same frequency returns equivalent data."""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    df = pd.DataFrame({"value": [100, 200, 300]}, index=dates)

    result = align_to_frequency(df, "ME")

    assert len(result) == 3
    assert list(result["value"]) == [100, 200, 300]


def test_align_to_frequency_quarterly_to_daily() -> None:
    """align_to_frequency can upsample quarterly to daily."""
    dates = pd.date_range("2024-03-31", periods=2, freq="QE")
    df = pd.DataFrame({"value": [1000, 2000]}, index=dates)

    result = align_to_frequency(df, "D")

    # First value should be 1000 (Q1 end)
    assert result.iloc[0]["value"] == 1000

    # Last value should be 2000 (Q2 end)
    assert result.iloc[-1]["value"] == 2000

    # Values in between should be forward-filled
    apr_1_value = result.loc["2024-04-01"]["value"]
    assert apr_1_value == 1000
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_processing.py -v`
Expected: FAIL - old functions still imported, signature mismatch

**Step 3: Update processing.py to simplify**

```python
"""Processing utilities for frequency alignment and data transformation."""

import logging

import pandas as pd

__all__ = ["align_to_frequency"]

logger = logging.getLogger(__name__)


def align_to_frequency(
    df: pd.DataFrame,
    target_frequency: str,
) -> pd.DataFrame:
    """
    Resample a DataFrame to a target frequency.

    Downsampling (e.g., daily to monthly) takes the last value of each period.
    Upsampling (e.g., monthly to daily) forward-fills values.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a DatetimeIndex to resample.
    target_frequency : str
        Target pandas frequency string (e.g., "D", "B", "ME", "QE", "YE").

    Returns
    -------
    pd.DataFrame
        Resampled DataFrame aligned to the target frequency.

    Raises
    ------
    ValueError
        If the frequency string is not valid for pandas.

    Examples
    --------
    >>> import pandas as pd
    >>> dates = pd.date_range("2024-01-01", periods=90, freq="D")
    >>> df = pd.DataFrame({"value": range(90)}, index=dates)
    >>> aligned = align_to_frequency(df, "ME")
    >>> len(aligned)
    3
    """
    logger.debug(
        "aligning_frequency: rows=%d, target=%s",
        len(df),
        target_frequency,
    )

    # Resample using last value for downsampling, forward-fill for upsampling
    resampled = df.resample(target_frequency).last()

    # Forward-fill NaN values (handles upsampling)
    resampled = resampled.ffill()

    logger.debug("alignment_complete: input_rows=%d, output_rows=%d", len(df), len(resampled))
    return resampled
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_processing.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/processing.py tests/unit/test_processing.py
git commit -m "refactor(processing): simplify align_to_frequency to use pandas frequency strings directly"
```

---

## Task 4: Update Client - Remove Frequency Check, Add Index Alignment Warning

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `tests/unit/test_client.py`

**Step 1: Write updated test file**

Replace the entire `tests/unit/test_client.py` with:

```python
"""Unit tests for Client class."""

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from metapyle.client import Client
from metapyle.exceptions import SymbolNotFoundError, UnknownSourceError
from metapyle.sources.base import BaseSource, register_source

# ============================================================================
# Mock Source Fixtures
# ============================================================================


@register_source("mock")
class MockSource(BaseSource):
    """Mock source for testing."""

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Return mock data based on symbol."""
        dates = pd.date_range(start, end, freq="D")
        data = list(range(len(dates)))
        return pd.DataFrame({"value": data}, index=dates)

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return mock metadata."""
        return {"symbol": symbol, "description": f"Mock data for {symbol}"}


@register_source("mock_monthly")
class MockMonthlySource(BaseSource):
    """Mock source that returns monthly data."""

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Return mock monthly data."""
        dates = pd.date_range(start, end, freq="ME")
        data = list(range(len(dates)))
        return pd.DataFrame({"value": data}, index=dates)

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return mock metadata."""
        return {"symbol": symbol, "frequency": "monthly"}


@pytest.fixture
def catalog_yaml(tmp_path: Path) -> Path:
    """Create a catalog YAML file for testing."""
    yaml_content = """
- my_name: TEST_DAILY
  source: mock
  symbol: MOCK_DAILY
  description: Test daily data

- my_name: TEST_DAILY_2
  source: mock
  symbol: MOCK_DAILY_2
  description: Another test daily data

- my_name: TEST_MONTHLY
  source: mock_monthly
  symbol: MOCK_MONTHLY
  description: Test monthly data
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)
    return yaml_file


@pytest.fixture
def catalog_yaml_2(tmp_path: Path) -> Path:
    """Create a second catalog YAML file for testing multiple files."""
    yaml_content = """
- my_name: TEST_DAILY_3
  source: mock
  symbol: MOCK_DAILY_3
  description: Third test daily data
"""
    yaml_file = tmp_path / "catalog2.yaml"
    yaml_file.write_text(yaml_content)
    return yaml_file


@pytest.fixture
def cache_path(tmp_path: Path) -> str:
    """Create a cache path for testing."""
    return str(tmp_path / "test_cache.db")


# ============================================================================
# Client Initialization Tests
# ============================================================================


def test_client_initialization(catalog_yaml: Path, cache_path: str) -> None:
    """Client initializes with catalog path and cache settings."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    assert client._catalog is not None
    assert len(client._catalog) == 3
    assert client._cache is not None
    assert client._cache._enabled is True


def test_client_initialization_cache_disabled(catalog_yaml: Path) -> None:
    """Client can be initialized with cache disabled."""
    client = Client(catalog=str(catalog_yaml), cache_enabled=False)

    assert client._cache._enabled is False


def test_client_multiple_catalog_files(
    catalog_yaml: Path,
    catalog_yaml_2: Path,
    cache_path: str,
) -> None:
    """Client can load multiple catalog files."""
    client = Client(
        catalog=[str(catalog_yaml), str(catalog_yaml_2)],
        cache_path=cache_path,
    )

    assert len(client._catalog) == 4
    assert "TEST_DAILY" in client._catalog
    assert "TEST_DAILY_3" in client._catalog


# ============================================================================
# Client.get() Tests
# ============================================================================


def test_client_get_single_symbol(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get() returns DataFrame for a single symbol."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    assert isinstance(df, pd.DataFrame)
    assert "TEST_DAILY" in df.columns
    assert len(df) > 0
    assert isinstance(df.index, pd.DatetimeIndex)


def test_client_get_multiple_symbols_same_frequency(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get() returns wide DataFrame for multiple symbols with same frequency."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df = client.get(
        ["TEST_DAILY", "TEST_DAILY_2"],
        start="2024-01-01",
        end="2024-01-10",
    )

    assert isinstance(df, pd.DataFrame)
    assert "TEST_DAILY" in df.columns
    assert "TEST_DAILY_2" in df.columns
    assert len(df) > 0


def test_client_get_mixed_frequencies_warns(
    catalog_yaml: Path,
    cache_path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Client.get() warns when mixing frequencies without alignment."""
    import logging

    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with caplog.at_level(logging.WARNING):
        df = client.get(
            ["TEST_DAILY", "TEST_MONTHLY"],
            start="2024-01-01",
            end="2024-06-30",
        )

    # Should return data (not raise)
    assert isinstance(df, pd.DataFrame)
    assert "TEST_DAILY" in df.columns
    assert "TEST_MONTHLY" in df.columns

    # Should have logged a warning about index mismatch
    assert any("index_mismatch" in record.message for record in caplog.records)


def test_client_get_with_frequency_alignment(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client aligns mixed frequencies when frequency parameter provided."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df = client.get(
        ["TEST_DAILY", "TEST_MONTHLY"],
        start="2024-01-01",
        end="2024-03-31",
        frequency="D",
    )

    assert isinstance(df, pd.DataFrame)
    assert "TEST_DAILY" in df.columns
    assert "TEST_MONTHLY" in df.columns
    assert len(df) > 0
    assert isinstance(df.index, pd.DatetimeIndex)


def test_client_get_frequency_alignment_upsample(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client upsamples monthly data to daily frequency."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df = client.get(
        ["TEST_MONTHLY"],
        start="2024-01-01",
        end="2024-03-31",
        frequency="D",
    )

    assert isinstance(df, pd.DataFrame)
    assert "TEST_MONTHLY" in df.columns
    assert len(df) > 3
    assert isinstance(df.index, pd.DatetimeIndex)


def test_client_get_frequency_alignment_downsample(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client downsamples daily data to monthly frequency."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df = client.get(
        ["TEST_DAILY"],
        start="2024-01-01",
        end="2024-03-31",
        frequency="ME",
    )

    assert isinstance(df, pd.DataFrame)
    assert "TEST_DAILY" in df.columns
    assert len(df) == 3
    assert isinstance(df.index, pd.DatetimeIndex)


def test_client_get_unknown_symbol_raises(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get() raises SymbolNotFoundError for unknown symbol."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with pytest.raises(SymbolNotFoundError, match="UNKNOWN"):
        client.get(["UNKNOWN"], start="2024-01-01", end="2024-01-10")


def test_client_get_invalid_frequency_raises(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get() raises ValueError for invalid pandas frequency string."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with pytest.raises(ValueError):
        client.get(
            ["TEST_DAILY"],
            start="2024-01-01",
            end="2024-01-10",
            frequency="INVALID_FREQ",
        )


# ============================================================================
# Client Cache Tests
# ============================================================================


def test_client_uses_cache(catalog_yaml: Path, cache_path: str) -> None:
    """Client uses cache for subsequent requests."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df1 = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    entry = client._catalog.get("TEST_DAILY")
    cached = client._cache.get(
        source=entry.source,
        symbol=entry.symbol,
        field=entry.field,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached is not None

    df2 = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    pd.testing.assert_frame_equal(df1, df2, check_freq=False)


def test_client_bypass_cache(catalog_yaml: Path, cache_path: str) -> None:
    """Client can bypass cache with use_cache=False."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df1 = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")
    df2 = client.get(
        ["TEST_DAILY"],
        start="2024-01-01",
        end="2024-01-10",
        use_cache=False,
    )

    assert len(df1) > 0
    assert len(df2) > 0


def test_client_clear_cache(catalog_yaml: Path, cache_path: str) -> None:
    """Client.clear_cache() clears cached data."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")
    client.clear_cache()

    entry = client._catalog.get("TEST_DAILY")
    cached = client._cache.get(
        source=entry.source,
        symbol=entry.symbol,
        field=entry.field,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached is None


def test_client_clear_cache_specific_symbol(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.clear_cache() can clear a specific symbol."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")
    client.get(["TEST_DAILY_2"], start="2024-01-01", end="2024-01-10")

    client.clear_cache(symbol="TEST_DAILY")

    entry1 = client._catalog.get("TEST_DAILY")
    cached1 = client._cache.get(
        source=entry1.source,
        symbol=entry1.symbol,
        field=entry1.field,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached1 is None

    entry2 = client._catalog.get("TEST_DAILY_2")
    cached2 = client._cache.get(
        source=entry2.source,
        symbol=entry2.symbol,
        field=entry2.field,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached2 is not None


# ============================================================================
# Client.get_metadata() Tests
# ============================================================================


def test_client_get_metadata(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get_metadata() returns metadata for a symbol."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    metadata = client.get_metadata("TEST_DAILY")

    assert isinstance(metadata, dict)
    assert metadata["my_name"] == "TEST_DAILY"
    assert metadata["source"] == "mock"
    assert metadata["symbol"] == "MOCK_DAILY"
    assert metadata["description"] == "Test daily data"
    # frequency is now inferred (may be None if no data fetched yet)
    assert "frequency" in metadata


def test_client_get_metadata_unknown_symbol_raises(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get_metadata() raises SymbolNotFoundError for unknown symbol."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with pytest.raises(SymbolNotFoundError, match="UNKNOWN"):
        client.get_metadata("UNKNOWN")


# ============================================================================
# Client.get_raw() Tests
# ============================================================================


def test_client_get_raw(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get_raw() fetches data directly from source, bypassing catalog."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df = client.get_raw(
        source="mock",
        symbol="RAW_SYMBOL",
        start="2024-01-01",
        end="2024-01-10",
    )

    assert isinstance(df, pd.DataFrame)
    assert "value" in df.columns
    assert len(df) > 0
    assert isinstance(df.index, pd.DatetimeIndex)


def test_client_get_raw_uses_cache(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get_raw() uses cache for subsequent requests."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    df1 = client.get_raw(
        source="mock",
        symbol="RAW_CACHED",
        start="2024-01-01",
        end="2024-01-10",
    )

    cached = client._cache.get(
        source="mock",
        symbol="RAW_CACHED",
        field=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached is not None

    df2 = client.get_raw(
        source="mock",
        symbol="RAW_CACHED",
        start="2024-01-01",
        end="2024-01-10",
    )

    pd.testing.assert_frame_equal(df1, df2, check_freq=False)


def test_client_get_raw_unknown_source(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get_raw() raises UnknownSourceError for invalid source."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with pytest.raises(UnknownSourceError, match="nonexistent_source"):
        client.get_raw(
            source="nonexistent_source",
            symbol="ANY",
            start="2024-01-01",
            end="2024-01-10",
        )


def test_client_context_manager(catalog_yaml: Path, cache_path: str) -> None:
    """Client can be used as a context manager."""
    with Client(catalog=str(catalog_yaml), cache_path=cache_path) as client:
        df = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")
        assert len(df) > 0


def test_client_close(catalog_yaml: Path, cache_path: str) -> None:
    """Client.close() closes the cache connection."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)
    client.close()
    client.close()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py -v`
Expected: FAIL - `FrequencyMismatchError` still imported, `_check_frequency_compatibility` still called

**Step 3: Update client.py**

```python
"""Client for querying financial time-series data."""

import logging
from typing import Any, Self

import pandas as pd

from metapyle.cache import Cache
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.sources.base import SourceRegistry, _global_registry

__all__ = ["Client"]

logger = logging.getLogger(__name__)


class Client:
    """
    Client for querying financial time-series data.

    Provides a unified interface for fetching data from multiple sources
    using a catalog for name mapping and optional caching.

    Parameters
    ----------
    catalog : str | list[str]
        Path or list of paths to YAML catalog files.
    cache_path : str | None, optional
        Path to SQLite cache database. If None, uses default path.
    cache_enabled : bool, optional
        Whether caching is enabled. Default is True.

    Examples
    --------
    >>> client = Client(catalog="catalog.yaml")
    >>> df = client.get(["GDP_US", "CPI_EU"], start="2020-01-01", end="2024-12-31")
    """

    def __init__(
        self,
        catalog: str | list[str],
        *,
        cache_path: str | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self._registry: SourceRegistry = _global_registry
        self._catalog = Catalog.from_yaml(catalog)
        self._catalog.validate_sources(self._registry)
        self._cache = Cache(path=cache_path, enabled=cache_enabled)

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
            Pandas frequency string for alignment (e.g., "D", "ME", "QE").
            If omitted, data is returned as-is with a warning if indexes
            don't align.
        use_cache : bool, optional
            Whether to use cached data. Default is True.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with DatetimeIndex and columns named by catalog names.

        Raises
        ------
        SymbolNotFoundError
            If any symbol is not in the catalog.
        FetchError
            If data retrieval fails for any symbol.
        ValueError
            If frequency is an invalid pandas frequency string.
        """
        # Resolve entries (raises SymbolNotFoundError if not found)
        entries = [self._catalog.get(symbol) for symbol in symbols]

        if frequency is not None:
            logger.info(
                "frequency_alignment_requested: target=%s, symbols=%d",
                frequency,
                len(symbols),
            )

        # Fetch data for each symbol
        dfs: dict[str, pd.DataFrame] = {}
        for entry in entries:
            df = self._fetch_symbol(entry, start, end, use_cache)

            # Apply frequency alignment if requested
            if frequency is not None:
                from metapyle.processing import align_to_frequency

                logger.debug(
                    "aligning_symbol: symbol=%s, target_frequency=%s",
                    entry.my_name,
                    frequency,
                )
                df = align_to_frequency(df, frequency)

            dfs[entry.my_name] = df

        # Check index alignment if no frequency specified
        if frequency is None:
            self._check_index_alignment(dfs)

        # Assemble into wide DataFrame
        return self._assemble_dataframe(dfs)

    def _check_index_alignment(self, dfs: dict[str, pd.DataFrame]) -> None:
        """
        Warn if series have misaligned indexes.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping symbol names to DataFrames.
        """
        if len(dfs) <= 1:
            return

        # Infer frequency for each series
        freqs = {name: pd.infer_freq(df.index) for name, df in dfs.items()}

        # Check for mismatches
        unique_freqs = set(freqs.values())

        if len(unique_freqs) > 1:
            # Different frequencies (including None for irregular)
            freq_summary = ", ".join(
                f"{name}={freq or 'irregular'}" for name, freq in freqs.items()
            )
            logger.warning(
                "index_mismatch: Series have different frequencies: %s. "
                "Outer join may produce NaN values. Consider specifying frequency parameter.",
                freq_summary,
            )
        elif unique_freqs == {None}:
            # All irregular — check if indexes actually match
            indexes = list(dfs.values())
            first_idx = indexes[0].index
            if not all(df.index.equals(first_idx) for df in indexes[1:]):
                logger.warning(
                    "index_mismatch: Irregular series have different dates. "
                    "Outer join may produce NaN values. Consider specifying frequency parameter.",
                )

    def _fetch_symbol(
        self,
        entry: CatalogEntry,
        start: str,
        end: str,
        use_cache: bool,
    ) -> pd.DataFrame:
        """
        Fetch data for a single symbol, using cache if available.

        Parameters
        ----------
        entry : CatalogEntry
            Catalog entry for the symbol.
        start : str
            Start date in ISO format.
        end : str
            End date in ISO format.
        use_cache : bool
            Whether to use cached data.

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
                logger.debug(
                    "fetch_from_cache: symbol=%s, rows=%d",
                    entry.my_name,
                    len(cached),
                )
                return cached

        # Fetch from source
        source = self._registry.get(entry.source)

        # Build kwargs for source
        kwargs: dict[str, str] = {}
        if entry.field is not None:
            kwargs["field"] = entry.field

        logger.debug(
            "fetch_from_source: symbol=%s, source=%s, range=%s/%s",
            entry.my_name,
            entry.source,
            start,
            end,
        )

        df = source.fetch(entry.symbol, start, end, **kwargs)

        # Store in cache
        if use_cache:
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
        Assemble individual DataFrames into a wide DataFrame.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping symbol names to DataFrames.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by symbol names.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename 'value' column to symbol name and concatenate
        renamed: list[pd.DataFrame] = []
        for name, df in dfs.items():
            if "value" in df.columns:
                renamed.append(df[["value"]].rename(columns={"value": name}))
            else:
                # If no 'value' column, use first column
                col = df.columns[0]
                renamed.append(df[[col]].rename(columns={col: name}))

        result = pd.concat(renamed, axis=1)
        return result

    def clear_cache(self, *, symbol: str | None = None) -> None:
        """
        Clear cached data.

        Parameters
        ----------
        symbol : str | None, optional
            If provided, only clear cache for this catalog symbol.
            If None, clears all cached data.
        """
        if symbol is not None:
            entry = self._catalog.get(symbol)
            self._cache.clear(source=entry.source, symbol=entry.symbol)
            logger.info("cache_cleared: symbol=%s", symbol)
        else:
            self._cache.clear()
            logger.info("cache_cleared: all")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a catalog symbol.

        Frequency is inferred from cached data if available, otherwise None.

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

        logger.debug(
            "get_metadata: symbol=%s, source=%s",
            symbol,
            entry.source,
        )

        # Infer frequency from source metadata or return None
        inferred_freq = source_meta.get("frequency")

        # Combine with catalog info (catalog takes precedence)
        return {
            **source_meta,
            "my_name": entry.my_name,
            "source": entry.source,
            "symbol": entry.symbol,
            "frequency": inferred_freq,
            "field": entry.field,
            "description": entry.description,
            "unit": entry.unit,
        }

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
        kwargs: dict[str, str] = {}
        if field is not None:
            kwargs["field"] = field

        logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
        df = source_adapter.fetch(symbol, start, end, **kwargs)

        # Store in cache
        if use_cache:
            self._cache.put(
                source=source,
                symbol=symbol,
                field=field,
                start_date=start,
                end_date=end,
                data=df,
            )

        return df

    def close(self) -> None:
        """
        Close the cache connection.

        Should be called when the client is no longer needed to release
        database resources. Alternatively, use the client as a context manager.

        Examples
        --------
        >>> client = Client(catalog="catalog.yaml")
        >>> try:
        ...     df = client.get(["GDP"], start="2020-01-01", end="2024-12-31")
        ... finally:
        ...     client.close()
        """
        self._cache.close()
        logger.debug("client_closed")

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit context manager, closing cache connection."""
        self.close()
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "refactor(client): replace FrequencyMismatchError with index alignment warnings"
```

---

## Task 5: Update Public API Exports

**Files:**
- Modify: `src/metapyle/__init__.py`
- Modify: `tests/unit/test_public_api.py`

**Step 1: Write updated test file (remove FrequencyMismatchError from exports)**

```python
"""Test that public API is correctly exported."""


def test_public_api_exports() -> None:
    """All public symbols should be importable from metapyle."""
    from metapyle import (
        BaseSource,
        CatalogError,
        CatalogValidationError,
        Client,
        DuplicateNameError,
        FetchError,
        MetapyleError,
        NoDataError,
        SymbolNotFoundError,
        UnknownSourceError,
        register_source,
    )

    assert Client is not None
    assert BaseSource is not None
    assert register_source is not None
    assert MetapyleError is not None
    assert CatalogError is not None
    assert CatalogValidationError is not None
    assert DuplicateNameError is not None
    assert FetchError is not None
    assert NoDataError is not None
    assert SymbolNotFoundError is not None
    assert UnknownSourceError is not None


def test_frequency_mismatch_error_not_exported() -> None:
    """FrequencyMismatchError should no longer be exported."""
    import metapyle

    assert not hasattr(metapyle, "FrequencyMismatchError")


def test_frequency_enum_not_exported() -> None:
    """Frequency enum should no longer be exported."""
    import metapyle

    assert not hasattr(metapyle, "Frequency")


def test_version_available() -> None:
    """Package version should be available."""
    import metapyle

    assert hasattr(metapyle, "__version__")
    assert metapyle.__version__ == "0.1.0"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_public_api.py -v`
Expected: FAIL - `FrequencyMismatchError` still exported

**Step 3: Update __init__.py**

```python
"""Metapyle - Unified interface for financial time-series data."""

__version__ = "0.1.0"

from metapyle.client import Client
from metapyle.exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
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
    "NoDataError",
    "SymbolNotFoundError",
    "UnknownSourceError",
]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_public_api.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/__init__.py tests/unit/test_public_api.py
git commit -m "refactor(api): remove Frequency and FrequencyMismatchError from public exports"
```

---

## Task 6: Run Full Test Suite and Verify

**Files:**
- None (verification only)

**Step 1: Run all unit tests**

Run: `pytest tests/unit/ -v`
Expected: ALL PASS

**Step 2: Run linting**

Run: `ruff check src/ tests/`
Expected: No errors

**Step 3: Run type checking**

Run: `mypy src/`
Expected: No errors (or only pre-existing errors)

**Step 4: Run formatting check**

Run: `ruff format --check src/ tests/`
Expected: No formatting issues

**Step 5: Final commit (if any fixes needed)**

If any issues found, fix and commit:
```bash
git add -A
git commit -m "fix: address linting/typing issues from frequency redesign"
```

---

## Summary

| Task | Description | Files Modified |
|------|-------------|----------------|
| 1 | Remove FrequencyMismatchError | `exceptions.py`, `test_exceptions.py` |
| 2 | Remove Frequency enum and update CatalogEntry | `catalog.py`, `test_catalog.py` |
| 3 | Simplify processing module | `processing.py`, `test_processing.py` |
| 4 | Update Client with index alignment warnings | `client.py`, `test_client.py` |
| 5 | Update public API exports | `__init__.py`, `test_public_api.py` |
| 6 | Full verification | None |

**Total estimated time:** 30-45 minutes