"""Unit tests for Catalog and CatalogEntry."""

from pathlib import Path

import pytest

from metapyle.catalog import Catalog, CatalogEntry, Frequency
from metapyle.exceptions import (
    CatalogValidationError,
    DuplicateNameError,
    SymbolNotFoundError,
)


def test_frequency_enum_values() -> None:
    """Frequency enum should have expected values."""
    assert Frequency.DAILY.value == "daily"
    assert Frequency.WEEKLY.value == "weekly"
    assert Frequency.MONTHLY.value == "monthly"
    assert Frequency.QUARTERLY.value == "quarterly"
    assert Frequency.ANNUAL.value == "annual"


def test_frequency_is_str_enum() -> None:
    """Frequency values should be usable as strings."""
    assert f"frequency is {Frequency.DAILY}" == "frequency is daily"


def test_catalog_entry_required_fields() -> None:
    """CatalogEntry requires my_name, source, symbol, frequency."""
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
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
        frequency=Frequency.QUARTERLY,
    )

    # Slots-based classes don't have __dict__
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


def test_catalog_load_missing_required_field(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for missing required fields."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  # missing symbol and frequency
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="symbol"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_invalid_frequency(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for invalid frequency."""
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


def test_catalog_load_duplicate_names(tmp_path: Path) -> None:
    """Catalog raises DuplicateNameError for duplicate my_name."""
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


def test_catalog_load_multiple_files(tmp_path: Path) -> None:
    """Catalog can load and merge multiple YAML files."""
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


def test_catalog_load_duplicate_across_files(tmp_path: Path) -> None:
    """Catalog raises DuplicateNameError for duplicates across files."""
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
    catalog = Catalog({})

    with pytest.raises(SymbolNotFoundError, match="UNKNOWN"):
        catalog.get("UNKNOWN")


def test_catalog_list_names(tmp_path: Path) -> None:
    """Catalog can list all entry names."""
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
    with pytest.raises(CatalogValidationError, match="not found"):
        Catalog.from_yaml("/nonexistent/path/catalog.yaml")


def test_catalog_malformed_yaml(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for malformed YAML."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  bad_key: [unclosed bracket
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="YAML"):
        Catalog.from_yaml(str(yaml_file))
