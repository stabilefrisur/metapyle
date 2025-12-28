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


def test_catalog_entry_path_field() -> None:
    """CatalogEntry supports optional path field."""
    entry = CatalogEntry(
        my_name="gdp_us",
        source="localfile",
        symbol="GDP_US",
        path="/data/macro.csv",
    )
    assert entry.path == "/data/macro.csv"


def test_catalog_entry_path_defaults_none() -> None:
    """CatalogEntry path defaults to None."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="SPX Index",
    )
    assert entry.path is None


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


def test_catalog_load_from_yaml_with_path_object(tmp_path: Path) -> None:
    """Catalog.from_yaml() accepts Path objects."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    # Pass Path object instead of string
    catalog = Catalog.from_yaml(yaml_file)

    assert len(catalog) == 1
    assert "GDP_US" in catalog


def test_catalog_load_from_yaml_with_mixed_path_types(tmp_path: Path) -> None:
    """Catalog.from_yaml() accepts mixed str and Path in list."""
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

    # Mix Path and str
    catalog = Catalog.from_yaml([file1, str(file2)])

    assert len(catalog) == 2


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
