"""Unit tests for Catalog and CatalogEntry."""

import pytest

from metapyle.catalog import CatalogEntry, Frequency


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
