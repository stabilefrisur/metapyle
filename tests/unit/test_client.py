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
        frequency="D",  # Changed from "daily"
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
        frequency="D",  # Changed from "daily"
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
        frequency="ME",  # Changed from "monthly"
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

    # First fetch - should populate cache
    df1 = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    # Manually check cache has data
    entry = client._catalog.get("TEST_DAILY")
    cached = client._cache.get(
        source=entry.source,
        symbol=entry.symbol,
        field=entry.field,
        path=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached is not None

    # Second fetch - should use cache
    df2 = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    # Compare values (ignore index frequency metadata which may differ after parquet)
    pd.testing.assert_frame_equal(df1, df2, check_freq=False)


def test_client_bypass_cache(catalog_yaml: Path, cache_path: str) -> None:
    """Client can bypass cache with use_cache=False."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    # First fetch with cache
    df1 = client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    # Second fetch bypassing cache
    df2 = client.get(
        ["TEST_DAILY"],
        start="2024-01-01",
        end="2024-01-10",
        use_cache=False,
    )

    # Both should have data (we can't easily verify cache was bypassed,
    # but the method should work)
    assert len(df1) > 0
    assert len(df2) > 0


def test_client_clear_cache(catalog_yaml: Path, cache_path: str) -> None:
    """Client.clear_cache() clears cached data."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    # Populate cache
    client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    # Clear cache
    client.clear_cache()

    # Verify cache is empty
    entry = client._catalog.get("TEST_DAILY")
    cached = client._cache.get(
        source=entry.source,
        symbol=entry.symbol,
        field=entry.field,
        path=None,
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

    # Populate cache for two symbols
    client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")
    client.get(["TEST_DAILY_2"], start="2024-01-01", end="2024-01-10")

    # Clear cache for one symbol
    client.clear_cache(symbol="TEST_DAILY")

    # Verify TEST_DAILY is cleared
    entry1 = client._catalog.get("TEST_DAILY")
    cached1 = client._cache.get(
        source=entry1.source,
        symbol=entry1.symbol,
        field=entry1.field,
        path=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached1 is None

    # Verify TEST_DAILY_2 is still cached
    entry2 = client._catalog.get("TEST_DAILY_2")
    cached2 = client._cache.get(
        source=entry2.source,
        symbol=entry2.symbol,
        field=entry2.field,
        path=None,
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
    # frequency is now inferred from source metadata
    assert "frequency" in metadata


def test_client_get_metadata_includes_source_info(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get_metadata() includes source-specific metadata."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    metadata = client.get_metadata("TEST_MONTHLY")

    # Catalog info
    assert metadata["my_name"] == "TEST_MONTHLY"
    assert metadata["source"] == "mock_monthly"
    assert metadata["symbol"] == "MOCK_MONTHLY"

    # Source-specific metadata from MockMonthlySource.get_metadata()
    # The source returns {"symbol": symbol, "frequency": "monthly"}
    # Note: source's "frequency" key may be present from the source adapter
    assert "symbol" in metadata  # From source metadata


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

    # First fetch - should populate cache
    df1 = client.get_raw(
        source="mock",
        symbol="RAW_CACHED",
        start="2024-01-01",
        end="2024-01-10",
    )

    # Manually check cache has data
    cached = client._cache.get(
        source="mock",
        symbol="RAW_CACHED",
        field=None,
        path=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached is not None

    # Second fetch - should use cache
    df2 = client.get_raw(
        source="mock",
        symbol="RAW_CACHED",
        start="2024-01-01",
        end="2024-01-10",
    )

    # Compare values (ignore index frequency metadata which may differ after parquet)
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

    # After context exit, cache should be closed
    # (no assertion needed - just verify it doesn't raise)


def test_client_close(catalog_yaml: Path, cache_path: str) -> None:
    """Client.close() closes the cache connection."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)
    client.close()
    # Calling close again should not raise
    client.close()
