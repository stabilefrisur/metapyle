"""Unit tests for Client class."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from metapyle.client import Client
from metapyle.exceptions import SymbolNotFoundError, UnknownSourceError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

# ============================================================================
# Mock Source Fixtures
# ============================================================================


@register_source("mock")
class MockSource(BaseSource):
    """Mock source for testing."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Return mock data based on symbols."""
        dates = pd.date_range(start, end, freq="D")
        result = pd.DataFrame(index=dates)
        for req in requests:
            data = list(range(len(dates)))
            result[req.symbol] = data
        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return mock metadata."""
        return {"symbol": symbol, "description": f"Mock data for {symbol}"}


@register_source("mock_monthly")
class MockMonthlySource(BaseSource):
    """Mock source that returns monthly data."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Return mock monthly data."""
        dates = pd.date_range(start, end, freq="ME")
        result = pd.DataFrame(index=dates)
        for req in requests:
            data = list(range(len(dates)))
            result[req.symbol] = data
        return result

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


def test_client_clear_cache_specific_source(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.clear_cache() can clear a specific source."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    # Populate cache
    client.get(["TEST_DAILY"], start="2024-01-01", end="2024-01-10")

    # Verify cached
    entry1 = client._catalog.get("TEST_DAILY")
    cached1 = client._cache.get(
        source=entry1.source,
        symbol=entry1.symbol,
        field=entry1.field,
        path=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached1 is not None

    # Clear cache for the source
    client.clear_cache(source=entry1.source)

    # Verify cleared
    cached2 = client._cache.get(
        source=entry1.source,
        symbol=entry1.symbol,
        field=entry1.field,
        path=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
    )
    assert cached2 is None


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
    assert "RAW_SYMBOL" in df.columns  # Column named by symbol
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


# ============================================================================
# Client Default End Date Tests
# ============================================================================


def test_client_get_end_defaults_to_today(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get() defaults end to today when not specified."""
    import datetime

    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)
    today = datetime.date.today()

    # end parameter omitted - should default to today
    df = client.get(["TEST_DAILY"], start="2024-01-01")

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    # Last date should be today (or earlier if source doesn't have today's data)
    assert df.index[-1].date() <= today


def test_client_get_raw_end_defaults_to_today(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get_raw() defaults end to today when not specified."""
    import datetime

    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)
    today = datetime.date.today()

    # end parameter omitted - should default to today
    df = client.get_raw(source="mock", symbol="RAW_SYMBOL", start="2024-01-01")

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    # Last date should be today (or earlier if source doesn't have today's data)
    assert df.index[-1].date() <= today


def test_client_get_renames_to_my_name(tmp_path: Path) -> None:
    """Client.get() renames source column to my_name."""
    # Create test CSV
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("date,GDP_US\n2024-01-01,100.0\n2024-01-02,101.0\n")

    # Create catalog with path
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(f"""
- my_name: gdp_us
  source: localfile
  symbol: GDP_US
  path: {csv_path}
""")

    client = Client(catalog=str(catalog_path), cache_enabled=False)
    df = client.get(["gdp_us"], start="2024-01-01", end="2024-01-02")

    assert "gdp_us" in df.columns
    assert "GDP_US" not in df.columns
    assert "value" not in df.columns
    client.close()


def test_client_get_raw_with_path(tmp_path: Path) -> None:
    """Client.get_raw() accepts path parameter for localfile."""
    # Create test CSV
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("date,GDP_US\n2024-01-01,100.0\n2024-01-02,101.0\n")

    # Create minimal catalog (required for Client init)
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text("""
- my_name: dummy
  source: localfile
  symbol: dummy
  path: /dummy
""")

    client = Client(catalog=str(catalog_path), cache_enabled=False)
    df = client.get_raw(
        source="localfile",
        symbol="GDP_US",
        start="2024-01-01",
        end="2024-01-02",
        path=str(csv_path),
    )

    # get_raw returns original column name
    assert "GDP_US" in df.columns
    assert "value" not in df.columns
    client.close()


def test_client_get_raw_bloomberg_returns_symbol_field(
    tmp_path: Path,
    mocker: Any,
) -> None:
    """Client.get_raw() for Bloomberg returns symbol::field column name."""
    # Mock Bloomberg
    mock_blp = mocker.MagicMock()
    mock_blp.bdh.return_value = pd.DataFrame(
        {("SPX Index", "PX_LAST"): [100.0, 101.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    mocker.patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp)

    # Create minimal catalog
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text("""
- my_name: dummy
  source: bloomberg
  symbol: dummy
  field: PX_LAST
""")

    client = Client(catalog=str(catalog_path), cache_enabled=False)
    df = client.get_raw(
        source="bloomberg",
        symbol="SPX Index",
        start="2024-01-01",
        end="2024-01-02",
        field="PX_LAST",
    )

    assert "SPX Index::PX_LAST" in df.columns  # Double colon separator
    client.close()


# ============================================================================
# Client Params Tests
# ============================================================================


# Track params received by mock source
_captured_requests: list[FetchRequest] = []


@register_source("mock_with_params")
class MockSourceWithParams(BaseSource):
    """Mock source that captures params from FetchRequest."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Capture requests and return mock data."""
        _captured_requests.clear()
        _captured_requests.extend(requests)

        dates = pd.date_range(start, end, freq="D")
        result = pd.DataFrame(index=dates)
        for req in requests:
            result[req.symbol] = list(range(len(dates)))
        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return mock metadata."""
        return {"symbol": symbol}


class TestColumnOrder:
    """Tests for preserving column order."""

    def test_fetch_preserves_column_order(self, tmp_path: Path) -> None:
        """Columns should appear in same order as input symbols."""
        catalog_yaml = tmp_path / "catalog.yaml"
        catalog_yaml.write_text(
            """
            - my_name: z_series
              source: mock
              symbol: Z_SYMBOL
            - my_name: a_series
              source: mock
              symbol: A_SYMBOL
            - my_name: m_series
              source: mock
              symbol: M_SYMBOL
            """
        )

        client = Client(catalog=str(catalog_yaml), cache_enabled=False)

        # Request in specific order: z, a, m
        df = client.get(["z_series", "a_series", "m_series"], "2024-01-01", "2024-01-05")

        assert list(df.columns) == ["z_series", "a_series", "m_series"]

        # Request in reverse order should also preserve order
        df2 = client.get(["m_series", "a_series", "z_series"], "2024-01-01", "2024-01-05")
        assert list(df2.columns) == ["m_series", "a_series", "z_series"]


class TestClientParams:
    """Tests for Client passing params to source."""

    def test_get_passes_params_to_fetch_request(self, tmp_path: Path) -> None:
        """Client passes catalog params to FetchRequest."""
        # Create catalog with params
        yaml_content = """
- my_name: test_series
  source: mock_with_params
  symbol: TEST
  params:
    tenor: 3m
    location: NYC
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        client = Client(catalog=yaml_file, cache_enabled=False)
        client.get(["test_series"], start="2024-01-01", end="2024-01-02")

        # Verify fetch was called with params
        assert len(_captured_requests) == 1
        assert _captured_requests[0].params == {"tenor": "3m", "location": "NYC"}

        client.close()

    def test_get_passes_none_params_when_not_specified(self, tmp_path: Path) -> None:
        """Client passes None for params when not specified in catalog."""
        # Create catalog without params
        yaml_content = """
- my_name: test_series_no_params
  source: mock_with_params
  symbol: TEST_NO_PARAMS
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        client = Client(catalog=yaml_file, cache_enabled=False)
        client.get(["test_series_no_params"], start="2024-01-01", end="2024-01-02")

        # Verify fetch was called with params=None
        assert len(_captured_requests) == 1
        assert _captured_requests[0].params is None

        client.close()


class TestGetRawParams:
    """Tests for get_raw params parameter."""

    def test_get_raw_accepts_params(self, tmp_path: Path) -> None:
        """get_raw should accept params parameter."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "date,test\n2024-01-01,100\n2024-01-02,101\n2024-01-03,102\n"
        )

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: test
  source: localfile
  symbol: test
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            # get_raw should accept params without error
            df = client.get_raw(
                source="localfile",
                symbol="test",
                start="2024-01-01",
                end="2024-01-03",
                path=str(csv_file),
                params={"some_param": "value"},  # Should be accepted
            )
            assert len(df) == 3
