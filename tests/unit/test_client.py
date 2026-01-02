"""Unit tests for Client class."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from metapyle.client import Client
from metapyle.exceptions import NameNotFoundError, UnknownSourceError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

# ============================================================================
# Mock Source Fixtures
# ============================================================================


_captured_kwargs: dict[str, Any] = {}


@register_source("mock_kwargs_capture")
class MockKwargsCaptureSource(BaseSource):
    """Mock source that captures **kwargs from fetch()."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Capture kwargs and return mock data."""
        _captured_kwargs.clear()
        _captured_kwargs.update(kwargs)

        dates = pd.date_range(start, end, freq="D")
        result = pd.DataFrame(index=dates)
        for req in requests:
            result[req.symbol] = list(range(len(dates)))
        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return mock metadata."""
        return {"symbol": symbol}


@register_source("mock")
class MockSource(BaseSource):
    """Mock source for testing."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        **kwargs: Any,
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
        **kwargs: Any,
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


def test_client_get_unknown_name_raises(catalog_yaml: Path, cache_path: str) -> None:
    """Client.get() raises NameNotFoundError for unknown name."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with pytest.raises(NameNotFoundError, match="UNKNOWN"):
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


def test_client_get_metadata_unknown_name_raises(
    catalog_yaml: Path,
    cache_path: str,
) -> None:
    """Client.get_metadata() raises NameNotFoundError for unknown name."""
    client = Client(catalog=str(catalog_yaml), cache_path=cache_path)

    with pytest.raises(NameNotFoundError, match="UNKNOWN"):
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
        **kwargs: Any,
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
        csv_file.write_text("date,test\n2024-01-01,100\n2024-01-02,101\n2024-01-03,102\n")

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


class TestClientKwargsPassthrough:
    """Tests for Client passing unified_options to sources."""

    def test_get_passes_unified_options_to_source(self, tmp_path: Path) -> None:
        """Client.get() passes unified and unified_options to source.fetch()."""
        yaml_content = """
- my_name: test_unified
  source: mock_kwargs_capture
  symbol: TEST_SYMBOL
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        client = Client(catalog=yaml_file, cache_enabled=False)
        client.get(
            ["test_unified"],
            start="2024-01-01",
            end="2024-01-02",
            unified=True,
            unified_options={"currency": "EUR"},
        )

        assert _captured_kwargs.get("unified") is True
        assert _captured_kwargs.get("unified_options") == {"currency": "EUR"}

        client.close()


def test_get_accepts_unified_options_parameter() -> None:
    """Test get() accepts unified_options as explicit parameter (not kwargs)."""
    # This tests the signature change - unified_options should be explicit, not via **kwargs
    from unittest.mock import Mock, patch

    with patch("metapyle.client.Catalog") as mock_catalog_cls:
        mock_catalog = Mock()
        mock_catalog.__len__ = Mock(return_value=0)
        mock_catalog.validate_sources = Mock()
        mock_catalog_cls.from_yaml.return_value = mock_catalog

        # Make catalog.get raise so we can test signature without full fetch
        mock_catalog.get.side_effect = NameNotFoundError("test")

        client = Client(catalog="test.yaml")

        # Should not raise TypeError for unified_options parameter
        with pytest.raises(NameNotFoundError):
            client.get(
                ["test"],
                start="2024-01-01",
                unified=True,
                unified_options={"currency": "EUR"},
            )


def test_frequency_and_unified_options_frequency_are_independent() -> None:
    """Test that frequency (pandas) and unified_options.frequency don't collide.

    This is the core issue we're solving - before this refactor, passing
    frequency=SeriesFrequency.MONTHLY would be used for both Macrobond
    server-side alignment AND pandas client-side resampling, causing errors.
    """
    from unittest.mock import Mock, patch

    with patch("metapyle.client.Catalog") as mock_catalog_cls:
        mock_catalog = Mock()
        mock_catalog.__len__ = Mock(return_value=1)
        mock_catalog.validate_sources = Mock()
        mock_catalog_cls.from_yaml.return_value = mock_catalog

        # Mock a macrobond entry
        mock_entry = Mock()
        mock_entry.my_name = "test_series"
        mock_entry.source = "macrobond"
        mock_entry.symbol = "usgdp"
        mock_entry.field = None
        mock_entry.path = None
        mock_entry.params = None
        mock_catalog.get.return_value = mock_entry

        client = Client(catalog="test.yaml")

        # Mock the source fetch to return some data
        with patch.object(client, "_fetch_from_source") as mock_fetch:
            mock_df = pd.DataFrame(
                {"usgdp": [100.0, 101.0]},
                index=pd.to_datetime(["2024-01-31", "2024-02-29"]),
            )
            mock_fetch.return_value = mock_df

            # This should work - frequency is pandas string, unified_options has Macrobond enum
            result = client.get(
                ["test_series"],
                start="2024-01-01",
                end="2024-12-31",
                unified=True,
                unified_options={"frequency": "MOCK_ENUM_VALUE"},  # Would be SeriesFrequency
                frequency="ME",  # pandas month-end
            )

            # Verify _fetch_from_source was called with unified_options
            mock_fetch.assert_called_once()
            call_kwargs = mock_fetch.call_args.kwargs
            assert call_kwargs["unified"] is True
            assert call_kwargs["unified_options"] == {"frequency": "MOCK_ENUM_VALUE"}

            # Result should exist (frequency alignment would have run)
            assert result is not None


class TestAssembleDataframeMixedTimezones:
    """Tests for _assemble_dataframe handling mixed timezones."""

    def test_concat_tz_naive_and_tz_aware(self, tmp_path: Path) -> None:
        """Should successfully concat tz-naive and tz-aware DataFrames."""
        # Create minimal catalog (list format required)
        catalog_path = tmp_path / "catalog.yaml"
        catalog_path.write_text("[]")

        client = Client(catalog=catalog_path, cache_enabled=False)

        # Simulate two DataFrames with different timezone handling
        # (mimics a misbehaving source that returns tz-naive)
        df_naive = pd.DataFrame(
            {"col1": [1.0, 2.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        df_aware = pd.DataFrame(
            {"col2": [3.0, 4.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]).tz_localize("UTC"),
        )

        dfs = {"series_a": df_naive, "series_b": df_aware}

        # Should not raise - safety net normalizes before concat
        result = client._assemble_dataframe(dfs, ["series_a", "series_b"])

        assert list(result.columns) == ["series_a", "series_b"]
        assert str(result.index.tz) == "UTC"
        assert len(result) == 2

        client.close()


class TestClientUnifiedCache:
    """Tests for cache behavior with unified=True."""

    def test_unified_bypasses_cache_for_macrobond(self, tmp_path: Path) -> None:
        """When unified=True and source=macrobond, entries skip cache."""
        # Register a mock "macrobond" source for this test
        from metapyle.sources.base import _global_registry

        @register_source("test_macrobond_unified")
        class TestMacrobondSource(BaseSource):
            """Test source registered as macrobond for cache bypass testing."""

            def fetch(
                self,
                requests: Sequence[FetchRequest],
                start: str,
                end: str,
                **kwargs: Any,
            ) -> pd.DataFrame:
                _captured_kwargs.clear()
                _captured_kwargs.update(kwargs)
                dates = pd.date_range(start, end, freq="D")
                result = pd.DataFrame(index=dates)
                for req in requests:
                    result[req.symbol] = list(range(len(dates)))
                return result

            def get_metadata(self, symbol: str) -> dict[str, Any]:
                return {"symbol": symbol}

        # Temporarily register as "macrobond" (store the class, not an instance)
        original_source = _global_registry._sources.get("macrobond")
        original_instance = _global_registry._instances.get("macrobond")
        _global_registry._sources["macrobond"] = TestMacrobondSource
        _global_registry._instances.pop("macrobond", None)  # Clear cached instance

        try:
            yaml_content = """
- my_name: test_mb
  source: macrobond
  symbol: TEST_MB
"""
            yaml_file = tmp_path / "catalog.yaml"
            yaml_file.write_text(yaml_content)

            cache_path = tmp_path / "cache.db"
            client = Client(catalog=yaml_file, cache_path=str(cache_path), cache_enabled=True)

            # First fetch with unified=True
            client.get(["test_mb"], start="2024-01-01", end="2024-01-02", unified=True)

            # unified=True should trigger fetch (not use cache)
            assert _captured_kwargs.get("unified") is True

            # Clear captured kwargs
            _captured_kwargs.clear()

            # Second fetch with unified=True should NOT hit cache - should fetch again
            client.get(["test_mb"], start="2024-01-01", end="2024-01-02", unified=True)

            # Should have fetched again (kwargs captured = fetch happened)
            assert _captured_kwargs.get("unified") is True

            client.close()
        finally:
            # Restore original macrobond source
            _global_registry._instances.pop("macrobond", None)
            if original_source is not None:
                _global_registry._sources["macrobond"] = original_source
            else:
                _global_registry._sources.pop("macrobond", None)
            if original_instance is not None:
                _global_registry._instances["macrobond"] = original_instance

    def test_unified_true_uses_cache_for_non_macrobond(self, tmp_path: Path) -> None:
        """When unified=True but source is not macrobond, cache still works."""
        yaml_content = """
- my_name: test_other
  source: mock_kwargs_capture
  symbol: TEST_OTHER
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        cache_path = tmp_path / "cache.db"
        client = Client(catalog=yaml_file, cache_path=str(cache_path), cache_enabled=True)

        # First fetch with unified=True (populates cache for non-macrobond)
        client.get(["test_other"], start="2024-01-01", end="2024-01-02", unified=True)
        assert _captured_kwargs.get("unified") is True

        # Clear captured kwargs
        _captured_kwargs.clear()

        # Second fetch should use cache (no fetch happens)
        client.get(["test_other"], start="2024-01-01", end="2024-01-02", unified=True)

        # If cache was used, no fetch happened, so kwargs should be empty
        assert _captured_kwargs == {}

        client.close()

    def test_unified_false_uses_cache(self, tmp_path: Path) -> None:
        """When unified=False, normal caching applies."""
        yaml_content = """
- my_name: test_cached
  source: mock_kwargs_capture
  symbol: TEST_CACHED
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        cache_path = tmp_path / "cache.db"
        client = Client(catalog=yaml_file, cache_path=str(cache_path), cache_enabled=True)

        # First fetch (populates cache)
        client.get(["test_cached"], start="2024-01-01", end="2024-01-02", unified=False)

        # Clear captured kwargs
        _captured_kwargs.clear()

        # Second fetch should use cache (no fetch - kwargs not captured)
        client.get(["test_cached"], start="2024-01-01", end="2024-01-02", unified=False)

        # If cache was used, no fetch happened, so kwargs should be empty
        assert _captured_kwargs == {}

        client.close()
