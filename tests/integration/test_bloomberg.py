"""Integration tests for Bloomberg source."""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from metapyle import Client
from tests.integration.conftest import BLOOMBERG_CATALOG

pytestmark = [pytest.mark.integration, pytest.mark.bloomberg]


class TestBloombergSingleSeries:
    """Test single series fetch from Bloomberg."""

    def test_single_series(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close and verify DataFrame structure."""
        df = bloomberg_client.get(["sp500_close"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) > 0


class TestBloombergMultipleFields:
    """Test multiple fields of same symbol."""

    def test_multiple_fields_same_symbol(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close and sp500_volume (same symbol, different fields)."""
        df = bloomberg_client.get(
            ["sp500_close", "sp500_volume"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "sp500_volume" in df.columns
        assert len(df.columns) == 2


class TestBloombergFrequencyAlignment:
    """Test frequency alignment with Bloomberg data."""

    def test_frequency_alignment(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch daily and monthly data with alignment to month-end."""
        df = bloomberg_client.get(
            ["sp500_close", "us_cpi_yoy"],  # daily + monthly
            start=test_start,
            end=test_end,
            frequency="ME",  # month-end
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "us_cpi_yoy" in df.columns
        # Month-end frequency should have ~12 rows for 2024
        assert len(df) >= 10
        assert len(df) <= 14


class TestBloombergRawAndMetadata:
    """Test get_raw and get_metadata for Bloomberg."""

    def test_get_raw(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Ad-hoc query using get_raw."""
        df = bloomberg_client.get_raw(
            source="bloomberg",
            symbol="SPX Index",
            field="PX_LAST",
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "SPX Index::PX_LAST" in df.columns

    def test_get_metadata(
        self,
        bloomberg_client: Client,
    ) -> None:
        """Verify metadata retrieval."""
        meta = bloomberg_client.get_metadata("sp500_close")

        assert isinstance(meta, dict)
        assert meta["my_name"] == "sp500_close"
        assert meta["source"] == "bloomberg"
        assert meta["symbol"] == "SPX Index"


class TestBloombergCacheAndRecent:
    """Test caching and recent data access."""

    def test_cache_hit(
        self,
        test_start: str,
        test_end: str,
        tmp_path: Path,
    ) -> None:
        """Fetch twice and verify cache is used."""
        cache_path = tmp_path / "test_cache.db"
        client = Client(
            catalog=str(BLOOMBERG_CATALOG),
            cache_enabled=True,
            cache_path=str(cache_path),
        )

        # First fetch (cache miss)
        start_time = time.time()
        df1 = client.get(["sp500_close"], start=test_start, end=test_end)
        first_duration = time.time() - start_time

        # Second fetch (cache hit - should be faster)
        start_time = time.time()
        df2 = client.get(["sp500_close"], start=test_start, end=test_end)
        second_duration = time.time() - start_time

        assert df1.equals(df2)
        # Cache hit should be significantly faster
        assert second_duration < first_duration

        client.close()

    def test_recent_data(
        self,
        bloomberg_client: Client,
    ) -> None:
        """Fetch last 7 days to confirm credentials are current."""
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        df = bloomberg_client.get(["sp500_close"], start=start, end=end)

        assert isinstance(df, pd.DataFrame)
        # May be empty on weekends/holidays, but should not error
