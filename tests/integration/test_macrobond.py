"""Integration tests for Macrobond source."""

import time
from pathlib import Path

import pandas as pd
import pytest

from metapyle import Client
from tests.integration.conftest import MACROBOND_CATALOG

pytestmark = [pytest.mark.integration, pytest.mark.macrobond]


class TestMacrobondSingleSeries:
    """Test single series fetch from Macrobond."""

    def test_single_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_mb and verify DataFrame structure."""
        df = macrobond_client.get(["sp500_mb"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) > 0


class TestMacrobondFrequencyAlignment:
    """Test frequency alignment with Macrobond data."""

    def test_frequency_alignment_client(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch daily and quarterly data with client-side alignment."""
        df = macrobond_client.get(
            ["sp500_mb", "us_gdp_mb"],  # daily + quarterly
            start=test_start,
            end=test_end,
            frequency="ME",  # month-end alignment
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert "us_gdp_mb" in df.columns


class TestMacrobondRawAndMetadata:
    """Test get_raw and get_metadata for Macrobond."""

    def test_get_raw(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Ad-hoc query using get_raw."""
        df = macrobond_client.get_raw(
            source="macrobond",
            symbol="usgdp",
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_metadata(
        self,
        macrobond_client: Client,
    ) -> None:
        """Verify metadata retrieval."""
        meta = macrobond_client.get_metadata("sp500_mb")

        assert isinstance(meta, dict)
        assert meta["my_name"] == "sp500_mb"
        assert meta["source"] == "macrobond"


@pytest.mark.private
class TestMacrobondPrivateSeries:
    """Test private/in-house Macrobond series."""

    def test_private_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch private cmbs_bbb series."""
        df = macrobond_client.get(["cmbs_bbb"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "cmbs_bbb" in df.columns


class TestMacrobondCache:
    """Test caching for Macrobond."""

    def test_cache_hit(
        self,
        test_start: str,
        test_end: str,
        tmp_path: Path,
    ) -> None:
        """Fetch twice and verify cache is used."""
        cache_path = tmp_path / "test_cache.db"
        client = Client(
            catalog=str(MACROBOND_CATALOG),
            cache_enabled=True,
            cache_path=str(cache_path),
        )

        # First fetch (cache miss)
        start_time = time.time()
        df1 = client.get(["sp500_mb"], start=test_start, end=test_end)
        first_duration = time.time() - start_time

        # Second fetch (cache hit - should be faster)
        start_time = time.time()
        df2 = client.get(["sp500_mb"], start=test_start, end=test_end)
        second_duration = time.time() - start_time

        assert df1.equals(df2)
        # Cache hit should be significantly faster
        assert second_duration < first_duration

        client.close()


class TestMacrobondCaseFallback:
    """Test case-insensitive column matching."""

    def test_mixed_case_symbol(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch with mixed-case symbol that Macrobond lowercases."""
        df = macrobond_client.get(
            ["sp500_mb_mixed_case"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb_mixed_case" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)


class TestMacrobondUnified:
    """Integration tests for unified series functionality."""

    def test_unified_basic(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Test basic unified series fetch."""
        df = macrobond_client.get(
            ["sp500_mb", "us_gdp_mb"],  # daily + quarterly
            start=test_start,
            end=test_end,
            unified=True,
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert "us_gdp_mb" in df.columns
        # Unified should produce aligned data (same index for both)
        assert df.index.is_monotonic_increasing

    def test_unified_with_frequency(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Test unified series with explicit frequency override."""
        from macrobond_data_api.common.enums import SeriesFrequency

        df = macrobond_client.get(
            ["sp500_mb", "us_gdp_mb"],  # daily + quarterly
            start=test_start,
            end=test_end,
            unified=True,
            unified_options={"frequency": SeriesFrequency.QUARTERLY},
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # Both columns should be present with quarterly frequency
        assert "sp500_mb" in df.columns
        assert "us_gdp_mb" in df.columns

    def test_unified_single_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Test unified with single series (edge case)."""
        df = macrobond_client.get(
            ["sp500_mb"],
            start=test_start,
            end=test_end,
            unified=True,
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
