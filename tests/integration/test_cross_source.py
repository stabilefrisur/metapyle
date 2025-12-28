"""Integration tests for cross-source queries."""

import pandas as pd
import pytest

from metapyle import Client

pytestmark = [pytest.mark.integration, pytest.mark.bloomberg, pytest.mark.macrobond]


class TestCrossSourceSameFrequency:
    """Test cross-source queries with same frequency."""

    def test_cross_source_same_frequency(
        self,
        combined_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close (BBG) and sp500_mb (MB) - both daily."""
        df = combined_client.get(
            ["sp500_close", "sp500_mb"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "sp500_mb" in df.columns


class TestCrossSourceDifferentFrequency:
    """Test cross-source queries with different frequencies."""

    def test_cross_source_different_frequency(
        self,
        combined_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close (daily) and us_gdp_mb (quarterly) - outer join."""
        df = combined_client.get(
            ["sp500_close", "us_gdp_mb"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "us_gdp_mb" in df.columns

    def test_cross_source_aligned(
        self,
        combined_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch with frequency alignment to business daily."""
        df = combined_client.get(
            ["sp500_close", "us_gdp_mb"],
            start=test_start,
            end=test_end,
            frequency="B",  # business daily
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "us_gdp_mb" in df.columns
        # Both columns should have values (forward-filled for quarterly)
        assert df["sp500_close"].notna().any()
        assert df["us_gdp_mb"].notna().any()
