"""Unit tests for BloombergSource adapter."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import _global_registry
from metapyle.sources.bloomberg import BloombergSource


class TestBloombergSourceFetchSingleTicker:
    """Tests for fetching single ticker data."""

    def test_bloomberg_source_fetch_single_ticker(self) -> None:
        """Test fetching data for a single Bloomberg ticker."""
        # Create mock DataFrame with MultiIndex columns (as bdh returns)
        mock_df = pd.DataFrame(
            {("SPX Index", "PX_LAST"): [5000.0, 5001.0]},
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        )
        mock_df.columns = pd.MultiIndex.from_tuples([("SPX Index", "PX_LAST")])

        mock_blp = MagicMock()
        mock_blp.bdh.return_value = mock_df

        with patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp):
            source = BloombergSource()
            result = source.fetch("SPX Index", "2024-01-02", "2024-01-03")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "value" in result.columns
            assert isinstance(result.index, pd.DatetimeIndex)
            assert result.iloc[0]["value"] == 5000.0
            assert result.iloc[1]["value"] == 5001.0

            mock_blp.bdh.assert_called_once_with("SPX Index", "PX_LAST", "2024-01-02", "2024-01-03")


class TestBloombergSourceFetchCustomField:
    """Tests for fetching with custom field."""

    def test_bloomberg_source_fetch_custom_field(self) -> None:
        """Test fetching data with a custom Bloomberg field."""
        mock_df = pd.DataFrame(
            {("AAPL US Equity", "PX_OPEN"): [150.0, 151.0]},
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        )
        mock_df.columns = pd.MultiIndex.from_tuples([("AAPL US Equity", "PX_OPEN")])

        mock_blp = MagicMock()
        mock_blp.bdh.return_value = mock_df

        with patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp):
            source = BloombergSource()
            result = source.fetch("AAPL US Equity", "2024-01-02", "2024-01-03", field="PX_OPEN")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "value" in result.columns
            assert result.iloc[0]["value"] == 150.0

            mock_blp.bdh.assert_called_once_with(
                "AAPL US Equity", "PX_OPEN", "2024-01-02", "2024-01-03"
            )


class TestBloombergSourceEmptyResponseRaises:
    """Tests for empty response handling."""

    def test_bloomberg_source_empty_response_raises(self) -> None:
        """Test that NoDataError is raised when Bloomberg returns empty data."""
        empty_df = pd.DataFrame()

        mock_blp = MagicMock()
        mock_blp.bdh.return_value = empty_df

        with patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp):
            source = BloombergSource()

            with pytest.raises(NoDataError, match="No data returned"):
                source.fetch("INVALID Index", "2024-01-01", "2024-01-31")


class TestBloombergSourceApiErrorRaises:
    """Tests for API error handling."""

    def test_bloomberg_source_api_error_raises(self) -> None:
        """Test that FetchError is raised when Bloomberg API fails."""
        mock_blp = MagicMock()
        mock_blp.bdh.side_effect = RuntimeError("Connection failed")

        with patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp):
            source = BloombergSource()

            with pytest.raises(FetchError, match="Bloomberg API error"):
                source.fetch("SPX Index", "2024-01-01", "2024-01-31")

    def test_bloomberg_source_xbbg_not_available_raises(self) -> None:
        """Test that FetchError is raised when xbbg is not installed."""
        with patch("metapyle.sources.bloomberg._get_blp", return_value=None):
            source = BloombergSource()

            with pytest.raises(FetchError, match="xbbg package is not installed"):
                source.fetch("SPX Index", "2024-01-01", "2024-01-31")


class TestBloombergSourceGetMetadata:
    """Tests for get_metadata method."""

    def test_bloomberg_source_get_metadata(self) -> None:
        """Test metadata retrieval for a Bloomberg symbol."""
        # Mock _get_blp to avoid triggering real xbbg import
        with patch("metapyle.sources.bloomberg._get_blp", return_value=None):
            source = BloombergSource()
            metadata = source.get_metadata("SPX Index")

            assert metadata["source"] == "bloomberg"
            assert metadata["symbol"] == "SPX Index"
            assert "xbbg_available" in metadata


class TestBloombergSourceIsRegistered:
    """Tests for source registration."""

    def test_bloomberg_source_is_registered(self) -> None:
        """Test that BloombergSource is registered with the global registry."""
        # Verify the source is registered
        registered_sources = _global_registry.list_sources()
        assert "bloomberg" in registered_sources

        # Verify we can get an instance
        source = _global_registry.get("bloomberg")
        assert isinstance(source, BloombergSource)
