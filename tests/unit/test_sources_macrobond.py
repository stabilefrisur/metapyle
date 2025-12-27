"""Unit tests for MacrobondSource."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.macrobond import MacrobondSource


class TestMacrobondSourceFetch:
    """Tests for MacrobondSource.fetch() method."""

    def test_fetch_returns_dataframe_with_correct_structure(self) -> None:
        """fetch() returns DataFrame with DatetimeIndex and symbol column."""
        # Create mock series object that mimics macrobond_data_api.get_one_series return
        mock_series = MagicMock()
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
                "value": [100.0, 101.0, 102.0],
            }
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            df = source.fetch("usgdp", "2020-01-01", "2020-12-31")

            assert isinstance(df, pd.DataFrame)
            assert isinstance(df.index, pd.DatetimeIndex)
            assert "usgdp" in df.columns
            assert len(df) == 3

    def test_fetch_filters_by_date_range(self) -> None:
        """fetch() filters data to requested start:end range."""
        mock_series = MagicMock()
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2019-12-01", "2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01"]
                ),
                "value": [99.0, 100.0, 101.0, 102.0, 103.0],
            }
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            df = source.fetch("usgdp", "2020-01-01", "2020-02-28")

            assert len(df) == 2
            assert df.index[0] == pd.Timestamp("2020-01-01")
            assert df.index[-1] == pd.Timestamp("2020-02-01")

    def test_fetch_raises_fetch_error_when_mda_not_available(self) -> None:
        """fetch() raises FetchError when macrobond_data_api not installed."""
        with patch("metapyle.sources.macrobond._get_mda", return_value=None):
            source = MacrobondSource()
            with pytest.raises(FetchError, match="macrobond_data_api"):
                source.fetch("usgdp", "2020-01-01", "2020-12-31")

    def test_fetch_raises_no_data_error_when_empty(self) -> None:
        """fetch() raises NoDataError when series returns no data."""
        mock_series = MagicMock()
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime([]),
                "value": [],
            }
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            with pytest.raises(NoDataError):
                source.fetch("usgdp", "2020-01-01", "2020-12-31")

    def test_fetch_raises_fetch_error_on_api_exception(self) -> None:
        """fetch() wraps API exceptions in FetchError."""
        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.side_effect = Exception("API connection failed")
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            with pytest.raises(FetchError, match="API"):
                source.fetch("usgdp", "2020-01-01", "2020-12-31")

    def test_fetch_raises_no_data_error_when_no_data_in_range(self) -> None:
        """fetch() raises NoDataError when data exists but none in requested range."""
        mock_series = MagicMock()
        # Data exists but outside requested range
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2019-01-01", "2019-06-01", "2019-12-01"]),
                "value": [97.0, 98.0, 99.0],
            }
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            with pytest.raises(NoDataError, match="No data in date range"):
                source.fetch("usgdp", "2020-01-01", "2020-12-31")


class TestMacrobondSourceFetchUnified:
    """Tests for MacrobondSource.fetch() with unified=True."""

    def test_fetch_unified_calls_get_unified_series(self) -> None:
        """fetch(unified=True) calls get_unified_series instead of get_one_series."""
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
                "usgdp": [100.0, 101.0],
            }
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            df = source.fetch("usgdp", "2020-01-01", "2020-12-31", unified=True)

            mock_mda.get_unified_series.assert_called_once()
            mock_mda.get_one_series.assert_not_called()
            assert isinstance(df, pd.DataFrame)
            assert "usgdp" in df.columns

    def test_fetch_unified_passes_kwargs(self) -> None:
        """fetch(unified=True) passes kwargs to get_unified_series."""
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2020-01-01"]),
                "usgdp": [100.0],
            }
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            source.fetch(
                "usgdp",
                "2020-01-01",
                "2020-12-31",
                unified=True,
                frequency="annual",
                currency="USD",
            )

            call_kwargs = mock_mda.get_unified_series.call_args
            assert call_kwargs[1].get("frequency") == "annual"
            assert call_kwargs[1].get("currency") == "USD"


class TestMacrobondSourceIsRegistered:
    """Tests for MacrobondSource source registration."""

    def test_macrobond_source_is_registered(self) -> None:
        """MacrobondSource is registered in source registry."""
        from metapyle.sources.base import _global_registry

        assert "macrobond" in _global_registry.list_sources()


class TestMacrobondSourceGetMetadata:
    """Tests for MacrobondSource.get_metadata() method."""

    def test_get_metadata_returns_dict(self) -> None:
        """get_metadata() returns metadata as dict."""
        mock_entity = MagicMock()
        mock_entity.metadata = {
            "FullDescription": "United States, GDP",
            "Frequency": "quarterly",
            "DisplayUnit": "USD",
            "Region": "us",
        }

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_entity.return_value = mock_entity
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            metadata = source.get_metadata("usgdp")

            assert isinstance(metadata, dict)
            assert metadata["FullDescription"] == "United States, GDP"
            assert metadata["Frequency"] == "quarterly"

    def test_get_metadata_raises_fetch_error_when_mda_not_available(self) -> None:
        """get_metadata() raises FetchError when macrobond_data_api not installed."""
        with patch("metapyle.sources.macrobond._get_mda", return_value=None):
            source = MacrobondSource()
            with pytest.raises(FetchError, match="macrobond_data_api"):
                source.get_metadata("usgdp")

    def test_get_metadata_raises_fetch_error_on_api_exception(self) -> None:
        """get_metadata() wraps API exceptions in FetchError."""
        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_entity.side_effect = Exception("Entity not found")
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            with pytest.raises(FetchError, match="metadata"):
                source.get_metadata("invalid_symbol")
