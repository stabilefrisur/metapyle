"""Tests for MacrobondSource."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import FetchRequest
from metapyle.sources.macrobond import MacrobondSource


@pytest.fixture
def source() -> MacrobondSource:
    """Create MacrobondSource instance."""
    return MacrobondSource()


def _make_mock_series(name: str, dates: list[str], values: list[float]) -> MagicMock:
    """Create mock Macrobond Series object."""
    mock = MagicMock()
    mock.is_error = False
    mock.primary_name = name
    mock.values_to_pd_data_frame.return_value = pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "value": values,
        }
    )
    return mock


class TestMacrobondSourceFetch:
    """Tests for MacrobondSource.fetch()."""

    def test_single_request(self, source: MacrobondSource) -> None:
        """Fetch single series."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert list(df.columns) == ["usgdp"]
            assert len(df) == 2
            mock_mda.get_series.assert_called_once_with(["usgdp"])

    def test_multiple_requests(self, source: MacrobondSource) -> None:
        """Fetch multiple series in single call."""
        mock_series_1 = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )
        mock_series_2 = _make_mock_series(
            "gbgdp",
            ["2024-01-01", "2024-01-02"],
            [200.0, 201.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series_1, mock_series_2]
            mock_get_mda.return_value = mock_mda

            requests = [
                FetchRequest(symbol="usgdp"),
                FetchRequest(symbol="gbgdp"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert "usgdp" in df.columns
            assert "gbgdp" in df.columns
            mock_mda.get_series.assert_called_once_with(["usgdp", "gbgdp"])

    def test_date_filtering(self, source: MacrobondSource) -> None:
        """Only return data within date range."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02", "2024-01-03"],
            [100.0, 101.0, 102.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            df = source.fetch(requests, "2024-01-02", "2024-01-02")

            assert len(df) == 1
            assert df["usgdp"].iloc[0] == 101.0

    def test_mda_not_available(self, source: MacrobondSource) -> None:
        """Raise FetchError when macrobond_data_api not installed."""
        with patch("metapyle.sources.macrobond._get_mda", return_value=None):
            requests = [FetchRequest(symbol="usgdp")]
            with pytest.raises(FetchError, match="macrobond"):
                source.fetch(requests, "2024-01-01", "2024-01-02")

    def test_no_data_in_range(self, source: MacrobondSource) -> None:
        """Raise NoDataError when no data in date range."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2020-01-01"],
            [100.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            with pytest.raises(NoDataError):
                source.fetch(requests, "2024-01-01", "2024-12-31")

    def test_series_error(self, source: MacrobondSource) -> None:
        """Raise FetchError when series has error."""
        mock_series = MagicMock()
        mock_series.is_error = True
        mock_series.error_message = "Series not found"
        mock_series.primary_name = "invalid"

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="invalid")]
            with pytest.raises(FetchError, match="not found"):
                source.fetch(requests, "2024-01-01", "2024-01-02")


class TestMacrobondSourceGetMetadata:
    """Tests for get_metadata."""

    def test_metadata(self, source: MacrobondSource) -> None:
        """get_metadata returns entity metadata."""
        mock_entity = MagicMock()
        mock_entity.metadata = {"title": "US GDP", "frequency": "Q"}

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_entity.return_value = mock_entity
            mock_get_mda.return_value = mock_mda

            meta = source.get_metadata("usgdp")
            assert meta["title"] == "US GDP"

    def test_metadata_mda_not_available(self, source: MacrobondSource) -> None:
        """Raise FetchError when mda not installed."""
        with patch("metapyle.sources.macrobond._get_mda", return_value=None):
            with pytest.raises(FetchError, match="macrobond"):
                source.get_metadata("usgdp")


class TestMacrobondSourceKwargs:
    """Tests for **kwargs handling in MacrobondSource."""

    def test_fetch_accepts_kwargs(self, source: MacrobondSource) -> None:
        """MacrobondSource.fetch() accepts **kwargs without error."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            # Pass kwargs - should be accepted (unified=False uses existing behavior)
            df = source.fetch(requests, "2024-01-01", "2024-01-02", unified=False, currency="EUR")

            assert list(df.columns) == ["usgdp"]
            mock_mda.get_series.assert_called_once_with(["usgdp"])


class TestMacrobondSourceUnified:
    """Tests for unified series support in MacrobondSource."""

    def test_unified_calls_get_unified_series(self, source: MacrobondSource) -> None:
        """When unified=True, fetch() calls get_unified_series()."""
        # Mock the unified series result
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame(
            {
                "usgdp": [100.0, 101.0],
                "gbgdp": [200.0, 201.0],
            },
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )

        # Mock the macrobond enums/types
        mock_enums = MagicMock()
        mock_enums.SeriesFrequency.DAILY = "DAILY"
        mock_enums.SeriesWeekdays.MONDAY_TO_FRIDAY = "MON_FRI"
        mock_enums.CalendarMergeMode.AVAILABLE_IN_ALL = "AVAILABLE_IN_ALL"

        mock_types = MagicMock()
        mock_types.StartOrEndPoint = lambda x: f"StartOrEndPoint({x})"

        with (
            patch("metapyle.sources.macrobond._get_mda") as mock_get_mda,
            patch.dict(
                "sys.modules",
                {
                    "macrobond_data_api.common.enums": mock_enums,
                    "macrobond_data_api.common.types": mock_types,
                },
            ),
        ):
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            requests = [
                FetchRequest(symbol="usgdp"),
                FetchRequest(symbol="gbgdp"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02", unified=True)

            # Verify get_unified_series was called (not get_series)
            mock_mda.get_unified_series.assert_called_once()
            assert mock_mda.get_series.call_count == 0

            assert "usgdp" in df.columns
            assert "gbgdp" in df.columns

    def test_unified_false_calls_get_series(self, source: MacrobondSource) -> None:
        """When unified=False, fetch() calls get_series() (existing behavior)."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            _ = source.fetch(requests, "2024-01-01", "2024-01-02", unified=False)

            # Verify get_series was called (not get_unified_series)
            mock_mda.get_series.assert_called_once_with(["usgdp"])
            assert mock_mda.get_unified_series.call_count == 0

    def test_unified_default_is_false(self, source: MacrobondSource) -> None:
        """When unified not specified, defaults to False (get_series)."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            # No unified kwarg
            _ = source.fetch(requests, "2024-01-01", "2024-01-02")

            mock_mda.get_series.assert_called_once()
            assert mock_mda.get_unified_series.call_count == 0

    def test_unified_uses_hardcoded_defaults(self, source: MacrobondSource) -> None:
        """Unified fetch uses hardcoded defaults for common settings."""
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame(
            {"usgdp": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        # Mock the macrobond enums/types that are imported inside _fetch_unified
        mock_enums = MagicMock()
        mock_enums.SeriesFrequency.DAILY = "DAILY"
        mock_enums.SeriesWeekdays.MONDAY_TO_FRIDAY = "MON_FRI"
        mock_enums.CalendarMergeMode.AVAILABLE_IN_ALL = "AVAILABLE_IN_ALL"

        mock_types = MagicMock()
        mock_types.StartOrEndPoint = lambda x: f"StartOrEndPoint({x})"

        with (
            patch("metapyle.sources.macrobond._get_mda") as mock_get_mda,
            patch.dict(
                "sys.modules",
                {
                    "macrobond_data_api.common.enums": mock_enums,
                    "macrobond_data_api.common.types": mock_types,
                },
            ),
        ):
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            source.fetch(requests, "2024-01-01", "2024-01-02", unified=True)

            # Verify get_unified_series was called with symbols and kwargs
            call_args = mock_mda.get_unified_series.call_args

            # First positional arg should be unpacked symbols
            assert call_args.args == ("usgdp",)

            # Check that default kwargs were passed
            call_kwargs = call_args.kwargs
            assert "frequency" in call_kwargs
            assert "weekdays" in call_kwargs
            assert "calendar_merge_mode" in call_kwargs
            assert "currency" in call_kwargs
            assert call_kwargs["currency"] == "USD"
            assert "start_point" in call_kwargs
            assert "end_point" in call_kwargs

    def test_unified_kwargs_override_defaults(self, source: MacrobondSource) -> None:
        """User kwargs override hardcoded defaults."""
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame(
            {"usgdp": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        # Mock the macrobond enums/types
        mock_enums = MagicMock()
        mock_enums.SeriesFrequency.DAILY = "DAILY"
        mock_enums.SeriesWeekdays.MONDAY_TO_FRIDAY = "MON_FRI"
        mock_enums.CalendarMergeMode.AVAILABLE_IN_ALL = "AVAILABLE_IN_ALL"

        mock_types = MagicMock()
        mock_types.StartOrEndPoint = lambda x: f"StartOrEndPoint({x})"

        with (
            patch("metapyle.sources.macrobond._get_mda") as mock_get_mda,
            patch.dict(
                "sys.modules",
                {
                    "macrobond_data_api.common.enums": mock_enums,
                    "macrobond_data_api.common.types": mock_types,
                },
            ),
        ):
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            # Override currency default
            source.fetch(requests, "2024-01-01", "2024-01-02", unified=True, currency="EUR")

            call_kwargs = mock_mda.get_unified_series.call_args.kwargs
            # User override should take precedence
            assert call_kwargs["currency"] == "EUR"

    def test_unified_mda_not_available(self, source: MacrobondSource) -> None:
        """Raise FetchError when unified=True but mda not installed."""
        with patch("metapyle.sources.macrobond._get_mda", return_value=None):
            requests = [FetchRequest(symbol="usgdp")]
            with pytest.raises(FetchError, match="macrobond"):
                source.fetch(requests, "2024-01-01", "2024-01-02", unified=True)


class TestMacrobondSourceIsRegistered:
    """Tests for source registration."""

    def test_registered(self) -> None:
        """MacrobondSource is registered as 'macrobond'."""
        from metapyle.sources.base import _global_registry

        source = _global_registry.get("macrobond")
        assert isinstance(source, MacrobondSource)
