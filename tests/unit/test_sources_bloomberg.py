"""Tests for BloombergSource."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import FetchRequest
from metapyle.sources.bloomberg import BloombergSource


@pytest.fixture
def source() -> BloombergSource:
    """Create BloombergSource instance."""
    return BloombergSource()


class TestBloombergSourceFetch:
    """Tests for BloombergSource.fetch()."""

    def test_single_request(self, source: BloombergSource) -> None:
        """Fetch single symbol with field."""
        mock_df = pd.DataFrame(
            {"PX_LAST": [100.0, 101.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        mock_df.columns = pd.MultiIndex.from_tuples([("SPX Index", "PX_LAST")])

        with patch("metapyle.sources.bloomberg._get_blp") as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [FetchRequest(symbol="SPX Index", field="PX_LAST")]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert list(df.columns) == ["SPX Index::PX_LAST"]
            assert len(df) == 2

    def test_multiple_requests_same_field(self, source: BloombergSource) -> None:
        """Fetch multiple symbols with same field."""
        mock_df = pd.DataFrame(
            [[100.0, 200.0], [101.0, 201.0]],
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
            columns=pd.MultiIndex.from_tuples(
                [
                    ("SPX Index", "PX_LAST"),
                    ("AAPL US Equity", "PX_LAST"),
                ]
            ),
        )

        with patch("metapyle.sources.bloomberg._get_blp") as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [
                FetchRequest(symbol="SPX Index", field="PX_LAST"),
                FetchRequest(symbol="AAPL US Equity", field="PX_LAST"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert "SPX Index::PX_LAST" in df.columns
            assert "AAPL US Equity::PX_LAST" in df.columns
            mock_blp.bdh.assert_called_once()

    def test_multiple_fields_same_symbol(self, source: BloombergSource) -> None:
        """Fetch multiple fields for same symbol."""
        mock_df = pd.DataFrame(
            [[100.0, 105.0], [101.0, 106.0]],
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
            columns=pd.MultiIndex.from_tuples(
                [
                    ("SPX Index", "PX_LAST"),
                    ("SPX Index", "PX_HIGH"),
                ]
            ),
        )

        with patch("metapyle.sources.bloomberg._get_blp") as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [
                FetchRequest(symbol="SPX Index", field="PX_LAST"),
                FetchRequest(symbol="SPX Index", field="PX_HIGH"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert "SPX Index::PX_LAST" in df.columns
            assert "SPX Index::PX_HIGH" in df.columns

    def test_default_field(self, source: BloombergSource) -> None:
        """Use PX_LAST as default field when not specified."""
        mock_df = pd.DataFrame(
            {"PX_LAST": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )
        mock_df.columns = pd.MultiIndex.from_tuples([("SPX Index", "PX_LAST")])

        with patch("metapyle.sources.bloomberg._get_blp") as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [FetchRequest(symbol="SPX Index")]  # no field
            df = source.fetch(requests, "2024-01-01", "2024-01-01")

            assert "SPX Index::PX_LAST" in df.columns

    def test_xbbg_not_available(self, source: BloombergSource) -> None:
        """Raise FetchError when xbbg not installed."""
        with patch("metapyle.sources.bloomberg._get_blp", return_value=None):
            requests = [FetchRequest(symbol="SPX Index", field="PX_LAST")]
            with pytest.raises(FetchError, match="xbbg"):
                source.fetch(requests, "2024-01-01", "2024-01-02")

    def test_empty_result_raises(self, source: BloombergSource) -> None:
        """Raise NoDataError when Bloomberg returns empty."""
        with patch("metapyle.sources.bloomberg._get_blp") as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = pd.DataFrame()
            mock_get_blp.return_value = mock_blp

            requests = [FetchRequest(symbol="INVALID", field="PX_LAST")]
            with pytest.raises(NoDataError):
                source.fetch(requests, "2024-01-01", "2024-01-02")


class TestBloombergSourceGetMetadata:
    """Tests for get_metadata."""

    def test_metadata(self, source: BloombergSource) -> None:
        """get_metadata returns basic info."""
        with patch("metapyle.sources.bloomberg._get_blp"):
            meta = source.get_metadata("SPX Index")
            assert meta["source"] == "bloomberg"
            assert meta["symbol"] == "SPX Index"


class TestBloombergSourceIsRegistered:
    """Tests for source registration."""

    def test_registered(self) -> None:
        """BloombergSource is registered as 'bloomberg'."""
        from metapyle.sources.base import _global_registry

        source = _global_registry.get("bloomberg")
        assert isinstance(source, BloombergSource)
