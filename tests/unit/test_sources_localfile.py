"""Tests for LocalFileSource."""

from pathlib import Path

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import FetchRequest
from metapyle.sources.localfile import LocalFileSource


@pytest.fixture
def source() -> LocalFileSource:
    """Create LocalFileSource instance."""
    return LocalFileSource()


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create sample CSV file with multiple columns."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "date,GDP_US,CPI_EU,RATE_JP\n"
        "2024-01-01,100.0,200.0,0.5\n"
        "2024-01-02,101.0,201.0,0.6\n"
        "2024-01-03,102.0,202.0,0.7\n"
    )
    return csv_path


class TestLocalFileSourceFetch:
    """Tests for LocalFileSource.fetch()."""

    def test_single_column(self, source: LocalFileSource, sample_csv: Path) -> None:
        """Fetch single column from CSV."""
        requests = [FetchRequest(symbol="GDP_US", path=str(sample_csv))]
        df = source.fetch(requests, "2024-01-01", "2024-01-03")

        assert list(df.columns) == ["GDP_US"]
        assert len(df) == 3
        assert df["GDP_US"].iloc[0] == 100.0

    def test_multiple_columns(self, source: LocalFileSource, sample_csv: Path) -> None:
        """Fetch multiple columns from CSV in single call."""
        requests = [
            FetchRequest(symbol="GDP_US", path=str(sample_csv)),
            FetchRequest(symbol="CPI_EU", path=str(sample_csv)),
        ]
        df = source.fetch(requests, "2024-01-01", "2024-01-03")

        assert list(df.columns) == ["GDP_US", "CPI_EU"]
        assert len(df) == 3

    def test_date_filtering(self, source: LocalFileSource, sample_csv: Path) -> None:
        """Only return data within date range."""
        requests = [FetchRequest(symbol="GDP_US", path=str(sample_csv))]
        df = source.fetch(requests, "2024-01-02", "2024-01-02")

        assert len(df) == 1
        assert df["GDP_US"].iloc[0] == 101.0

    def test_missing_path_raises(self, source: LocalFileSource) -> None:
        """Raise FetchError if path not provided."""
        requests = [FetchRequest(symbol="GDP_US")]
        with pytest.raises(FetchError, match="path is required"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_different_paths_raises(self, source: LocalFileSource, tmp_path: Path) -> None:
        """Raise FetchError if requests have different paths."""
        requests = [
            FetchRequest(symbol="A", path=str(tmp_path / "a.csv")),
            FetchRequest(symbol="B", path=str(tmp_path / "b.csv")),
        ]
        with pytest.raises(FetchError, match="same path"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_file_not_found_raises(self, source: LocalFileSource, tmp_path: Path) -> None:
        """Raise FetchError if file does not exist."""
        requests = [FetchRequest(symbol="X", path=str(tmp_path / "missing.csv"))]
        with pytest.raises(FetchError, match="not found"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_column_not_found_raises(self, source: LocalFileSource, sample_csv: Path) -> None:
        """Raise FetchError if column not in file."""
        requests = [FetchRequest(symbol="MISSING", path=str(sample_csv))]
        with pytest.raises(FetchError, match="not found"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_no_data_in_range_raises(self, source: LocalFileSource, sample_csv: Path) -> None:
        """Raise NoDataError if no data in date range."""
        requests = [FetchRequest(symbol="GDP_US", path=str(sample_csv))]
        with pytest.raises(NoDataError):
            source.fetch(requests, "2025-01-01", "2025-12-31")


class TestLocalFileSourceParquet:
    """Tests for parquet file support."""

    def test_parquet_fetch(self, source: LocalFileSource, tmp_path: Path) -> None:
        """Fetch from parquet file."""
        # Create parquet file
        df = pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        parquet_path = tmp_path / "data.parquet"
        df.to_parquet(parquet_path)

        requests = [FetchRequest(symbol="value", path=str(parquet_path))]
        result = source.fetch(requests, "2024-01-01", "2024-01-02")

        assert "value" in result.columns
        assert len(result) == 2


class TestLocalFileSourceGetMetadata:
    """Tests for get_metadata."""

    def test_metadata(self, source: LocalFileSource, sample_csv: Path) -> None:
        """get_metadata returns file info."""
        meta = source.get_metadata(str(sample_csv))
        assert meta["source"] == "localfile"
        assert meta["exists"] is True


class TestLocalFileSourceIsRegistered:
    """Tests for source registration."""

    def test_registered(self) -> None:
        """LocalFileSource is registered as 'localfile'."""
        from metapyle.sources.base import _global_registry

        source = _global_registry.get("localfile")
        assert isinstance(source, LocalFileSource)


class TestLocalFileSourceKwargs:
    """Tests for **kwargs handling in LocalFileSource."""

    def test_fetch_ignores_kwargs(self, tmp_path: Path) -> None:
        """LocalFileSource.fetch() accepts and ignores **kwargs."""
        # Create test CSV
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("date,value\n2024-01-01,100.0\n")

        source = LocalFileSource()
        requests = [FetchRequest(symbol="value", path=str(csv_path))]
        # Pass kwargs that should be ignored
        df = source.fetch(requests, "2024-01-01", "2024-01-01", unified=True, currency="EUR")

        assert not df.empty
        assert "value" in df.columns
