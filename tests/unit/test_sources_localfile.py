"""Unit tests for LocalFileSource adapter."""

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import _global_registry
from metapyle.sources.localfile import LocalFileSource


class TestLocalFileSourceFetchCSV:
    """Tests for CSV file fetching."""

    def test_localfile_source_fetch_csv(self, tmp_path: pytest.TempPathFactory) -> None:
        """Test fetching data from a CSV file."""
        # Create test CSV
        csv_path = tmp_path / "test_data.csv"
        df = pd.DataFrame(
            {"value": [100.0, 101.0, 102.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        )
        df.index.name = "date"
        df.to_csv(csv_path)

        source = LocalFileSource()
        result = source.fetch(str(csv_path), "2024-01-01", "2024-01-03")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "value" in result.columns
        assert isinstance(result.index, pd.DatetimeIndex)


class TestLocalFileSourceFetchParquet:
    """Tests for Parquet file fetching."""

    def test_localfile_source_fetch_parquet(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Test fetching data from a Parquet file."""
        # Create test Parquet
        parquet_path = tmp_path / "test_data.parquet"
        df = pd.DataFrame(
            {"date": pd.to_datetime(["2024-01-01", "2024-01-02"]), "value": [50.0, 51.0]}
        )
        df.to_parquet(parquet_path, index=False)

        source = LocalFileSource()
        result = source.fetch(str(parquet_path), "2024-01-01", "2024-01-02")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "value" in result.columns


class TestLocalFileSourceDateFiltering:
    """Tests for date range filtering."""

    def test_localfile_source_date_filtering(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Test that date filtering correctly limits results."""
        csv_path = tmp_path / "date_filter.csv"
        df = pd.DataFrame(
            {"value": [1.0, 2.0, 3.0, 4.0, 5.0]},
            index=pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
            ),
        )
        df.index.name = "date"
        df.to_csv(csv_path)

        source = LocalFileSource()
        result = source.fetch(str(csv_path), "2024-01-02", "2024-01-04")

        assert len(result) == 3
        assert result.iloc[0]["value"] == 2.0
        assert result.iloc[-1]["value"] == 4.0


class TestLocalFileSourceFileNotFound:
    """Tests for file not found error handling."""

    def test_localfile_source_file_not_found(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Test that FetchError is raised when file doesn't exist."""
        source = LocalFileSource()
        nonexistent = tmp_path / "nonexistent.csv"

        with pytest.raises(FetchError, match="File not found"):
            source.fetch(str(nonexistent), "2024-01-01", "2024-01-31")


class TestLocalFileSourceEmptyFile:
    """Tests for empty file handling."""

    def test_localfile_source_empty_file(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Test that NoDataError is raised for empty files."""
        csv_path = tmp_path / "empty.csv"
        # Create CSV with headers only (empty data)
        df = pd.DataFrame(columns=["value"])
        df.index.name = "date"
        df.to_csv(csv_path)

        source = LocalFileSource()

        with pytest.raises(NoDataError, match="File is empty"):
            source.fetch(str(csv_path), "2024-01-01", "2024-01-31")


class TestLocalFileSourceNoDataInRange:
    """Tests for no data in date range."""

    def test_localfile_source_no_data_in_range(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Test that NoDataError is raised when no data matches date range."""
        csv_path = tmp_path / "out_of_range.csv"
        df = pd.DataFrame(
            {"value": [100.0, 101.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        df.index.name = "date"
        df.to_csv(csv_path)

        source = LocalFileSource()

        with pytest.raises(NoDataError, match="No data in date range"):
            source.fetch(str(csv_path), "2025-01-01", "2025-01-31")


class TestLocalFileSourceGetMetadata:
    """Tests for get_metadata method."""

    def test_localfile_source_get_metadata(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Test metadata retrieval for a file."""
        csv_path = tmp_path / "metadata_test.csv"
        df = pd.DataFrame({"value": [1.0]}, index=pd.to_datetime(["2024-01-01"]))
        df.index.name = "date"
        df.to_csv(csv_path)

        source = LocalFileSource()
        metadata = source.get_metadata(str(csv_path))

        assert metadata["source"] == "localfile"
        assert metadata["filename"] == "metadata_test.csv"
        assert metadata["extension"] == ".csv"
        assert metadata["exists"] is True
        assert "size_bytes" in metadata
        assert metadata["size_bytes"] > 0


class TestLocalFileSourceIsRegistered:
    """Tests for source registration."""

    def test_localfile_source_is_registered(self) -> None:
        """Test that LocalFileSource is registered with the global registry."""
        # Verify the source is registered
        registered_sources = _global_registry.list_sources()
        assert "localfile" in registered_sources

        # Verify we can get an instance
        source = _global_registry.get("localfile")
        assert isinstance(source, LocalFileSource)
