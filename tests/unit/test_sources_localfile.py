"""Unit tests for LocalFileSource adapter."""

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import _global_registry
from metapyle.sources.localfile import LocalFileSource


class TestLocalFileSourceFetch:
    """Tests for LocalFileSource.fetch method."""

    def test_localfile_fetch_extracts_column_by_symbol(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch extracts specific column from CSV using symbol as column name."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,GDP_US,CPI_US\n"
            "2024-01-01,100.0,200.0\n"
            "2024-01-02,101.0,201.0\n"
            "2024-01-03,102.0,202.0\n"
        )

        source = LocalFileSource()
        df = source.fetch("GDP_US", "2024-01-01", "2024-01-03", path=str(csv_path))

        assert list(df.columns) == ["GDP_US"]
        assert len(df) == 3
        assert df["GDP_US"].iloc[0] == 100.0

    def test_localfile_fetch_returns_original_column_name(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch returns DataFrame with original column name, not 'value'."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,MyColumn\n"
            "2024-01-01,1.0\n"
            "2024-01-02,2.0\n"
        )

        source = LocalFileSource()
        df = source.fetch("MyColumn", "2024-01-01", "2024-01-02", path=str(csv_path))

        assert "MyColumn" in df.columns
        assert "value" not in df.columns

    def test_localfile_fetch_requires_path(self) -> None:
        """Fetch raises FetchError if path not provided."""
        source = LocalFileSource()

        with pytest.raises(FetchError, match="path is required"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-03")

    def test_localfile_fetch_column_not_found(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch raises FetchError if column not found in file."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,OTHER_COL\n"
            "2024-01-01,1.0\n"
        )

        source = LocalFileSource()

        with pytest.raises(FetchError, match="Column 'GDP_US' not found"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-03", path=str(csv_path))

    def test_localfile_fetch_file_not_found(self) -> None:
        """Fetch raises FetchError if file not found."""
        source = LocalFileSource()

        with pytest.raises(FetchError, match="File not found"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-03", path="/nonexistent.csv")

    def test_localfile_fetch_filters_date_range(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch filters data to requested date range."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,GDP_US\n"
            "2024-01-01,100.0\n"
            "2024-01-02,101.0\n"
            "2024-01-03,102.0\n"
            "2024-01-04,103.0\n"
            "2024-01-05,104.0\n"
        )

        source = LocalFileSource()
        df = source.fetch("GDP_US", "2024-01-02", "2024-01-04", path=str(csv_path))

        assert len(df) == 3
        assert df["GDP_US"].iloc[0] == 101.0
        assert df["GDP_US"].iloc[-1] == 103.0

    def test_localfile_fetch_no_data_in_range(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch raises NoDataError if no data in date range."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,GDP_US\n"
            "2024-01-01,100.0\n"
        )

        source = LocalFileSource()

        with pytest.raises(NoDataError, match="No data in date range"):
            source.fetch("GDP_US", "2025-01-01", "2025-01-31", path=str(csv_path))

    def test_localfile_fetch_empty_file(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch raises NoDataError for empty file."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("date,GDP_US\n")  # header only, no data

        source = LocalFileSource()

        with pytest.raises(NoDataError, match="File is empty"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-31", path=str(csv_path))


class TestLocalFileSourceParquet:
    """Tests for LocalFileSource with Parquet files."""

    def test_localfile_fetch_parquet(self, tmp_path: pytest.TempPathFactory) -> None:
        """Fetch works with Parquet files."""
        pytest.importorskip("pyarrow")

        parquet_path = tmp_path / "data.parquet"
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "GDP_US": [100.0, 101.0],
        })
        df.to_parquet(parquet_path, index=False)

        source = LocalFileSource()
        result = source.fetch("GDP_US", "2024-01-01", "2024-01-02", path=str(parquet_path))

        assert list(result.columns) == ["GDP_US"]
        assert len(result) == 2


class TestLocalFileSourceGetMetadata:
    """Tests for get_metadata method."""

    def test_localfile_source_get_metadata(self, tmp_path: pytest.TempPathFactory) -> None:
        """Test metadata retrieval for a file."""
        csv_path = tmp_path / "metadata_test.csv"
        csv_path.write_text("date,value\n2024-01-01,1.0\n")

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
