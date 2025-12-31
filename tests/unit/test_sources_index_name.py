"""Tests for consistent date index naming across sources."""

import pandas as pd
import pytest


class TestIndexNameConsistency:
    """All sources should return DatetimeIndex named 'date'."""

    def test_localfile_index_name(self, tmp_path):
        """LocalFile source should return index named 'date' regardless of source column."""
        from metapyle.sources.localfile import LocalFileSource
        from metapyle.sources.base import FetchRequest

        # Use 'timestamp' column to verify normalization to 'date'
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("timestamp,value\n2024-01-01,100\n2024-01-02,101\n")

        source = LocalFileSource()
        req = FetchRequest(symbol="value", path=str(csv_file))
        df = source.fetch([req], "2024-01-01", "2024-01-02")

        assert df.index.name == "date"
