"""Tests for cache utility functions."""

from pathlib import Path

import pandas as pd

from metapyle import Client
from metapyle.cache import Cache


class TestListCachedEntries:
    """Tests for list_cached_entries method."""

    def test_list_cached_entries_empty(self, tmp_path: Path) -> None:
        """Empty cache returns empty list."""
        cache = Cache(path=str(tmp_path / "cache.db"))

        assert cache.list_cached_entries() == []

    def test_list_cached_entries_returns_entries(self, tmp_path: Path) -> None:
        """Returns list of cached entry metadata."""
        cache = Cache(path=str(tmp_path / "cache.db"))

        # Add some data
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        cache.put("bloomberg", "SPX Index", "PX_LAST", None, "2024-01-01", "2024-01-02", df)
        cache.put("macrobond", "usgdp", None, None, "2024-01-01", "2024-01-02", df)

        symbols = cache.list_cached_entries()

        assert len(symbols) == 2
        # Should return dicts with source, symbol, field, path, start_date, end_date
        sources = {s["source"] for s in symbols}
        assert sources == {"bloomberg", "macrobond"}


class TestClientListCached:
    """Tests for Client.list_cached() wrapper."""

    def test_list_cached_via_client(self, tmp_path: Path) -> None:
        """Client.list_cached() returns cached entries."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,value\n2024-01-01,100\n2024-01-02,101\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: test_symbol
  source: localfile
  symbol: value
  path: {csv_file}
""")

        cache_path = tmp_path / "cache.db"
        with Client(catalog=catalog, cache_path=str(cache_path)) as client:
            # Initially empty
            assert client.list_cached() == []

            # Fetch to populate cache
            client.get(["test_symbol"], start="2024-01-01", end="2024-01-02")

            # Now should have one entry
            cached = client.list_cached()
            assert len(cached) == 1
            assert cached[0]["source"] == "localfile"
            assert cached[0]["symbol"] == "value"


class TestClientClearCache:
    """Tests for Client.clear_cache() method."""

    def test_clear_cache_by_source(self, tmp_path: Path) -> None:
        """clear_cache(source=...) clears only that source."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,a,b\n2024-01-01,1,2\n2024-01-02,3,4\n2024-01-03,5,6\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: alpha
  source: localfile
  symbol: a
  path: {csv_file}
- my_name: beta
  source: localfile
  symbol: b
  path: {csv_file}
""")

        cache_path = tmp_path / "cache.db"
        with Client(catalog=catalog, cache_path=str(cache_path)) as client:
            # Fetch to populate cache
            client.get(["alpha", "beta"], start="2024-01-01", end="2024-01-03")
            assert len(client.list_cached()) == 2

            # Clear by source
            client.clear_cache(source="localfile")
            assert len(client.list_cached()) == 0

    def test_clear_cache_all(self, tmp_path: Path) -> None:
        """clear_cache() with no args clears everything."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,value\n2024-01-01,100\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: test
  source: localfile
  symbol: value
  path: {csv_file}
""")

        cache_path = tmp_path / "cache.db"
        with Client(catalog=catalog, cache_path=str(cache_path)) as client:
            client.get(["test"], start="2024-01-01", end="2024-01-01")
            assert len(client.list_cached()) == 1

            client.clear_cache()
            assert len(client.list_cached()) == 0
