"""Tests for cache utility functions."""

from pathlib import Path

import pandas as pd

from metapyle import Client
from metapyle.cache import Cache


class TestListCachedSymbols:
    """Tests for list_cached_symbols method."""

    def test_list_cached_symbols_empty(self, tmp_path: Path) -> None:
        """Empty cache returns empty list."""
        cache = Cache(path=str(tmp_path / "cache.db"))

        assert cache.list_cached_symbols() == []

    def test_list_cached_symbols_returns_entries(self, tmp_path: Path) -> None:
        """Returns list of cached symbol metadata."""
        cache = Cache(path=str(tmp_path / "cache.db"))

        # Add some data
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        cache.put("bloomberg", "SPX Index", "PX_LAST", None, "2024-01-01", "2024-01-02", df)
        cache.put("macrobond", "usgdp", None, None, "2024-01-01", "2024-01-02", df)

        symbols = cache.list_cached_symbols()

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


class TestClearSymbol:
    """Tests for clear_symbol method."""

    def test_clear_symbol_removes_entry(self, tmp_path: Path) -> None:
        """clear_symbol removes specific entry from cache."""
        cache = Cache(path=str(tmp_path / "cache.db"))

        df = pd.DataFrame(
            {"value": [1, 2]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        cache.put("bloomberg", "SPX Index", "PX_LAST", None, "2024-01-01", "2024-01-02", df)
        cache.put("bloomberg", "VIX Index", "PX_LAST", None, "2024-01-01", "2024-01-02", df)

        # Clear one symbol
        cache.clear_symbol("bloomberg", "SPX Index", "PX_LAST", None)

        # Should be gone
        result = cache.get("bloomberg", "SPX Index", "PX_LAST", None, "2024-01-01", "2024-01-02")
        assert result is None

        # Other symbol should still exist
        result = cache.get("bloomberg", "VIX Index", "PX_LAST", None, "2024-01-01", "2024-01-02")
        assert result is not None

    def test_clear_symbol_returns_count(self, tmp_path: Path) -> None:
        """clear_symbol returns number of entries cleared."""
        cache = Cache(path=str(tmp_path / "cache.db"))

        df = pd.DataFrame(
            {"value": [1, 2]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        cache.put("bloomberg", "SPX Index", "PX_LAST", None, "2024-01-01", "2024-01-02", df)

        count = cache.clear_symbol("bloomberg", "SPX Index", "PX_LAST", None)
        assert count == 1

        # Clearing again should return 0
        count = cache.clear_symbol("bloomberg", "SPX Index", "PX_LAST", None)
        assert count == 0
