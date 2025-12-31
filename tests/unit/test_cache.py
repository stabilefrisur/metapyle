"""Tests for Cache class with SQLite storage."""

from pathlib import Path

import pandas as pd
import pytest

from metapyle.cache import Cache


class TestCacheInitialization:
    """Tests for Cache initialization."""

    def test_cache_initializes_database(self, tmp_path: Path) -> None:
        """Cache creates database file and schema on initialization."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        assert db_path.exists()
        cache.close()

    def test_cache_default_path_from_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cache uses METAPYLE_CACHE_PATH environment variable when set."""
        env_path = tmp_path / "env_cache.db"
        monkeypatch.setenv("METAPYLE_CACHE_PATH", str(env_path))

        cache = Cache()

        assert env_path.exists()
        cache.close()

    def test_cache_disabled(self, tmp_path: Path) -> None:
        """Disabled cache does not create database or store data."""
        db_path = tmp_path / "disabled_cache.db"
        cache = Cache(path=str(db_path), enabled=False)

        # Should not create file when disabled
        assert not db_path.exists()

        # Put should be no-op
        df = pd.DataFrame({"date": ["2024-01-01"], "value": [100.0]})
        cache.put("source", "symbol", None, None, "2024-01-01", "2024-01-31", df)

        # Get should return None
        result = cache.get("source", "symbol", None, None, "2024-01-01", "2024-01-31")
        assert result is None

        cache.close()


class TestCachePutAndGet:
    """Tests for Cache put and get operations."""

    def test_cache_put_and_get(self, tmp_path: Path) -> None:
        """Cache stores and retrieves data correctly."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        # Create test data
        dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}, index=dates)

        # Store data
        cache.put("test_source", "TEST_SYM", "close", None, "2024-01-01", "2024-01-05", df)

        # Retrieve data
        result = cache.get("test_source", "TEST_SYM", "close", None, "2024-01-01", "2024-01-05")

        assert result is not None
        assert len(result) == 5
        assert list(result["value"]) == [1.0, 2.0, 3.0, 4.0, 5.0]
        cache.close()

    def test_cache_get_returns_none_for_miss(self, tmp_path: Path) -> None:
        """Cache returns None when data is not found."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        result = cache.get("nonexistent", "SYMBOL", None, None, "2024-01-01", "2024-01-31")

        assert result is None
        cache.close()

    def test_cache_get_subset_of_cached_range(self, tmp_path: Path) -> None:
        """Cache returns data when requested range is subset of cached range."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        # Cache data for full month
        dates = pd.date_range("2024-01-01", "2024-01-31", freq="D")
        df = pd.DataFrame({"value": range(1, 32)}, index=dates)
        cache.put("source", "SYM", "price", None, "2024-01-01", "2024-01-31", df)

        # Request subset (middle of month)
        result = cache.get("source", "SYM", "price", None, "2024-01-10", "2024-01-20")

        assert result is not None
        # Should return only data within requested range
        assert len(result) == 11  # 10th to 20th inclusive
        assert result["value"].iloc[0] == 10  # Jan 10 = 10th value
        assert result["value"].iloc[-1] == 20  # Jan 20 = 20th value
        cache.close()

    def test_cache_miss_when_range_exceeds_cached(self, tmp_path: Path) -> None:
        """Cache returns None when requested range exceeds cached range."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        # Cache partial data
        dates = pd.date_range("2024-01-10", "2024-01-20", freq="D")
        df = pd.DataFrame({"value": range(10, 21)}, index=dates)
        cache.put("source", "SYM", "price", None, "2024-01-10", "2024-01-20", df)

        # Request larger range (starts before cached)
        result = cache.get("source", "SYM", "price", None, "2024-01-01", "2024-01-20")
        assert result is None

        # Request larger range (ends after cached)
        result = cache.get("source", "SYM", "price", None, "2024-01-10", "2024-01-31")
        assert result is None

        # Request completely outside cached range
        result = cache.get("source", "SYM", "price", None, "2024-02-01", "2024-02-28")
        assert result is None

        cache.close()

    def test_cache_null_field(self, tmp_path: Path) -> None:
        """Cache handles None field correctly."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}, index=dates)

        # Store with None field
        cache.put("source", "SYM", None, None, "2024-01-01", "2024-01-05", df)

        # Retrieve with None field
        result = cache.get("source", "SYM", None, None, "2024-01-01", "2024-01-05")

        assert result is not None
        assert len(result) == 5

        # Different field should not match
        result_different = cache.get("source", "SYM", "close", None, "2024-01-01", "2024-01-05")
        assert result_different is None

        cache.close()

    def test_cache_put_overwrites_existing(self, tmp_path: Path) -> None:
        """Putting data with same key overwrites existing entry."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")

        # Store initial data
        df1 = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}, index=dates)
        cache.put("source", "SYM", "price", None, "2024-01-01", "2024-01-05", df1)

        # Overwrite with new data
        df2 = pd.DataFrame({"value": [10.0, 20.0, 30.0, 40.0, 50.0]}, index=dates)
        cache.put("source", "SYM", "price", None, "2024-01-01", "2024-01-05", df2)

        # Retrieve should return new data
        result = cache.get("source", "SYM", "price", None, "2024-01-01", "2024-01-05")

        assert result is not None
        assert list(result["value"]) == [10.0, 20.0, 30.0, 40.0, 50.0]
        cache.close()


class TestCacheClear:
    """Tests for Cache clear operations."""

    def test_cache_clear_all(self, tmp_path: Path) -> None:
        """Clear without arguments removes all entries."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}, index=dates)

        # Store multiple entries
        cache.put("source1", "SYM1", "price", None, "2024-01-01", "2024-01-05", df)
        cache.put("source2", "SYM2", "volume", None, "2024-01-01", "2024-01-05", df)

        # Clear all
        cache.clear()

        # Both should be gone
        assert cache.get("source1", "SYM1", "price", None, "2024-01-01", "2024-01-05") is None
        assert cache.get("source2", "SYM2", "volume", None, "2024-01-01", "2024-01-05") is None
        cache.close()

    def test_cache_clear_specific_source(self, tmp_path: Path) -> None:
        """Clear with source only removes entries for that source."""
        db_path = tmp_path / "test_cache.db"
        cache = Cache(path=str(db_path))

        dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}, index=dates)

        # Store entries from different sources
        cache.put("bloomberg", "SPX", "price", None, "2024-01-01", "2024-01-05", df)
        cache.put("bloomberg", "VIX", "price", None, "2024-01-01", "2024-01-05", df)
        cache.put("macrobond", "usgdp", None, None, "2024-01-01", "2024-01-05", df)

        # Clear only bloomberg
        cache.clear(source="bloomberg")

        # bloomberg entries should be gone
        assert cache.get("bloomberg", "SPX", "price", None, "2024-01-01", "2024-01-05") is None
        assert cache.get("bloomberg", "VIX", "price", None, "2024-01-01", "2024-01-05") is None

        # macrobond should remain
        assert cache.get("macrobond", "usgdp", None, None, "2024-01-01", "2024-01-05") is not None
        cache.close()


class TestCacheWithPath:
    """Tests for Cache path field in cache key."""

    def test_cache_with_path(self, tmp_path: Path) -> None:
        """Cache distinguishes entries by path."""
        cache_path = str(tmp_path / "test.db")
        cache = Cache(path=cache_path)

        df1 = pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        df2 = pd.DataFrame(
            {"value": [10.0, 20.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )

        # Store with different paths
        cache.put("localfile", "GDP_US", None, "/data/file1.csv", "2024-01-01", "2024-01-02", df1)
        cache.put("localfile", "GDP_US", None, "/data/file2.csv", "2024-01-01", "2024-01-02", df2)

        # Retrieve by path
        result1 = cache.get(
            "localfile", "GDP_US", None, "/data/file1.csv", "2024-01-01", "2024-01-02"
        )
        result2 = cache.get(
            "localfile", "GDP_US", None, "/data/file2.csv", "2024-01-01", "2024-01-02"
        )

        assert result1 is not None
        assert result2 is not None
        assert result1["value"].iloc[0] == 1.0
        assert result2["value"].iloc[0] == 10.0

        cache.close()
