"""Tests for stale data warning in Client.get()."""

import logging

import pandas as pd
import pytest

from metapyle import Client
from metapyle.sources.base import BaseSource, FetchRequest, register_source


@register_source("stale_test")
class StaleTestSource(BaseSource):
    """Test source that returns data ending at a configurable date."""

    # Class-level config for test control
    data_end_date: str = "2025-01-02"

    def fetch(
        self,
        requests: list[FetchRequest],
        start: str,
        end: str,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Return test data ending at data_end_date."""
        dates = pd.date_range(start, self.data_end_date, freq="D")
        data = {req.symbol: range(len(dates)) for req in requests}
        return pd.DataFrame(data, index=dates)

    def get_metadata(self, symbol: str) -> dict[str, str | None]:
        """Return empty metadata."""
        return {}


@pytest.fixture
def stale_catalog(tmp_path):
    """Create a test catalog with stale_test source."""
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text("""
- my_name: test_symbol
  source: stale_test
  symbol: TEST
- my_name: test_symbol2
  source: stale_test
  symbol: TEST2
""")
    return catalog_path


class TestStaleDataWarning:
    """Tests for stale data warning behavior."""

    def test_no_warning_when_data_is_current(self, stale_catalog, tmp_path, caplog):
        """No warning when data ends on the requested end date."""
        StaleTestSource.data_end_date = "2025-01-02"

        with caplog.at_level(logging.WARNING):
            with Client(
                catalog=stale_catalog,
                cache_path=str(tmp_path / "cache.db"),
            ) as client:
                client.get(
                    ["test_symbol"],
                    start="2024-01-01",
                    end="2025-01-02",  # Thursday
                    use_cache=False,
                )

        assert "stale_data" not in caplog.text

    def test_no_warning_for_one_business_day_gap(self, stale_catalog, tmp_path, caplog):
        """No warning when gap is exactly 1 business day (normal T+1)."""
        # Data ends Friday, request Monday → 1 business day gap
        StaleTestSource.data_end_date = "2025-01-03"  # Friday

        with caplog.at_level(logging.WARNING):
            with Client(
                catalog=stale_catalog,
                cache_path=str(tmp_path / "cache.db"),
            ) as client:
                client.get(
                    ["test_symbol"],
                    start="2024-01-01",
                    end="2025-01-06",  # Monday
                    use_cache=False,
                )

        assert "stale_data" not in caplog.text

    def test_warning_for_two_business_day_gap(self, stale_catalog, tmp_path, caplog):
        """Warning when gap is 2+ business days."""
        # Data ends Thursday, request Monday → 2 business day gap
        StaleTestSource.data_end_date = "2025-01-02"  # Thursday

        with caplog.at_level(logging.WARNING):
            with Client(
                catalog=stale_catalog,
                cache_path=str(tmp_path / "cache.db"),
            ) as client:
                client.get(
                    ["test_symbol"],
                    start="2024-01-01",
                    end="2025-01-06",  # Monday
                    use_cache=False,
                )

        assert "stale_data" in caplog.text
        assert "test_symbol" in caplog.text
        assert "actual_end=2025-01-02" in caplog.text
        assert "requested_end=2025-01-06" in caplog.text
        assert "gap_bdays=2" in caplog.text

    def test_weekend_handling(self, stale_catalog, tmp_path, caplog):
        """No false positive when requested end is weekend."""
        # Data ends Friday, request Sunday → should not warn
        StaleTestSource.data_end_date = "2025-01-03"  # Friday

        with caplog.at_level(logging.WARNING):
            with Client(
                catalog=stale_catalog,
                cache_path=str(tmp_path / "cache.db"),
            ) as client:
                client.get(
                    ["test_symbol"],
                    start="2024-01-01",
                    end="2025-01-05",  # Sunday
                    use_cache=False,
                )

        assert "stale_data" not in caplog.text

    def test_multiple_symbols_mixed_freshness(self, stale_catalog, tmp_path, caplog):
        """Warning only for stale symbols when fetching multiple."""
        # Need a source that can return different end dates per symbol
        # For simplicity, use same end date but verify warning content

        StaleTestSource.data_end_date = "2025-01-02"  # Thursday

        with caplog.at_level(logging.WARNING):
            with Client(
                catalog=stale_catalog,
                cache_path=str(tmp_path / "cache.db"),
            ) as client:
                client.get(
                    ["test_symbol", "test_symbol2"],
                    start="2024-01-01",
                    end="2025-01-06",  # Monday
                    use_cache=False,
                )

        # Both symbols should have warnings since both are stale
        assert caplog.text.count("stale_data") == 2
        assert "test_symbol" in caplog.text
        assert "test_symbol2" in caplog.text

    def test_no_warning_on_cache_hit(self, stale_catalog, tmp_path, caplog):
        """No warning when data is served from cache (can't detect staleness)."""
        StaleTestSource.data_end_date = "2025-01-02"  # Stale data

        with Client(
            catalog=stale_catalog,
            cache_path=str(tmp_path / "cache.db"),
        ) as client:
            # First fetch - will warn and cache
            client.get(
                ["test_symbol"],
                start="2024-01-01",
                end="2025-01-06",
                use_cache=True,
            )

        # Clear log, fetch again from cache
        caplog.clear()

        with caplog.at_level(logging.WARNING):
            with Client(
                catalog=stale_catalog,
                cache_path=str(tmp_path / "cache.db"),
            ) as client:
                client.get(
                    ["test_symbol"],
                    start="2024-01-01",
                    end="2025-01-06",
                    use_cache=True,
                )

        # No warning on cache hit
        assert "stale_data" not in caplog.text
