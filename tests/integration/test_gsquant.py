"""Integration tests for gs-quant source.

Requires:
- gs-quant installed: pip install gs-quant
- GS session authenticated before running tests
"""

from pathlib import Path

import pytest

# Skip all tests if gs-quant not installed
gs_quant = pytest.importorskip("gs_quant")

pytestmark = [pytest.mark.integration, pytest.mark.gsquant]


@pytest.fixture
def gsquant_catalog_path() -> Path:
    """Path to gsquant test catalog."""
    return Path(__file__).parent / "fixtures" / "gsquant.yaml"


class TestGSQuantIntegration:
    """Integration tests for gs-quant data fetching."""

    def test_fetch_single_symbol(self, gsquant_catalog_path: Path) -> None:
        """Fetch single symbol from gs-quant."""
        from metapyle import Client

        client = Client(catalog=gsquant_catalog_path)
        df = client.get(["EURUSD_VOL"], start="2024-01-01", end="2024-01-31")

        assert len(df) > 0
        assert "EURUSD_VOL" in df.columns

    def test_fetch_multiple_symbols(self, gsquant_catalog_path: Path) -> None:
        """Fetch multiple symbols from same dataset."""
        from metapyle import Client

        client = Client(catalog=gsquant_catalog_path)
        df = client.get(
            ["EURUSD_VOL", "USDJPY_VOL"],
            start="2024-01-01",
            end="2024-01-31",
        )

        assert len(df) > 0
        assert "EURUSD_VOL" in df.columns
        assert "USDJPY_VOL" in df.columns
