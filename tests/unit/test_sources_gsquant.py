"""Unit tests for gs-quant source adapter."""

from unittest.mock import patch

import pytest

from metapyle.exceptions import FetchError
from metapyle.sources.base import FetchRequest


class TestGSQuantSourceImport:
    """Tests for GSQuantSource lazy import."""

    def test_gsquant_not_installed(self) -> None:
        """GSQuantSource raises FetchError when gs-quant not installed."""
        with patch.dict("sys.modules", {"gs_quant": None, "gs_quant.data": None, "gs_quant.session": None}):
            # Force reimport
            import importlib

            from metapyle.sources import gsquant
            importlib.reload(gsquant)

            # Reset the lazy import state
            gsquant._GSQUANT_AVAILABLE = None
            gsquant._gsquant_modules = {}

            source = gsquant.GSQuantSource()
            request = FetchRequest(
                symbol="EURUSD",
                field="FXIMPLIEDVOL::impliedVolatility",
            )

            with pytest.raises(FetchError, match="gs-quant package is not installed"):
                source.fetch([request], "2024-01-01", "2024-12-31")
