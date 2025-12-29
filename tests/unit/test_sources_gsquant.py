"""Unit tests for gs-quant source adapter."""

from unittest.mock import patch

import pytest

from metapyle.exceptions import FetchError
from metapyle.sources.base import FetchRequest


class TestGSQuantSourceRegistration:
    """Tests for GSQuantSource registration."""

    def test_gsquant_registered(self) -> None:
        """GSQuantSource is registered in global registry."""
        from metapyle.sources.base import _global_registry

        assert "gsquant" in _global_registry.list_sources()

    def test_gsquant_importable_from_sources(self) -> None:
        """GSQuantSource is importable from metapyle.sources."""
        from metapyle.sources import GSQuantSource

        assert GSQuantSource is not None


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


class TestFieldParsing:
    """Tests for field parsing."""

    def test_parse_field_valid(self) -> None:
        """_parse_field extracts dataset_id and value_column."""
        from metapyle.sources.gsquant import _parse_field

        dataset_id, value_column = _parse_field("FXIMPLIEDVOL::impliedVolatility")

        assert dataset_id == "FXIMPLIEDVOL"
        assert value_column == "impliedVolatility"

    def test_parse_field_with_underscores(self) -> None:
        """_parse_field handles underscores in names."""
        from metapyle.sources.gsquant import _parse_field

        dataset_id, value_column = _parse_field("S3_PARTNERS_EQUITY::dailyShortInterest")

        assert dataset_id == "S3_PARTNERS_EQUITY"
        assert value_column == "dailyShortInterest"

    def test_parse_field_missing_separator(self) -> None:
        """_parse_field raises ValueError if :: missing."""
        from metapyle.sources.gsquant import _parse_field

        with pytest.raises(ValueError, match="Invalid field format"):
            _parse_field("FXIMPLIEDVOL")

    def test_parse_field_empty_parts(self) -> None:
        """_parse_field raises ValueError if parts empty."""
        from metapyle.sources.gsquant import _parse_field

        with pytest.raises(ValueError, match="Invalid field format"):
            _parse_field("::impliedVolatility")

        with pytest.raises(ValueError, match="Invalid field format"):
            _parse_field("FXIMPLIEDVOL::")
