"""Tests for Bloomberg field requirement validation."""

import pytest

from metapyle.catalog import Catalog
from metapyle.exceptions import CatalogValidationError


class TestBloombergFieldRequired:
    """Bloomberg entries must have a field."""

    def test_bloomberg_entry_without_field_raises(self, tmp_path):
        """Bloomberg entry missing field should raise CatalogValidationError."""
        catalog_file = tmp_path / "catalog.yaml"
        catalog_file.write_text("""
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
""")
        with pytest.raises(CatalogValidationError, match="Bloomberg.*requires.*field"):
            Catalog.from_yaml(catalog_file)

    def test_bloomberg_entry_with_field_succeeds(self, tmp_path):
        """Bloomberg entry with field should load successfully."""
        catalog_file = tmp_path / "catalog.yaml"
        catalog_file.write_text("""
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
""")
        catalog = Catalog.from_yaml(catalog_file)
        assert "sp500_close" in catalog

    def test_other_sources_without_field_succeed(self, tmp_path):
        """Non-Bloomberg entries without field should load successfully."""
        catalog_file = tmp_path / "catalog.yaml"
        catalog_file.write_text("""
- my_name: us_gdp
  source: macrobond
  symbol: usgdp
""")
        catalog = Catalog.from_yaml(catalog_file)
        assert "us_gdp" in catalog
