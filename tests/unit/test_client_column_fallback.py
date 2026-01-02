"""Tests for column name fallback when source ignores field."""

import pytest

from metapyle.exceptions import CatalogValidationError
from metapyle.sources.base import make_column_name


class TestColumnLookupFallback:
    """Test column name fallback for sources that ignore field."""

    def test_make_column_name_with_field(self) -> None:
        """Column name with field should use :: separator."""
        result = make_column_name("SPX Index", "PX_LAST")
        assert result == "SPX Index::PX_LAST"

    def test_make_column_name_without_field(self) -> None:
        """Column name without field should just be symbol."""
        result = make_column_name("usgdp", None)
        assert result == "usgdp"

    def test_localfile_with_field_is_rejected(self, tmp_path) -> None:
        """Localfile entries must not have field set.

        Field attribute is not used by localfile source, so it should
        be rejected during catalog validation to prevent confusion.
        """
        from metapyle import Client

        # Create a CSV file
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,usgdp\n2024-01-01,100\n2024-01-02,101\n2024-01-03,102\n")

        # Catalog has field defined (invalid for localfile)
        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: us_gdp
  source: localfile
  symbol: usgdp
  field: some_field_invalid_for_localfile
  path: {csv_file}
""")

        with pytest.raises(CatalogValidationError, match="localfile.*field"):
            Client(catalog=catalog, cache_enabled=False)
