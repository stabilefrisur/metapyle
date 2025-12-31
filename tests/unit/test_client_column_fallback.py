"""Tests for column name fallback when source ignores field."""


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

    def test_client_extracts_column_with_fallback(self, tmp_path) -> None:
        """Client should find column even when source ignores field.

        This simulates Macrobond behavior where field is in catalog
        but source returns column named just by symbol.
        """
        from metapyle import Client

        # Create a CSV that mimics Macrobond behavior
        # (column named by symbol only, not symbol::field)
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,usgdp\n2024-01-01,100\n2024-01-02,101\n2024-01-03,102\n")

        # Catalog has field defined (like Macrobond entries often do)
        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: us_gdp
  source: localfile
  symbol: usgdp
  field: some_field_ignored_by_localfile
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            # This should work even though localfile ignores field
            df = client.get(["us_gdp"], start="2024-01-01", end="2024-01-03")

        assert "us_gdp" in df.columns
        assert len(df) == 3
