"""Unit tests for Catalog and CatalogEntry."""

from pathlib import Path

import pytest

from metapyle.catalog import Catalog, CatalogEntry
from metapyle.exceptions import (
    CatalogValidationError,
    DuplicateNameError,
    NameNotFoundError,
    UnknownSourceError,
)
from metapyle.sources.base import SourceRegistry


class TestCatalogEntryParams:
    """Tests for CatalogEntry params field."""

    def test_catalog_entry_with_params(self) -> None:
        """CatalogEntry accepts params dict."""
        params = {"tenor": "3m", "location": "NYC"}
        entry = CatalogEntry(
            my_name="eurusd_vol",
            source="gsquant",
            symbol="EURUSD",
            field="FXIMPLIEDVOL::impliedVolatility",
            params=params,
        )

        assert entry.params == params

    def test_catalog_entry_params_default_none(self) -> None:
        """CatalogEntry params defaults to None."""
        entry = CatalogEntry(
            my_name="test",
            source="bloomberg",
            symbol="SPX Index",
        )

        assert entry.params is None


class TestCatalogYamlParams:
    """Tests for YAML parsing with params field."""

    def test_from_yaml_with_params(self, tmp_path: Path) -> None:
        """Catalog.from_yaml parses params field."""
        yaml_content = """
- my_name: eurusd_vol
  source: gsquant
  symbol: EURUSD
  field: FXIMPLIEDVOL::impliedVolatility
  params:
    tenor: 3m
    deltaStrike: DN
    location: NYC
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        catalog = Catalog.from_yaml(yaml_file)
        entry = catalog.get("eurusd_vol")

        assert entry.params == {"tenor": "3m", "deltaStrike": "DN", "location": "NYC"}

    def test_from_yaml_without_params(self, tmp_path: Path) -> None:
        """Catalog.from_yaml works without params field."""
        yaml_content = """
- my_name: test_series
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        catalog = Catalog.from_yaml(yaml_file)
        entry = catalog.get("test_series")

        assert entry.params is None


class TestCatalogCsvParams:
    """Tests for CSV parsing - params column without JSON."""

    def test_from_csv_no_params_column(self, tmp_path: Path) -> None:
        """CSV without params column results in params=None."""
        csv_content = """my_name,source,symbol,field,path,description,unit
test_series,bloomberg,SPX Index,PX_LAST,,,
"""
        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text(csv_content)

        catalog = Catalog.from_csv(csv_file)
        entry = catalog.get("test_series")

        # params is None when column not present
        assert entry.params is None


class TestFromCsvParams:
    """Tests for CSV params column parsing."""

    def test_from_csv_parses_params_json(self, tmp_path: Path) -> None:
        """params column should be parsed as JSON dict."""
        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text(
            "my_name,source,symbol,params\n"
            'test_entry,gsquant,AAPL,"{""interval"": ""1d"", ""limit"": 100}"\n'
        )

        catalog = Catalog.from_csv(str(csv_file))
        entry = catalog.get("test_entry")

        assert entry.params == {"interval": "1d", "limit": 100}

    def test_from_csv_handles_empty_params(self, tmp_path: Path) -> None:
        """Empty params column should result in None."""
        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text("my_name,source,symbol,params\ntest_entry,localfile,value,\n")

        catalog = Catalog.from_csv(str(csv_file))
        entry = catalog.get("test_entry")

        assert entry.params is None

    def test_from_csv_invalid_json_raises_error(self, tmp_path: Path) -> None:
        """Invalid JSON in params column should raise CatalogValidationError."""
        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text(
            "my_name,source,symbol,params\ntest_entry,gsquant,AAPL,{not valid json}\n"
        )

        with pytest.raises(CatalogValidationError, match="Invalid JSON in params"):
            Catalog.from_csv(str(csv_file))


class TestCatalogExportParams:
    """Tests for catalog export with params."""

    def test_to_yaml_includes_params(self, tmp_path: Path) -> None:
        """Catalog.to_yaml exports params field."""
        # Create catalog with params
        yaml_content = """
- my_name: test_series
  source: gsquant
  symbol: EURUSD
  field: FXIMPLIEDVOL::impliedVolatility
  params:
    tenor: 3m
    location: NYC
"""
        yaml_file = tmp_path / "input.yaml"
        yaml_file.write_text(yaml_content)

        catalog = Catalog.from_yaml(yaml_file)

        # Export to new file
        output_file = tmp_path / "output.yaml"
        catalog.to_yaml(output_file)

        # Re-load and verify params preserved
        reloaded = Catalog.from_yaml(output_file)
        entry = reloaded.get("test_series")

        assert entry.params == {"tenor": "3m", "location": "NYC"}


class TestToYamlCleanOutput:
    """Tests for to_yaml omitting None fields."""

    def test_to_yaml_omits_none_fields(self, tmp_path: Path) -> None:
        """to_yaml should not include fields with None values."""
        # Entry with minimal fields (field, path, description, unit, params all None)
        # Use macrobond which doesn't require field or path
        yaml_content = """
- my_name: simple_entry
  source: macrobond
  symbol: price
"""
        input_file = tmp_path / "input.yaml"
        input_file.write_text(yaml_content)

        catalog = Catalog.from_yaml(input_file)

        output_file = tmp_path / "output.yaml"
        catalog.to_yaml(output_file)

        output_text = output_file.read_text()

        # Should NOT contain None fields
        assert "field:" not in output_text
        assert "path:" not in output_text
        assert "description:" not in output_text
        assert "unit:" not in output_text
        assert "params:" not in output_text

        # Should contain required fields
        assert "my_name: simple_entry" in output_text
        assert "source: macrobond" in output_text
        assert "symbol: price" in output_text


def test_catalog_entry_required_fields() -> None:
    """CatalogEntry requires my_name, source, symbol."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    assert entry.my_name == "GDP_US"
    assert entry.source == "bloomberg"
    assert entry.symbol == "GDP CUR$ Index"


def test_catalog_entry_optional_fields_default_none() -> None:
    """CatalogEntry optional fields default to None."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    assert entry.field is None
    assert entry.description is None
    assert entry.unit is None


def test_catalog_entry_with_optional_fields() -> None:
    """CatalogEntry can have optional fields set."""
    entry = CatalogEntry(
        my_name="SPX_CLOSE",
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        description="S&P 500 closing price",
        unit="points",
    )

    assert entry.field == "PX_LAST"
    assert entry.description == "S&P 500 closing price"
    assert entry.unit == "points"


def test_catalog_entry_is_frozen() -> None:
    """CatalogEntry should be immutable."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    with pytest.raises(AttributeError):
        entry.my_name = "changed"  # type: ignore[misc]


def test_catalog_entry_is_keyword_only() -> None:
    """CatalogEntry must use keyword arguments."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )
    assert entry.my_name == "GDP_US"

    with pytest.raises(TypeError):
        CatalogEntry(  # type: ignore[misc]
            "GDP_US",
            "bloomberg",
            "GDP CUR$ Index",
        )


def test_catalog_entry_uses_slots() -> None:
    """CatalogEntry should use slots for memory efficiency."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )

    assert not hasattr(entry, "__dict__")


def test_catalog_entry_path_field() -> None:
    """CatalogEntry supports optional path field."""
    entry = CatalogEntry(
        my_name="gdp_us",
        source="localfile",
        symbol="GDP_US",
        path="/data/macro.csv",
    )
    assert entry.path == "/data/macro.csv"


def test_catalog_entry_path_defaults_none() -> None:
    """CatalogEntry path defaults to None."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="SPX Index",
    )
    assert entry.path is None


# ============================================================================
# Catalog Tests
# ============================================================================


def test_catalog_load_from_yaml(tmp_path: Path) -> None:
    """Catalog can load entries from a YAML file."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST
  description: US GDP

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))

    assert len(catalog) == 2
    assert "GDP_US" in catalog
    assert "SPX_CLOSE" in catalog

    gdp = catalog.get("GDP_US")
    assert gdp.source == "bloomberg"
    assert gdp.description == "US GDP"

    spx = catalog.get("SPX_CLOSE")
    assert spx.field == "PX_LAST"


def test_catalog_load_from_yaml_with_path_object(tmp_path: Path) -> None:
    """Catalog.from_yaml() accepts Path objects."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    # Pass Path object instead of string
    catalog = Catalog.from_yaml(yaml_file)

    assert len(catalog) == 1
    assert "GDP_US" in catalog


def test_catalog_load_from_yaml_with_mixed_path_types(tmp_path: Path) -> None:
    """Catalog.from_yaml() accepts mixed str and Path in list."""
    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST
"""
    yaml2 = """
- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    # Mix Path and str
    catalog = Catalog.from_yaml([file1, str(file2)])

    assert len(catalog) == 2


def test_catalog_load_missing_required_field(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for missing required fields."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  # missing symbol
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="symbol"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_duplicate_names(tmp_path: Path) -> None:
    """Catalog raises DuplicateNameError for duplicate my_name."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST

- my_name: GDP_US
  source: localfile
  symbol: gdp
  path: /data/gdp.csv
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(DuplicateNameError, match="GDP_US"):
        Catalog.from_yaml(str(yaml_file))


def test_catalog_load_multiple_files(tmp_path: Path) -> None:
    """Catalog can load and merge multiple YAML files."""
    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST
"""
    yaml2 = """
- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    catalog = Catalog.from_yaml([str(file1), str(file2)])

    assert len(catalog) == 2
    assert "GDP_US" in catalog
    assert "SPX_CLOSE" in catalog


def test_catalog_load_duplicate_across_files(tmp_path: Path) -> None:
    """Catalog raises DuplicateNameError for duplicates across files."""
    yaml1 = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST
"""
    yaml2 = """
- my_name: GDP_US
  source: localfile
  symbol: gdp
  path: /data/gdp.csv
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    with pytest.raises(DuplicateNameError, match="GDP_US"):
        Catalog.from_yaml([str(file1), str(file2)])


def test_catalog_get_unknown_name() -> None:
    """Catalog raises NameNotFoundError for unknown name."""
    catalog = Catalog({})

    with pytest.raises(NameNotFoundError, match="UNKNOWN"):
        catalog.get("UNKNOWN")


def test_catalog_list_names(tmp_path: Path) -> None:
    """Catalog can list all entry names."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  field: PX_LAST

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    catalog = Catalog.from_yaml(str(yaml_file))
    names = catalog.list_names()

    assert sorted(names) == ["GDP_US", "SPX_CLOSE"]


def test_catalog_file_not_found() -> None:
    """Catalog raises CatalogValidationError for missing file."""
    with pytest.raises(CatalogValidationError, match="not found"):
        Catalog.from_yaml("/nonexistent/path/catalog.yaml")


def test_catalog_malformed_yaml(tmp_path: Path) -> None:
    """Catalog raises CatalogValidationError for malformed YAML."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  bad_key: [unclosed bracket
"""
    yaml_file = tmp_path / "catalog.yaml"
    yaml_file.write_text(yaml_content)

    with pytest.raises(CatalogValidationError, match="YAML"):
        Catalog.from_yaml(str(yaml_file))


# ============================================================================
# Catalog Source Validation Tests
# ============================================================================


def test_catalog_validate_sources() -> None:
    """Catalog raises UnknownSourceError for unregistered sources."""
    entry = CatalogEntry(
        my_name="GDP_US",
        source="unknown_source",
        symbol="GDP CUR$ Index",
    )
    catalog = Catalog({"GDP_US": entry})

    registry = SourceRegistry()
    registry.register("bloomberg", type)

    with pytest.raises(UnknownSourceError, match="unknown_source"):
        catalog.validate_sources(registry)


def test_catalog_validate_sources_success() -> None:
    """Catalog validation passes when all sources are registered."""
    entry1 = CatalogEntry(
        my_name="GDP_US",
        source="bloomberg",
        symbol="GDP CUR$ Index",
    )
    entry2 = CatalogEntry(
        my_name="LOCAL_DATA",
        source="localfile",
        symbol="/data/local.csv",
    )
    catalog = Catalog({"GDP_US": entry1, "LOCAL_DATA": entry2})

    registry = SourceRegistry()
    registry.register("bloomberg", type)
    registry.register("localfile", type)

    catalog.validate_sources(registry)


# ============================================================================
# CSV Template Tests
# ============================================================================


def test_csv_template_generic() -> None:
    """csv_template() without source returns all columns, no example row."""
    template = Catalog.csv_template()

    assert template == "my_name,source,symbol,field,path,description,unit\n"


def test_csv_template_generic_writes_file(tmp_path: Path) -> None:
    """csv_template() writes to file when path provided."""
    output = tmp_path / "template.csv"

    result = Catalog.csv_template(path=output)

    assert output.exists()
    assert output.read_text() == "my_name,source,symbol,field,path,description,unit\n"
    assert result == "my_name,source,symbol,field,path,description,unit\n"


def test_csv_template_generic_writes_file_str_path(tmp_path: Path) -> None:
    """csv_template() accepts string path."""
    output = tmp_path / "template.csv"

    result = Catalog.csv_template(path=str(output))

    assert output.exists()
    assert result == "my_name,source,symbol,field,path,description,unit\n"


def test_csv_template_bloomberg() -> None:
    """csv_template(source='bloomberg') returns bloomberg columns + example row."""
    template = Catalog.csv_template(source="bloomberg")

    lines = template.strip().split("\n")
    assert len(lines) == 2
    assert lines[0] == "my_name,source,symbol,field,description,unit"
    assert lines[1] == ",bloomberg,,,,"


def test_csv_template_localfile() -> None:
    """csv_template(source='localfile') returns localfile columns + example row."""
    template = Catalog.csv_template(source="localfile")

    lines = template.strip().split("\n")
    assert len(lines) == 2
    assert lines[0] == "my_name,source,symbol,path,description,unit"
    assert lines[1] == ",localfile,,,,"


def test_csv_template_macrobond() -> None:
    """csv_template(source='macrobond') returns macrobond columns + example row."""
    template = Catalog.csv_template(source="macrobond")

    lines = template.strip().split("\n")
    assert len(lines) == 2
    assert lines[0] == "my_name,source,symbol,description,unit"
    assert lines[1] == ",macrobond,,,"


def test_csv_template_gsquant() -> None:
    """csv_template(source='gsquant') returns gsquant columns + example row."""
    template = Catalog.csv_template(source="gsquant")

    lines = template.strip().split("\n")
    assert len(lines) == 2
    assert lines[0] == "my_name,source,symbol,field,description,unit"
    assert lines[1] == ",gsquant,,,,"


def test_csv_template_unknown_source_raises() -> None:
    """csv_template() raises ValueError for unknown source."""
    with pytest.raises(ValueError, match="Unknown source"):
        Catalog.csv_template(source="unknown")


# ============================================================================
# CSV Loading Tests
# ============================================================================


def test_catalog_from_csv_single_file(tmp_path: Path) -> None:
    """Catalog.from_csv() loads entries from CSV file."""
    csv_content = """my_name,source,symbol,field,path,description,unit
sp500_close,bloomberg,SPX Index,PX_LAST,,S&P 500 close,points
gdp_us,localfile,GDP_US,,/data/macro.csv,US GDP,USD billions
"""
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    catalog = Catalog.from_csv(csv_file)

    assert len(catalog) == 2
    assert "sp500_close" in catalog
    assert "gdp_us" in catalog

    spx = catalog.get("sp500_close")
    assert spx.source == "bloomberg"
    assert spx.symbol == "SPX Index"
    assert spx.field == "PX_LAST"
    assert spx.path is None
    assert spx.description == "S&P 500 close"
    assert spx.unit == "points"

    gdp = catalog.get("gdp_us")
    assert gdp.source == "localfile"
    assert gdp.path == "/data/macro.csv"


def test_catalog_from_csv_accepts_string_path(tmp_path: Path) -> None:
    """Catalog.from_csv() accepts string path."""
    csv_content = """my_name,source,symbol
test_entry,bloomberg,TEST Index
"""
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    catalog = Catalog.from_csv(str(csv_file))

    assert len(catalog) == 1
    assert "test_entry" in catalog


def test_catalog_from_csv_empty_optional_fields_are_none(tmp_path: Path) -> None:
    """Empty optional fields in CSV become None."""
    csv_content = """my_name,source,symbol,field,path,description,unit
test_entry,bloomberg,TEST Index,,,,
"""
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    catalog = Catalog.from_csv(csv_file)
    entry = catalog.get("test_entry")

    assert entry.field is None
    assert entry.path is None
    assert entry.description is None
    assert entry.unit is None


def test_catalog_from_csv_row_with_missing_fields_and_duplicate_name(tmp_path: Path) -> None:
    """Row with both missing fields and duplicate name reports only missing field errors.

    A row with missing required fields should be skipped before checking for
    duplicates, so only the missing field error should be reported, not a
    duplicate error for a name that may also appear in a valid row.
    """
    csv_content = """my_name,source,symbol
valid_entry,bloomberg,SPX Index
valid_entry,,
"""
    # Row 3 has valid_entry (duplicate), but also missing source and symbol.
    # Should report only missing field errors, not duplicate error.
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    with pytest.raises(CatalogValidationError) as exc_info:
        Catalog.from_csv(csv_file)

    error_message = str(exc_info.value)
    # Should have errors for missing source and symbol on row 3
    assert "Row 3:" in error_message
    assert "source" in error_message
    assert "symbol" in error_message
    # Should NOT have a duplicate error - row should be skipped before that check
    assert "Duplicate" not in error_message


def test_catalog_from_csv_missing_required_field_reports_all_errors(tmp_path: Path) -> None:
    """from_csv() reports all validation errors at once."""
    csv_content = """my_name,source,symbol
entry1,,TEST Index
entry2,bloomberg,
,localfile,DATA
"""
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    with pytest.raises(CatalogValidationError) as exc_info:
        Catalog.from_csv(csv_file)

    error_msg = str(exc_info.value)
    assert "Row 2" in error_msg  # missing source
    assert "Row 3" in error_msg  # missing symbol
    assert "Row 4" in error_msg  # missing my_name


def test_catalog_from_csv_duplicate_within_file(tmp_path: Path) -> None:
    """from_csv() detects duplicate my_name within same file."""
    csv_content = """my_name,source,symbol
duplicate,bloomberg,TEST1
other,bloomberg,TEST2
duplicate,localfile,TEST3
"""
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    with pytest.raises(CatalogValidationError) as exc_info:
        Catalog.from_csv(csv_file)

    error_msg = str(exc_info.value)
    assert "duplicate" in error_msg.lower()
    assert "Row 4" in error_msg


def test_catalog_from_csv_whitespace_trimmed(tmp_path: Path) -> None:
    """from_csv() trims whitespace from all values."""
    # Note: spaces around values test trimming; no trailing whitespace in source
    csv_content = (
        "my_name,source,symbol,description\n"
        "  spaced_entry  ,  bloomberg  ,  SPX Index  ,  Has spaces  \n"
    )
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    catalog = Catalog.from_csv(csv_file)
    entry = catalog.get("spaced_entry")

    assert entry.my_name == "spaced_entry"
    assert entry.source == "bloomberg"
    assert entry.symbol == "SPX Index"
    assert entry.description == "Has spaces"


def test_catalog_from_csv_extra_columns_ignored(tmp_path: Path) -> None:
    """from_csv() ignores extra columns in CSV."""
    csv_content = """my_name,source,symbol,notes,internal_id
test_entry,bloomberg,TEST Index,some note,12345
"""
    csv_file = tmp_path / "catalog.csv"
    csv_file.write_text(csv_content)

    catalog = Catalog.from_csv(csv_file)

    assert len(catalog) == 1
    entry = catalog.get("test_entry")
    assert entry.source == "bloomberg"


def test_catalog_from_csv_file_not_found() -> None:
    """from_csv() raises CatalogValidationError for missing file."""
    with pytest.raises(CatalogValidationError, match="not found"):
        Catalog.from_csv("/nonexistent/path/catalog.csv")


def test_catalog_from_csv_multiple_files(tmp_path: Path) -> None:
    """from_csv() loads and merges multiple CSV files."""
    csv1 = """my_name,source,symbol
entry1,bloomberg,TEST1
"""
    csv2 = """my_name,source,symbol
entry2,localfile,TEST2
"""
    file1 = tmp_path / "catalog1.csv"
    file2 = tmp_path / "catalog2.csv"
    file1.write_text(csv1)
    file2.write_text(csv2)

    catalog = Catalog.from_csv([file1, file2])

    assert len(catalog) == 2
    assert "entry1" in catalog
    assert "entry2" in catalog


def test_catalog_from_csv_duplicate_across_files(tmp_path: Path) -> None:
    """from_csv() detects duplicate my_name across files."""
    csv1 = """my_name,source,symbol
duplicate,bloomberg,TEST1
"""
    csv2 = """my_name,source,symbol
duplicate,localfile,TEST2
"""
    file1 = tmp_path / "catalog1.csv"
    file2 = tmp_path / "catalog2.csv"
    file1.write_text(csv1)
    file2.write_text(csv2)

    with pytest.raises(CatalogValidationError) as exc_info:
        Catalog.from_csv([file1, file2])

    error_msg = str(exc_info.value)
    assert "duplicate" in error_msg.lower()


def test_catalog_from_csv_mixed_path_types(tmp_path: Path) -> None:
    """from_csv() accepts mixed str and Path in list."""
    csv1 = """my_name,source,symbol
entry1,bloomberg,TEST1
"""
    csv2 = """my_name,source,symbol
entry2,localfile,TEST2
"""
    file1 = tmp_path / "catalog1.csv"
    file2 = tmp_path / "catalog2.csv"
    file1.write_text(csv1)
    file2.write_text(csv2)

    catalog = Catalog.from_csv([file1, str(file2)])

    assert len(catalog) == 2


# ============================================================================
# CSV Export Tests
# ============================================================================


def test_catalog_to_csv(tmp_path: Path) -> None:
    """Catalog.to_csv() exports entries to CSV file."""
    entry1 = CatalogEntry(
        my_name="sp500_close",
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        description="S&P 500 close",
        unit="points",
    )
    entry2 = CatalogEntry(
        my_name="gdp_us",
        source="localfile",
        symbol="GDP_US",
        path="/data/macro.csv",
    )
    catalog = Catalog({"sp500_close": entry1, "gdp_us": entry2})

    output = tmp_path / "output.csv"
    catalog.to_csv(output)

    assert output.exists()
    content = output.read_text()
    lines = content.strip().split("\n")

    # Check header
    assert lines[0] == "my_name,source,symbol,field,path,description,unit"

    # Check we have 3 lines (header + 2 entries)
    assert len(lines) == 3


def test_catalog_to_csv_accepts_string_path(tmp_path: Path) -> None:
    """to_csv() accepts string path."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="TEST",
    )
    catalog = Catalog({"test": entry})

    output = tmp_path / "output.csv"
    catalog.to_csv(str(output))

    assert output.exists()


def test_catalog_to_csv_none_values_as_empty(tmp_path: Path) -> None:
    """to_csv() writes None values as empty strings."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="TEST",
        # field, path, description, unit all None
    )
    catalog = Catalog({"test": entry})

    output = tmp_path / "output.csv"
    catalog.to_csv(output)

    content = output.read_text()
    lines = content.strip().split("\n")

    # Entry line should have empty fields for None values
    assert lines[1] == "test,bloomberg,TEST,,,,"


def test_catalog_to_csv_roundtrip(tmp_path: Path) -> None:
    """to_csv() output can be loaded back with from_csv()."""
    entry = CatalogEntry(
        my_name="sp500_close",
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        description="S&P 500 close",
        unit="points",
    )
    original = Catalog({"sp500_close": entry})

    csv_file = tmp_path / "roundtrip.csv"
    original.to_csv(csv_file)

    reloaded = Catalog.from_csv(csv_file)

    assert len(reloaded) == 1
    reloaded_entry = reloaded.get("sp500_close")
    assert reloaded_entry.source == entry.source
    assert reloaded_entry.symbol == entry.symbol
    assert reloaded_entry.field == entry.field
    assert reloaded_entry.description == entry.description
    assert reloaded_entry.unit == entry.unit


# ============================================================================
# YAML Export Tests
# ============================================================================


def test_catalog_to_yaml(tmp_path: Path) -> None:
    """Catalog.to_yaml() exports entries to YAML file."""
    entry = CatalogEntry(
        my_name="sp500_close",
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        description="S&P 500 close",
    )
    catalog = Catalog({"sp500_close": entry})

    output = tmp_path / "output.yaml"
    catalog.to_yaml(output)

    assert output.exists()
    content = output.read_text()
    assert "my_name: sp500_close" in content
    assert "source: bloomberg" in content
    assert "symbol: SPX Index" in content


def test_catalog_to_yaml_accepts_string_path(tmp_path: Path) -> None:
    """to_yaml() accepts string path."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="TEST",
    )
    catalog = Catalog({"test": entry})

    output = tmp_path / "output.yaml"
    catalog.to_yaml(str(output))

    assert output.exists()


def test_catalog_to_yaml_omits_none_fields(tmp_path: Path) -> None:
    """to_yaml() omits fields that are None."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="TEST",
        # field, path, description, unit all None
    )
    catalog = Catalog({"test": entry})

    output = tmp_path / "output.yaml"
    catalog.to_yaml(output)

    content = output.read_text()
    assert "field:" not in content
    assert "path:" not in content
    assert "description:" not in content
    assert "unit:" not in content


def test_catalog_to_yaml_roundtrip(tmp_path: Path) -> None:
    """to_yaml() output can be loaded back with from_yaml()."""
    entry = CatalogEntry(
        my_name="sp500_close",
        source="bloomberg",
        symbol="SPX Index",
        field="PX_LAST",
        description="S&P 500 close",
        unit="points",
    )
    original = Catalog({"sp500_close": entry})

    yaml_file = tmp_path / "roundtrip.yaml"
    original.to_yaml(yaml_file)

    reloaded = Catalog.from_yaml(yaml_file)

    assert len(reloaded) == 1
    reloaded_entry = reloaded.get("sp500_close")
    assert reloaded_entry.source == entry.source
    assert reloaded_entry.symbol == entry.symbol
    assert reloaded_entry.field == entry.field
    assert reloaded_entry.description == entry.description
    assert reloaded_entry.unit == entry.unit


# ============================================================================
# Full Roundtrip Tests
# ============================================================================


def test_catalog_csv_to_yaml_roundtrip(tmp_path: Path) -> None:
    """Catalog loaded from CSV, exported to YAML, reloaded from YAML matches original."""
    # Create CSV with comprehensive test data:
    # - Entry with all fields populated (bloomberg)
    # - Entry with optional fields as None (localfile)
    # - Entry from different source (macrobond)
    csv_content = """my_name,source,symbol,field,path,description,unit
sp500_close,bloomberg,SPX Index,PX_LAST,,S&P 500 closing price,points
gdp_us,localfile,GDP_US,,/data/macro.csv,,
uscpi,macrobond,uscpi,,,US Consumer Price Index,percent
minimal,bloomberg,TEST Index,PX_LAST,,,
"""
    csv_file = tmp_path / "original.csv"
    csv_file.write_text(csv_content)

    # Load from CSV
    original = Catalog.from_csv(csv_file)

    # Export to YAML
    yaml_file = tmp_path / "exported.yaml"
    original.to_yaml(yaml_file)

    # Reload from YAML
    reloaded = Catalog.from_yaml(yaml_file)

    # Assert all entries match
    assert len(reloaded) == len(original)
    for name in original.list_names():
        orig_entry = original.get(name)
        reload_entry = reloaded.get(name)

        assert reload_entry.my_name == orig_entry.my_name
        assert reload_entry.source == orig_entry.source
        assert reload_entry.symbol == orig_entry.symbol
        assert reload_entry.field == orig_entry.field
        assert reload_entry.path == orig_entry.path
        assert reload_entry.description == orig_entry.description
        assert reload_entry.unit == orig_entry.unit


class TestFromCsvSanitization:
    """Tests for CSV column sanitization."""

    def test_from_csv_strips_column_whitespace(self, tmp_path: Path) -> None:
        """Column names with trailing whitespace should be handled."""
        csv_file = tmp_path / "catalog.csv"
        # Note: columns have trailing spaces
        csv_file.write_text("my_name , source , symbol \ntest_entry,localfile,value\n")

        catalog = Catalog.from_csv(str(csv_file))
        entry = catalog.get("test_entry")

        assert entry.my_name == "test_entry"
        assert entry.source == "localfile"
        assert entry.symbol == "value"

    def test_from_csv_strips_value_whitespace(self, tmp_path: Path) -> None:
        """Values with whitespace should be stripped."""
        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text("my_name,source,symbol\n test_entry , localfile , value \n")

        catalog = Catalog.from_csv(str(csv_file))
        entry = catalog.get("test_entry")

        assert entry.my_name == "test_entry"
        assert entry.source == "localfile"
        assert entry.symbol == "value"


def test_from_yaml_rejects_macrobond_with_field(tmp_path: Path) -> None:
    """Macrobond entries must not have field set."""
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text(
        """
- my_name: us_gdp
  source: macrobond
  symbol: usgdp
  field: should_not_be_here
"""
    )

    with pytest.raises(CatalogValidationError, match="macrobond.*field"):
        Catalog.from_yaml(catalog_file)


def test_catalog_yaml_to_csv_roundtrip(tmp_path: Path) -> None:
    """Catalog loaded from YAML, exported to CSV, reloaded from CSV matches original."""
    # Create YAML with comprehensive test data:
    # - Entry with all fields populated (bloomberg)
    # - Entry with optional fields as None (localfile)
    # - Entry from different source (macrobond)
    yaml_content = """
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  description: S&P 500 closing price
  unit: points

- my_name: gdp_us
  source: localfile
  symbol: GDP_US
  path: /data/macro.csv

- my_name: uscpi
  source: macrobond
  symbol: uscpi
  description: US Consumer Price Index
  unit: percent

- my_name: minimal
  source: bloomberg
  symbol: TEST Index
  field: PX_LAST
"""
    yaml_file = tmp_path / "original.yaml"
    yaml_file.write_text(yaml_content)

    # Load from YAML
    original = Catalog.from_yaml(yaml_file)

    # Export to CSV
    csv_file = tmp_path / "exported.csv"
    original.to_csv(csv_file)

    # Reload from CSV
    reloaded = Catalog.from_csv(csv_file)

    # Assert all entries match
    assert len(reloaded) == len(original)
    for name in original.list_names():
        orig_entry = original.get(name)
        reload_entry = reloaded.get(name)

        assert reload_entry.my_name == orig_entry.my_name
        assert reload_entry.source == orig_entry.source
        assert reload_entry.symbol == orig_entry.symbol
        assert reload_entry.field == orig_entry.field
        assert reload_entry.path == orig_entry.path
        assert reload_entry.description == orig_entry.description
        assert reload_entry.unit == orig_entry.unit
