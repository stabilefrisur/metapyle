# Catalog CSV Tools Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Add CSV import/export methods to the Catalog class for easier catalog creation and maintenance.

**Architecture:** Extend existing `Catalog` class with `from_csv()`, `to_csv()`, `to_yaml()`, and `csv_template()` methods. Update `from_yaml()` signature to accept `Path` objects. Use stdlib `csv` module for CSV handling.

**Tech Stack:** Python 3.12+, stdlib csv module, PyYAML (existing dependency)

---

## Task 1: Update from_yaml() to Accept Path Objects

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
def test_catalog_load_from_yaml_with_path_object(tmp_path: Path) -> None:
    """Catalog.from_yaml() accepts Path objects."""
    yaml_content = """
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
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
"""
    yaml2 = """
- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
"""
    file1 = tmp_path / "catalog1.yaml"
    file2 = tmp_path / "catalog2.yaml"
    file1.write_text(yaml1)
    file2.write_text(yaml2)

    # Mix Path and str
    catalog = Catalog.from_yaml([file1, str(file2)])

    assert len(catalog) == 2
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_catalog_load_from_yaml_with_path_object tests/unit/test_catalog.py::test_catalog_load_from_yaml_with_mixed_path_types -v`

Expected: FAIL (type error or Path not converted)

**Step 3: Update from_yaml() signature and implementation**

In `src/metapyle/catalog.py`, update the method signature and normalize paths:

```python
@classmethod
def from_yaml(cls, paths: str | Path | list[str | Path]) -> Self:
    """
    Load catalog entries from one or more YAML files.

    Parameters
    ----------
    paths : str | Path | list[str | Path]
        Path or list of paths to YAML catalog files.

    Returns
    -------
    Catalog
        Catalog instance with loaded entries.

    Raises
    ------
    CatalogValidationError
        If file not found, YAML malformed, or entries invalid.
    DuplicateNameError
        If the same my_name appears in multiple entries.
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]

    entries: dict[str, CatalogEntry] = {}

    for path in paths:
        file_path = Path(path)

        if not file_path.exists():
            raise CatalogValidationError(f"Catalog file not found: {path}")

        logger.info("loading_catalog: path=%s", path)

        try:
            with open(file_path) as f:
                raw_entries = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise CatalogValidationError(f"Malformed YAML in {path}: {e}") from e

        if not isinstance(raw_entries, list):
            raise CatalogValidationError(f"Catalog file {path} must contain a list of entries")

        for raw in raw_entries:
            entry = cls._parse_entry(raw, str(path))

            if entry.my_name in entries:
                raise DuplicateNameError(f"Duplicate catalog name: {entry.my_name}")

            entries[entry.my_name] = entry

    logger.info("catalog_loaded: entries=%d", len(entries))
    return cls(entries)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -v`

Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): accept Path objects in from_yaml()"
```

---

## Task 2: Implement csv_template() - Generic Template

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_csv_template_generic tests/unit/test_catalog.py::test_csv_template_generic_writes_file tests/unit/test_catalog.py::test_csv_template_generic_writes_file_str_path -v`

Expected: FAIL with "AttributeError: type object 'Catalog' has no attribute 'csv_template'"

**Step 3: Implement csv_template() for generic case**

Add to `src/metapyle/catalog.py`:

```python
# Column definitions at module level
_ALL_COLUMNS = ["my_name", "source", "symbol", "field", "path", "description", "unit"]

# In Catalog class:
@staticmethod
def csv_template(source: str | None = None, path: str | Path | None = None) -> str:
    """
    Generate CSV template with headers.

    Parameters
    ----------
    source : str | None, optional
        If provided, generates source-specific template with relevant
        columns only. Valid: "bloomberg", "localfile", "macrobond".
        If None, includes all columns.
    path : str | Path | None, optional
        If provided, writes template to file.

    Returns
    -------
    str
        Template string (header row + optional example row).
    """
    if source is None:
        columns = _ALL_COLUMNS
        template = ",".join(columns) + "\n"
    else:
        # Source-specific templates handled in next task
        raise NotImplementedError(f"Source-specific template not yet implemented: {source}")

    if path is not None:
        Path(path).write_text(template)

    return template
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::test_csv_template_generic tests/unit/test_catalog.py::test_csv_template_generic_writes_file tests/unit/test_catalog.py::test_csv_template_generic_writes_file_str_path -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add csv_template() for generic templates"
```

---

## Task 3: Implement csv_template() - Source-Specific Templates

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_csv_template_bloomberg tests/unit/test_catalog.py::test_csv_template_localfile tests/unit/test_catalog.py::test_csv_template_macrobond -v`

Expected: FAIL with "NotImplementedError"

**Step 3: Implement source-specific templates**

Update `src/metapyle/catalog.py`:

```python
# Column definitions at module level
_ALL_COLUMNS = ["my_name", "source", "symbol", "field", "path", "description", "unit"]
_SOURCE_COLUMNS: dict[str, list[str]] = {
    "bloomberg": ["my_name", "source", "symbol", "field", "description", "unit"],
    "localfile": ["my_name", "source", "symbol", "path", "description", "unit"],
    "macrobond": ["my_name", "source", "symbol", "description", "unit"],
}

# Update csv_template() method:
@staticmethod
def csv_template(source: str | None = None, path: str | Path | None = None) -> str:
    """
    Generate CSV template with headers.

    Parameters
    ----------
    source : str | None, optional
        If provided, generates source-specific template with relevant
        columns only. Valid: "bloomberg", "localfile", "macrobond".
        If None, includes all columns.
    path : str | Path | None, optional
        If provided, writes template to file.

    Returns
    -------
    str
        Template string (header row + optional example row).
    """
    if source is None:
        columns = _ALL_COLUMNS
        template = ",".join(columns) + "\n"
    else:
        if source not in _SOURCE_COLUMNS:
            valid = ", ".join(sorted(_SOURCE_COLUMNS.keys()))
            raise ValueError(f"Unknown source: {source}. Valid sources: {valid}")

        columns = _SOURCE_COLUMNS[source]
        header = ",".join(columns)
        # Example row: only source column filled, rest empty
        example_values = [source if col == "source" else "" for col in columns]
        example = ",".join(example_values)
        template = f"{header}\n{example}\n"

    if path is not None:
        Path(path).write_text(template)

    return template
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::test_csv_template_bloomberg tests/unit/test_catalog.py::test_csv_template_localfile tests/unit/test_catalog.py::test_csv_template_macrobond -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add source-specific csv_template() support"
```

---

## Task 4: Implement from_csv() - Basic Loading

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_catalog.py`:

```python
def test_catalog_from_csv_single_file(tmp_path: Path) -> None:
    """Catalog.from_csv() loads entries from CSV file."""
    csv_content = """my_name,source,symbol,field,path,description,unit
sp500_close,bloomberg,SPX Index,PX_LAST,,S&P 500 close,points
gdp_us,localfile,GDP_US,/data/macro.csv,,US GDP,USD billions
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_catalog_from_csv_single_file tests/unit/test_catalog.py::test_catalog_from_csv_accepts_string_path tests/unit/test_catalog.py::test_catalog_from_csv_empty_optional_fields_are_none -v`

Expected: FAIL with "AttributeError: type object 'Catalog' has no attribute 'from_csv'"

**Step 3: Implement from_csv() basic functionality**

Add import at top of `src/metapyle/catalog.py`:

```python
import csv
```

Add method to Catalog class:

```python
@classmethod
def from_csv(cls, paths: str | Path | list[str | Path]) -> Self:
    """
    Load catalog entries from one or more CSV files.

    Parameters
    ----------
    paths : str | Path | list[str | Path]
        Path or list of paths to CSV catalog files.

    Returns
    -------
    Catalog
        Catalog instance with loaded entries.

    Raises
    ------
    CatalogValidationError
        If file not found, CSV malformed, or entries invalid.
        Reports all validation errors at once.
    DuplicateNameError
        If the same my_name appears in multiple entries.
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]

    entries: dict[str, CatalogEntry] = {}
    errors: list[str] = []

    for path in paths:
        file_path = Path(path)

        if not file_path.exists():
            raise CatalogValidationError(f"Catalog file not found: {path}")

        logger.info("loading_catalog_csv: path=%s", path)

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, start=2):  # Row 1 is header
                # Trim whitespace from all values
                row = {k: v.strip() if v else v for k, v in row.items()}

                # Check required fields
                for field in ["my_name", "source", "symbol"]:
                    if not row.get(field):
                        errors.append(f"Row {row_num}: Missing required field '{field}'")

                if errors and not row.get("my_name"):
                    continue  # Skip row if my_name missing

                my_name = row.get("my_name", "")

                # Check for duplicates
                if my_name in entries:
                    errors.append(f"Row {row_num}: Duplicate my_name '{my_name}'")
                    continue

                # Skip if we have errors for this row
                if any(f"Row {row_num}:" in e for e in errors):
                    continue

                # Create entry (empty strings become None)
                entry = CatalogEntry(
                    my_name=my_name,
                    source=row.get("source", ""),
                    symbol=row.get("symbol", ""),
                    field=row.get("field") or None,
                    path=row.get("path") or None,
                    description=row.get("description") or None,
                    unit=row.get("unit") or None,
                )
                entries[my_name] = entry

    if errors:
        error_list = "\n  ".join(errors)
        raise CatalogValidationError(f"{len(errors)} error(s) in CSV:\n  {error_list}")

    logger.info("catalog_loaded_csv: entries=%d", len(entries))
    return cls(entries)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::test_catalog_from_csv_single_file tests/unit/test_catalog.py::test_catalog_from_csv_accepts_string_path tests/unit/test_catalog.py::test_catalog_from_csv_empty_optional_fields_are_none -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add from_csv() basic loading"
```

---

## Task 5: Implement from_csv() - Validation & Error Handling

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_catalog.py`:

```python
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
    csv_content = """my_name,source,symbol,description
  spaced_entry  ,  bloomberg  ,  SPX Index  ,  Has spaces  
"""
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
```

**Step 2: Run test to verify current state**

Run: `pytest tests/unit/test_catalog.py -k "from_csv" -v`

Expected: Most should pass with existing implementation; verify all pass

**Step 3: Commit (if tests pass)**

```bash
git add tests/unit/test_catalog.py
git commit -m "test(catalog): add from_csv() validation tests"
```

---

## Task 6: Implement from_csv() - Multiple Files

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify current state**

Run: `pytest tests/unit/test_catalog.py::test_catalog_from_csv_multiple_files tests/unit/test_catalog.py::test_catalog_from_csv_duplicate_across_files tests/unit/test_catalog.py::test_catalog_from_csv_mixed_path_types -v`

Expected: Should pass with existing implementation

**Step 3: Commit**

```bash
git add tests/unit/test_catalog.py
git commit -m "test(catalog): add from_csv() multi-file tests"
```

---

## Task 7: Implement to_csv()

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_catalog_to_csv tests/unit/test_catalog.py::test_catalog_to_csv_accepts_string_path tests/unit/test_catalog.py::test_catalog_to_csv_none_values_as_empty tests/unit/test_catalog.py::test_catalog_to_csv_roundtrip -v`

Expected: FAIL with "AttributeError: 'Catalog' object has no attribute 'to_csv'"

**Step 3: Implement to_csv()**

Add method to Catalog class in `src/metapyle/catalog.py`:

```python
def to_csv(self, path: str | Path) -> None:
    """
    Export catalog entries to CSV file.

    Parameters
    ----------
    path : str | Path
        Path to output CSV file.
    """
    file_path = Path(path)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_ALL_COLUMNS)
        writer.writeheader()

        for entry in self._entries.values():
            writer.writerow({
                "my_name": entry.my_name,
                "source": entry.source,
                "symbol": entry.symbol,
                "field": entry.field or "",
                "path": entry.path or "",
                "description": entry.description or "",
                "unit": entry.unit or "",
            })

    logger.info("catalog_exported_csv: path=%s, entries=%d", path, len(self._entries))
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -k "to_csv" -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add to_csv() export method"
```

---

## Task 8: Implement to_yaml()

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_catalog_to_yaml tests/unit/test_catalog.py::test_catalog_to_yaml_accepts_string_path tests/unit/test_catalog.py::test_catalog_to_yaml_omits_none_fields tests/unit/test_catalog.py::test_catalog_to_yaml_roundtrip -v`

Expected: FAIL with "AttributeError: 'Catalog' object has no attribute 'to_yaml'"

**Step 3: Implement to_yaml()**

Add method to Catalog class in `src/metapyle/catalog.py`:

```python
def to_yaml(self, path: str | Path) -> None:
    """
    Export catalog entries to YAML file.

    Parameters
    ----------
    path : str | Path
        Path to output YAML file.
    """
    file_path = Path(path)

    entries_list = []
    for entry in self._entries.values():
        entry_dict: dict[str, str] = {
            "my_name": entry.my_name,
            "source": entry.source,
            "symbol": entry.symbol,
        }
        # Only include non-None optional fields
        if entry.field is not None:
            entry_dict["field"] = entry.field
        if entry.path is not None:
            entry_dict["path"] = entry.path
        if entry.description is not None:
            entry_dict["description"] = entry.description
        if entry.unit is not None:
            entry_dict["unit"] = entry.unit

        entries_list.append(entry_dict)

    with open(file_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(entries_list, f, default_flow_style=False, sort_keys=False)

    logger.info("catalog_exported_yaml: path=%s, entries=%d", path, len(self._entries))
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py -k "to_yaml" -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add to_yaml() export method"
```

---

## Task 9: Full Roundtrip Tests

**Files:**
- Test: `tests/unit/test_catalog.py`

**Step 1: Write roundtrip tests**

Add to `tests/unit/test_catalog.py`:

```python
def test_roundtrip_csv_to_yaml_to_csv(tmp_path: Path) -> None:
    """CSV → YAML → CSV roundtrip preserves data."""
    original_csv = """my_name,source,symbol,field,path,description,unit
sp500_close,bloomberg,SPX Index,PX_LAST,,S&P 500 close,points
gdp_us,localfile,GDP_US,,/data/macro.csv,US GDP,USD billions
macro_data,macrobond,usgdp,,,US GDP from Macrobond,
"""
    csv1 = tmp_path / "original.csv"
    csv1.write_text(original_csv)

    # CSV → Catalog → YAML
    catalog1 = Catalog.from_csv(csv1)
    yaml_file = tmp_path / "intermediate.yaml"
    catalog1.to_yaml(yaml_file)

    # YAML → Catalog → CSV
    catalog2 = Catalog.from_yaml(yaml_file)
    csv2 = tmp_path / "final.csv"
    catalog2.to_csv(csv2)

    # Reload and compare
    catalog3 = Catalog.from_csv(csv2)

    assert len(catalog3) == 3

    spx = catalog3.get("sp500_close")
    assert spx.source == "bloomberg"
    assert spx.field == "PX_LAST"
    assert spx.description == "S&P 500 close"

    gdp = catalog3.get("gdp_us")
    assert gdp.source == "localfile"
    assert gdp.path == "/data/macro.csv"


def test_roundtrip_yaml_to_csv_to_yaml(tmp_path: Path) -> None:
    """YAML → CSV → YAML roundtrip preserves data."""
    original_yaml = """
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  description: S&P 500 close
  unit: points

- my_name: gdp_us
  source: localfile
  symbol: GDP_US
  path: /data/macro.csv
"""
    yaml1 = tmp_path / "original.yaml"
    yaml1.write_text(original_yaml)

    # YAML → Catalog → CSV
    catalog1 = Catalog.from_yaml(yaml1)
    csv_file = tmp_path / "intermediate.csv"
    catalog1.to_csv(csv_file)

    # CSV → Catalog → YAML
    catalog2 = Catalog.from_csv(csv_file)
    yaml2 = tmp_path / "final.yaml"
    catalog2.to_yaml(yaml2)

    # Reload and compare
    catalog3 = Catalog.from_yaml(yaml2)

    assert len(catalog3) == 2

    spx = catalog3.get("sp500_close")
    assert spx.source == "bloomberg"
    assert spx.field == "PX_LAST"

    gdp = catalog3.get("gdp_us")
    assert gdp.path == "/data/macro.csv"
```

**Step 2: Run tests**

Run: `pytest tests/unit/test_catalog.py::test_roundtrip_csv_to_yaml_to_csv tests/unit/test_catalog.py::test_roundtrip_yaml_to_csv_to_yaml -v`

Expected: PASS

**Step 3: Commit**

```bash
git add tests/unit/test_catalog.py
git commit -m "test(catalog): add full roundtrip tests"
```

---

## Task 10: Update Public API Exports

**Files:**
- Modify: `src/metapyle/__init__.py`

**Step 1: Verify current exports**

Read `src/metapyle/__init__.py` to check what's currently exported.

**Step 2: Update __all__ if needed**

The `Catalog` class is already exported. The new methods (`from_csv`, `to_csv`, `to_yaml`, `csv_template`) are class/instance methods and will be automatically available.

No changes needed to `__init__.py`.

**Step 3: Commit (skip if no changes)**

If no changes needed, skip this commit.

---

## Task 11: Update User Guide Documentation

**Files:**
- Modify: `docs/user-guide.md`

**Step 1: Add Catalog Tools section**

Add the following section after "Organizing Large Catalogs" in the Catalog Configuration section:

```markdown
### Creating Catalogs from CSV

For bulk catalog creation, use CSV files instead of writing YAML by hand:

#### Generate a Template

```python
from metapyle import Catalog

# Generic template (all columns)
template = Catalog.csv_template()
print(template)
# my_name,source,symbol,field,path,description,unit

# Source-specific template (includes example row)
template = Catalog.csv_template(source="bloomberg")
print(template)
# my_name,source,symbol,field,description,unit
# ,bloomberg,,,,

# Write template to file
Catalog.csv_template(source="bloomberg", path="bloomberg_template.csv")
```

#### Fill in the CSV

Open the template in Excel or Google Sheets and fill in your data:

| my_name | source | symbol | field | description | unit |
|---------|--------|--------|-------|-------------|------|
| sp500_close | bloomberg | SPX Index | PX_LAST | S&P 500 close | points |
| nasdaq_close | bloomberg | NDX Index | PX_LAST | Nasdaq close | points |

#### Convert to YAML

```python
from metapyle import Catalog

# Load from CSV
catalog = Catalog.from_csv("my_entries.csv")

# Export to YAML
catalog.to_yaml("catalog.yaml")
```

#### Editing Existing Catalogs

Export an existing catalog to CSV for bulk editing:

```python
from metapyle import Catalog

# Load existing catalog
catalog = Catalog.from_yaml("catalog.yaml")

# Export to CSV for editing in Excel
catalog.to_csv("catalog_edit.csv")

# After editing, convert back
edited = Catalog.from_csv("catalog_edit.csv")
edited.to_yaml("catalog.yaml")
```

#### Validation

`from_csv()` validates all rows and reports all errors at once:

```python
# If there are errors:
# CatalogValidationError: 3 error(s) in CSV:
#   Row 2: Missing required field 'symbol'
#   Row 5: Missing required field 'source'
#   Row 8: Duplicate my_name 'sp500_close'
```
```

**Step 2: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: add catalog CSV tools to user guide"
```

---

## Task 12: Run Full Test Suite & Type Check

**Files:**
- None (verification only)

**Step 1: Run all tests**

Run: `pytest tests/ -v`

Expected: All tests PASS

**Step 2: Run type check**

Run: `mypy src/`

Expected: No errors

**Step 3: Run linter**

Run: `ruff check .`

Expected: No errors

**Step 4: Format check**

Run: `ruff format --check .`

Expected: No changes needed

**Step 5: Final commit (if any fixes needed)**

If any fixes were needed, commit them:

```bash
git add -A
git commit -m "fix: address type/lint issues"
```

---

## Summary

| Task | Description | Commit Message |
|------|-------------|----------------|
| 1 | Update `from_yaml()` to accept Path | `feat(catalog): accept Path objects in from_yaml()` |
| 2 | Implement `csv_template()` generic | `feat(catalog): add csv_template() for generic templates` |
| 3 | Implement `csv_template()` source-specific | `feat(catalog): add source-specific csv_template() support` |
| 4 | Implement `from_csv()` basic | `feat(catalog): add from_csv() basic loading` |
| 5 | Add `from_csv()` validation tests | `test(catalog): add from_csv() validation tests` |
| 6 | Add `from_csv()` multi-file tests | `test(catalog): add from_csv() multi-file tests` |
| 7 | Implement `to_csv()` | `feat(catalog): add to_csv() export method` |
| 8 | Implement `to_yaml()` | `feat(catalog): add to_yaml() export method` |
| 9 | Full roundtrip tests | `test(catalog): add full roundtrip tests` |
| 10 | Update public API (if needed) | (skip if no changes) |
| 11 | Update user guide | `docs: add catalog CSV tools to user guide` |
| 12 | Final verification | (fix commits if needed) |
