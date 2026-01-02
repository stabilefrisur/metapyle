# Source Attribute Validation Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Fail-fast validation at catalog load rejecting invalid source-attribute combinations.

**Architecture:** Add `_SOURCE_VALIDATION` dict with rules per source. Helper function `_validate_source_attributes()` returns error message or None. YAML loader raises immediately, CSV loader collects all errors.

**Tech Stack:** Python 3.12+, pytest, existing `CatalogValidationError`

---

## Task 1: Add Validation Data Structure and Helper

**Files:**
- Modify: `src/metapyle/catalog.py`

**Step 1: Write the failing test for macrobond with field**

Create test in `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_from_yaml_rejects_macrobond_with_field -v`
Expected: FAIL (currently no validation for this case)

**Step 3: Add validation structure and helper to catalog.py**

Add after `_SOURCE_COLUMNS` dict (around line 20):

```python
from enum import StrEnum, auto


class _AttrRule(StrEnum):
    """Validation rule for catalog entry attributes."""

    REQUIRED = auto()
    FORBIDDEN = auto()
    ALLOWED = auto()


_SOURCE_VALIDATION: dict[str, dict[str, _AttrRule]] = {
    "bloomberg": {"field": _AttrRule.REQUIRED, "path": _AttrRule.FORBIDDEN},
    "gsquant": {"field": _AttrRule.REQUIRED, "path": _AttrRule.FORBIDDEN, "params": _AttrRule.ALLOWED},
    "macrobond": {"field": _AttrRule.FORBIDDEN, "path": _AttrRule.FORBIDDEN},
    "localfile": {"field": _AttrRule.FORBIDDEN, "path": _AttrRule.REQUIRED},
}


def _validate_source_attributes(
    entry: CatalogEntry,
    source_file: str | Path,
) -> str | None:
    """
    Validate entry attributes against source-specific rules.

    Parameters
    ----------
    entry : CatalogEntry
        The catalog entry to validate.
    source_file : str | Path
        Path to the catalog file (for error messages).

    Returns
    -------
    str | None
        Error message if validation fails, None if valid.
        Unknown sources are not validated.
    """
    rules = _SOURCE_VALIDATION.get(entry.source)
    if rules is None:
        return None  # Unknown source - validated elsewhere

    attr_values = {
        "field": entry.field,
        "path": entry.path,
        "params": entry.params,
    }

    for attr, rule in rules.items():
        value = attr_values.get(attr)

        if rule == _AttrRule.REQUIRED and value is None:
            return (
                f"{entry.source.capitalize()} entry '{entry.my_name}' requires "
                f"'{attr}' but none provided in {source_file}"
            )
        if rule == _AttrRule.FORBIDDEN and value is not None:
            return (
                f"{entry.source.capitalize()} entry '{entry.my_name}' has '{attr}' set, "
                f"but {entry.source} does not use {attr}. Remove it. In {source_file}"
            )

    return None
```

**Step 4: Integrate into `_parse_entry()` and remove old Bloomberg check**

Replace the existing `_parse_entry` method:

```python
    @staticmethod
    def _parse_entry(raw: dict[str, Any], source_file: str | Path) -> CatalogEntry:
        """Parse a raw dictionary into a CatalogEntry."""
        required_fields = ["my_name", "source", "symbol"]

        for field in required_fields:
            if field not in raw:
                raise CatalogValidationError(f"Missing required field '{field}' in {source_file}")

        entry = CatalogEntry(
            my_name=raw["my_name"],
            source=raw["source"],
            symbol=raw["symbol"],
            field=raw.get("field"),
            path=raw.get("path"),
            description=raw.get("description"),
            unit=raw.get("unit"),
            params=raw.get("params"),
        )

        # Validate source-specific attributes
        error = _validate_source_attributes(entry, source_file)
        if error:
            raise CatalogValidationError(error)

        return entry
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::test_from_yaml_rejects_macrobond_with_field -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add source attribute validation for macrobond field"
```

---

## Task 2: Add Remaining YAML Validation Tests

**Files:**
- Modify: `tests/unit/test_catalog.py`

**Step 1: Write tests for all forbidden combinations**

Add to `tests/unit/test_catalog.py`:

```python
def test_from_yaml_rejects_macrobond_with_path(tmp_path: Path) -> None:
    """Macrobond entries must not have path set."""
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text(
        """
- my_name: us_gdp
  source: macrobond
  symbol: usgdp
  path: /some/path.csv
"""
    )

    with pytest.raises(CatalogValidationError, match="macrobond.*path"):
        Catalog.from_yaml(catalog_file)


def test_from_yaml_rejects_bloomberg_with_path(tmp_path: Path) -> None:
    """Bloomberg entries must not have path set."""
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text(
        """
- my_name: spx
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  path: /some/path.csv
"""
    )

    with pytest.raises(CatalogValidationError, match="bloomberg.*path"):
        Catalog.from_yaml(catalog_file)


def test_from_yaml_rejects_gsquant_with_path(tmp_path: Path) -> None:
    """GSQuant entries must not have path set."""
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text(
        """
- my_name: vol_data
  source: gsquant
  symbol: SPX
  field: EDRVOL_PERCENT_STOCK_1M::impliedVolatility
  path: /some/path.csv
"""
    )

    with pytest.raises(CatalogValidationError, match="gsquant.*path"):
        Catalog.from_yaml(catalog_file)


def test_from_yaml_rejects_localfile_with_field(tmp_path: Path) -> None:
    """Localfile entries must not have field set."""
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text(
        """
- my_name: sp500
  source: localfile
  symbol: close
  path: /data/prices.csv
  field: should_not_be_here
"""
    )

    with pytest.raises(CatalogValidationError, match="localfile.*field"):
        Catalog.from_yaml(catalog_file)


def test_from_yaml_rejects_localfile_without_path(tmp_path: Path) -> None:
    """Localfile entries require path."""
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text(
        """
- my_name: sp500
  source: localfile
  symbol: close
"""
    )

    with pytest.raises(CatalogValidationError, match="localfile.*path.*required"):
        Catalog.from_yaml(catalog_file)
```

**Step 2: Run all new tests**

Run: `pytest tests/unit/test_catalog.py -k "rejects" -v`
Expected: All PASS

**Step 3: Commit**

```bash
git add tests/unit/test_catalog.py
git commit -m "test(catalog): add validation tests for all source-attribute rules"
```

---

## Task 3: Integrate Validation into CSV Loader

**Files:**
- Modify: `src/metapyle/catalog.py`

**Step 1: Write test for CSV collecting multiple errors**

Add to `tests/unit/test_catalog.py`:

```python
def test_from_csv_collects_multiple_validation_errors(tmp_path: Path) -> None:
    """CSV loader should report all validation errors at once."""
    catalog_file = tmp_path / "catalog.csv"
    catalog_file.write_text(
        """my_name,source,symbol,field,path,description,unit
us_gdp,macrobond,usgdp,bad_field,,,
sp500,localfile,close,,,,
"""
    )

    with pytest.raises(CatalogValidationError) as exc_info:
        Catalog.from_csv(catalog_file)

    error_msg = str(exc_info.value)
    assert "2 error" in error_msg
    assert "macrobond" in error_msg
    assert "field" in error_msg
    assert "localfile" in error_msg
    assert "path" in error_msg
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::test_from_csv_collects_multiple_validation_errors -v`
Expected: FAIL (CSV loader doesn't validate attributes yet)

**Step 3: Add validation to from_csv()**

In `from_csv()`, add validation after entry creation (around line 180, after the `entries[my_name] = entry` line):

Find this block:
```python
                    # Create entry (empty strings become None)
                    entry = CatalogEntry(
                        my_name=my_name,
                        source=row.get("source", ""),
                        symbol=row.get("symbol", ""),
                        field=row.get("field") or None,
                        path=row.get("path") or None,
                        description=row.get("description") or None,
                        unit=row.get("unit") or None,
                        params=params,
                    )
                    entries[my_name] = entry
```

Replace with:
```python
                    # Create entry (empty strings become None)
                    entry = CatalogEntry(
                        my_name=my_name,
                        source=row.get("source", ""),
                        symbol=row.get("symbol", ""),
                        field=row.get("field") or None,
                        path=row.get("path") or None,
                        description=row.get("description") or None,
                        unit=row.get("unit") or None,
                        params=params,
                    )

                    # Validate source-specific attributes
                    attr_error = _validate_source_attributes(entry, path)
                    if attr_error:
                        errors.append(f"Row {row_num}: {attr_error}")
                        continue

                    entries[my_name] = entry
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::test_from_csv_collects_multiple_validation_errors -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add source attribute validation to CSV loader"
```

---

## Task 4: Run Full Test Suite and Fix Regressions

**Files:**
- Potentially modify: existing test fixtures if they have invalid data

**Step 1: Run full test suite**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: All pass (check for regressions from old Bloomberg-only validation)

**Step 2: Run all unit tests**

Run: `pytest tests/unit/ -v`
Expected: All pass

**Step 3: Run linting and type checking**

Run: `ruff check src/metapyle/catalog.py && mypy src/metapyle/catalog.py`
Expected: No errors

**Step 4: Commit any fixes**

If any test fixtures needed updating:
```bash
git add -A
git commit -m "fix(tests): update fixtures for new validation rules"
```

---

## Task 5: Update TODO.md

**Files:**
- Modify: `TODO.md`

**Step 1: Remove completed item from TODO**

Remove this line from TODO.md:
```
- Column matching for macrobond fails when the user inputs field by mistake in the catalog. should fast fail on catalog validation.
```

**Step 2: Commit**

```bash
git add TODO.md
git commit -m "docs: mark source attribute validation as complete"
```

---

## Verification

After all tasks complete:

```bash
pytest tests/unit/ -v
ruff check .
mypy src/
```

All must pass before merging.
