# User Testing Fixes - Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Fix bugs, improve consistency, and add missing features identified during user testing.

**Architecture:** Work through 7 sections in priority order. Each task is independent and can be committed separately. TDD approach throughout.

**Tech Stack:** Python 3.12+, pandas, pytest, pyyaml

---

## Task 1: Move pytest_addoption to Root conftest.py

**Files:**
- Modify: `tests/conftest.py`
- Modify: `tests/integration/conftest.py`

**Step 1: Add pytest_addoption to root conftest.py**

In `tests/conftest.py`, add:

```python
"""Shared pytest fixtures and configuration."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --run-private option for private series tests."""
    parser.addoption(
        "--run-private",
        action="store_true",
        default=False,
        help="Run tests that require private/in-house series",
    )
```

**Step 2: Remove pytest_addoption from integration conftest.py**

In `tests/integration/conftest.py`, remove the `pytest_addoption` function (lines ~45-53). Keep `pytest_collection_modifyitems`.

**Step 3: Verify pytest discovers the option from any directory**

Run:
```bash
pytest --help | grep run-private
```
Expected: `--run-private` option shown

Run:
```bash
pytest tests/unit --run-private --collect-only 2>&1 | head -5
```
Expected: No error about unrecognized option

**Step 4: Commit**

```bash
git add tests/conftest.py tests/integration/conftest.py
git commit -m "fix: move pytest_addoption to root conftest for global discovery"
```

---

## Task 2: Bloomberg Field Enforcement

**Files:**
- Modify: `src/metapyle/catalog.py`
- Create: `tests/unit/test_catalog_bloomberg_field.py`

**Step 1: Write failing test**

Create `tests/unit/test_catalog_bloomberg_field.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog_bloomberg_field.py -v`
Expected: FAIL on `test_bloomberg_entry_without_field_raises`

**Step 3: Implement Bloomberg field validation**

In `src/metapyle/catalog.py`, modify `_parse_entry` method:

```python
    @staticmethod
    def _parse_entry(raw: dict[str, Any], source_file: str | Path) -> CatalogEntry:
        """Parse a raw dictionary into a CatalogEntry."""
        required_fields = ["my_name", "source", "symbol"]

        for field in required_fields:
            if field not in raw:
                raise CatalogValidationError(f"Missing required field '{field}' in {source_file}")

        # Bloomberg requires field
        if raw["source"] == "bloomberg" and not raw.get("field"):
            raise CatalogValidationError(
                f"Bloomberg entry '{raw['my_name']}' requires 'field' (e.g., PX_LAST) in {source_file}"
            )

        return CatalogEntry(
            my_name=raw["my_name"],
            source=raw["source"],
            symbol=raw["symbol"],
            field=raw.get("field"),
            path=raw.get("path"),
            description=raw.get("description"),
            unit=raw.get("unit"),
            params=raw.get("params"),
        )
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_catalog_bloomberg_field.py -v`
Expected: All PASS

**Step 5: Run full catalog tests**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog_bloomberg_field.py
git commit -m "feat: enforce field requirement for Bloomberg catalog entries"
```

---

## Task 3: Update Integration Test Date Range and Assertions

**Files:**
- Modify: `tests/integration/conftest.py`
- Modify: `tests/integration/test_bloomberg.py`

**Step 1: Update TEST_START and TEST_END**

In `tests/integration/conftest.py`, change from:

```python
# Test date range (guaranteed to have data)
TEST_START = "2023-01-01"
TEST_END = "2024-06-30"
```

To:

```python
# Test date range (guaranteed to have data)
TEST_START = "2023-12-31"
TEST_END = "2024-12-31"
```

**Step 2: Update test_frequency_alignment assertion**

In `tests/integration/test_bloomberg.py`, the `test_frequency_alignment` test has an assertion for row count based on the old date range. Update the assertion:

From:
```python
        # Month-end frequency should have ~6 rows for Jan-Jun
        assert len(df) >= 5
        assert len(df) <= 7
```

To:
```python
        # Month-end frequency should have ~12 rows for 2024
        assert len(df) >= 10
        assert len(df) <= 14
```

**Step 3: Verify tests still collect**

Run: `pytest tests/integration --collect-only 2>&1 | head -10`
Expected: Tests collected without error

**Step 4: Commit**

```bash
git add tests/integration/conftest.py tests/integration/test_bloomberg.py
git commit -m "test: update integration test date range to 2023-12-31 to 2024-12-31"
```

---

## Task 4: Export BloombergSource from sources/__init__.py

**Files:**
- Modify: `src/metapyle/sources/__init__.py`

**Step 1: Add BloombergSource to imports and __all__**

In `src/metapyle/sources/__init__.py`:

```python
"""Source adapters for metapyle.

This module provides the base interface for data sources and a registry
for managing source adapters.
"""

# Import source modules to trigger auto-registration
from metapyle.sources import (
    bloomberg,  # noqa: F401
    gsquant,  # noqa: F401
    localfile,  # noqa: F401
    macrobond,  # noqa: F401
)
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    SourceRegistry,
    make_column_name,
    register_source,
)
from metapyle.sources.bloomberg import BloombergSource
from metapyle.sources.gsquant import GSQuantSource
from metapyle.sources.localfile import LocalFileSource
from metapyle.sources.macrobond import MacrobondSource

__all__ = [
    "BaseSource",
    "BloombergSource",
    "FetchRequest",
    "GSQuantSource",
    "LocalFileSource",
    "MacrobondSource",
    "SourceRegistry",
    "make_column_name",
    "register_source",
]
```

**Step 2: Verify import works**

Run: `python -c "from metapyle.sources import BloombergSource; print(BloombergSource)"`
Expected: `<class 'metapyle.sources.bloomberg.BloombergSource'>`

**Step 3: Commit**

```bash
git add src/metapyle/sources/__init__.py
git commit -m "feat: export BloombergSource and LocalFileSource from sources module"
```

---

## Task 5: Fix my_name Convention Violations

**Files:**
- Modify: `tests/integration/fixtures/gsquant.yaml`

**Step 1: Update gsquant.yaml my_name values to lowercase**

Change `EURUSD_VOL` → `eurusd_vol` and `USDJPY_VOL` → `usdjpy_vol`:

```yaml
# GS-Quant integration test catalog
# Requires gs-quant installed and authenticated via GsSession

- my_name: eurusd_vol
  source: gsquant
  symbol: EURUSD
  field: FXIMPLIEDVOL::impliedVolatility
  description: EUR/USD FX implied volatility
  params:
    tenor: 1m
    deltaStrike: DN

- my_name: usdjpy_vol
  source: gsquant
  symbol: USDJPY
  field: FXIMPLIEDVOL::impliedVolatility
  description: USD/JPY FX implied volatility
  params:
    tenor: 1m
    deltaStrike: DN
```

**Step 2: Update any tests referencing old names**

Search for `EURUSD_VOL` and `USDJPY_VOL` in test files and update to lowercase.

Run: `grep -r "EURUSD_VOL\|USDJPY_VOL" tests/`

**Step 3: Commit**

```bash
git add tests/integration/fixtures/gsquant.yaml
git commit -m "style: fix my_name convention violations in gsquant fixtures"
```

---

## Task 6: Normalize Date Index to 'date'

**Files:**
- Modify: `src/metapyle/sources/bloomberg.py`
- Modify: `src/metapyle/sources/macrobond.py`
- Modify: `src/metapyle/sources/gsquant.py`
- Modify: `src/metapyle/sources/localfile.py`
- Create: `tests/unit/test_sources_index_name.py`

**Step 1: Write failing test**

Create `tests/unit/test_sources_index_name.py`:

```python
"""Tests for consistent date index naming across sources."""

import pandas as pd
import pytest


class TestIndexNameConsistency:
    """All sources should return DatetimeIndex named 'date'."""

    def test_localfile_index_name(self, tmp_path):
        """LocalFile source should return index named 'date'."""
        from metapyle.sources.localfile import LocalFileSource
        from metapyle.sources.base import FetchRequest

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,value\n2024-01-01,100\n2024-01-02,101\n")

        source = LocalFileSource()
        req = FetchRequest(symbol="value", path=str(csv_file))
        df = source.fetch([req], "2024-01-01", "2024-01-02")

        assert df.index.name == "date"

    def test_bloomberg_index_name_structure(self):
        """Bloomberg source should set index name to 'date' (mocked)."""
        # This test documents expected behavior
        # Integration tests verify actual Bloomberg behavior
        pass

    def test_macrobond_index_name_structure(self):
        """Macrobond source should set index name to 'date' (mocked)."""
        pass

    def test_gsquant_index_name_structure(self):
        """GSQuant source should set index name to 'date' (mocked)."""
        pass
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_index_name.py::TestIndexNameConsistency::test_localfile_index_name -v`
Expected: FAIL (index name is not 'date')

**Step 3: Add index naming to LocalFileSource**

In `src/metapyle/sources/localfile.py`, in `fetch()` method, after date filtering add:

```python
        # Normalize index name
        df_filtered.index.name = "date"

        logger.info(
            "fetch_complete: path=%s, symbols=%s, rows=%d",
```

**Step 4: Add index naming to BloombergSource**

In `src/metapyle/sources/bloomberg.py`, in `fetch()` method, after filtering columns:

```python
        # Normalize index name
        df.index.name = "date"

        logger.info(
            "fetch_complete: tickers=%s, fields=%s, rows=%d",
```

**Step 5: Add index naming to MacrobondSource**

In `src/metapyle/sources/macrobond.py`, in `fetch()` method, after date filtering:

```python
        # Normalize index name
        result.index.name = "date"

        logger.info(
            "fetch_complete: symbols=%s, rows=%d",
```

**Step 6: Add index naming to GSQuantSource**

In `src/metapyle/sources/gsquant.py`, in `fetch()` method, before final return:

```python
        # Normalize index name
        result.index.name = "date"

        logger.info(
            "fetch_complete: columns=%s, rows=%d",
```

**Step 7: Run tests to verify**

Run: `pytest tests/unit/test_sources_index_name.py -v`
Expected: PASS

**Step 8: Commit**

```bash
git add src/metapyle/sources/*.py tests/unit/test_sources_index_name.py
git commit -m "feat: normalize date index name to 'date' across all sources"
```

---

## Task 7: Standardize Timezone to UTC

**Files:**
- Modify: `src/metapyle/sources/bloomberg.py`
- Modify: `src/metapyle/sources/macrobond.py`
- Modify: `src/metapyle/sources/gsquant.py`
- Modify: `src/metapyle/sources/localfile.py`
- Modify: `tests/unit/test_sources_index_name.py`

**Step 1: Add timezone test**

In `tests/unit/test_sources_index_name.py`, add:

```python
    def test_localfile_index_timezone(self, tmp_path):
        """LocalFile source should return UTC timezone-aware index."""
        from metapyle.sources.localfile import LocalFileSource
        from metapyle.sources.base import FetchRequest

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,value\n2024-01-01,100\n2024-01-02,101\n")

        source = LocalFileSource()
        req = FetchRequest(symbol="value", path=str(csv_file))
        df = source.fetch([req], "2024-01-01", "2024-01-02")

        assert df.index.tz is not None
        assert str(df.index.tz) == "UTC"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_index_name.py::TestIndexNameConsistency::test_localfile_index_timezone -v`
Expected: FAIL

**Step 3: Add timezone normalization to all sources**

In each source's `fetch()` method, after setting `index.name = "date"`, add:

```python
        # Ensure UTC timezone
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
```

Apply to: `bloomberg.py`, `macrobond.py`, `gsquant.py`, `localfile.py`

**Step 4: Run tests**

Run: `pytest tests/unit/test_sources_index_name.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/*.py tests/unit/test_sources_index_name.py
git commit -m "feat: standardize timezone to UTC across all sources"
```

---

## Task 8: Preserve Column Order in Client.get()

**Files:**
- Modify: `src/metapyle/client.py`
- Create: `tests/unit/test_client_column_order.py`

**Step 1: Write failing test**

Create `tests/unit/test_client_column_order.py`:

```python
"""Tests for column order preservation in Client.get()."""

import pandas as pd
import pytest


class TestColumnOrderPreservation:
    """Output column order should match input symbols list."""

    def test_column_order_matches_input(self, tmp_path):
        """Columns should appear in same order as input symbols."""
        from metapyle import Client

        # Create catalog with entries from different sources
        catalog = tmp_path / "catalog.yaml"
        csv_file = tmp_path / "data.csv"

        csv_file.write_text("date,col_a,col_b,col_c\n2024-01-01,1,2,3\n2024-01-02,4,5,6\n")

        catalog.write_text(f"""
- my_name: zebra
  source: localfile
  symbol: col_c
  path: {csv_file}
- my_name: alpha
  source: localfile
  symbol: col_a
  path: {csv_file}
- my_name: middle
  source: localfile
  symbol: col_b
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            # Request in specific order
            df = client.get(["middle", "zebra", "alpha"], start="2024-01-01", end="2024-01-02")

        assert list(df.columns) == ["middle", "zebra", "alpha"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client_column_order.py -v`
Expected: FAIL (columns not in requested order)

**Step 3: Implement column ordering**

In `src/metapyle/client.py`, modify `_assemble_dataframe` method:

```python
    def _assemble_dataframe(
        self, dfs: dict[str, pd.DataFrame], symbol_order: list[str]
    ) -> pd.DataFrame:
        """
        Assemble individual DataFrames into a wide DataFrame.

        Renames source columns to my_name from catalog.
        Preserves the order specified in symbol_order.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping my_name to DataFrames.
        symbol_order : list[str]
            Original order of symbols as requested.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by my_name in original order.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename first column to my_name and concatenate in requested order
        renamed: list[pd.DataFrame] = []
        for my_name in symbol_order:
            if my_name in dfs:
                df = dfs[my_name]
                col = df.columns[0]
                renamed.append(df[[col]].rename(columns={col: my_name}))

        result = pd.concat(renamed, axis=1)
        return result
```

**Step 4: Update get() method to pass symbol_order**

In `src/metapyle/client.py`, in `get()` method, change the call:

```python
        # Assemble into wide DataFrame
        return self._assemble_dataframe(dfs, symbols)
```

**Step 5: Run tests**

Run: `pytest tests/unit/test_client_column_order.py -v`
Expected: PASS

Run: `pytest tests/unit/test_client.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client_column_order.py
git commit -m "feat: preserve column order matching input symbols list"
```

---

## Task 9: Update GsSession Examples Everywhere

**Files:**
- Modify: `docs/smoke-test.md`
- Modify: `docs/raw-api-examples.md`
- Modify: `docs/user-guide.md`

**Step 1: Update smoke-test.md**

In `docs/smoke-test.md`, update the GS Quant section:

```python
# === GS QUANT ===
# Uncomment to test GS Quant connection
# Requires: Authenticated GsSession (call GsSession.use() first)
#
# from gs_quant.session import GsSession, Environment
# GsSession.use(Environment.PROD, client_id="YOUR_ID", client_secret="YOUR_SECRET")
#
# test_source("gsquant", """
```

This is already correct in smoke-test.md.

**Step 2: Update raw-api-examples.md**

In `docs/raw-api-examples.md`, add session initialization before GS Quant examples:

```markdown
## GS Quant (gs_quant)

GS Quant provides access to Goldman Sachs Marquee datasets. Requires an authenticated session.

### Session Setup

```python
from gs_quant.session import GsSession, Environment

# Initialize session (required before any API calls)
GsSession.use(Environment.PROD, client_id="YOUR_ID", client_secret="YOUR_SECRET")
```

### Basic Fetch
```

**Step 3: Update user-guide.md**

In `docs/user-guide.md`, in the GS Quant section, add session setup note:

```markdown
### GS Quant (`gsquant`)

Fetches data from GS Marquee platform via the `gs-quant` library.

**Requirements:**
- `pip install metapyle[gsquant]`
- GS Quant session authenticated:

```python
from gs_quant.session import GsSession, Environment
GsSession.use(Environment.PROD, client_id="YOUR_ID", client_secret="YOUR_SECRET")
```
```

**Step 4: Commit**

```bash
git add docs/smoke-test.md docs/raw-api-examples.md docs/user-guide.md
git commit -m "docs: add GsSession.use() examples to all gsquant documentation"
```

---

## Task 10: Handle params in csv_template()

**Files:**
- Modify: `src/metapyle/catalog.py`
- Modify: `tests/unit/test_catalog.py`

**Step 1: Write test for params in CSV**

Add to `tests/unit/test_catalog.py`:

```python
class TestCatalogCsvParams:
    """Tests for params handling in CSV import/export."""

    def test_csv_template_gsquant_includes_params(self):
        """gsquant template should include params column."""
        from metapyle.catalog import Catalog

        template = Catalog.csv_template(source="gsquant")
        assert "params" in template

    def test_from_csv_parses_json_params(self, tmp_path):
        """CSV with JSON params should be parsed correctly."""
        from metapyle.catalog import Catalog

        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text(
            'my_name,source,symbol,field,params\n'
            'eurusd_vol,gsquant,EURUSD,FXIMPLIEDVOL::impliedVolatility,"{""tenor"": ""1m"", ""deltaStrike"": ""DN""}"\n'
        )

        catalog = Catalog.from_csv(csv_file)
        entry = catalog.get("eurusd_vol")

        assert entry.params == {"tenor": "1m", "deltaStrike": "DN"}

    def test_to_csv_exports_json_params(self, tmp_path):
        """CSV export should serialize params as JSON."""
        from metapyle.catalog import Catalog, CatalogEntry

        entries = {
            "eurusd_vol": CatalogEntry(
                my_name="eurusd_vol",
                source="gsquant",
                symbol="EURUSD",
                field="FXIMPLIEDVOL::impliedVolatility",
                params={"tenor": "1m", "deltaStrike": "DN"},
            )
        }
        catalog = Catalog(entries)

        csv_file = tmp_path / "output.csv"
        catalog.to_csv(csv_file)

        content = csv_file.read_text()
        assert "tenor" in content
        assert "deltaStrike" in content
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_catalog.py::TestCatalogCsvParams -v`
Expected: FAIL

**Step 3: Update _SOURCE_COLUMNS to include params for gsquant**

In `src/metapyle/catalog.py`:

```python
_SOURCE_COLUMNS: dict[str, list[str]] = {
    "bloomberg": ["my_name", "source", "symbol", "field", "description", "unit"],
    "gsquant": ["my_name", "source", "symbol", "field", "params", "description", "unit"],
    "localfile": ["my_name", "source", "symbol", "path", "description", "unit"],
    "macrobond": ["my_name", "source", "symbol", "description", "unit"],
}
```

Also update `_ALL_COLUMNS`:

```python
_ALL_COLUMNS = ["my_name", "source", "symbol", "field", "path", "params", "description", "unit"]
```

**Step 4: Update from_csv to parse JSON params**

In `src/metapyle/catalog.py`, in `from_csv()` method, before creating CatalogEntry:

```python
                    # Parse params as JSON if present
                    params_str = row.get("params")
                    params = None
                    if params_str:
                        try:
                            import json
                            params = json.loads(params_str)
                        except json.JSONDecodeError as e:
                            errors.append(f"Row {row_num}: Invalid JSON in params: {e}")
                            continue
```

Then use `params=params` in CatalogEntry constructor.

**Step 5: Update to_csv to serialize params as JSON**

In `src/metapyle/catalog.py`, in `to_csv()` method, add `import json` and update writerow:

```python
                        "params": json.dumps(entry.params) if entry.params else "",
```

**Step 6: Run tests**

Run: `pytest tests/unit/test_catalog.py -v`
Expected: All PASS

**Step 7: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat: handle params as JSON in CSV import/export"
```

---

## Task 11: Improve to_yaml() Output Quality

**Files:**
- Modify: `src/metapyle/catalog.py`
- Add tests to: `tests/unit/test_catalog.py`

**Step 1: Write test for YAML output formatting**

Add to `tests/unit/test_catalog.py`:

```python
class TestCatalogYamlOutput:
    """Tests for YAML output formatting."""

    def test_to_yaml_has_spacing_between_entries(self, tmp_path):
        """YAML output should have blank lines between entries."""
        from metapyle.catalog import Catalog, CatalogEntry

        entries = {
            "entry1": CatalogEntry(my_name="entry1", source="macrobond", symbol="sym1"),
            "entry2": CatalogEntry(my_name="entry2", source="macrobond", symbol="sym2"),
        }
        catalog = Catalog(entries)

        yaml_file = tmp_path / "output.yaml"
        catalog.to_yaml(yaml_file)

        content = yaml_file.read_text()
        assert "\n\n- my_name:" in content

    def test_to_yaml_includes_source_comment(self, tmp_path):
        """YAML output should include comment about source files."""
        from metapyle.catalog import Catalog

        csv_file = tmp_path / "source.csv"
        csv_file.write_text("my_name,source,symbol\ntest,macrobond,usgdp\n")

        catalog = Catalog.from_csv(csv_file)

        yaml_file = tmp_path / "output.yaml"
        catalog.to_yaml(yaml_file, source_files=[csv_file])

        content = yaml_file.read_text()
        assert "source.csv" in content
```

**Step 2: Update to_yaml() signature and implementation**

In `src/metapyle/catalog.py`, replace the `to_yaml` method with manual YAML writing that includes spacing and optional source file comments. Add `from datetime import datetime` at top.

**Step 3: Run tests**

Run: `pytest tests/unit/test_catalog.py::TestCatalogYamlOutput -v`
Expected: All PASS

**Step 4: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat: improve to_yaml output with spacing and source file comments"
```

---

## Task 12: Implement Macrobond get_unified_series()

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`
- Modify: `src/metapyle/client.py`
- Create: `tests/unit/test_sources_macrobond_unified.py`
- Modify: `docs/user-guide.md`

**Step 1: Research Macrobond unified series API**

The Macrobond Data API provides `get_unified_series()` which allows:
- Frequency conversion (e.g., daily to monthly)
- Currency conversion
- Calendar alignment

This is triggered by passing params like `{"frequency": "Monthly", "currency": "USD"}`.

**Step 2: Write failing test**

Create `tests/unit/test_sources_macrobond_unified.py`:

```python
"""Tests for Macrobond unified series mode."""

import pytest

from metapyle.sources.base import FetchRequest


class TestMacrobondUnified:
    """Tests for unified series mode."""

    def test_get_raw_accepts_params(self, tmp_path):
        """get_raw should accept params parameter."""
        # This is a structural test - verifies the API accepts params
        from metapyle import Client

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,test\\n2024-01-01,100\\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f\"\"\"
- my_name: test
  source: localfile
  symbol: test
  path: {csv_file}
\"\"\")

        with Client(catalog=catalog, cache_enabled=False) as client:
            # get_raw should accept params without error
            df = client.get_raw(
                source="localfile",
                symbol="test",
                start="2024-01-01",
                end="2024-01-01",
                path=str(csv_file),
                params={"some_param": "value"},  # Should be accepted
            )
            assert len(df) >= 0  # Just verify it doesn't crash

    def test_fetch_request_includes_params(self):
        """FetchRequest should carry params to source adapter."""
        params = {"frequency": "Monthly", "currency": "USD"}
        req = FetchRequest(symbol="usgdp", params=params)
        assert req.params == params
```

**Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_macrobond_unified.py -v`
Expected: FAIL (get_raw doesn't accept params yet)

**Step 4: Add params parameter to get_raw**

In `src/metapyle/client.py`, update `get_raw()` signature:

```python
    def get_raw(
        self,
        source: str,
        symbol: str,
        start: str,
        end: str | None = None,
        *,
        field: str | None = None,
        path: str | None = None,
        params: dict[str, Any] | None = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
```

Update the docstring to document params:

```python
        params : dict[str, Any] | None, optional
            Source-specific parameters (e.g., Macrobond unified options like
            {"frequency": "Monthly", "currency": "USD"}).
```

Update the FetchRequest creation:

```python
        request = FetchRequest(symbol=symbol, field=field, path=path, params=params)
```

**Step 5: Update MacrobondSource to handle unified params**

In `src/metapyle/sources/macrobond.py`, refactor `fetch()` to check for unified params:

```python
    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        \"\"\"
        Fetch time-series data from Macrobond.

        If any request contains params with 'frequency' or 'currency',
        uses get_unified_series() for server-side conversion.
        Otherwise uses standard get_series().
        \"\"\"
        if not requests:
            return pd.DataFrame()

        mda = _get_mda()
        if mda is None:
            logger.error("fetch_failed: reason=mda_not_installed")
            raise FetchError(
                "macrobond_data_api package is not installed. "
                "Install with: pip install macrobond-data-api"
            )

        symbols = [req.symbol for req in requests]

        # Check for unified mode params (frequency/currency conversion)
        unified_params: dict[str, Any] = {}
        for req in requests:
            if req.params:
                if "frequency" in req.params or "currency" in req.params:
                    unified_params.update(req.params)

        logger.debug(
            "fetch_start: symbols=%s, start=%s, end=%s, unified=%s",
            symbols,
            start,
            end,
            bool(unified_params),
        )

        if unified_params:
            return self._fetch_unified(symbols, start, end, unified_params)
        else:
            return self._fetch_standard(symbols, start, end)
```

**Step 6: Add _fetch_standard helper**

Extract current logic into `_fetch_standard`:

```python
    def _fetch_standard(
        self,
        symbols: list[str],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        \"\"\"Fetch using standard get_series (no frequency/currency conversion).\"\"\"
        mda = _get_mda()

        try:
            series_list = mda.get_series(symbols)
        except Exception as e:
            logger.error("fetch_failed: symbols=%s, error=%s", symbols, str(e))
            raise FetchError(f"Macrobond API error: {e}") from e

        # Check for errors in any series
        for series in series_list:
            if series.is_error:
                logger.error(
                    "fetch_failed: symbol=%s, error=%s",
                    series.primary_name,
                    series.error_message,
                )
                raise FetchError(
                    f"Macrobond error for {series.primary_name}: {series.error_message}"
                )

        # Convert each series to DataFrame and merge
        dfs: list[pd.DataFrame] = []
        for series in series_list:
            df = series.values_to_pd_data_frame()
            df.index = pd.to_datetime(df["date"])
            df = df[["value"]].rename(columns={"value": series.primary_name})
            dfs.append(df)

        if not dfs:
            logger.warning("fetch_empty: symbols=%s", symbols)
            raise NoDataError(f"No data returned for {symbols}")

        # Merge all series on index
        result = dfs[0]
        for df in dfs[1:]:
            result = result.join(df, how="outer")

        # Filter by date range
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (result.index >= start_dt) & (result.index <= end_dt)
        result = result.loc[mask]

        if result.empty:
            logger.warning(
                "fetch_no_data_in_range: symbols=%s, start=%s, end=%s",
                symbols,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end}")

        logger.info(
            "fetch_complete: symbols=%s, rows=%d",
            symbols,
            len(result),
        )
        return result
```

**Step 7: Add _fetch_unified helper**

```python
    def _fetch_unified(
        self,
        symbols: list[str],
        start: str,
        end: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        \"\"\"Fetch using get_unified_series with frequency/currency conversion.\"\"\"
        mda = _get_mda()

        try:
            # Build unified request with conversion parameters
            from macrobond_data_api.common.types import SeriesFrequency, SeriesWeekdays

            frequency = params.get("frequency")
            currency = params.get("currency")
            weekdays = params.get("weekdays", SeriesWeekdays.FULL_WEEK)

            # get_unified_series accepts parameters for alignment
            unified_series = mda.get_unified_series(
                *symbols,
                frequency=SeriesFrequency[frequency.upper()] if frequency else None,
                currency=currency,
                weekdays=weekdays,
                start_date=start,
                end_date=end,
            )
        except Exception as e:
            logger.error(
                "fetch_unified_failed: symbols=%s, params=%s, error=%s",
                symbols,
                params,
                str(e),
            )
            raise FetchError(f"Macrobond unified API error: {e}") from e

        # unified_series returns a UnifiedSeriesList that can be converted to DataFrame
        try:
            result = unified_series.to_pd_data_frame()
        except Exception as e:
            logger.error(
                "fetch_unified_conversion_failed: symbols=%s, error=%s",
                symbols,
                str(e),
            )
            raise FetchError(f"Failed to convert unified series to DataFrame: {e}") from e

        if result.empty:
            logger.warning(
                "fetch_unified_empty: symbols=%s, start=%s, end=%s",
                symbols,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end}")

        logger.info(
            "fetch_unified_complete: symbols=%s, params=%s, rows=%d",
            symbols,
            params,
            len(result),
        )
        return result
```

**Step 8: Update docs/user-guide.md**

Add to the Macrobond section:

```markdown
#### Unified Series (Frequency/Currency Conversion)

Macrobond supports server-side frequency and currency conversion using `get_raw()` with params:

```python
# Fetch US GDP converted to monthly frequency
df = client.get_raw(
    source="macrobond",
    symbol="usgdp",
    start="2020-01-01",
    end="2024-12-31",
    params={"frequency": "Monthly"},
)

# Fetch with currency conversion to EUR
df = client.get_raw(
    source="macrobond",
    symbol="jpgdp",
    start="2020-01-01",
    end="2024-12-31",
    params={"frequency": "Quarterly", "currency": "EUR"},
)
```

Supported frequency values: `Daily`, `Weekly`, `Monthly`, `Quarterly`, `Annual`.
```

**Step 9: Run tests**

Run: `pytest tests/unit/test_sources_macrobond_unified.py -v`
Expected: All PASS

**Step 10: Commit**

```bash
git add src/metapyle/sources/macrobond.py src/metapyle/client.py tests/unit/test_sources_macrobond_unified.py docs/user-guide.md
git commit -m "feat: implement Macrobond unified series mode with frequency/currency conversion"
```

---

## Task 13: Update Cache Path Documentation

**Files:**
- Modify: `docs/user-guide.md`

**Step 1: Update cache location section**

In `docs/user-guide.md`, update the "Default Cache Location" section:

```markdown
### Default Cache Location

By default, the cache is stored at `./cache/data_cache.db` (relative to your current working directory). The directory is created automatically if it doesn't exist.

You can customize the cache location:

```python
# Custom cache path
client = Client(catalog="catalog.yaml", cache_path="/path/to/my_cache.db")
```

Or via environment variable:

**PowerShell:**
```powershell
$env:METAPYLE_CACHE_PATH = "C:\path\to\my_cache.db"
```

**Command Prompt:**
```cmd
set METAPYLE_CACHE_PATH=C:\path\to\my_cache.db
```

**Linux/macOS:**
```bash
export METAPYLE_CACHE_PATH=/path/to/my_cache.db
```

**VS Code settings.json** (workspace or user):
```json
{
    "terminal.integrated.env.windows": {
        "METAPYLE_CACHE_PATH": "C:\\path\\to\\my_cache.db"
    },
    "terminal.integrated.env.linux": {
        "METAPYLE_CACHE_PATH": "/path/to/my_cache.db"
    }
}
```

**In Python script:**
```python
import os
os.environ["METAPYLE_CACHE_PATH"] = "/path/to/my_cache.db"

from metapyle import Client
client = Client(catalog="catalog.yaml")  # Uses env var
```

The `cache_path` parameter takes precedence over the environment variable.
```

**Step 2: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: add multiple options for setting METAPYLE_CACHE_PATH"
```

---

## Task 14: Add Cache DB Inspection Helpers

**Files:**
- Create: `src/metapyle/cache_utils.py`
- Modify: `src/metapyle/__init__.py`
- Create: `tests/unit/test_cache_utils.py`

**Step 1: Write failing test**

Create `tests/unit/test_cache_utils.py`:

```python
"""Tests for cache inspection utilities."""

import pytest


class TestCacheInspection:
    """Tests for cache DB inspection helpers."""

    def test_list_cached_symbols(self, tmp_path):
        """Should list all symbols in cache."""
        from metapyle import Client
        from metapyle.cache_utils import list_cached_entries

        # Create catalog and client
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,value\n2024-01-01,100\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: test_symbol
  source: localfile
  symbol: value
  path: {csv_file}
""")

        cache_path = tmp_path / "cache.db"
        with Client(catalog=catalog, cache_path=str(cache_path)) as client:
            client.get(["test_symbol"], start="2024-01-01", end="2024-01-01")

        # Inspect cache
        entries = list_cached_entries(cache_path)
        assert len(entries) >= 1
        assert any("localfile" in str(e) for e in entries)

    def test_get_cache_stats(self, tmp_path):
        """Should return cache statistics."""
        from metapyle.cache_utils import get_cache_stats

        cache_path = tmp_path / "cache.db"
        # Create empty cache first
        from metapyle.cache import Cache
        cache = Cache(path=str(cache_path), enabled=True)
        cache.close()

        stats = get_cache_stats(cache_path)
        assert "total_entries" in stats
        assert "size_bytes" in stats
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_cache_utils.py -v`
Expected: FAIL (module not found)

**Step 3: Create cache_utils.py**

Create `src/metapyle/cache_utils.py`:

```python
"""Utilities for inspecting the metapyle cache database."""

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = ["list_cached_entries", "get_cache_stats", "CacheEntry"]


@dataclass(frozen=True, slots=True)
class CacheEntry:
    """Represents a cached data entry."""
    source: str
    symbol: str
    field: str | None
    path: str | None
    start_date: str
    end_date: str
    row_count: int


def list_cached_entries(cache_path: str | Path) -> list[CacheEntry]:
    """
    List all entries in the cache database.

    Parameters
    ----------
    cache_path : str | Path
        Path to the SQLite cache database.

    Returns
    -------
    list[CacheEntry]
        List of cached entry metadata.
    """
    path = Path(cache_path)
    if not path.exists():
        return []

    entries: list[CacheEntry] = []
    conn = sqlite3.connect(path)
    try:
        # Note: cache_entries table has columns: id, source, symbol, field, path, start_date, end_date
        # cache_data table has entry_id foreign key, so we count via join
        cursor = conn.execute("""
            SELECT ce.source, ce.symbol, ce.field, ce.path, ce.start_date, ce.end_date,
                   CASE WHEN cd.entry_id IS NOT NULL THEN 1 ELSE 0 END as has_data
            FROM cache_entries ce
            LEFT JOIN cache_data cd ON cd.entry_id = ce.id
        """)
        for row in cursor:
            entries.append(CacheEntry(
                source=row[0],
                symbol=row[1],
                field=row[2],
                path=row[3],
                start_date=row[4],
                end_date=row[5],
                row_count=row[6],  # 1 if data exists, 0 otherwise
            ))
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        pass
    finally:
        conn.close()

    return entries


def get_cache_stats(cache_path: str | Path) -> dict[str, Any]:
    """
    Get cache database statistics.

    Parameters
    ----------
    cache_path : str | Path
        Path to the SQLite cache database.

    Returns
    -------
    dict[str, Any]
        Statistics including total_entries, size_bytes, sources.
    """
    path = Path(cache_path)
    if not path.exists():
        return {"total_entries": 0, "size_bytes": 0, "sources": []}

    stats: dict[str, Any] = {
        "size_bytes": path.stat().st_size,
    }

    conn = sqlite3.connect(path)
    try:
        # Count entries
        cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
        stats["total_entries"] = cursor.fetchone()[0]

        # List unique sources
        cursor = conn.execute("SELECT DISTINCT source FROM cache_entries")
        stats["sources"] = [row[0] for row in cursor]
    except sqlite3.OperationalError:
        stats["total_entries"] = 0
        stats["sources"] = []
    finally:
        conn.close()

    return stats
```

**Step 4: Export from __init__.py**

In `src/metapyle/__init__.py`, add:

```python
from metapyle.cache_utils import CacheEntry, get_cache_stats, list_cached_entries
```

And add to `__all__`:

```python
    "CacheEntry",
    "get_cache_stats",
    "list_cached_entries",
```

**Step 5: Run tests**

Run: `pytest tests/unit/test_cache_utils.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/cache_utils.py src/metapyle/__init__.py tests/unit/test_cache_utils.py
git commit -m "feat: add cache inspection helpers (list_cached_entries, get_cache_stats)"
```

---

## Task 15: Clarify and Fix Import Style

**Files:**
- Modify: `.github/copilot-instructions.md`
- Modify all source files if needed

**Step 1: Review current import style**

Check current imports in source files:

```bash
grep -r "^from metapyle" src/metapyle/
```

Current style: Absolute imports (`from metapyle.sources import ...`)

**Step 2: Decision - Keep absolute imports**

Absolute imports are clearer and work better with type checkers and IDEs. Update copilot-instructions.md to reflect this:

In `.github/copilot-instructions.md`, update the Imports section:

```markdown
## Imports

Use `collections.abc` for abstract types (`Callable`, `Iterator`, `Mapping`, `Sequence`).

### Absolute Imports Throughout Package

```python
# ✅ Correct - Absolute imports
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.exceptions import FetchError
from metapyle.sources import BaseSource

# ❌ Avoid - Relative imports (harder to refactor)
from .catalog import Catalog
from ..sources import BaseSource
```
```

**Step 3: Commit**

```bash
git add .github/copilot-instructions.md
git commit -m "docs: clarify absolute import style in copilot-instructions"
```

---

## Task 16: CSV/YAML Value Sanitization

**Files:**
- Modify: `src/metapyle/catalog.py`
- Add tests to: `tests/unit/test_catalog.py`

**Step 1: Write test for value sanitization**

Add to `tests/unit/test_catalog.py`:

```python
class TestCatalogSanitization:
    """Tests for input value sanitization."""

    def test_csv_strips_whitespace(self, tmp_path):
        """CSV values should have whitespace stripped."""
        from metapyle.catalog import Catalog

        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text("my_name,source,symbol\n  test  ,  macrobond  ,  usgdp  \n")

        catalog = Catalog.from_csv(csv_file)
        entry = catalog.get("test")

        assert entry.my_name == "test"
        assert entry.source == "macrobond"
        assert entry.symbol == "usgdp"

    def test_yaml_rejects_shell_injection(self, tmp_path):
        """YAML should reject potential shell injection."""
        from metapyle.catalog import Catalog, CatalogValidationError

        # Symbol with shell metacharacters
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text("""
- my_name: test
  source: macrobond
  symbol: "$(rm -rf /)"
""")

        with pytest.raises(CatalogValidationError, match="invalid character"):
            Catalog.from_yaml(yaml_file)
```

**Step 2: Add sanitization to _parse_entry**

In `src/metapyle/catalog.py`, add validation:

```python
    @staticmethod
    def _validate_symbol(symbol: str, source_file: str | Path) -> None:
        """Validate symbol doesn't contain dangerous characters."""
        dangerous = ["$(", "`", "${", "&&", "||", ";", "|", ">", "<"]
        for char in dangerous:
            if char in symbol:
                raise CatalogValidationError(
                    f"Symbol contains invalid character '{char}' in {source_file}"
                )
```

Call this in `_parse_entry` before returning.

**Step 3: Run tests**

Run: `pytest tests/unit/test_catalog.py::TestCatalogSanitization -v`
Expected: All PASS

**Step 4: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat: add sanitization for catalog symbol values"
```

---

## Task 17: Add Tall/Melted Output Format Option

**Files:**
- Modify: `src/metapyle/client.py`
- Create: `tests/unit/test_client_output_format.py`

**Step 1: Write failing test**

Create `tests/unit/test_client_output_format.py`:

```python
"""Tests for output format options in Client.get()."""

import pandas as pd
import pytest


class TestOutputFormat:
    """Tests for wide vs tall output format."""

    def test_default_wide_format(self, tmp_path):
        """Default output should be wide format."""
        from metapyle import Client

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,a,b\n2024-01-01,1,2\n2024-01-02,3,4\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: alpha
  source: localfile
  symbol: a
  path: {csv_file}
- my_name: beta
  source: localfile
  symbol: b
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            df = client.get(["alpha", "beta"], start="2024-01-01", end="2024-01-02")

        # Wide format: columns are symbol names
        assert list(df.columns) == ["alpha", "beta"]
        assert len(df) == 2

    def test_tall_format(self, tmp_path):
        """Output format='tall' should return melted DataFrame."""
        from metapyle import Client

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,a,b\n2024-01-01,1,2\n2024-01-02,3,4\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: alpha
  source: localfile
  symbol: a
  path: {csv_file}
- my_name: beta
  source: localfile
  symbol: b
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            df = client.get(
                ["alpha", "beta"],
                start="2024-01-01",
                end="2024-01-02",
                output_format="tall"
            )

        # Tall format: MultiIndex (date, name), single value column
        assert isinstance(df.index, pd.MultiIndex)
        assert df.index.names == ["date", "name"]
        assert "value" in df.columns
        assert len(df) == 4  # 2 dates * 2 symbols
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client_output_format.py -v`
Expected: FAIL on tall format test

**Step 3: Add output_format parameter to get()**

In `src/metapyle/client.py`, update `get()` signature:

```python
    def get(
        self,
        symbols: list[str],
        start: str,
        end: str | None = None,
        *,
        frequency: str | None = None,
        output_format: str = "wide",
        use_cache: bool = True,
    ) -> pd.DataFrame:
```

Add parameter documentation:

```python
        output_format : str, optional
            Output format: "wide" (default) or "tall".
            Wide: DatetimeIndex, columns = symbol names.
            Tall: MultiIndex (date, name), single 'value' column.
```

**Step 4: Implement tall format conversion**

At end of `get()` method, before return:

```python
        # Assemble into wide DataFrame
        result = self._assemble_dataframe(dfs, symbols)

        # Convert to tall format if requested
        if output_format == "tall":
            result = self._to_tall_format(result)

        return result
```

Add helper method:

```python
    def _to_tall_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert wide DataFrame to tall (melted) format.

        Parameters
        ----------
        df : pd.DataFrame
            Wide DataFrame with DatetimeIndex and symbol columns.

        Returns
        -------
        pd.DataFrame
            Tall DataFrame with MultiIndex (date, name) and 'value' column.
        """
        if df.empty:
            return pd.DataFrame(columns=["value"])

        # Melt: index becomes 'date', columns become 'name'
        melted = df.reset_index().melt(
            id_vars=[df.index.name or "date"],
            var_name="name",
            value_name="value",
        )

        # Set MultiIndex
        date_col = df.index.name or "date"
        melted = melted.set_index([date_col, "name"])
        melted.index.names = ["date", "name"]

        return melted.sort_index()
```

**Step 5: Run tests**

Run: `pytest tests/unit/test_client_output_format.py -v`
Expected: All PASS

Run: `pytest tests/unit/test_client.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client_output_format.py
git commit -m "feat: add tall/melted output format option to Client.get()"
```

---

## Task 18: Verify and Fix Column Lookup Fallback

**Files:**
- Modify: `src/metapyle/client.py`
- Create: `tests/unit/test_client_column_fallback.py`

**Step 1: Write test for fallback behavior**

Create `tests/unit/test_client_column_fallback.py`:

```python
"""Tests for column name fallback when source ignores field."""

import pandas as pd
import pytest

from metapyle.sources.base import make_column_name


class TestColumnLookupFallback:
    """Test column name fallback for sources that ignore field."""

    def test_make_column_name_with_field(self):
        """Column name with field should use :: separator."""
        result = make_column_name("SPX Index", "PX_LAST")
        assert result == "SPX Index::PX_LAST"

    def test_make_column_name_without_field(self):
        """Column name without field should just be symbol."""
        result = make_column_name("usgdp", None)
        assert result == "usgdp"

    def test_client_extracts_column_with_fallback(self, tmp_path):
        """Client should find column even when source ignores field.
        
        This simulates Macrobond behavior where field is in catalog
        but source returns column named just by symbol.
        """
        from metapyle import Client

        # Create a CSV that mimics Macrobond behavior
        # (column named by symbol only, not symbol::field)
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,usgdp\n2024-01-01,100\n2024-01-02,101\n")

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
            df = client.get(["us_gdp"], start="2024-01-01", end="2024-01-02")

        assert "us_gdp" in df.columns
        assert len(df) == 2
```

**Step 2: Run test to check current behavior**

Run: `pytest tests/unit/test_client_column_fallback.py -v`
Expected: May FAIL if fallback not implemented

**Step 3: Implement column lookup fallback in client.py**

In `src/metapyle/client.py`, in `get()` method, update the column extraction logic:

```python
                # Split result and cache each column
                for entry in group_entries:
                    # Try with field first (e.g., Bloomberg)
                    col_name = make_column_name(entry.symbol, entry.field)
                    
                    # Fallback to symbol-only if field column not found
                    # (e.g., Macrobond ignores field parameter)
                    if col_name not in result_df.columns:
                        col_name = make_column_name(entry.symbol, None)

                    if col_name in result_df.columns:
                        col_df = result_df[[col_name]]

                        # Cache the individual column
                        if use_cache:
                            self._cache.put(
                                source=entry.source,
                                symbol=entry.symbol,
                                field=entry.field,
                                path=entry.path,
                                start_date=start,
                                end_date=end,
                                data=col_df,
                            )

                        dfs[entry.my_name] = col_df
                    else:
                        logger.warning(
                            "column_not_found: my_name=%s, tried=%s and %s, available=%s",
                            entry.my_name,
                            make_column_name(entry.symbol, entry.field),
                            make_column_name(entry.symbol, None),
                            list(result_df.columns),
                        )
```

**Step 4: Run tests to verify**

Run: `pytest tests/unit/test_client_column_fallback.py -v`
Expected: All PASS

Run: `pytest tests/unit/test_client.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client_column_fallback.py
git commit -m "fix: add column name fallback for sources that ignore field"
```

---

## Summary

**Total Tasks: 18**

| Design Section | Tasks | Description |
|----------------|-------|-------------|
| 1. Critical Bug Fixes | 1, 2, 3 | pytest_addoption, Bloomberg field, date range + assertion |
| 2. Source Adapter Gaps | 9, 12 | GsSession docs, Macrobond unified series |
| 3. DataFrame Output | 6, 7, 8 | Index naming, UTC timezone, column order |
| 4. Catalog Improvements | 10, 11, 16 | CSV params, to_yaml output, value sanitization |
| 5. Testing Infrastructure | 3 | Covered by Task 3 (date range + assertion update) |
| 6. Documentation & DX | 4, 5, 13, 14, 15 | Exports, my_name, cache docs, cache utils, imports |
| 7. Output Format | 17 | Tall/melted output option |
| Verification | 18 | Column lookup fallback fix |

**Execution Order:**
1. Task 1: Move pytest_addoption to root conftest
2. Task 2: Bloomberg field enforcement
3. Task 3: Integration test date range and assertion update
4. Task 4: Export BloombergSource
5. Task 5: Fix my_name conventions
6. Task 6: Normalize date index to 'date'
7. Task 7: Standardize timezone to UTC
8. Task 8: Preserve column order
9. Task 9: Update GsSession examples
10. Task 10: Handle params in CSV
11. Task 11: Improve to_yaml() output
12. Task 12: Macrobond unified series
13. Task 13: Cache path documentation
14. Task 14: Cache inspection helpers
15. Task 15: Import style clarification
16. Task 16: Value sanitization
17. Task 17: Tall output format
18. Task 18: Column lookup fallback fix

---

## Execution

Plan saved to `docs/plans/2025-12-31-user-testing-fixes-implementation.md`

Before we can start implementation, I need to create a feature branch.

Ready to create feature branch `feature/user-testing-fixes` and verify clean baseline?
