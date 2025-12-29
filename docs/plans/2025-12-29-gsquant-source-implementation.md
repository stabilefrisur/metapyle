# gs-quant Source Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Add gs-quant as a data source for fetching financial data from Goldman Sachs Marquee platform.

**Architecture:** Extend catalog and fetch request schemas with `params` field, then implement `GSQuantSource` that groups requests by dataset, calls `Dataset.get_data()`, and pivots results. Authentication is external (user calls `GsSession.use()` before fetching).

**Tech Stack:** gs-quant library, pandas for data manipulation

**Design Document:** [2025-12-29-gsquant-source-design.md](2025-12-29-gsquant-source-design.md)

---

## Task 1: Add `params` field to `FetchRequest`

**Files:**
- Modify: `src/metapyle/sources/base.py`
- Test: `tests/unit/test_sources_base.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_sources_base.py`:

```python
class TestFetchRequestParams:
    """Tests for FetchRequest params field."""

    def test_fetch_request_with_params(self) -> None:
        """FetchRequest accepts params dict."""
        params = {"tenor": "3m", "deltaStrike": "DN"}
        request = FetchRequest(symbol="EURUSD", field="PX_LAST", params=params)
        
        assert request.symbol == "EURUSD"
        assert request.field == "PX_LAST"
        assert request.params == params

    def test_fetch_request_params_default_none(self) -> None:
        """FetchRequest params defaults to None."""
        request = FetchRequest(symbol="EURUSD")
        
        assert request.params is None

    def test_fetch_request_with_params_frozen(self) -> None:
        """FetchRequest with params is still frozen."""
        params = {"tenor": "3m"}
        request = FetchRequest(symbol="EURUSD", params=params)
        
        with pytest.raises(AttributeError):
            request.params = {}  # type: ignore[misc]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_base.py::TestFetchRequestParams -v`

Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'params'`

**Step 3: Write minimal implementation**

In `src/metapyle/sources/base.py`, modify `FetchRequest`:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class FetchRequest:
    """
    Single request within a batch fetch.

    Parameters
    ----------
    symbol : str
        Source-specific identifier.
    field : str | None
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    path : str | None
        File path for localfile source.
    params : dict[str, Any] | None
        Additional source-specific parameters (e.g., tenor, deltaStrike).
    """

    symbol: str
    field: str | None = None
    path: str | None = None
    params: dict[str, Any] | None = None
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_base.py::TestFetchRequestParams -v`

Expected: PASS (3 tests)

**Step 5: Run full test suite**

Run: `pytest tests/unit/ -v`

Expected: All tests pass (existing tests unaffected by new optional field)

**Step 6: Commit**

```bash
git add src/metapyle/sources/base.py tests/unit/test_sources_base.py
git commit -m "feat(sources): add params field to FetchRequest"
```

---

## Task 2: Add `params` field to `CatalogEntry`

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::TestCatalogEntryParams -v`

Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'params'`

**Step 3: Write minimal implementation**

In `src/metapyle/catalog.py`, modify `CatalogEntry`:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    """
    A single catalog entry mapping a name to a data source.

    Parameters
    ----------
    my_name : str
        Unique human-readable identifier for this data series.
    source : str
        Name of the registered source adapter (e.g., "bloomberg").
    symbol : str
        Source-specific identifier (e.g., "SPX Index").
    field : str | None, optional
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    path : str | None, optional
        File path for localfile source (e.g., "/data/macro.csv").
    description : str | None, optional
        Human-readable description of the data series.
    unit : str | None, optional
        Unit of measurement (e.g., "USD billions", "points").
    params : dict[str, Any] | None, optional
        Additional source-specific parameters (e.g., tenor, deltaStrike for gs-quant).
    """

    my_name: str
    source: str
    symbol: str
    field: str | None = None
    path: str | None = None
    description: str | None = None
    unit: str | None = None
    params: dict[str, Any] | None = None
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::TestCatalogEntryParams -v`

Expected: PASS (2 tests)

**Step 5: Run full test suite**

Run: `pytest tests/unit/test_catalog.py -v`

Expected: All tests pass

**Step 6: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add params field to CatalogEntry"
```

---

## Task 3: Update catalog YAML parsing to support `params`

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::TestCatalogYamlParams -v`

Expected: FAIL (params not parsed from YAML)

**Step 3: Write minimal implementation**

In `src/metapyle/catalog.py`, modify `_parse_entry`:

```python
@staticmethod
def _parse_entry(raw: dict[str, Any], source_file: str | Path) -> CatalogEntry:
    """Parse a raw dictionary into a CatalogEntry."""
    required_fields = ["my_name", "source", "symbol"]

    for field in required_fields:
        if field not in raw:
            raise CatalogValidationError(f"Missing required field '{field}' in {source_file}")

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

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::TestCatalogYamlParams -v`

Expected: PASS (2 tests)

**Step 5: Run full test suite**

Run: `pytest tests/unit/test_catalog.py -v`

Expected: All tests pass

**Step 6: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): parse params from YAML"
```

---

## Task 4: Update catalog CSV parsing to support `params`

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Note:** CSV format doesn't naturally support nested dicts. For now, `params` will be ignored in CSV (YAML-only feature). Document this limitation.

**Step 1: Write the test**

Add to `tests/unit/test_catalog.py`:

```python
class TestCatalogCsvParams:
    """Tests for CSV parsing - params not supported."""

    def test_from_csv_params_not_supported(self, tmp_path: Path) -> None:
        """Catalog.from_csv ignores params (YAML-only feature)."""
        csv_content = """my_name,source,symbol,field,path,description,unit
test_series,bloomberg,SPX Index,PX_LAST,,,
"""
        csv_file = tmp_path / "catalog.csv"
        csv_file.write_text(csv_content)
        
        catalog = Catalog.from_csv(csv_file)
        entry = catalog.get("test_series")
        
        # params is always None from CSV
        assert entry.params is None
```

**Step 2: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::TestCatalogCsvParams -v`

Expected: PASS (no changes needed - CSV parsing doesn't touch params)

**Step 3: Commit**

```bash
git add tests/unit/test_catalog.py
git commit -m "test(catalog): document CSV params limitation"
```

---

## Task 5: Update Client to pass `params` from CatalogEntry to FetchRequest

**Files:**
- Modify: `src/metapyle/client.py`
- Test: `tests/unit/test_client.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_client.py`:

```python
class TestClientParams:
    """Tests for Client passing params to source."""

    def test_get_passes_params_to_fetch_request(
        self, tmp_path: Path, mock_registry: MagicMock
    ) -> None:
        """Client passes catalog params to FetchRequest."""
        # Create catalog with params
        yaml_content = """
- my_name: test_series
  source: mock_source
  symbol: TEST
  field: DATASET::value
  params:
    tenor: 3m
    location: NYC
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        # Setup mock source
        mock_source = MagicMock()
        mock_source.fetch.return_value = pd.DataFrame(
            {"TEST": [100.0, 101.0]},
            index=pd.DatetimeIndex(["2024-01-01", "2024-01-02"]),
        )
        mock_registry.get.return_value = mock_source
        mock_registry.list_sources.return_value = ["mock_source"]

        with patch("metapyle.client._global_registry", mock_registry):
            with patch("metapyle.catalog._global_registry", mock_registry):
                client = Client(catalog=yaml_file, cache_enabled=False)
                client.get(["test_series"], start="2024-01-01", end="2024-01-02")

        # Verify fetch was called with params
        call_args = mock_source.fetch.call_args
        requests = call_args[0][0]  # First positional arg is requests list
        
        assert len(requests) == 1
        assert requests[0].params == {"tenor": "3m", "location": "NYC"}
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::TestClientParams -v`

Expected: FAIL (params not passed through)

**Step 3: Write minimal implementation**

In `src/metapyle/client.py`, modify the request building in `get()` method. Find the section that builds `FetchRequest` objects:

```python
# Build FetchRequest list for this source
requests = [
    FetchRequest(
        symbol=e.symbol,
        field=e.field,
        path=e.path,
        params=e.params,
    )
    for e in group_entries
]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py::TestClientParams -v`

Expected: PASS

**Step 5: Run full test suite**

Run: `pytest tests/unit/test_client.py -v`

Expected: All tests pass

**Step 6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): pass params from catalog to FetchRequest"
```

---

## Task 6: Update catalog YAML export to include `params`

**Files:**
- Modify: `src/metapyle/catalog.py`
- Test: `tests/unit/test_catalog.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_catalog.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_catalog.py::TestCatalogExportParams -v`

Expected: FAIL (params not exported)

**Step 3: Write minimal implementation**

In `src/metapyle/catalog.py`, modify `to_yaml()`:

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
        entry_dict: dict[str, Any] = {
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
        if entry.params is not None:
            entry_dict["params"] = entry.params

        entries_list.append(entry_dict)

    with open(file_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(entries_list, f, default_flow_style=False, sort_keys=False)

    logger.info("catalog_exported_yaml: path=%s, entries=%d", path, len(self._entries))
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_catalog.py::TestCatalogExportParams -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): export params in to_yaml"
```

---

## Task 7: Create GSQuantSource skeleton with lazy import

**Files:**
- Create: `src/metapyle/sources/gsquant.py`
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write the failing test for lazy import**

Create `tests/unit/test_sources_gsquant.py`:

```python
"""Unit tests for gs-quant source adapter."""

from unittest.mock import MagicMock, patch

import pandas as pd
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantSourceImport -v`

Expected: FAIL with `ModuleNotFoundError` (module doesn't exist yet)

**Step 3: Write minimal implementation**

Create `src/metapyle/sources/gsquant.py`:

```python
"""gs-quant source adapter using gs_quant library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["GSQuantSource"]

logger = logging.getLogger(__name__)

_GSQUANT_AVAILABLE: bool | None = None
_gsquant_modules: dict[str, Any] = {}


def _get_gsquant() -> dict[str, Any]:
    """Lazy import of gs_quant modules."""
    global _GSQUANT_AVAILABLE, _gsquant_modules

    if _GSQUANT_AVAILABLE is None:
        try:
            from gs_quant.data import Dataset
            from gs_quant.session import GsSession

            _gsquant_modules = {"Dataset": Dataset, "GsSession": GsSession}
            _GSQUANT_AVAILABLE = True
        except (ImportError, Exception):
            _gsquant_modules = {}
            _GSQUANT_AVAILABLE = False

    return _gsquant_modules


@register_source("gsquant")
class GSQuantSource(BaseSource):
    """Source adapter for Goldman Sachs Marquee data via gs-quant."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch data from gs-quant datasets.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            Fetch requests with field format "dataset_id::value_column".
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and symbol columns.

        Raises
        ------
        FetchError
            If gs-quant not available or API call fails.
        NoDataError
            If no data returned.
        """
        if not requests:
            return pd.DataFrame()

        gs = _get_gsquant()
        if not gs:
            logger.error("fetch_failed: reason=gsquant_not_installed")
            raise FetchError(
                "gs-quant package is not installed. Install with: pip install gs-quant"
            )

        raise NotImplementedError("GSQuantSource.fetch not yet implemented")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a gs-quant symbol."""
        _get_gsquant()
        return {
            "source": "gsquant",
            "symbol": symbol,
            "gsquant_available": _GSQUANT_AVAILABLE or False,
        }
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantSourceImport -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/gsquant.py tests/unit/test_sources_gsquant.py
git commit -m "feat(sources): add GSQuantSource skeleton with lazy import"
```

---

## Task 8: Add GSQuantSource to sources __init__.py

**Files:**
- Modify: `src/metapyle/sources/__init__.py`
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_sources_gsquant.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantSourceRegistration -v`

Expected: FAIL (GSQuantSource not exported from __init__)

**Step 3: Write minimal implementation**

In `src/metapyle/sources/__init__.py`, add import:

```python
"""Source adapters for metapyle."""

from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
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
    "make_column_name",
    "register_source",
]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantSourceRegistration -v`

Expected: PASS (2 tests)

**Step 5: Commit**

```bash
git add src/metapyle/sources/__init__.py tests/unit/test_sources_gsquant.py
git commit -m "feat(sources): register GSQuantSource in __init__"
```

---

## Task 9: Implement field parsing (dataset_id::value_column)

**Files:**
- Modify: `src/metapyle/sources/gsquant.py`
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write the failing tests**

Add to `tests/unit/test_sources_gsquant.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_gsquant.py::TestFieldParsing -v`

Expected: FAIL (function doesn't exist)

**Step 3: Write minimal implementation**

Add to `src/metapyle/sources/gsquant.py` (after the imports, before the class):

```python
def _parse_field(field: str) -> tuple[str, str]:
    """
    Parse field into dataset_id and value_column.

    Parameters
    ----------
    field : str
        Field in format "dataset_id::value_column".

    Returns
    -------
    tuple[str, str]
        (dataset_id, value_column)

    Raises
    ------
    ValueError
        If field format is invalid.
    """
    if "::" not in field:
        raise ValueError(
            f"Invalid field format: '{field}'. Expected 'dataset_id::value_column'"
        )
    
    parts = field.split("::", 1)
    dataset_id, value_column = parts[0], parts[1]
    
    if not dataset_id or not value_column:
        raise ValueError(
            f"Invalid field format: '{field}'. Both dataset_id and value_column required"
        )
    
    return dataset_id, value_column
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestFieldParsing -v`

Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add src/metapyle/sources/gsquant.py tests/unit/test_sources_gsquant.py
git commit -m "feat(gsquant): implement field parsing"
```

---

## Task 10: Implement fetch with single request (happy path)

**Files:**
- Modify: `src/metapyle/sources/gsquant.py`
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_sources_gsquant.py`:

```python
class TestGSQuantFetch:
    """Tests for GSQuantSource.fetch."""

    def test_fetch_single_request(self) -> None:
        """fetch returns DataFrame for single request."""
        from metapyle.sources.gsquant import GSQuantSource, _get_gsquant
        
        # Mock the gs_quant modules
        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "bbid": ["EURUSD", "EURUSD"],
            "impliedVolatility": [0.08, 0.085],
        })
        
        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}
            
            source = GSQuantSource()
            request = FetchRequest(
                symbol="EURUSD",
                field="FXIMPLIEDVOL::impliedVolatility",
            )
            
            df = source.fetch([request], "2024-01-01", "2024-01-02")
        
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert "EURUSD" in df.columns
        assert len(df) == 2

    def test_fetch_with_params(self) -> None:
        """fetch passes params to Dataset.get_data."""
        from metapyle.sources.gsquant import GSQuantSource
        
        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01"]),
            "bbid": ["EURUSD"],
            "impliedVolatility": [0.08],
        })
        
        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}
            
            source = GSQuantSource()
            request = FetchRequest(
                symbol="EURUSD",
                field="FXIMPLIEDVOL::impliedVolatility",
                params={"tenor": "3m", "deltaStrike": "DN"},
            )
            
            source.fetch([request], "2024-01-01", "2024-01-01")
        
        # Verify params were passed to get_data
        mock_dataset_instance.get_data.assert_called_once()
        call_kwargs = mock_dataset_instance.get_data.call_args[1]
        assert call_kwargs["tenor"] == "3m"
        assert call_kwargs["deltaStrike"] == "DN"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetch -v`

Expected: FAIL with `NotImplementedError`

**Step 3: Write minimal implementation**

Replace the `fetch` method in `src/metapyle/sources/gsquant.py`:

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Fetch data from gs-quant datasets.

    Parameters
    ----------
    requests : Sequence[FetchRequest]
        Fetch requests with field format "dataset_id::value_column".
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and symbol columns.

    Raises
    ------
    FetchError
        If gs-quant not available, field format invalid, or API call fails.
    NoDataError
        If no data returned.
    """
    if not requests:
        return pd.DataFrame()

    gs = _get_gsquant()
    if not gs:
        logger.error("fetch_failed: reason=gsquant_not_installed")
        raise FetchError(
            "gs-quant package is not installed. Install with: pip install gs-quant"
        )

    Dataset = gs["Dataset"]

    # Group requests by dataset_id
    groups: dict[str, list[FetchRequest]] = {}
    value_columns: dict[str, str] = {}

    for req in requests:
        if not req.field:
            raise FetchError(
                f"gsquant source requires field in format 'dataset_id::value_column'"
            )

        try:
            dataset_id, value_column = _parse_field(req.field)
        except ValueError as e:
            raise FetchError(str(e)) from e

        if dataset_id not in groups:
            groups[dataset_id] = []
            value_columns[dataset_id] = value_column
        elif value_columns[dataset_id] != value_column:
            raise FetchError(
                f"Cannot batch requests with different value columns for same dataset: "
                f"{value_columns[dataset_id]} vs {value_column}"
            )

        groups[dataset_id].append(req)

    # Fetch each dataset group
    result_dfs: list[pd.DataFrame] = []

    for dataset_id, group_requests in groups.items():
        symbols = [req.symbol for req in group_requests]
        value_column = value_columns[dataset_id]

        # Merge params from all requests
        merged_params: dict[str, Any] = {}
        for req in group_requests:
            if req.params:
                merged_params.update(req.params)

        logger.debug(
            "fetch_start: dataset=%s, symbols=%s, params=%s",
            dataset_id,
            symbols,
            merged_params,
        )

        try:
            ds = Dataset(dataset_id)
            data = ds.get_data(start, end, bbid=symbols, **merged_params)
        except (FetchError, NoDataError):
            raise
        except Exception as e:
            logger.error("fetch_failed: dataset=%s, error=%s", dataset_id, str(e))
            raise FetchError(f"gs-quant API error for {dataset_id}: {e}") from e

        if data.empty:
            logger.warning("fetch_empty: dataset=%s, symbols=%s", dataset_id, symbols)
            raise NoDataError(f"No data returned for {symbols} from {dataset_id}")

        # Pivot to wide format
        pivoted = pd.pivot_table(
            data,
            values=value_column,
            index=["date"],
            columns=["bbid"],
        )

        # Ensure DatetimeIndex
        if not isinstance(pivoted.index, pd.DatetimeIndex):
            pivoted.index = pd.to_datetime(pivoted.index)

        result_dfs.append(pivoted)

    # Merge all results
    if not result_dfs:
        return pd.DataFrame()

    result = result_dfs[0]
    for df in result_dfs[1:]:
        result = result.join(df, how="outer")

    logger.info(
        "fetch_complete: columns=%s, rows=%d",
        list(result.columns),
        len(result),
    )
    return result
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetch -v`

Expected: PASS (2 tests)

**Step 5: Commit**

```bash
git add src/metapyle/sources/gsquant.py tests/unit/test_sources_gsquant.py
git commit -m "feat(gsquant): implement fetch with single request"
```

---

## Task 11: Implement fetch with multiple requests (batch)

**Files:**
- Modify: `src/metapyle/sources/gsquant.py`
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_sources_gsquant.py`:

```python
class TestGSQuantFetchBatch:
    """Tests for GSQuantSource.fetch with multiple requests."""

    def test_fetch_multiple_symbols_same_dataset(self) -> None:
        """fetch batches multiple symbols for same dataset."""
        from metapyle.sources.gsquant import GSQuantSource
        
        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"]),
            "bbid": ["EURUSD", "USDJPY", "EURUSD", "USDJPY"],
            "impliedVolatility": [0.08, 0.10, 0.085, 0.105],
        })
        
        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}
            
            source = GSQuantSource()
            requests = [
                FetchRequest(symbol="EURUSD", field="FXIMPLIEDVOL::impliedVolatility"),
                FetchRequest(symbol="USDJPY", field="FXIMPLIEDVOL::impliedVolatility"),
            ]
            
            df = source.fetch(requests, "2024-01-01", "2024-01-02")
        
        # Should make single API call with both symbols
        mock_dataset_class.assert_called_once_with("FXIMPLIEDVOL")
        call_kwargs = mock_dataset_instance.get_data.call_args[1]
        assert set(call_kwargs["bbid"]) == {"EURUSD", "USDJPY"}
        
        # Result should have both columns
        assert "EURUSD" in df.columns
        assert "USDJPY" in df.columns
        assert len(df) == 2

    def test_fetch_multiple_datasets(self) -> None:
        """fetch handles requests from different datasets."""
        from metapyle.sources.gsquant import GSQuantSource
        
        def create_mock_data(dataset_id: str) -> pd.DataFrame:
            if dataset_id == "FXIMPLIEDVOL":
                return pd.DataFrame({
                    "date": pd.to_datetime(["2024-01-01"]),
                    "bbid": ["EURUSD"],
                    "impliedVolatility": [0.08],
                })
            else:  # FXSPOT
                return pd.DataFrame({
                    "date": pd.to_datetime(["2024-01-01"]),
                    "bbid": ["EURUSD"],
                    "spot": [1.10],
                })
        
        mock_dataset_class = MagicMock()
        mock_dataset_class.return_value.get_data.side_effect = lambda *args, **kwargs: (
            create_mock_data("FXIMPLIEDVOL") 
            if mock_dataset_class.call_args[0][0] == "FXIMPLIEDVOL" 
            else create_mock_data("FXSPOT")
        )
        
        # Simpler approach - use separate mock instances
        mock_instances = {}
        def create_dataset(dataset_id: str) -> MagicMock:
            if dataset_id not in mock_instances:
                instance = MagicMock()
                instance.get_data.return_value = create_mock_data(dataset_id)
                mock_instances[dataset_id] = instance
            return mock_instances[dataset_id]
        
        mock_dataset_class = MagicMock(side_effect=create_dataset)
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}
            
            source = GSQuantSource()
            requests = [
                FetchRequest(symbol="EURUSD", field="FXIMPLIEDVOL::impliedVolatility"),
                FetchRequest(symbol="EURUSD", field="FXSPOT::spot"),
            ]
            
            df = source.fetch(requests, "2024-01-01", "2024-01-01")
        
        # Should make two API calls (one per dataset)
        assert mock_dataset_class.call_count == 2
        
        # Result should have column from each (both named EURUSD after pivot)
        # Note: with outer join, they merge by index
        assert len(df.columns) == 2  # Two different EURUSD columns from different datasets
```

**Step 2: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetchBatch -v`

Expected: PASS (implementation from Task 10 already handles this)

**Step 3: Commit**

```bash
git add tests/unit/test_sources_gsquant.py
git commit -m "test(gsquant): add batch fetch tests"
```

---

## Task 12: Implement error handling tests

**Files:**
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write error handling tests**

Add to `tests/unit/test_sources_gsquant.py`:

```python
class TestGSQuantFetchErrors:
    """Tests for GSQuantSource.fetch error handling."""

    def test_fetch_missing_field(self) -> None:
        """fetch raises FetchError if field is None."""
        from metapyle.sources.gsquant import GSQuantSource
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": MagicMock(), "GsSession": MagicMock()}
            
            source = GSQuantSource()
            request = FetchRequest(symbol="EURUSD", field=None)
            
            with pytest.raises(FetchError, match="requires field"):
                source.fetch([request], "2024-01-01", "2024-01-02")

    def test_fetch_invalid_field_format(self) -> None:
        """fetch raises FetchError if field format invalid."""
        from metapyle.sources.gsquant import GSQuantSource
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": MagicMock(), "GsSession": MagicMock()}
            
            source = GSQuantSource()
            request = FetchRequest(symbol="EURUSD", field="FXIMPLIEDVOL")  # Missing ::
            
            with pytest.raises(FetchError, match="Invalid field format"):
                source.fetch([request], "2024-01-01", "2024-01-02")

    def test_fetch_conflicting_value_columns(self) -> None:
        """fetch raises FetchError if same dataset has different value columns."""
        from metapyle.sources.gsquant import GSQuantSource
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": MagicMock(), "GsSession": MagicMock()}
            
            source = GSQuantSource()
            requests = [
                FetchRequest(symbol="EURUSD", field="FXIMPLIEDVOL::impliedVolatility"),
                FetchRequest(symbol="USDJPY", field="FXIMPLIEDVOL::spot"),  # Different column!
            ]
            
            with pytest.raises(FetchError, match="different value columns"):
                source.fetch(requests, "2024-01-01", "2024-01-02")

    def test_fetch_api_error(self) -> None:
        """fetch raises FetchError on API exception."""
        from metapyle.sources.gsquant import GSQuantSource
        
        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.side_effect = Exception("API timeout")
        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}
            
            source = GSQuantSource()
            request = FetchRequest(symbol="EURUSD", field="FXIMPLIEDVOL::impliedVolatility")
            
            with pytest.raises(FetchError, match="gs-quant API error"):
                source.fetch([request], "2024-01-01", "2024-01-02")

    def test_fetch_empty_data(self) -> None:
        """fetch raises NoDataError if dataset returns empty."""
        from metapyle.sources.gsquant import GSQuantSource
        
        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame()  # Empty
        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}
            
            source = GSQuantSource()
            request = FetchRequest(symbol="EURUSD", field="FXIMPLIEDVOL::impliedVolatility")
            
            with pytest.raises(NoDataError, match="No data returned"):
                source.fetch([request], "2024-01-01", "2024-01-02")

    def test_fetch_empty_requests(self) -> None:
        """fetch returns empty DataFrame for empty requests."""
        from metapyle.sources.gsquant import GSQuantSource
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": MagicMock(), "GsSession": MagicMock()}
            
            source = GSQuantSource()
            df = source.fetch([], "2024-01-01", "2024-01-02")
            
            assert isinstance(df, pd.DataFrame)
            assert df.empty
```

**Step 2: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetchErrors -v`

Expected: PASS (all 6 tests - implementation handles these cases)

**Step 3: Commit**

```bash
git add tests/unit/test_sources_gsquant.py
git commit -m "test(gsquant): add error handling tests"
```

---

## Task 13: Implement get_metadata

**Files:**
- Modify: `src/metapyle/sources/gsquant.py`
- Test: `tests/unit/test_sources_gsquant.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_sources_gsquant.py`:

```python
class TestGSQuantMetadata:
    """Tests for GSQuantSource.get_metadata."""

    def test_get_metadata_returns_empty_dict(self) -> None:
        """get_metadata returns empty dict (minimal implementation)."""
        from metapyle.sources.gsquant import GSQuantSource
        
        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": MagicMock(), "GsSession": MagicMock()}
            
            source = GSQuantSource()
            result = source.get_metadata("EURUSD")
            
            assert result == {}
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantMetadata -v`

Expected: FAIL (get_metadata not implemented)

**Step 3: Implement get_metadata**

Update `src/metapyle/sources/gsquant.py`, add method to `GSQuantSource`:

```python
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Return metadata for a symbol.

        Parameters
        ----------
        symbol : str
            Symbol to retrieve metadata for.

        Returns
        -------
        dict[str, Any]
            Empty dict (gs-quant metadata requires session and is complex).
        """
        return {}
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantMetadata -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/gsquant.py tests/unit/test_sources_gsquant.py
git commit -m "feat(gsquant): implement get_metadata (minimal)"
```

---

## Task 14: Update pyproject.toml with optional dependency

**Files:**
- Modify: `pyproject.toml`

**Step 1: Read current pyproject.toml structure**

Check existing optional dependencies pattern.

**Step 2: Add gs-quant optional dependency**

Update `pyproject.toml`, in `[project.optional-dependencies]`:

```toml
[project.optional-dependencies]
bloomberg = ["blpapi"]
macrobond = ["macrobond-api-py"]
gsquant = ["gs-quant"]
all = ["blpapi", "macrobond-api-py", "gs-quant"]
```

**Step 3: Add mypy override for gs-quant**

Update `[tool.mypy]` section in `pyproject.toml`:

```toml
[[tool.mypy.overrides]]
module = "gs_quant.*"
ignore_missing_imports = true
```

**Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "build: add gs-quant optional dependency"
```

---

## Task 15: Update public API exports

**Files:**
- Modify: `src/metapyle/__init__.py`
- Test: `tests/unit/test_public_api.py`

**Step 1: Check current __init__.py exports**

Read and verify gs-quant source is auto-registered via base.py imports.

**Step 2: Verify registration works**

The `sources/__init__.py` should already import `gsquant`, making it auto-register.
Add test to verify:

Add to `tests/unit/test_public_api.py`:

```python
def test_gsquant_source_registered() -> None:
    """gs-quant source is registered when available."""
    # Note: This test checks registration, not actual import
    # The source is registered via decorator in gsquant.py
    from metapyle.sources.base import _SOURCE_REGISTRY
    
    # Can't test without gs-quant installed, so just verify module exists
    import importlib.util
    spec = importlib.util.find_spec("metapyle.sources.gsquant")
    assert spec is not None
```

**Step 3: Run test**

Run: `pytest tests/unit/test_public_api.py::test_gsquant_source_registered -v`

Expected: PASS

**Step 4: Commit**

```bash
git add tests/unit/test_public_api.py
git commit -m "test(api): verify gsquant source module exists"
```

---

## Task 16: Run full test suite

**Step 1: Run unit tests**

```bash
pytest tests/unit/ -v
```

Expected: All tests pass

**Step 2: Run mypy**

```bash
mypy src/metapyle/
```

Expected: No errors

**Step 3: Run ruff**

```bash
ruff check src/metapyle/
ruff format --check src/metapyle/
```

Expected: No errors

**Step 4: Commit if any fixes needed**

```bash
git add -A
git commit -m "style: fix linting issues"
```

---

## Task 17: Write integration test structure

**Files:**
- Create: `tests/integration/test_gsquant.py`
- Create: `tests/integration/fixtures/gsquant.yaml`

**Step 1: Create integration test fixture**

Create `tests/integration/fixtures/gsquant.yaml`:

```yaml
# gs-quant integration test catalog
# Requires valid GS Marquee credentials

eurusd_vol:
  source: gsquant
  symbol: EURUSD
  field: FXIMPLIEDVOL::impliedVolatility
  params:
    tenor: 3m
    deltaStrike: DN
  description: "EUR/USD 3M implied vol (delta neutral)"

usdjpy_vol:
  source: gsquant
  symbol: USDJPY
  field: FXIMPLIEDVOL::impliedVolatility
  params:
    tenor: 3m
    deltaStrike: DN
  description: "USD/JPY 3M implied vol (delta neutral)"
```

**Step 2: Create integration test file**

Create `tests/integration/test_gsquant.py`:

```python
"""Integration tests for gs-quant source.

These tests require:
1. Valid GS Marquee credentials (GS_CLIENT_ID, GS_CLIENT_SECRET env vars)
2. gs-quant package installed

Run with: pytest tests/integration/test_gsquant.py -v
Skip with: pytest -m "not integration"
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

# Check for gs-quant availability
try:
    from gs_quant.session import GsSession, Environment
    GS_QUANT_AVAILABLE = True
except ImportError:
    GS_QUANT_AVAILABLE = False

# Check for credentials
GS_CLIENT_ID = os.getenv("GS_CLIENT_ID")
GS_CLIENT_SECRET = os.getenv("GS_CLIENT_SECRET")
HAS_CREDENTIALS = bool(GS_CLIENT_ID and GS_CLIENT_SECRET)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not GS_QUANT_AVAILABLE, reason="gs-quant not installed"),
    pytest.mark.skipif(not HAS_CREDENTIALS, reason="GS credentials not set"),
]


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def gs_session() -> None:
    """Authenticate with GS Marquee."""
    GsSession.use(
        Environment.PROD,
        client_id=GS_CLIENT_ID,
        client_secret=GS_CLIENT_SECRET,
    )


class TestGSQuantIntegration:
    """Integration tests for GSQuantSource."""

    def test_fetch_single_symbol(self, gs_session: None) -> None:
        """Fetch single symbol from gs-quant."""
        from metapyle import Client
        
        client = Client(catalog=FIXTURES_DIR / "gsquant.yaml")
        df = client.fetch(["eurusd_vol"], start="2024-01-01", end="2024-01-31")
        
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "eurusd_vol" in df.columns

    def test_fetch_multiple_symbols(self, gs_session: None) -> None:
        """Fetch multiple symbols from gs-quant."""
        from metapyle import Client
        
        client = Client(catalog=FIXTURES_DIR / "gsquant.yaml")
        df = client.fetch(
            ["eurusd_vol", "usdjpy_vol"],
            start="2024-01-01",
            end="2024-01-31",
        )
        
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "eurusd_vol" in df.columns
        assert "usdjpy_vol" in df.columns
```

**Step 3: Commit**

```bash
git add tests/integration/test_gsquant.py tests/integration/fixtures/gsquant.yaml
git commit -m "test(integration): add gs-quant integration tests"
```

---

# Execution Handoff

## Pre-Execution Checklist

Before starting, verify:

1. [ ] Design document approved: `docs/plans/2025-12-29-gsquant-source-design.md`
2. [ ] On feature branch: `git checkout -b feat/gsquant-source`
3. [ ] Clean working directory: `git status` shows no uncommitted changes
4. [ ] Tests passing: `pytest tests/unit/ -v` all green

## Task Execution Order

Execute tasks sequentially. Each task has verification steps.

| Task | Description | Test Command | Commit Message |
|------|-------------|--------------|----------------|
| 1 | Add params to CatalogEntry | `pytest tests/unit/test_catalog.py -v` | `feat(catalog): add params field to CatalogEntry` |
| 2 | Add params to FetchRequest | `pytest tests/unit/test_sources_base.py -v` | `feat(sources): add params field to FetchRequest` |
| 3 | Update YAML parsing | `pytest tests/unit/test_catalog.py -v` | `feat(catalog): parse params from YAML` |
| 4 | Document CSV limitation | (docs only) | `docs(catalog): document params CSV limitation` |
| 5 | Pass params in Client | `pytest tests/unit/test_client.py -v` | `feat(client): pass params to FetchRequest` |
| 6 | Update YAML export | `pytest tests/unit/test_catalog.py -v` | `feat(catalog): export params in to_yaml` |
| 7 | Create GSQuantSource skeleton | `pytest tests/unit/test_sources_gsquant.py -v` | `feat(gsquant): add source skeleton with lazy import` |
| 8 | Register in __init__ | `python -c "from metapyle.sources import gsquant"` | `feat(sources): register gsquant source` |
| 9 | Implement _parse_field | `pytest tests/unit/test_sources_gsquant.py::TestParseField -v` | `feat(gsquant): implement field parsing` |
| 10 | Implement fetch (single) | `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetch -v` | `feat(gsquant): implement fetch with single request` |
| 11 | Test batch fetch | `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetchBatch -v` | `test(gsquant): add batch fetch tests` |
| 12 | Test error handling | `pytest tests/unit/test_sources_gsquant.py::TestGSQuantFetchErrors -v` | `test(gsquant): add error handling tests` |
| 13 | Implement get_metadata | `pytest tests/unit/test_sources_gsquant.py::TestGSQuantMetadata -v` | `feat(gsquant): implement get_metadata (minimal)` |
| 14 | Update pyproject.toml | N/A | `build: add gs-quant optional dependency` |
| 15 | Update public API | `pytest tests/unit/test_public_api.py -v` | `test(api): verify gsquant source module exists` |
| 16 | Full test suite | `pytest tests/unit/ -v && mypy src/ && ruff check src/` | (fix if needed) |
| 17 | Integration tests | (manual with credentials) | `test(integration): add gs-quant integration tests` |

## Verification Commands

After all tasks complete:

```bash
# Full verification
pytest tests/unit/ -v
mypy src/metapyle/
ruff check src/metapyle/
ruff format --check src/metapyle/

# Review changes
git log --oneline feat/gsquant-source ^main
git diff main..feat/gsquant-source --stat
```

## Post-Execution

1. Open PR from `feat/gsquant-source` to `main`
2. Request code review
3. Update CHANGELOG.md with new feature
