# DataFrame Normalization Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Create a shared `normalize_dataframe()` utility and refactor all sources to use it, with a safety net in `_assemble_dataframe`.

**Architecture:** Add `normalize_dataframe()` to `sources/base.py`, update each source to call it instead of inline normalization, add defensive call in client's `_assemble_dataframe`.

**Tech Stack:** pandas (DatetimeIndex, timezone handling)

---

### Task 1: Add `normalize_dataframe()` to sources/base.py

**Files:**
- Modify: `src/metapyle/sources/base.py`
- Test: `tests/unit/test_sources_normalize.py` (new)

**Step 1: Write the failing tests**

Create `tests/unit/test_sources_normalize.py`:

```python
"""Tests for normalize_dataframe utility."""

import pandas as pd
import pytest

from metapyle.sources.base import normalize_dataframe


class TestNormalizeDataframe:
    """Tests for normalize_dataframe function."""

    def test_tz_naive_localized_to_utc(self) -> None:
        """Tz-naive index should be localized to UTC."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        )
        assert df.index.tz is None

        result = normalize_dataframe(df)

        assert result.index.tz is not None
        assert str(result.index.tz) == "UTC"

    def test_tz_aware_non_utc_converted_to_utc(self) -> None:
        """Tz-aware non-UTC index should be converted to UTC."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]).tz_localize(
                "America/New_York"
            ),
        )
        assert str(df.index.tz) == "America/New_York"

        result = normalize_dataframe(df)

        assert str(result.index.tz) == "UTC"

    def test_already_utc_unchanged(self) -> None:
        """Already UTC index should remain UTC (idempotent)."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]).tz_localize("UTC"),
        )

        result = normalize_dataframe(df)

        assert str(result.index.tz) == "UTC"
        # Values unchanged
        assert list(result["value"]) == [1, 2, 3]

    def test_index_name_set_to_date(self) -> None:
        """Index name should be set to 'date'."""
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        df.index.name = "timestamp"

        result = normalize_dataframe(df)

        assert result.index.name == "date"

    def test_non_datetime_index_converted(self) -> None:
        """String index should be converted to DatetimeIndex."""
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=["2024-01-01", "2024-01-02"],
        )
        assert not isinstance(df.index, pd.DatetimeIndex)

        result = normalize_dataframe(df)

        assert isinstance(result.index, pd.DatetimeIndex)
        assert str(result.index.tz) == "UTC"

    def test_invalid_index_raises_valueerror(self) -> None:
        """Non-convertible index should raise ValueError."""
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=["not-a-date", "also-not-a-date"],
        )

        with pytest.raises(ValueError, match="Cannot convert index to DatetimeIndex"):
            normalize_dataframe(df)

    def test_returns_dataframe(self) -> None:
        """Should return a DataFrame for chaining."""
        df = pd.DataFrame(
            {"value": [1]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        result = normalize_dataframe(df)

        assert isinstance(result, pd.DataFrame)
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_normalize.py -v`
Expected: FAIL with "cannot import name 'normalize_dataframe'"

**Step 3: Write the implementation**

Add to `src/metapyle/sources/base.py` after the `make_column_name` function:

```python
def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame for consistent source output.

    Ensures:
    - Index is DatetimeIndex (converts if needed)
    - Index timezone is UTC (localizes naive, converts aware)
    - Index name is "date"

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with datetime-like index.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame (mutated in place, returned for chaining).

    Raises
    ------
    ValueError
        If index cannot be converted to DatetimeIndex.
    """
    # Convert to DatetimeIndex if needed
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception as e:
            raise ValueError(f"Cannot convert index to DatetimeIndex: {e}") from e

    # Normalize timezone to UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    elif str(df.index.tz) != "UTC":
        df.index = df.index.tz_convert("UTC")

    # Set index name
    df.index.name = "date"

    return df
```

Also update `__all__` at the top of the file:

```python
__all__ = ["BaseSource", "FetchRequest", "make_column_name", "normalize_dataframe", "register_source"]
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_normalize.py -v`
Expected: All 7 tests PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/base.py tests/unit/test_sources_normalize.py
git commit -m "feat(sources): add normalize_dataframe utility function"
```

---

### Task 2: Refactor bloomberg source

**Files:**
- Modify: `src/metapyle/sources/bloomberg.py`

**Step 1: Update import**

Change:
```python
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    make_column_name,
    register_source,
)
```

To:
```python
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    make_column_name,
    normalize_dataframe,
    register_source,
)
```

**Step 2: Replace inline normalization**

In the `fetch` method, replace these lines (near end of method):

```python
        # Normalize index name
        df.index.name = "date"

        # Ensure UTC timezone
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        logger.info(
```

With:
```python
        logger.info(
```

And change `return df` at the end to `return normalize_dataframe(df)`.

**Step 3: Run existing bloomberg tests**

Run: `pytest tests/unit/test_sources_bloomberg.py -v`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add src/metapyle/sources/bloomberg.py
git commit -m "refactor(bloomberg): use normalize_dataframe utility"
```

---

### Task 3: Refactor macrobond source

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`

**Step 1: Update import**

Add `normalize_dataframe` to the import:

```python
from metapyle.sources.base import BaseSource, FetchRequest, normalize_dataframe, register_source
```

**Step 2: Replace inline normalization in `_fetch_regular`**

Replace these lines (near end of `_fetch_regular`):

```python
        # Normalize index name
        result.index.name = "date"

        # Ensure UTC timezone
        if result.index.tz is None:
            result.index = result.index.tz_localize("UTC")
        else:
            result.index = result.index.tz_convert("UTC")

        if result.empty:
```

With:
```python
        result = normalize_dataframe(result)

        if result.empty:
```

**Step 3: Replace inline normalization in `_fetch_unified`**

Replace these lines (near end of `_fetch_unified`):

```python
        # Normalize index name
        df.index.name = "date"

        # Ensure UTC timezone
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        logger.info(
```

With:
```python
        df = normalize_dataframe(df)

        logger.info(
```

**Step 4: Run existing macrobond tests**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/macrobond.py
git commit -m "refactor(macrobond): use normalize_dataframe utility"
```

---

### Task 4: Refactor localfile source

**Files:**
- Modify: `src/metapyle/sources/localfile.py`

**Step 1: Update import**

Add `normalize_dataframe` to the import:

```python
from metapyle.sources.base import BaseSource, FetchRequest, normalize_dataframe, register_source
```

**Step 2: Replace inline normalization**

Replace these lines (near end of `fetch`):

```python
        # Normalize index name
        df_filtered.index.name = "date"

        # Ensure UTC timezone
        if df_filtered.index.tz is None:
            df_filtered.index = df_filtered.index.tz_localize("UTC")
        else:
            df_filtered.index = df_filtered.index.tz_convert("UTC")

        logger.info(
```

With:
```python
        df_filtered = normalize_dataframe(df_filtered)

        logger.info(
```

**Step 3: Run existing localfile tests**

Run: `pytest tests/unit/test_sources_localfile.py -v`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add src/metapyle/sources/localfile.py
git commit -m "refactor(localfile): use normalize_dataframe utility"
```

---

### Task 5: Refactor gsquant source

**Files:**
- Modify: `src/metapyle/sources/gsquant.py`

**Step 1: Update import**

Change:
```python
from metapyle.sources.base import BaseSource, FetchRequest, make_column_name, register_source
```

To:
```python
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    make_column_name,
    normalize_dataframe,
    register_source,
)
```

**Step 2: Replace inline normalization**

Replace these lines (near end of `fetch`):

```python
        # Normalize index name
        result.index.name = "date"

        # Ensure UTC timezone
        if result.index.tz is None:
            result.index = result.index.tz_localize("UTC")
        else:
            result.index = result.index.tz_convert("UTC")

        logger.info(
```

With:
```python
        result = normalize_dataframe(result)

        logger.info(
```

**Step 3: Run existing gsquant tests**

Run: `pytest tests/unit/test_sources_gsquant.py -v`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add src/metapyle/sources/gsquant.py
git commit -m "refactor(gsquant): use normalize_dataframe utility"
```

---

### Task 6: Add safety net in client._assemble_dataframe

**Files:**
- Modify: `src/metapyle/client.py`
- Test: `tests/unit/test_client.py`

**Step 1: Write test for mixed timezone concat**

Add to `tests/unit/test_client.py`:

```python
class TestAssembleDataframeMixedTimezones:
    """Tests for _assemble_dataframe handling mixed timezones."""

    def test_concat_tz_naive_and_tz_aware(self, tmp_path: Path) -> None:
        """Should successfully concat tz-naive and tz-aware DataFrames."""
        # Create minimal catalog
        catalog_path = tmp_path / "catalog.yaml"
        catalog_path.write_text("entries: []")

        client = Client(catalog=catalog_path, cache_enabled=False)

        # Simulate two DataFrames with different timezone handling
        # (mimics a misbehaving source that returns tz-naive)
        df_naive = pd.DataFrame(
            {"col1": [1.0, 2.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        df_aware = pd.DataFrame(
            {"col2": [3.0, 4.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]).tz_localize("UTC"),
        )

        dfs = {"series_a": df_naive, "series_b": df_aware}

        # Should not raise - safety net normalizes before concat
        result = client._assemble_dataframe(dfs, ["series_a", "series_b"])

        assert list(result.columns) == ["series_a", "series_b"]
        assert str(result.index.tz) == "UTC"
        assert len(result) == 2

        client.close()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::TestAssembleDataframeMixedTimezones -v`
Expected: FAIL with "Cannot join tz-naive with tz-aware DatetimeIndex"

**Step 3: Update client.py import**

Add `normalize_dataframe` to the import:

```python
from metapyle.sources.base import FetchRequest, SourceRegistry, _global_registry, make_column_name, normalize_dataframe
```

**Step 4: Update _assemble_dataframe method**

Replace the method body:

```python
    def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame], names: list[str]) -> pd.DataFrame:
        """
        Assemble individual DataFrames into a wide DataFrame.

        Renames source columns to my_name from catalog and preserves
        the order specified by names.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping my_name to DataFrames.
        names : list[str]
            Original input symbol names, used to preserve column order.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by my_name in input order.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename first column to my_name and normalize each DataFrame
        renamed: list[pd.DataFrame] = []
        for my_name, df in dfs.items():
            # Take first column regardless of name, rename to my_name
            col = df.columns[0]
            df_renamed = df[[col]].rename(columns={col: my_name})
            # Defensive normalization (sources should already normalize,
            # but this ensures safe concat even with misbehaving sources)
            renamed.append(normalize_dataframe(df_renamed))

        combined = pd.concat(renamed, axis=1)

        # Preserve input order
        ordered_cols = [name for name in names if name in combined.columns]
        return combined[ordered_cols]
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py::TestAssembleDataframeMixedTimezones -v`
Expected: PASS

**Step 6: Run all client tests**

Run: `pytest tests/unit/test_client.py -v`
Expected: All tests PASS

**Step 7: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): add defensive normalization in _assemble_dataframe"
```

---

### Task 7: Final verification and cleanup

**Step 1: Run full test suite**

Run: `pytest -v`
Expected: All tests PASS

**Step 2: Run linting**

Run: `ruff check . && ruff format --check .`
Expected: No errors

**Step 3: Run type checking**

Run: `mypy src/`
Expected: No errors

**Step 4: Remove TODO item**

Edit `TODO.md` to remove the first line about tz-aware/tz-naive concat.

**Step 5: Final commit**

```bash
git add TODO.md
git commit -m "docs: remove completed TODO item for df normalization"
```

---

## Summary

| Task | Description | Tests |
|------|-------------|-------|
| 1 | Add `normalize_dataframe()` to base.py | 7 new unit tests |
| 2 | Refactor bloomberg source | existing tests |
| 3 | Refactor macrobond source | existing tests |
| 4 | Refactor localfile source | existing tests |
| 5 | Refactor gsquant source | existing tests |
| 6 | Add safety net in client | 1 new test |
| 7 | Final verification | full suite |
