# Macrobond Source Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Add a Macrobond source adapter to metapyle for fetching macroeconomic time series data.

**Architecture:** Lazy-import `macrobond_data_api`, auto-detect ComClient/WebClient, support both raw fetch and unified series with kwargs pass-through.

**Tech Stack:** Python 3.12+, macrobond-data-api (optional), pandas, pytest

---

## Task 1: Create Source File with Lazy Import

**Files:**
- Create: `src/metapyle/sources/macrobond.py`

**Step 1: Create the source file with lazy import pattern**

```python
"""Macrobond source adapter using macrobond_data_api library."""

import logging
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, register_source

__all__ = ["MacrobondSource"]

logger = logging.getLogger(__name__)

# Lazy import of macrobond_data_api to avoid import-time errors
_MDA_AVAILABLE: bool | None = None
_mda_module: Any = None


def _get_mda() -> Any:
    """Lazy import of macrobond_data_api module.

    Returns
    -------
    Any
        The macrobond_data_api module, or None if not available.
    """
    global _MDA_AVAILABLE, _mda_module

    if _MDA_AVAILABLE is None:
        try:
            import macrobond_data_api as mda

            _mda_module = mda
            _MDA_AVAILABLE = True
        except (ImportError, Exception):
            _mda_module = None
            _MDA_AVAILABLE = False

    return _mda_module
```

**Step 2: Verify file is syntactically correct**

Run: `python -c "import ast; ast.parse(open('src/metapyle/sources/macrobond.py').read())"`
Expected: No output (success)

**Step 3: Commit**

```bash
git add src/metapyle/sources/macrobond.py
git commit -m "feat(sources): add macrobond source file with lazy import"
```

---

## Task 2: Add MacrobondSource Class Skeleton

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`
- Modify: `src/metapyle/sources/__init__.py`

**Step 1: Add the class skeleton to macrobond.py**

Append to `src/metapyle/sources/macrobond.py`:

```python


@register_source("macrobond")
class MacrobondSource(BaseSource):
    """Source adapter for Macrobond data via macrobond_data_api.

    Uses macrobond_data_api for data retrieval. Automatically detects
    whether to use ComClient (desktop app) or WebClient (API credentials).

    Examples
    --------
    >>> source = MacrobondSource()
    >>> df = source.fetch("usgdp", "2020-01-01", "2024-12-31")
    """

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        unified: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from Macrobond.

        Parameters
        ----------
        symbol : str
            Macrobond series name (e.g., "usgdp", "gbcpi").
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        unified : bool, optional
            If True, use get_unified_series with kwargs pass-through.
            If False (default), use get_one_series.
        **kwargs : Any
            Additional parameters passed to get_unified_series when unified=True.
            E.g., frequency, currency, calendar_merge_mode.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named by symbol.

        Raises
        ------
        FetchError
            If macrobond_data_api is not available or API call fails.
        NoDataError
            If no data is returned for the symbol.
        """
        raise NotImplementedError

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a Macrobond symbol.

        Parameters
        ----------
        symbol : str
            Macrobond series name.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary containing series information.
        """
        raise NotImplementedError
```

**Step 2: Update sources __init__.py to import macrobond**

Modify `src/metapyle/sources/__init__.py` - add import:

```python
from metapyle.sources.macrobond import MacrobondSource
```

And add to `__all__`:

```python
__all__ = [..., "MacrobondSource"]
```

**Step 3: Verify imports work**

Run: `python -c "from metapyle.sources import MacrobondSource; print('OK')"`
Expected: `OK`

**Step 4: Commit**

```bash
git add src/metapyle/sources/macrobond.py src/metapyle/sources/__init__.py
git commit -m "feat(sources): add MacrobondSource class skeleton"
```

---

## Task 3: Write Tests for fetch() with unified=False

**Files:**
- Create: `tests/unit/test_sources_macrobond.py`

**Step 1: Write the test file with mocking**

```python
"""Unit tests for MacrobondSource."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.macrobond import MacrobondSource, _get_mda


class TestMacrobondSourceFetch:
    """Tests for MacrobondSource.fetch() method."""

    def test_fetch_returns_dataframe_with_correct_structure(self) -> None:
        """fetch() returns DataFrame with DatetimeIndex and symbol column."""
        # Create mock series object
        mock_series = MagicMock()
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame({
            "date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
            "value": [100.0, 101.0, 102.0],
        })

        with patch(
            "metapyle.sources.macrobond._get_mda"
        ) as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            df = source.fetch("usgdp", "2020-01-01", "2020-12-31")

            assert isinstance(df, pd.DataFrame)
            assert isinstance(df.index, pd.DatetimeIndex)
            assert "usgdp" in df.columns
            assert len(df) == 3

    def test_fetch_filters_by_date_range(self) -> None:
        """fetch() filters data to requested start:end range."""
        mock_series = MagicMock()
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame({
            "date": pd.to_datetime([
                "2019-12-01", "2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01"
            ]),
            "value": [99.0, 100.0, 101.0, 102.0, 103.0],
        })

        with patch(
            "metapyle.sources.macrobond._get_mda"
        ) as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            df = source.fetch("usgdp", "2020-01-01", "2020-02-28")

            assert len(df) == 2
            assert df.index[0] == pd.Timestamp("2020-01-01")
            assert df.index[-1] == pd.Timestamp("2020-02-01")

    def test_fetch_raises_fetch_error_when_mda_not_available(self) -> None:
        """fetch() raises FetchError when macrobond_data_api not installed."""
        with patch(
            "metapyle.sources.macrobond._get_mda", return_value=None
        ):
            source = MacrobondSource()
            with pytest.raises(FetchError, match="macrobond_data_api"):
                source.fetch("usgdp", "2020-01-01", "2020-12-31")

    def test_fetch_raises_no_data_error_when_empty(self) -> None:
        """fetch() raises NoDataError when series returns no data."""
        mock_series = MagicMock()
        mock_series.values_to_pd_data_frame.return_value = pd.DataFrame({
            "date": pd.to_datetime([]),
            "value": [],
        })

        with patch(
            "metapyle.sources.macrobond._get_mda"
        ) as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_series.return_value = mock_series
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            with pytest.raises(NoDataError):
                source.fetch("usgdp", "2020-01-01", "2020-12-31")
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: FAIL with "NotImplementedError"

**Step 3: Commit test file**

```bash
git add tests/unit/test_sources_macrobond.py
git commit -m "test(sources): add failing tests for MacrobondSource.fetch()"
```

---

## Task 4: Implement fetch() for unified=False

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`

**Step 1: Implement the fetch method**

Replace the `fetch` method in `MacrobondSource`:

```python
    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        unified: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from Macrobond.

        Parameters
        ----------
        symbol : str
            Macrobond series name (e.g., "usgdp", "gbcpi").
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        unified : bool, optional
            If True, use get_unified_series with kwargs pass-through.
            If False (default), use get_one_series.
        **kwargs : Any
            Additional parameters passed to get_unified_series when unified=True.
            E.g., frequency, currency, calendar_merge_mode.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named by symbol.

        Raises
        ------
        FetchError
            If macrobond_data_api is not available or API call fails.
        NoDataError
            If no data is returned for the symbol.
        """
        mda = _get_mda()
        if mda is None:
            logger.error("fetch_failed: symbol=%s, reason=mda_not_installed", symbol)
            raise FetchError(
                "macrobond_data_api package is not installed. "
                "Install with: pip install macrobond-data-api"
            )

        logger.debug(
            "fetch_start: symbol=%s, start=%s, end=%s, unified=%s",
            symbol,
            start,
            end,
            unified,
        )

        try:
            if unified:
                df = self._fetch_unified(mda, symbol, **kwargs)
            else:
                df = self._fetch_raw(mda, symbol)
        except Exception as e:
            logger.error("fetch_failed: symbol=%s, error=%s", symbol, str(e))
            raise FetchError(f"Macrobond API error for {symbol}: {e}") from e

        if df.empty:
            logger.warning("fetch_empty: symbol=%s", symbol)
            raise NoDataError(f"No data returned for {symbol}")

        # Filter by date range
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (df.index >= start_dt) & (df.index <= end_dt)
        df_filtered = df.loc[mask]

        if df_filtered.empty:
            logger.warning(
                "fetch_no_data_in_range: symbol=%s, start=%s, end=%s",
                symbol,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end} for {symbol}")

        logger.info("fetch_complete: symbol=%s, rows=%d", symbol, len(df_filtered))
        return df_filtered

    def _fetch_raw(self, mda: Any, symbol: str) -> pd.DataFrame:
        """Fetch using get_one_series."""
        series = mda.get_one_series(symbol)
        df = series.values_to_pd_data_frame()

        # Convert to proper DataFrame structure
        df.index = pd.to_datetime(df["date"])
        df = df[["value"]].rename(columns={"value": symbol})
        return df

    def _fetch_unified(self, mda: Any, symbol: str, **kwargs: Any) -> pd.DataFrame:
        """Fetch using get_unified_series."""
        result = mda.get_unified_series(symbol, **kwargs)
        df = result.to_pd_data_frame()

        # First column is typically date, rest are values
        if len(df.columns) >= 2:
            df.index = pd.to_datetime(df.iloc[:, 0])
            df = df.iloc[:, 1:2]
            df.columns = [symbol]
        return df
```

**Step 2: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add src/metapyle/sources/macrobond.py
git commit -m "feat(sources): implement MacrobondSource.fetch() for raw mode"
```

---

## Task 5: Write Tests for fetch() with unified=True

**Files:**
- Modify: `tests/unit/test_sources_macrobond.py`

**Step 1: Add tests for unified mode**

Append to `tests/unit/test_sources_macrobond.py`:

```python


class TestMacrobondSourceFetchUnified:
    """Tests for MacrobondSource.fetch() with unified=True."""

    def test_fetch_unified_calls_get_unified_series(self) -> None:
        """fetch(unified=True) calls get_unified_series instead of get_one_series."""
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame({
            "Date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            "usgdp": [100.0, 101.0],
        })

        with patch(
            "metapyle.sources.macrobond._get_mda"
        ) as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            df = source.fetch("usgdp", "2020-01-01", "2020-12-31", unified=True)

            mock_mda.get_unified_series.assert_called_once()
            mock_mda.get_one_series.assert_not_called()
            assert isinstance(df, pd.DataFrame)

    def test_fetch_unified_passes_kwargs(self) -> None:
        """fetch(unified=True) passes kwargs to get_unified_series."""
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame({
            "Date": pd.to_datetime(["2020-01-01"]),
            "usgdp": [100.0],
        })

        with patch(
            "metapyle.sources.macrobond._get_mda"
        ) as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            source.fetch(
                "usgdp",
                "2020-01-01",
                "2020-12-31",
                unified=True,
                frequency="annual",
                currency="USD",
            )

            call_kwargs = mock_mda.get_unified_series.call_args
            assert call_kwargs[1].get("frequency") == "annual"
            assert call_kwargs[1].get("currency") == "USD"
```

**Step 2: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceFetchUnified -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add tests/unit/test_sources_macrobond.py
git commit -m "test(sources): add tests for MacrobondSource.fetch() unified mode"
```

---

## Task 6: Write Tests for get_metadata()

**Files:**
- Modify: `tests/unit/test_sources_macrobond.py`

**Step 1: Add tests for get_metadata**

Append to `tests/unit/test_sources_macrobond.py`:

```python


class TestMacrobondSourceGetMetadata:
    """Tests for MacrobondSource.get_metadata() method."""

    def test_get_metadata_returns_dict(self) -> None:
        """get_metadata() returns metadata as dict."""
        mock_entity = MagicMock()
        mock_entity.metadata = {
            "FullDescription": "United States, GDP",
            "Frequency": "quarterly",
            "DisplayUnit": "USD",
            "Region": "us",
        }

        with patch(
            "metapyle.sources.macrobond._get_mda"
        ) as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_one_entity.return_value = mock_entity
            mock_get_mda.return_value = mock_mda

            source = MacrobondSource()
            metadata = source.get_metadata("usgdp")

            assert isinstance(metadata, dict)
            assert metadata["FullDescription"] == "United States, GDP"
            assert metadata["Frequency"] == "quarterly"

    def test_get_metadata_raises_fetch_error_when_mda_not_available(self) -> None:
        """get_metadata() raises FetchError when macrobond_data_api not installed."""
        with patch(
            "metapyle.sources.macrobond._get_mda", return_value=None
        ):
            source = MacrobondSource()
            with pytest.raises(FetchError, match="macrobond_data_api"):
                source.get_metadata("usgdp")
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceGetMetadata -v`
Expected: FAIL with "NotImplementedError"

**Step 3: Commit**

```bash
git add tests/unit/test_sources_macrobond.py
git commit -m "test(sources): add failing tests for MacrobondSource.get_metadata()"
```

---

## Task 7: Implement get_metadata()

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`

**Step 1: Implement the get_metadata method**

Replace the `get_metadata` method in `MacrobondSource`:

```python
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a Macrobond symbol.

        Parameters
        ----------
        symbol : str
            Macrobond series name.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary containing series information.
        """
        mda = _get_mda()
        if mda is None:
            logger.error("get_metadata_failed: symbol=%s, reason=mda_not_installed", symbol)
            raise FetchError(
                "macrobond_data_api package is not installed. "
                "Install with: pip install macrobond-data-api"
            )

        logger.debug("get_metadata: symbol=%s", symbol)

        try:
            entity = mda.get_one_entity(symbol)
            metadata = dict(entity.metadata)
        except Exception as e:
            logger.error("get_metadata_failed: symbol=%s, error=%s", symbol, str(e))
            raise FetchError(f"Failed to get metadata for {symbol}: {e}") from e

        return metadata
```

**Step 2: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceGetMetadata -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add src/metapyle/sources/macrobond.py
git commit -m "feat(sources): implement MacrobondSource.get_metadata()"
```

---

## Task 8: Final Verification and Cleanup

**Files:**
- All source and test files

**Step 1: Run all macrobond tests**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All tests PASS

**Step 2: Run full test suite**

Run: `pytest`
Expected: All tests PASS

**Step 3: Run linting**

Run: `ruff check src/metapyle/sources/macrobond.py tests/unit/test_sources_macrobond.py`
Expected: No errors

**Step 4: Run type checking**

Run: `mypy src/metapyle/sources/macrobond.py`
Expected: No errors

**Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "chore(sources): fix linting issues in macrobond source"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Create source file with lazy import | `macrobond.py` |
| 2 | Add class skeleton | `macrobond.py`, `__init__.py` |
| 3 | Write tests for fetch() raw mode | `test_sources_macrobond.py` |
| 4 | Implement fetch() raw mode | `macrobond.py` |
| 5 | Write tests for fetch() unified mode | `test_sources_macrobond.py` |
| 6 | Write tests for get_metadata() | `test_sources_macrobond.py` |
| 7 | Implement get_metadata() | `macrobond.py` |
| 8 | Final verification | All files |
