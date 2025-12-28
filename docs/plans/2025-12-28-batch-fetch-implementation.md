# Batch Fetch Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Replace single-symbol `fetch()` with batch-capable `fetch(requests, start, end)` across all source adapters.

**Architecture:** Add `FetchRequest` dataclass, update `BaseSource` interface, implement batch fetch in all sources, update Client to group requests by source and handle caching per-symbol.

**Tech Stack:** Python 3.12+, pandas, pytest

---

## Task 1: Add FetchRequest and make_column_name to base.py

**Files:**
- Modify: `src/metapyle/sources/base.py`
- Test: `tests/unit/test_sources_base.py`

**Step 1: Write test for make_column_name**

Add to `tests/unit/test_sources_base.py`:

```python
from metapyle.sources.base import make_column_name


class TestMakeColumnName:
    """Tests for make_column_name utility."""

    def test_symbol_only(self) -> None:
        """Column name is symbol when no field."""
        result = make_column_name("usgdp", None)
        assert result == "usgdp"

    def test_symbol_with_field(self) -> None:
        """Column name is symbol::field when field present."""
        result = make_column_name("SPX Index", "PX_LAST")
        assert result == "SPX Index::PX_LAST"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_base.py::TestMakeColumnName -v`

Expected: FAIL with `ImportError: cannot import name 'make_column_name'`

**Step 3: Write make_column_name implementation**

Add to `src/metapyle/sources/base.py` after imports:

```python
def make_column_name(symbol: str, field: str | None) -> str:
    """
    Generate consistent column name for source output.

    Parameters
    ----------
    symbol : str
        The symbol identifier.
    field : str | None
        Optional field name.

    Returns
    -------
    str
        "symbol::field" if field present, otherwise "symbol".
    """
    return f"{symbol}::{field}" if field else symbol
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_base.py::TestMakeColumnName -v`

Expected: PASS

**Step 5: Write test for FetchRequest**

Add to `tests/unit/test_sources_base.py`:

```python
from metapyle.sources.base import FetchRequest


class TestFetchRequest:
    """Tests for FetchRequest dataclass."""

    def test_symbol_only(self) -> None:
        """FetchRequest with only symbol."""
        req = FetchRequest(symbol="usgdp")
        assert req.symbol == "usgdp"
        assert req.field is None
        assert req.path is None

    def test_all_fields(self) -> None:
        """FetchRequest with all fields."""
        req = FetchRequest(symbol="GDP", field="value", path="/data/file.csv")
        assert req.symbol == "GDP"
        assert req.field == "value"
        assert req.path == "/data/file.csv"

    def test_frozen(self) -> None:
        """FetchRequest is immutable."""
        req = FetchRequest(symbol="test")
        with pytest.raises(AttributeError):
            req.symbol = "changed"  # type: ignore[misc]
```

**Step 6: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_base.py::TestFetchRequest -v`

Expected: FAIL with `ImportError: cannot import name 'FetchRequest'`

**Step 7: Write FetchRequest implementation**

Add to `src/metapyle/sources/base.py` after imports:

```python
from dataclasses import dataclass


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
    """

    symbol: str
    field: str | None = None
    path: str | None = None
```

**Step 8: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_base.py::TestFetchRequest -v`

Expected: PASS

**Step 9: Update __all__ export**

Update `__all__` in `src/metapyle/sources/base.py`:

```python
__all__ = ["BaseSource", "FetchRequest", "make_column_name", "register_source"]
```

**Step 10: Run all base tests**

Run: `pytest tests/unit/test_sources_base.py -v`

Expected: All PASS

**Step 11: Commit**

```bash
git add src/metapyle/sources/base.py tests/unit/test_sources_base.py
git commit -m "feat(sources): add FetchRequest and make_column_name"
```

---

## Task 2: Update BaseSource.fetch() signature

**Files:**
- Modify: `src/metapyle/sources/base.py`
- Test: `tests/unit/test_sources_base.py`

**Step 1: Update BaseSource.fetch() abstract method**

Replace the existing `fetch` method in `BaseSource` class:

```python
from collections.abc import Sequence

@abstractmethod
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Fetch time-series data for one or more symbols.

    Parameters
    ----------
    requests : Sequence[FetchRequest]
        One or more fetch requests.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and one column per request.
        Column naming: "symbol::field" if field present, otherwise "symbol".

    Raises
    ------
    NoDataError
        If no data is returned for any symbol.
    FetchError
        If data retrieval fails.
    """
    pass
```

**Step 2: Run existing tests to verify they fail**

Run: `pytest tests/unit/test_sources_base.py -v`

Expected: Existing tests may need updating. Check and fix import issues.

**Step 3: Commit interface change**

```bash
git add src/metapyle/sources/base.py
git commit -m "feat(sources): update BaseSource.fetch() to accept Sequence[FetchRequest]"
```

---

## Task 3: Update LocalFileSource for batch fetch

**Files:**
- Modify: `src/metapyle/sources/localfile.py`
- Test: `tests/unit/test_sources_localfile.py`

**Step 1: Write test for batch fetch from localfile**

Replace test class in `tests/unit/test_sources_localfile.py`:

```python
"""Tests for LocalFileSource."""

from pathlib import Path

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import FetchRequest
from metapyle.sources.localfile import LocalFileSource


@pytest.fixture
def source() -> LocalFileSource:
    """Create LocalFileSource instance."""
    return LocalFileSource()


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create sample CSV file with multiple columns."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "date,GDP_US,CPI_EU,RATE_JP\n"
        "2024-01-01,100.0,200.0,0.5\n"
        "2024-01-02,101.0,201.0,0.6\n"
        "2024-01-03,102.0,202.0,0.7\n"
    )
    return csv_path


class TestLocalFileSourceFetch:
    """Tests for LocalFileSource.fetch()."""

    def test_single_column(
        self, source: LocalFileSource, sample_csv: Path
    ) -> None:
        """Fetch single column from CSV."""
        requests = [FetchRequest(symbol="GDP_US", path=str(sample_csv))]
        df = source.fetch(requests, "2024-01-01", "2024-01-03")

        assert list(df.columns) == ["GDP_US"]
        assert len(df) == 3
        assert df["GDP_US"].iloc[0] == 100.0

    def test_multiple_columns(
        self, source: LocalFileSource, sample_csv: Path
    ) -> None:
        """Fetch multiple columns from CSV in single call."""
        requests = [
            FetchRequest(symbol="GDP_US", path=str(sample_csv)),
            FetchRequest(symbol="CPI_EU", path=str(sample_csv)),
        ]
        df = source.fetch(requests, "2024-01-01", "2024-01-03")

        assert list(df.columns) == ["GDP_US", "CPI_EU"]
        assert len(df) == 3

    def test_date_filtering(
        self, source: LocalFileSource, sample_csv: Path
    ) -> None:
        """Only return data within date range."""
        requests = [FetchRequest(symbol="GDP_US", path=str(sample_csv))]
        df = source.fetch(requests, "2024-01-02", "2024-01-02")

        assert len(df) == 1
        assert df["GDP_US"].iloc[0] == 101.0

    def test_missing_path_raises(self, source: LocalFileSource) -> None:
        """Raise FetchError if path not provided."""
        requests = [FetchRequest(symbol="GDP_US")]
        with pytest.raises(FetchError, match="path is required"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_different_paths_raises(
        self, source: LocalFileSource, tmp_path: Path
    ) -> None:
        """Raise FetchError if requests have different paths."""
        requests = [
            FetchRequest(symbol="A", path=str(tmp_path / "a.csv")),
            FetchRequest(symbol="B", path=str(tmp_path / "b.csv")),
        ]
        with pytest.raises(FetchError, match="same path"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_file_not_found_raises(
        self, source: LocalFileSource, tmp_path: Path
    ) -> None:
        """Raise FetchError if file does not exist."""
        requests = [FetchRequest(symbol="X", path=str(tmp_path / "missing.csv"))]
        with pytest.raises(FetchError, match="not found"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_column_not_found_raises(
        self, source: LocalFileSource, sample_csv: Path
    ) -> None:
        """Raise FetchError if column not in file."""
        requests = [FetchRequest(symbol="MISSING", path=str(sample_csv))]
        with pytest.raises(FetchError, match="not found"):
            source.fetch(requests, "2024-01-01", "2024-01-03")

    def test_no_data_in_range_raises(
        self, source: LocalFileSource, sample_csv: Path
    ) -> None:
        """Raise NoDataError if no data in date range."""
        requests = [FetchRequest(symbol="GDP_US", path=str(sample_csv))]
        with pytest.raises(NoDataError):
            source.fetch(requests, "2025-01-01", "2025-12-31")
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_localfile.py -v`

Expected: FAIL (signature mismatch)

**Step 3: Implement batch fetch for LocalFileSource**

Replace `src/metapyle/sources/localfile.py`:

```python
"""Local file source adapter for CSV and Parquet files."""

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["LocalFileSource"]

logger = logging.getLogger(__name__)


@register_source("localfile")
class LocalFileSource(BaseSource):
    """Source adapter for reading local CSV and Parquet files.

    All requests in a batch must reference the same file path.
    The symbol parameter is the column name to extract from the file.
    """

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from a local file.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests. All must have same path.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and one column per request.

        Raises
        ------
        FetchError
            If path missing, paths differ, file not found, or column not found.
        NoDataError
            If file is empty or no data in date range.
        """
        if not requests:
            return pd.DataFrame()

        # Validate all requests have same path
        paths = {req.path for req in requests}
        if None in paths:
            logger.error("fetch_failed: reason=path_not_provided")
            raise FetchError("path is required for localfile source")
        if len(paths) > 1:
            logger.error("fetch_failed: reason=different_paths")
            raise FetchError("All requests must reference the same path")

        path = requests[0].path
        assert path is not None  # for type checker
        file_path = Path(path)
        symbols = [req.symbol for req in requests]

        logger.debug(
            "fetch_start: path=%s, symbols=%s, start=%s, end=%s",
            path,
            symbols,
            start,
            end,
        )

        if not file_path.exists():
            logger.error("fetch_failed: path=%s, reason=file_not_found", path)
            raise FetchError(f"File not found: {path}")

        try:
            df = self._read_file(file_path)
        except FetchError:
            raise
        except Exception as e:
            logger.error("fetch_failed: path=%s, reason=%s", path, str(e))
            raise FetchError(f"Failed to read file: {path}") from e

        if df.empty:
            logger.warning("fetch_empty: path=%s, reason=empty_file", path)
            raise NoDataError(f"File is empty: {path}")

        # Check all requested columns exist
        missing = [s for s in symbols if s not in df.columns]
        if missing:
            available = ", ".join(str(c) for c in df.columns)
            logger.error(
                "fetch_failed: path=%s, missing=%s", path, missing
            )
            raise FetchError(
                f"Column(s) {missing} not found in {path}. Available: {available}"
            )

        # Extract requested columns
        df = df[symbols]

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.error(
                    "fetch_failed: path=%s, reason=invalid_datetime_index", path
                )
                raise FetchError(f"Cannot convert index to datetime: {path}") from e

        # Filter by date range
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (df.index >= start_dt) & (df.index <= end_dt)
        df_filtered = df.loc[mask]

        if df_filtered.empty:
            logger.warning(
                "fetch_no_data_in_range: path=%s, start=%s, end=%s",
                path,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end}: {path}")

        logger.info(
            "fetch_complete: path=%s, symbols=%s, rows=%d",
            path,
            symbols,
            len(df_filtered),
        )
        return df_filtered

    def _read_file(self, path: Path) -> pd.DataFrame:
        """Read a file based on its extension."""
        suffix = path.suffix.lower()

        if suffix == ".csv":
            logger.debug("reading_csv: path=%s", path)
            return pd.read_csv(path, index_col=0, parse_dates=True)
        elif suffix == ".parquet":
            logger.debug("reading_parquet: path=%s", path)
            df = pd.read_parquet(path)
            if not isinstance(df.index, pd.DatetimeIndex):
                date_cols = [
                    c for c in df.columns if c.lower() in ("date", "datetime", "time")
                ]
                if date_cols:
                    df = df.set_index(date_cols[0])
                    df.index = pd.to_datetime(df.index)
            return df
        else:
            raise FetchError(f"Unsupported file extension: {suffix}")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a local file."""
        path = Path(symbol)
        logger.debug("get_metadata: path=%s", symbol)

        metadata: dict[str, Any] = {
            "source": "localfile",
            "path": str(path.absolute()),
            "filename": path.name,
            "extension": path.suffix.lower(),
        }

        if path.exists():
            metadata["exists"] = True
            metadata["size_bytes"] = path.stat().st_size
        else:
            metadata["exists"] = False

        return metadata
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_localfile.py -v`

Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/localfile.py tests/unit/test_sources_localfile.py
git commit -m "feat(localfile): implement batch fetch"
```

---

## Task 4: Update BloombergSource for batch fetch

**Files:**
- Modify: `src/metapyle/sources/bloomberg.py`
- Test: `tests/unit/test_sources_bloomberg.py`

**Step 1: Write test for batch fetch from Bloomberg**

Replace `tests/unit/test_sources_bloomberg.py`:

```python
"""Tests for BloombergSource."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import FetchRequest
from metapyle.sources.bloomberg import BloombergSource


@pytest.fixture
def source() -> BloombergSource:
    """Create BloombergSource instance."""
    return BloombergSource()


class TestBloombergSourceFetch:
    """Tests for BloombergSource.fetch()."""

    def test_single_request(self, source: BloombergSource) -> None:
        """Fetch single symbol with field."""
        mock_df = pd.DataFrame(
            {"PX_LAST": [100.0, 101.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        mock_df.columns = pd.MultiIndex.from_tuples(
            [("SPX Index", "PX_LAST")]
        )

        with patch(
            "metapyle.sources.bloomberg._get_blp"
        ) as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [FetchRequest(symbol="SPX Index", field="PX_LAST")]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert list(df.columns) == ["SPX Index::PX_LAST"]
            assert len(df) == 2

    def test_multiple_requests_same_field(
        self, source: BloombergSource
    ) -> None:
        """Fetch multiple symbols with same field."""
        mock_df = pd.DataFrame(
            [[100.0, 200.0], [101.0, 201.0]],
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
            columns=pd.MultiIndex.from_tuples([
                ("SPX Index", "PX_LAST"),
                ("AAPL US Equity", "PX_LAST"),
            ]),
        )

        with patch(
            "metapyle.sources.bloomberg._get_blp"
        ) as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [
                FetchRequest(symbol="SPX Index", field="PX_LAST"),
                FetchRequest(symbol="AAPL US Equity", field="PX_LAST"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert "SPX Index::PX_LAST" in df.columns
            assert "AAPL US Equity::PX_LAST" in df.columns
            mock_blp.bdh.assert_called_once()

    def test_multiple_fields_same_symbol(
        self, source: BloombergSource
    ) -> None:
        """Fetch multiple fields for same symbol."""
        mock_df = pd.DataFrame(
            [[100.0, 105.0], [101.0, 106.0]],
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
            columns=pd.MultiIndex.from_tuples([
                ("SPX Index", "PX_LAST"),
                ("SPX Index", "PX_HIGH"),
            ]),
        )

        with patch(
            "metapyle.sources.bloomberg._get_blp"
        ) as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [
                FetchRequest(symbol="SPX Index", field="PX_LAST"),
                FetchRequest(symbol="SPX Index", field="PX_HIGH"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert "SPX Index::PX_LAST" in df.columns
            assert "SPX Index::PX_HIGH" in df.columns

    def test_default_field(self, source: BloombergSource) -> None:
        """Use PX_LAST as default field when not specified."""
        mock_df = pd.DataFrame(
            {"PX_LAST": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )
        mock_df.columns = pd.MultiIndex.from_tuples(
            [("SPX Index", "PX_LAST")]
        )

        with patch(
            "metapyle.sources.bloomberg._get_blp"
        ) as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = mock_df
            mock_get_blp.return_value = mock_blp

            requests = [FetchRequest(symbol="SPX Index")]  # no field
            df = source.fetch(requests, "2024-01-01", "2024-01-01")

            assert "SPX Index::PX_LAST" in df.columns

    def test_xbbg_not_available(self, source: BloombergSource) -> None:
        """Raise FetchError when xbbg not installed."""
        with patch(
            "metapyle.sources.bloomberg._get_blp", return_value=None
        ):
            requests = [FetchRequest(symbol="SPX Index", field="PX_LAST")]
            with pytest.raises(FetchError, match="xbbg"):
                source.fetch(requests, "2024-01-01", "2024-01-02")

    def test_empty_result_raises(self, source: BloombergSource) -> None:
        """Raise NoDataError when Bloomberg returns empty."""
        with patch(
            "metapyle.sources.bloomberg._get_blp"
        ) as mock_get_blp:
            mock_blp = MagicMock()
            mock_blp.bdh.return_value = pd.DataFrame()
            mock_get_blp.return_value = mock_blp

            requests = [FetchRequest(symbol="INVALID", field="PX_LAST")]
            with pytest.raises(NoDataError):
                source.fetch(requests, "2024-01-01", "2024-01-02")
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_bloomberg.py -v`

Expected: FAIL (signature mismatch)

**Step 3: Implement batch fetch for BloombergSource**

Replace `src/metapyle/sources/bloomberg.py`:

```python
"""Bloomberg source adapter using xbbg library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    make_column_name,
    register_source,
)

__all__ = ["BloombergSource"]

logger = logging.getLogger(__name__)

_XBBG_AVAILABLE: bool | None = None
_blp_module: Any = None


def _get_blp() -> Any:
    """Lazy import of xbbg.blp module."""
    global _XBBG_AVAILABLE, _blp_module

    if _XBBG_AVAILABLE is None:
        try:
            from xbbg import blp

            _blp_module = blp
            _XBBG_AVAILABLE = True
        except (ImportError, Exception):
            _blp_module = None
            _XBBG_AVAILABLE = False

    return _blp_module


@register_source("bloomberg")
class BloombergSource(BaseSource):
    """Source adapter for Bloomberg data via xbbg.

    Uses xbbg.blp.bdh for historical data retrieval. Supports batch
    fetching of multiple tickers and fields in a single API call.
    """

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch historical data from Bloomberg.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests. Field defaults to PX_LAST if not specified.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and columns named "symbol::field".

        Raises
        ------
        FetchError
            If xbbg not available or API call fails.
        NoDataError
            If no data returned.
        """
        if not requests:
            return pd.DataFrame()

        blp = _get_blp()
        if blp is None:
            logger.error("fetch_failed: reason=xbbg_not_installed")
            raise FetchError(
                "xbbg package is not installed. Install with: pip install xbbg"
            )

        # Collect unique tickers and fields
        tickers = list(dict.fromkeys(req.symbol for req in requests))
        fields = list(dict.fromkeys(req.field or "PX_LAST" for req in requests))

        logger.debug(
            "fetch_start: tickers=%s, fields=%s, start=%s, end=%s",
            tickers,
            fields,
            start,
            end,
        )

        try:
            df = blp.bdh(tickers, fields, start, end)
        except Exception as e:
            logger.error("fetch_failed: error=%s", str(e))
            raise FetchError(f"Bloomberg API error: {e}") from e

        if df.empty:
            logger.warning("fetch_empty: tickers=%s, fields=%s", tickers, fields)
            raise NoDataError(f"No data returned for {tickers} with fields {fields}")

        # Ensure DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # Rename columns from MultiIndex (ticker, field) to "ticker::field"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                make_column_name(ticker, field)
                for ticker, field in df.columns
            ]
        else:
            # Single ticker/field case
            req = requests[0]
            field = req.field or "PX_LAST"
            df.columns = [make_column_name(req.symbol, field)]

        # Filter to only requested symbol::field combinations
        requested_cols = [
            make_column_name(req.symbol, req.field or "PX_LAST")
            for req in requests
        ]
        df = df[[c for c in requested_cols if c in df.columns]]

        logger.info(
            "fetch_complete: tickers=%s, fields=%s, rows=%d",
            tickers,
            fields,
            len(df),
        )
        return df

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a Bloomberg symbol."""
        logger.debug("get_metadata: symbol=%s", symbol)
        _get_blp()

        return {
            "source": "bloomberg",
            "symbol": symbol,
            "xbbg_available": _XBBG_AVAILABLE or False,
        }
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_bloomberg.py -v`

Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/bloomberg.py tests/unit/test_sources_bloomberg.py
git commit -m "feat(bloomberg): implement batch fetch"
```

---

## Task 5: Update MacrobondSource for batch fetch

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`
- Test: `tests/unit/test_sources_macrobond.py`

**Step 1: Write test for batch fetch from Macrobond**

Replace `tests/unit/test_sources_macrobond.py`:

```python
"""Tests for MacrobondSource."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import FetchRequest
from metapyle.sources.macrobond import MacrobondSource


@pytest.fixture
def source() -> MacrobondSource:
    """Create MacrobondSource instance."""
    return MacrobondSource()


def _make_mock_series(name: str, dates: list[str], values: list[float]) -> MagicMock:
    """Create mock Macrobond Series object."""
    mock = MagicMock()
    mock.is_error = False
    mock.primary_name = name
    mock.values_to_pd_data_frame.return_value = pd.DataFrame({
        "date": pd.to_datetime(dates),
        "value": values,
    })
    return mock


class TestMacrobondSourceFetch:
    """Tests for MacrobondSource.fetch()."""

    def test_single_request(self, source: MacrobondSource) -> None:
        """Fetch single series."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert list(df.columns) == ["usgdp"]
            assert len(df) == 2
            mock_mda.get_series.assert_called_once_with(["usgdp"])

    def test_multiple_requests(self, source: MacrobondSource) -> None:
        """Fetch multiple series in single call."""
        mock_series_1 = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02"],
            [100.0, 101.0],
        )
        mock_series_2 = _make_mock_series(
            "gbgdp",
            ["2024-01-01", "2024-01-02"],
            [200.0, 201.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series_1, mock_series_2]
            mock_get_mda.return_value = mock_mda

            requests = [
                FetchRequest(symbol="usgdp"),
                FetchRequest(symbol="gbgdp"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            assert "usgdp" in df.columns
            assert "gbgdp" in df.columns
            mock_mda.get_series.assert_called_once_with(["usgdp", "gbgdp"])

    def test_date_filtering(self, source: MacrobondSource) -> None:
        """Only return data within date range."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2024-01-01", "2024-01-02", "2024-01-03"],
            [100.0, 101.0, 102.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            df = source.fetch(requests, "2024-01-02", "2024-01-02")

            assert len(df) == 1
            assert df["usgdp"].iloc[0] == 101.0

    def test_mda_not_available(self, source: MacrobondSource) -> None:
        """Raise FetchError when macrobond_data_api not installed."""
        with patch("metapyle.sources.macrobond._get_mda", return_value=None):
            requests = [FetchRequest(symbol="usgdp")]
            with pytest.raises(FetchError, match="macrobond"):
                source.fetch(requests, "2024-01-01", "2024-01-02")

    def test_no_data_in_range(self, source: MacrobondSource) -> None:
        """Raise NoDataError when no data in date range."""
        mock_series = _make_mock_series(
            "usgdp",
            ["2020-01-01"],
            [100.0],
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="usgdp")]
            with pytest.raises(NoDataError):
                source.fetch(requests, "2024-01-01", "2024-12-31")

    def test_series_error(self, source: MacrobondSource) -> None:
        """Raise FetchError when series has error."""
        mock_series = MagicMock()
        mock_series.is_error = True
        mock_series.error_message = "Series not found"
        mock_series.primary_name = "invalid"

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_series.return_value = [mock_series]
            mock_get_mda.return_value = mock_mda

            requests = [FetchRequest(symbol="invalid")]
            with pytest.raises(FetchError, match="not found"):
                source.fetch(requests, "2024-01-01", "2024-01-02")
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_macrobond.py -v`

Expected: FAIL (signature mismatch)

**Step 3: Implement batch fetch for MacrobondSource**

Replace `src/metapyle/sources/macrobond.py`:

```python
"""Macrobond source adapter using macrobond_data_api library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["MacrobondSource"]

logger = logging.getLogger(__name__)

_MDA_AVAILABLE: bool | None = None
_mda_module: Any = None


def _get_mda() -> Any:
    """Lazy import of macrobond_data_api module."""
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


@register_source("macrobond")
class MacrobondSource(BaseSource):
    """Source adapter for Macrobond data via macrobond_data_api.

    Uses get_series for batch fetching of multiple series in a single call.
    """

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from Macrobond.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests. Field and path are ignored.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and columns named by symbol.

        Raises
        ------
        FetchError
            If macrobond_data_api not available or API call fails.
        NoDataError
            If no data returned or no data in date range.
        """
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

        logger.debug(
            "fetch_start: symbols=%s, start=%s, end=%s",
            symbols,
            start,
            end,
        )

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

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a Macrobond symbol."""
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

        logger.info("get_metadata_complete: symbol=%s", symbol)
        return metadata
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_macrobond.py -v`

Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/macrobond.py tests/unit/test_sources_macrobond.py
git commit -m "feat(macrobond): implement batch fetch"
```

---

## Task 6: Update Client for batch fetch

**Files:**
- Modify: `src/metapyle/client.py`
- Test: `tests/unit/test_client.py`

**Step 1: Write test for batch fetch in Client.get()**

Add to `tests/unit/test_client.py`:

```python
"""Tests for Client batch fetch behavior."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metapyle.client import Client
from metapyle.sources.base import FetchRequest


@pytest.fixture
def mock_catalog_yaml(tmp_path):
    """Create mock catalog YAML file."""
    catalog = tmp_path / "catalog.yaml"
    catalog.write_text("""
- my_name: US_GDP
  source: macrobond
  symbol: usgdp

- my_name: GB_GDP
  source: macrobond
  symbol: gbgdp

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
""")
    return catalog


class TestClientBatchFetch:
    """Tests for Client batch fetching."""

    def test_get_batches_by_source(self, mock_catalog_yaml, tmp_path) -> None:
        """Client groups requests by source and batches calls."""
        # Mock source returns
        macrobond_df = pd.DataFrame(
            {"usgdp": [100.0], "gbgdp": [200.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )
        bloomberg_df = pd.DataFrame(
            {"SPX Index::PX_LAST": [4500.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        with (
            patch("metapyle.sources.macrobond._get_mda") as mock_mda,
            patch("metapyle.sources.bloomberg._get_blp") as mock_blp,
        ):
            # Setup mocks
            mock_mda_instance = MagicMock()
            mock_mda.return_value = mock_mda_instance

            mock_blp_instance = MagicMock()
            mock_blp.return_value = mock_blp_instance

            with patch.object(
                Client, "_fetch_from_source"
            ) as mock_fetch:
                # Return appropriate df based on source
                def side_effect(source_name, requests, start, end):
                    if source_name == "macrobond":
                        return macrobond_df
                    return bloomberg_df

                mock_fetch.side_effect = side_effect

                client = Client(
                    catalog=str(mock_catalog_yaml),
                    cache_enabled=False,
                )

                df = client.get(
                    ["US_GDP", "GB_GDP", "SPX_CLOSE"],
                    start="2024-01-01",
                    end="2024-01-01",
                )

                # Should have made 2 calls (one per source)
                assert mock_fetch.call_count == 2

                # Result should have all 3 columns
                assert "US_GDP" in df.columns
                assert "GB_GDP" in df.columns
                assert "SPX_CLOSE" in df.columns

    def test_get_uses_cache_per_symbol(
        self, mock_catalog_yaml, tmp_path
    ) -> None:
        """Client checks cache per symbol, only fetches uncached."""
        cached_df = pd.DataFrame(
            {"usgdp": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        with patch("metapyle.cache.Cache") as MockCache:
            mock_cache = MagicMock()
            # First symbol cached, second not
            mock_cache.get.side_effect = [cached_df, None]
            MockCache.return_value = mock_cache

            with patch.object(
                Client, "_fetch_from_source"
            ) as mock_fetch:
                uncached_df = pd.DataFrame(
                    {"gbgdp": [200.0]},
                    index=pd.to_datetime(["2024-01-01"]),
                )
                mock_fetch.return_value = uncached_df

                client = Client(
                    catalog=str(mock_catalog_yaml),
                    cache_enabled=True,
                )
                client._cache = mock_cache

                df = client.get(
                    ["US_GDP", "GB_GDP"],
                    start="2024-01-01",
                    end="2024-01-01",
                )

                # Only one fetch call (for uncached symbol)
                assert mock_fetch.call_count == 1
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_client.py::TestClientBatchFetch -v`

Expected: FAIL

**Step 3: Update Client._fetch_from_source helper**

Add new method to `src/metapyle/client.py`:

```python
def _fetch_from_source(
    self,
    source_name: str,
    requests: list[FetchRequest],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Fetch data from a source for multiple requests.

    Parameters
    ----------
    source_name : str
        Name of the source adapter.
    requests : list[FetchRequest]
        Fetch requests for this source.
    start : str
        Start date.
    end : str
        End date.

    Returns
    -------
    pd.DataFrame
        DataFrame with one column per request.
    """
    source = self._registry.get(source_name)

    logger.debug(
        "fetch_from_source: source=%s, requests=%d, range=%s/%s",
        source_name,
        len(requests),
        start,
        end,
    )

    return source.fetch(requests, start, end)
```

**Step 4: Update Client.get() for batch fetching**

Replace the `get` method in `src/metapyle/client.py`:

```python
def get(
    self,
    symbols: list[str],
    start: str,
    end: str | None = None,
    *,
    frequency: str | None = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    Fetch time-series data for multiple symbols.

    Parameters
    ----------
    symbols : list[str]
        List of catalog names to fetch.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str | None, optional
        End date in ISO format (YYYY-MM-DD). Defaults to today.
    frequency : str | None, optional
        Pandas frequency string for alignment (e.g., "D", "ME", "QE").
    use_cache : bool, optional
        Whether to use cached data. Default is True.

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with DatetimeIndex and columns named by catalog names.
    """
    from itertools import groupby
    from metapyle.sources.base import FetchRequest, make_column_name

    # Default end to today if not specified
    if end is None:
        end = datetime.date.today().isoformat()

    # Resolve entries
    entries = [self._catalog.get(symbol) for symbol in symbols]

    if frequency is not None:
        logger.info(
            "frequency_alignment_requested: target=%s, symbols=%d",
            frequency,
            len(symbols),
        )

    # Check cache for each entry
    cached: dict[str, pd.DataFrame] = {}
    uncached: list[CatalogEntry] = []

    for entry in entries:
        if use_cache:
            df = self._cache.get(
                source=entry.source,
                symbol=entry.symbol,
                field=entry.field,
                path=entry.path,
                start_date=start,
                end_date=end,
            )
            if df is not None:
                logger.debug(
                    "fetch_from_cache: symbol=%s, rows=%d",
                    entry.my_name,
                    len(df),
                )
                cached[entry.my_name] = df
                continue
        uncached.append(entry)

    # Group uncached by source and batch fetch
    if uncached:
        # Sort by source for groupby
        uncached_sorted = sorted(uncached, key=lambda e: e.source)

        for source_name, group_iter in groupby(uncached_sorted, key=lambda e: e.source):
            group = list(group_iter)
            requests = [
                FetchRequest(symbol=e.symbol, field=e.field, path=e.path)
                for e in group
            ]

            df = self._fetch_from_source(source_name, requests, start, end)

            # Split result and cache each column
            for entry in group:
                col_name = make_column_name(entry.symbol, entry.field)
                if col_name in df.columns:
                    single_df = df[[col_name]]

                    # Cache the result
                    if use_cache:
                        self._cache.put(
                            source=entry.source,
                            symbol=entry.symbol,
                            field=entry.field,
                            path=entry.path,
                            start_date=start,
                            end_date=end,
                            data=single_df,
                        )

                    cached[entry.my_name] = single_df

    # Apply frequency alignment if requested
    if frequency is not None:
        from metapyle.processing import align_to_frequency

        for name in cached:
            logger.debug(
                "aligning_symbol: symbol=%s, target_frequency=%s",
                name,
                frequency,
            )
            cached[name] = align_to_frequency(cached[name], frequency)

    # Check index alignment if no frequency specified
    if frequency is None:
        self._check_index_alignment(cached)

    # Assemble into wide DataFrame
    return self._assemble_dataframe(cached)
```

**Step 5: Run tests to verify they pass**

Run: `pytest tests/unit/test_client.py::TestClientBatchFetch -v`

Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): implement batch fetch with per-source grouping"
```

---

## Task 7: Update Client.get_raw() for new interface

**Files:**
- Modify: `src/metapyle/client.py`
- Test: `tests/unit/test_client.py`

**Step 1: Write test for get_raw() with new interface**

Add to `tests/unit/test_client.py`:

```python
class TestClientGetRaw:
    """Tests for Client.get_raw()."""

    def test_get_raw_single_symbol(self, mock_catalog_yaml) -> None:
        """get_raw fetches single symbol with correct column naming."""
        result_df = pd.DataFrame(
            {"usgdp": [100.0, 101.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )

        with patch.object(Client, "_fetch_from_source") as mock_fetch:
            mock_fetch.return_value = result_df

            client = Client(
                catalog=str(mock_catalog_yaml),
                cache_enabled=False,
            )

            df = client.get_raw(
                source="macrobond",
                symbol="usgdp",
                start="2024-01-01",
                end="2024-01-02",
            )

            assert "usgdp" in df.columns
            assert len(df) == 2

    def test_get_raw_with_field(self, mock_catalog_yaml) -> None:
        """get_raw includes field in column name."""
        result_df = pd.DataFrame(
            {"SPX Index::PX_LAST": [4500.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        with patch.object(Client, "_fetch_from_source") as mock_fetch:
            mock_fetch.return_value = result_df

            client = Client(
                catalog=str(mock_catalog_yaml),
                cache_enabled=False,
            )

            df = client.get_raw(
                source="bloomberg",
                symbol="SPX Index",
                field="PX_LAST",
                start="2024-01-01",
                end="2024-01-01",
            )

            assert "SPX Index::PX_LAST" in df.columns

    def test_get_raw_uses_cache(self, mock_catalog_yaml) -> None:
        """get_raw checks cache before fetching."""
        cached_df = pd.DataFrame(
            {"usgdp": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        with patch("metapyle.cache.Cache") as MockCache:
            mock_cache = MagicMock()
            mock_cache.get.return_value = cached_df
            MockCache.return_value = mock_cache

            with patch.object(Client, "_fetch_from_source") as mock_fetch:
                client = Client(
                    catalog=str(mock_catalog_yaml),
                    cache_enabled=True,
                )
                client._cache = mock_cache

                df = client.get_raw(
                    source="macrobond",
                    symbol="usgdp",
                    start="2024-01-01",
                    end="2024-01-01",
                )

                # Should not call source fetch
                mock_fetch.assert_not_called()
                assert "usgdp" in df.columns
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_client.py::TestClientGetRaw -v`

Expected: FAIL

**Step 3: Update Client.get_raw()**

Replace the `get_raw` method in `src/metapyle/client.py`:

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
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    Fetch data directly from a source, bypassing the catalog.

    Parameters
    ----------
    source : str
        Name of registered source adapter.
    symbol : str
        Source-specific symbol identifier.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str | None, optional
        End date in ISO format (YYYY-MM-DD). Defaults to today.
    field : str | None, optional
        Source-specific field (e.g., "PX_LAST" for Bloomberg).
    path : str | None, optional
        Path to local file for localfile source.
    use_cache : bool, optional
        If False, bypass cache. Default True.

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and column named "symbol::field" or "symbol".

    Raises
    ------
    UnknownSourceError
        If source is not registered.
    FetchError
        If data retrieval fails.
    """
    from metapyle.sources.base import FetchRequest

    # Default end to today if not specified
    if end is None:
        end = datetime.date.today().isoformat()

    # Try cache first
    if use_cache:
        cached = self._cache.get(
            source=source,
            symbol=symbol,
            field=field,
            path=path,
            start_date=start,
            end_date=end,
        )
        if cached is not None:
            logger.debug("get_raw_from_cache: source=%s, symbol=%s", source, symbol)
            return cached

    # Fetch from source
    request = FetchRequest(symbol=symbol, field=field, path=path)
    logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
    df = self._fetch_from_source(source, [request], start, end)

    # Store in cache
    if use_cache:
        self._cache.put(
            source=source,
            symbol=symbol,
            field=field,
            path=path,
            start_date=start,
            end_date=end,
            data=df,
        )

    return df
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_client.py::TestClientGetRaw -v`

Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): update get_raw for batch fetch interface"
```

---

## Task 8: Remove old _fetch_symbol method and clean up

**Files:**
- Modify: `src/metapyle/client.py`

**Step 1: Remove _fetch_symbol method**

Delete the `_fetch_symbol` method from `src/metapyle/client.py` (no longer needed).

**Step 2: Update imports in client.py**

Ensure these imports are at the top of `src/metapyle/client.py`:

```python
from metapyle.sources.base import FetchRequest, make_column_name
```

Remove this from inside methods (move to top-level imports).

**Step 3: Run all tests**

Run: `pytest tests/unit/ -v`

Expected: All PASS

**Step 4: Commit**

```bash
git add src/metapyle/client.py
git commit -m "refactor(client): remove deprecated _fetch_symbol method"
```

---

## Task 9: Update public API exports

**Files:**
- Modify: `src/metapyle/__init__.py`
- Modify: `src/metapyle/sources/__init__.py`

**Step 1: Update sources/__init__.py exports**

Add to `src/metapyle/sources/__init__.py`:

```python
from metapyle.sources.base import FetchRequest, make_column_name

__all__ = [
    "BaseSource",
    "FetchRequest",
    "make_column_name",
    "register_source",
    "BloombergSource",
    "LocalFileSource",
    "MacrobondSource",
]
```

**Step 2: Update main __init__.py if needed**

Check if `FetchRequest` should be exposed at top level. If users need it for custom sources:

```python
from metapyle.sources.base import FetchRequest
```

**Step 3: Run public API tests**

Run: `pytest tests/unit/test_public_api.py -v`

Expected: All PASS

**Step 4: Commit**

```bash
git add src/metapyle/__init__.py src/metapyle/sources/__init__.py
git commit -m "feat: export FetchRequest and make_column_name in public API"
```

---

## Task 10: Run full test suite and lint

**Step 1: Run all tests**

Run: `pytest tests/ -v --tb=short`

Expected: All PASS

**Step 2: Run ruff check**

Run: `ruff check src/ tests/`

Expected: No errors

**Step 3: Run ruff format**

Run: `ruff format src/ tests/`

Expected: Files formatted (or already formatted)

**Step 4: Run mypy**

Run: `mypy src/`

Expected: No errors

**Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "chore: fix lint and type errors"
```

---

## Summary

After completing all tasks:

1. **FetchRequest** dataclass added to `sources/base.py`
2. **BaseSource.fetch()** now accepts `Sequence[FetchRequest]`
3. All three sources (localfile, bloomberg, macrobond) implement batch fetch
4. **Client.get()** groups requests by source, batches API calls, caches per-symbol
5. **Client.get_raw()** uses new interface with consistent column naming
6. Public API exports updated

Total commits: ~10 focused commits following TDD approach.

---

## Execution Handoff

Choose your preferred execution approach:

### Option A: Subagent-Driven Execution
Execute tasks in this session with parallel subagents where dependencies allow.

**Parallelization opportunities:**
- Tasks 1-2 (FetchRequest + make_column_name + BaseSource) are sequential
- Tasks 3, 4, 5 (LocalFile, Bloomberg, Macrobond sources) are independent - can run in parallel after Task 2
- Tasks 6, 7 (Client.get, Client.get_raw) are sequential after all sources complete
- Tasks 8, 9, 10 are sequential cleanup

### Option B: Parallel Session Execution
Open new VS Code windows/sessions and work on independent tasks simultaneously.

**Session split:**
- Session 1: Tasks 1-2 (base infrastructure)
- Session 2: Task 3 (LocalFileSource) - after Session 1 completes
- Session 3: Task 4 (BloombergSource) - after Session 1 completes
- Session 4: Task 5 (MacrobondSource) - after Session 1 completes
- Session 5: Tasks 6-10 (Client + cleanup) - after Sessions 2-4 complete

### Option C: Sequential Manual Execution
Work through tasks one by one, committing after each passes tests.

---

**Recommended:** Option A (Subagent-Driven) for this plan - tasks are well-defined with clear boundaries, and sources can be parallelized.

Reply with your choice: **A**, **B**, or **C**
