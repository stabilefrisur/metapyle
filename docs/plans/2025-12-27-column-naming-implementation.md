# Column Naming Redesign Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Make sources return meaningful column names instead of `value`, with `get()` renaming to `my_name` and `get_raw()` passing through source column names.

**Architecture:** Sources return original/meaningful column names. Client's `_assemble_dataframe()` renames to `my_name`. Cache key includes new `path` field.

**Tech Stack:** Python 3.12+, pandas, SQLite (cache)

---

## Task 1: Add `path` Field to CatalogEntry

**Files:**
- Modify: `src/metapyle/catalog.py`
- Modify: `tests/unit/test_catalog.py`

**Step 1.1: Write failing test for `path` field**

Add to `tests/unit/test_catalog.py`:

```python
def test_catalog_entry_path_field():
    """CatalogEntry supports optional path field."""
    entry = CatalogEntry(
        my_name="gdp_us",
        source="localfile",
        symbol="GDP_US",
        path="/data/macro.csv",
    )
    assert entry.path == "/data/macro.csv"


def test_catalog_entry_path_defaults_none():
    """CatalogEntry path defaults to None."""
    entry = CatalogEntry(
        my_name="test",
        source="bloomberg",
        symbol="SPX Index",
    )
    assert entry.path is None
```

**Step 1.2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_catalog.py::test_catalog_entry_path_field -v`
Expected: FAIL with "unexpected keyword argument 'path'"

**Step 1.3: Add `path` field to CatalogEntry**

In `src/metapyle/catalog.py`, modify the `CatalogEntry` dataclass:

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
        File path for file-based sources (e.g., localfile).
    description : str | None, optional
        Human-readable description of the data series.
    unit : str | None, optional
        Unit of measurement (e.g., "USD billions", "points").
    """

    my_name: str
    source: str
    symbol: str
    field: str | None = None
    path: str | None = None
    description: str | None = None
    unit: str | None = None
```

**Step 1.4: Update `_parse_entry` to handle `path`**

In `src/metapyle/catalog.py`, modify `_parse_entry`:

```python
@staticmethod
def _parse_entry(raw: dict[str, Any], source_file: str) -> CatalogEntry:
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
    )
```

**Step 1.5: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_catalog.py -v`
Expected: All PASS

**Step 1.6: Commit**

```bash
git add src/metapyle/catalog.py tests/unit/test_catalog.py
git commit -m "feat(catalog): add path field to CatalogEntry"
```

---

## Task 2: Update Cache Schema with `path` Field

**Files:**
- Modify: `src/metapyle/cache.py`
- Modify: `tests/unit/test_cache.py`

**Step 2.1: Write failing test for cache with `path`**

Add to `tests/unit/test_cache.py`:

```python
def test_cache_with_path(tmp_path):
    """Cache distinguishes entries by path."""
    cache_path = str(tmp_path / "test.db")
    cache = Cache(path=cache_path)

    df1 = pd.DataFrame(
        {"value": [1.0, 2.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    df2 = pd.DataFrame(
        {"value": [10.0, 20.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    # Store with different paths
    cache.put("localfile", "GDP_US", None, "/data/file1.csv", "2024-01-01", "2024-01-02", df1)
    cache.put("localfile", "GDP_US", None, "/data/file2.csv", "2024-01-01", "2024-01-02", df2)

    # Retrieve by path
    result1 = cache.get("localfile", "GDP_US", None, "/data/file1.csv", "2024-01-01", "2024-01-02")
    result2 = cache.get("localfile", "GDP_US", None, "/data/file2.csv", "2024-01-01", "2024-01-02")

    assert result1 is not None
    assert result2 is not None
    assert result1["value"].iloc[0] == 1.0
    assert result2["value"].iloc[0] == 10.0

    cache.close()
```

**Step 2.2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_cache.py::test_cache_with_path -v`
Expected: FAIL (signature mismatch)

**Step 2.3: Update Cache.put() signature and implementation**

In `src/metapyle/cache.py`, update `put` method:

```python
def put(
    self,
    source: str,
    symbol: str,
    field: str | None,
    path: str | None,
    start_date: str,
    end_date: str,
    data: pd.DataFrame,
) -> None:
    """
    Store DataFrame in cache.

    Parameters
    ----------
    source : str
        Data source name.
    symbol : str
        Source-specific symbol.
    field : str | None
        Field name (can be None for sources without fields).
    path : str | None
        File path (can be None for sources without paths).
    start_date : str
        Start date in ISO format (YYYY-MM-DD).
    end_date : str
        End date in ISO format (YYYY-MM-DD).
    data : pd.DataFrame
        DataFrame to cache.
    """
    if not self._enabled:
        return

    if self._conn is None:
        return

    try:
        # Serialize DataFrame to Parquet bytes
        data_bytes = data.to_parquet()

        # Delete existing entry if present (for overwrite)
        self._delete_entry(source, symbol, field, path, start_date, end_date)

        # Insert new entry
        cursor = self._conn.execute(
            """
            INSERT INTO cache_entries (source, symbol, field, path, start_date, end_date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source, symbol, field, path, start_date, end_date),
        )
        entry_id = cursor.lastrowid

        # Insert data
        self._conn.execute(
            """
            INSERT INTO cache_data (entry_id, data)
            VALUES (?, ?)
            """,
            (entry_id, data_bytes),
        )

        self._conn.commit()
        logger.debug(
            "cache_put: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
            source,
            symbol,
            field,
            path,
            start_date,
            end_date,
        )
    except Exception:
        logger.warning(
            "cache_put_failed: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
            source,
            symbol,
            field,
            path,
            start_date,
            end_date,
            exc_info=True,
        )
```

**Step 2.4: Update Cache.get() signature and implementation**

In `src/metapyle/cache.py`, update `get` method:

```python
def get(
    self,
    source: str,
    symbol: str,
    field: str | None,
    path: str | None,
    start_date: str,
    end_date: str,
) -> pd.DataFrame | None:
    """
    Retrieve DataFrame from cache.

    Returns data if requested range is a subset of a cached range.

    Parameters
    ----------
    source : str
        Data source name.
    symbol : str
        Source-specific symbol.
    field : str | None
        Field name (can be None for sources without fields).
    path : str | None
        File path (can be None for sources without paths).
    start_date : str
        Start date in ISO format (YYYY-MM-DD).
    end_date : str
        End date in ISO format (YYYY-MM-DD).

    Returns
    -------
    pd.DataFrame | None
        Cached DataFrame if found, None otherwise.
    """
    if not self._enabled:
        return None

    if self._conn is None:
        return None

    try:
        # Find a cached entry that covers the requested range
        # field and path can be None, so we need special handling
        if field is None and path is None:
            cursor = self._conn.execute(
                """
                SELECT ce.id, ce.start_date, ce.end_date, cd.data
                FROM cache_entries ce
                JOIN cache_data cd ON cd.entry_id = ce.id
                WHERE ce.source = ?
                  AND ce.symbol = ?
                  AND ce.field IS NULL
                  AND ce.path IS NULL
                  AND ce.start_date <= ?
                  AND ce.end_date >= ?
                """,
                (source, symbol, start_date, end_date),
            )
        elif field is None:
            cursor = self._conn.execute(
                """
                SELECT ce.id, ce.start_date, ce.end_date, cd.data
                FROM cache_entries ce
                JOIN cache_data cd ON cd.entry_id = ce.id
                WHERE ce.source = ?
                  AND ce.symbol = ?
                  AND ce.field IS NULL
                  AND ce.path = ?
                  AND ce.start_date <= ?
                  AND ce.end_date >= ?
                """,
                (source, symbol, path, start_date, end_date),
            )
        elif path is None:
            cursor = self._conn.execute(
                """
                SELECT ce.id, ce.start_date, ce.end_date, cd.data
                FROM cache_entries ce
                JOIN cache_data cd ON cd.entry_id = ce.id
                WHERE ce.source = ?
                  AND ce.symbol = ?
                  AND ce.field = ?
                  AND ce.path IS NULL
                  AND ce.start_date <= ?
                  AND ce.end_date >= ?
                """,
                (source, symbol, field, start_date, end_date),
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT ce.id, ce.start_date, ce.end_date, cd.data
                FROM cache_entries ce
                JOIN cache_data cd ON cd.entry_id = ce.id
                WHERE ce.source = ?
                  AND ce.symbol = ?
                  AND ce.field = ?
                  AND ce.path = ?
                  AND ce.start_date <= ?
                  AND ce.end_date >= ?
                """,
                (source, symbol, field, path, start_date, end_date),
            )

        row = cursor.fetchone()
        if row is None:
            logger.debug(
                "cache_miss: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
                source,
                symbol,
                field,
                path,
                start_date,
                end_date,
            )
            return None

        _, cached_start, cached_end, data_bytes = row

        # Deserialize DataFrame
        df = pd.read_parquet(io.BytesIO(data_bytes))

        # If requested range is subset, filter the data
        if start_date != cached_start or end_date != cached_end:
            start_dt = pd.Timestamp(start_date)
            end_dt = pd.Timestamp(end_date)
            df = df[(df.index >= start_dt) & (df.index <= end_dt)]

        logger.debug(
            "cache_hit: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
            source,
            symbol,
            field,
            path,
            start_date,
            end_date,
        )
        return df
    except Exception:
        logger.warning(
            "cache_get_failed: source=%s, symbol=%s, field=%s, path=%s, range=%s/%s",
            source,
            symbol,
            field,
            path,
            start_date,
            end_date,
            exc_info=True,
        )
        return None
```

**Step 2.5: Update `_initialize_database` schema**

In `src/metapyle/cache.py`, update `_initialize_database`:

```python
def _initialize_database(self) -> None:
    """Create database and tables if they don't exist."""
    # Ensure parent directory exists
    db_path = Path(self._path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    self._conn = sqlite3.connect(self._path)

    # Check if we need to migrate (old schema without path column)
    cursor = self._conn.execute("PRAGMA table_info(cache_entries)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if columns and "path" not in columns:
        # Old schema - drop tables and recreate
        logger.info("cache_schema_migration: dropping old tables")
        self._conn.execute("DROP TABLE IF EXISTS cache_data")
        self._conn.execute("DROP TABLE IF EXISTS cache_entries")

    # Create cache_entries table for metadata
    self._conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            field TEXT,
            path TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, symbol, field, path, start_date, end_date)
        )
    """)

    # Create cache_data table for storing DataFrame as blob
    self._conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_data (
            entry_id INTEGER PRIMARY KEY,
            data BLOB NOT NULL,
            FOREIGN KEY (entry_id) REFERENCES cache_entries(id)
                ON DELETE CASCADE
        )
    """)

    # Create index for faster lookups
    self._conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cache_lookup
        ON cache_entries(source, symbol, field, path)
    """)

    self._conn.commit()
    logger.info("cache_initialized: path=%s", self._path)
```

**Step 2.6: Update `_delete_entry` method**

In `src/metapyle/cache.py`, update `_delete_entry`:

```python
def _delete_entry(
    self,
    source: str,
    symbol: str,
    field: str | None,
    path: str | None,
    start_date: str,
    end_date: str,
) -> None:
    """Delete a specific cache entry if it exists."""
    if self._conn is None:
        return

    # Build query based on NULL fields
    if field is None and path is None:
        cursor = self._conn.execute(
            """
            SELECT id FROM cache_entries
            WHERE source = ? AND symbol = ? AND field IS NULL AND path IS NULL
              AND start_date = ? AND end_date = ?
            """,
            (source, symbol, start_date, end_date),
        )
    elif field is None:
        cursor = self._conn.execute(
            """
            SELECT id FROM cache_entries
            WHERE source = ? AND symbol = ? AND field IS NULL AND path = ?
              AND start_date = ? AND end_date = ?
            """,
            (source, symbol, path, start_date, end_date),
        )
    elif path is None:
        cursor = self._conn.execute(
            """
            SELECT id FROM cache_entries
            WHERE source = ? AND symbol = ? AND field = ? AND path IS NULL
              AND start_date = ? AND end_date = ?
            """,
            (source, symbol, field, start_date, end_date),
        )
    else:
        cursor = self._conn.execute(
            """
            SELECT id FROM cache_entries
            WHERE source = ? AND symbol = ? AND field = ? AND path = ?
              AND start_date = ? AND end_date = ?
            """,
            (source, symbol, field, path, start_date, end_date),
        )

    row = cursor.fetchone()
    if row is not None:
        entry_id = row[0]
        self._conn.execute("DELETE FROM cache_data WHERE entry_id = ?", (entry_id,))
        self._conn.execute("DELETE FROM cache_entries WHERE id = ?", (entry_id,))
```

**Step 2.7: Update existing cache tests**

Update all existing cache tests to include the new `path` parameter (as `None` for backward compatibility). This affects:
- `test_cache_put_and_get`
- `test_cache_get_returns_none_for_miss`
- `test_cache_get_subset_of_cached_range`
- `test_cache_miss_when_range_exceeds_cached`
- `test_cache_null_field`
- `test_cache_put_overwrites_existing`
- `test_cache_clear_specific_symbol`

Example fix for `test_cache_put_and_get`:

```python
def test_cache_put_and_get(self, cache):
    """Cache stores and retrieves data."""
    df = pd.DataFrame(
        {"value": [1.0, 2.0, 3.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    )

    cache.put("source", "SYM", "price", None, "2024-01-01", "2024-01-03", df)
    result = cache.get("source", "SYM", "price", None, "2024-01-01", "2024-01-03")

    assert result is not None
    pd.testing.assert_frame_equal(result, df)
```

**Step 2.8: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_cache.py -v`
Expected: All PASS

**Step 2.9: Commit**

```bash
git add src/metapyle/cache.py tests/unit/test_cache.py
git commit -m "feat(cache): add path to cache key"
```

---

## Task 3: Redesign LocalFileSource

**Files:**
- Modify: `src/metapyle/sources/localfile.py`
- Modify: `tests/unit/test_sources_localfile.py`

**Step 3.1: Write failing tests for new LocalFileSource behavior**

Replace/update tests in `tests/unit/test_sources_localfile.py`:

```python
"""Tests for LocalFileSource."""

import pandas as pd
import pytest

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.localfile import LocalFileSource


class TestLocalFileSourceFetch:
    """Tests for LocalFileSource.fetch method."""

    def test_localfile_fetch_extracts_column_by_symbol(self, tmp_path):
        """Fetch extracts specific column from CSV using symbol as column name."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,GDP_US,CPI_US\n"
            "2024-01-01,100.0,200.0\n"
            "2024-01-02,101.0,201.0\n"
            "2024-01-03,102.0,202.0\n"
        )

        source = LocalFileSource()
        df = source.fetch("GDP_US", "2024-01-01", "2024-01-03", path=str(csv_path))

        assert list(df.columns) == ["GDP_US"]
        assert len(df) == 3
        assert df["GDP_US"].iloc[0] == 100.0

    def test_localfile_fetch_returns_original_column_name(self, tmp_path):
        """Fetch returns DataFrame with original column name, not 'value'."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,MyColumn\n"
            "2024-01-01,1.0\n"
            "2024-01-02,2.0\n"
        )

        source = LocalFileSource()
        df = source.fetch("MyColumn", "2024-01-01", "2024-01-02", path=str(csv_path))

        assert "MyColumn" in df.columns
        assert "value" not in df.columns

    def test_localfile_fetch_requires_path(self):
        """Fetch raises FetchError if path not provided."""
        source = LocalFileSource()

        with pytest.raises(FetchError, match="path is required"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-03")

    def test_localfile_fetch_column_not_found(self, tmp_path):
        """Fetch raises FetchError if column not found in file."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,OTHER_COL\n"
            "2024-01-01,1.0\n"
        )

        source = LocalFileSource()

        with pytest.raises(FetchError, match="Column 'GDP_US' not found"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-03", path=str(csv_path))

    def test_localfile_fetch_file_not_found(self):
        """Fetch raises FetchError if file not found."""
        source = LocalFileSource()

        with pytest.raises(FetchError, match="File not found"):
            source.fetch("GDP_US", "2024-01-01", "2024-01-03", path="/nonexistent.csv")

    def test_localfile_fetch_filters_date_range(self, tmp_path):
        """Fetch filters data to requested date range."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,GDP_US\n"
            "2024-01-01,100.0\n"
            "2024-01-02,101.0\n"
            "2024-01-03,102.0\n"
            "2024-01-04,103.0\n"
            "2024-01-05,104.0\n"
        )

        source = LocalFileSource()
        df = source.fetch("GDP_US", "2024-01-02", "2024-01-04", path=str(csv_path))

        assert len(df) == 3
        assert df["GDP_US"].iloc[0] == 101.0
        assert df["GDP_US"].iloc[-1] == 103.0

    def test_localfile_fetch_no_data_in_range(self, tmp_path):
        """Fetch raises NoDataError if no data in date range."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "date,GDP_US\n"
            "2024-01-01,100.0\n"
        )

        source = LocalFileSource()

        with pytest.raises(NoDataError, match="No data in date range"):
            source.fetch("GDP_US", "2025-01-01", "2025-01-31", path=str(csv_path))


class TestLocalFileSourceParquet:
    """Tests for LocalFileSource with Parquet files."""

    def test_localfile_fetch_parquet(self, tmp_path):
        """Fetch works with Parquet files."""
        pytest.importorskip("pyarrow")
        
        parquet_path = tmp_path / "data.parquet"
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "GDP_US": [100.0, 101.0],
        })
        df.to_parquet(parquet_path, index=False)

        source = LocalFileSource()
        result = source.fetch("GDP_US", "2024-01-01", "2024-01-02", path=str(parquet_path))

        assert list(result.columns) == ["GDP_US"]
        assert len(result) == 2
```

**Step 3.2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_sources_localfile.py -v`
Expected: Multiple FAILs (old implementation)

**Step 3.3: Rewrite LocalFileSource.fetch**

Replace the `fetch` method in `src/metapyle/sources/localfile.py`:

```python
def fetch(
    self,
    symbol: str,
    start: str,
    end: str,
    *,
    path: str | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Fetch time-series data from a local file.

    Parameters
    ----------
    symbol : str
        Column name to extract from the file.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).
    path : str | None
        Path to the data file (CSV or Parquet). Required.
    **kwargs : Any
        Additional parameters (currently unused).

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and single column named by symbol.

    Raises
    ------
    FetchError
        If path not provided, file not found, column not found, or read fails.
    NoDataError
        If file is empty or no data in date range.
    """
    if path is None:
        logger.error("fetch_failed: symbol=%s, reason=path_required", symbol)
        raise FetchError("path is required for localfile source")

    file_path = Path(path)
    logger.debug("fetch_start: path=%s, symbol=%s, start=%s, end=%s", path, symbol, start, end)

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

    # Check if symbol (column name) exists in file
    if symbol not in df.columns:
        available = ", ".join(df.columns[:5])
        if len(df.columns) > 5:
            available += "..."
        logger.error("fetch_failed: path=%s, symbol=%s, reason=column_not_found", path, symbol)
        raise FetchError(f"Column '{symbol}' not found in {path}. Available: {available}")

    # Extract the requested column
    df = df[[symbol]]

    # Ensure datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception as e:
            logger.error("fetch_failed: path=%s, reason=invalid_datetime_index", path)
            raise FetchError(f"Cannot convert index to datetime: {path}") from e

    # Filter by date range using boolean indexing for type safety
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)
    mask = (df.index >= start_dt) & (df.index <= end_dt)
    df_filtered = df.loc[mask]

    if df_filtered.empty:
        logger.warning("fetch_no_data_in_range: path=%s, start=%s, end=%s", path, start, end)
        raise NoDataError(f"No data in date range {start} to {end}: {path}")

    logger.info("fetch_complete: path=%s, symbol=%s, rows=%d", path, symbol, len(df_filtered))
    return df_filtered
```

**Step 3.4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_sources_localfile.py -v`
Expected: All PASS

**Step 3.5: Commit**

```bash
git add src/metapyle/sources/localfile.py tests/unit/test_sources_localfile.py
git commit -m "feat(localfile): symbol is column name, path is file path"
```

---

## Task 4: Update BloombergSource Column Naming

**Files:**
- Modify: `src/metapyle/sources/bloomberg.py`
- Modify: `tests/unit/test_sources_bloomberg.py`

**Step 4.1: Write failing test for Bloomberg column naming**

Add to `tests/unit/test_sources_bloomberg.py`:

```python
def test_bloomberg_returns_symbol_field_column_name(mock_blp):
    """Bloomberg source returns column named symbol_field."""
    mock_blp.bdh.return_value = pd.DataFrame(
        {("SPX Index", "PX_LAST"): [100.0, 101.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    source = BloombergSource()
    df = source.fetch("SPX Index", "2024-01-01", "2024-01-02", field="PX_LAST")

    assert "SPX Index_PX_LAST" in df.columns
    assert "value" not in df.columns
```

**Step 4.2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_sources_bloomberg.py::test_bloomberg_returns_symbol_field_column_name -v`
Expected: FAIL (returns 'value' column)

**Step 4.3: Update BloombergSource.fetch column naming**

In `src/metapyle/sources/bloomberg.py`, update the column renaming section:

```python
def fetch(
    self,
    symbol: str,
    start: str,
    end: str,
    *,
    field: str = "PX_LAST",
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Fetch historical data from Bloomberg.

    Parameters
    ----------
    symbol : str
        Bloomberg ticker (e.g., "SPX Index", "AAPL US Equity").
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).
    field : str, optional
        Bloomberg field to fetch, by default "PX_LAST".
    **kwargs : Any
        Additional parameters passed to blp.bdh.

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and single column named 'symbol_field'.

    Raises
    ------
    FetchError
        If xbbg is not available or API call fails.
    NoDataError
        If no data is returned for the symbol.
    """
    blp = _get_blp()
    if blp is None:
        logger.error("fetch_failed: symbol=%s, reason=xbbg_not_installed", symbol)
        raise FetchError("xbbg package is not installed. Install with: pip install xbbg")

    logger.debug(
        "fetch_start: symbol=%s, start=%s, end=%s, field=%s",
        symbol,
        start,
        end,
        field,
    )

    try:
        df = blp.bdh(symbol, field, start, end, **kwargs)
    except Exception as e:
        logger.error("fetch_failed: symbol=%s, error=%s", symbol, str(e))
        raise FetchError(f"Bloomberg API error for {symbol}: {e}") from e

    if df.empty:
        logger.warning("fetch_empty: symbol=%s, field=%s", symbol, field)
        raise NoDataError(f"No data returned for {symbol} with field {field}")

    # Convert MultiIndex columns from bdh response to symbol_field column name
    # bdh returns DataFrame with MultiIndex columns: (ticker, field)
    col_name = f"{symbol}_{field}"
    if isinstance(df.columns, pd.MultiIndex):
        # Extract the first column's data
        df = df.iloc[:, 0].to_frame(name=col_name)
    elif len(df.columns) == 1:
        df = df.rename(columns={df.columns[0]: col_name})

    # Ensure DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    logger.info("fetch_complete: symbol=%s, rows=%d", symbol, len(df))
    return df
```

**Step 4.4: Update existing Bloomberg tests**

Update tests to expect `symbol_field` column name instead of `value`:

```python
def test_bloomberg_fetch_success(mock_blp):
    """Bloomberg source fetches data successfully."""
    mock_blp.bdh.return_value = pd.DataFrame(
        {("SPX Index", "PX_LAST"): [100.0, 101.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    source = BloombergSource()
    df = source.fetch("SPX Index", "2024-01-01", "2024-01-02")

    assert len(df) == 2
    assert "SPX Index_PX_LAST" in df.columns
    mock_blp.bdh.assert_called_once()
```

**Step 4.5: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_sources_bloomberg.py -v`
Expected: All PASS

**Step 4.6: Commit**

```bash
git add src/metapyle/sources/bloomberg.py tests/unit/test_sources_bloomberg.py
git commit -m "feat(bloomberg): return symbol_field column name"
```

---

## Task 5: Update Client to Pass `path` and Handle Column Renaming

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `tests/unit/test_client.py`

**Step 5.1: Write failing test for get() renaming to my_name**

Add to `tests/unit/test_client.py`:

```python
def test_client_get_renames_to_my_name(tmp_path, mocker):
    """Client.get() renames source column to my_name."""
    # Create test CSV
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "date,GDP_US\n"
        "2024-01-01,100.0\n"
        "2024-01-02,101.0\n"
    )

    # Create catalog
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(f"""
- my_name: gdp_us
  source: localfile
  symbol: GDP_US
  path: {csv_path}
""")

    client = Client(catalog=str(catalog_path), cache_enabled=False)
    df = client.get(["gdp_us"], start="2024-01-01", end="2024-01-02")

    assert "gdp_us" in df.columns
    assert "GDP_US" not in df.columns
    assert "value" not in df.columns
    client.close()
```

**Step 5.2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_client.py::test_client_get_renames_to_my_name -v`
Expected: FAIL

**Step 5.3: Update `_fetch_symbol` to pass `path`**

In `src/metapyle/client.py`, update `_fetch_symbol`:

```python
def _fetch_symbol(
    self,
    entry: CatalogEntry,
    start: str,
    end: str,
    use_cache: bool,
) -> pd.DataFrame:
    """
    Fetch data for a single symbol, using cache if available.

    Parameters
    ----------
    entry : CatalogEntry
        Catalog entry for the symbol.
    start : str
        Start date in ISO format.
    end : str
        End date in ISO format.
    use_cache : bool
        Whether to use cached data.

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and source-specific column name.
    """
    # Try cache first
    if use_cache:
        cached = self._cache.get(
            source=entry.source,
            symbol=entry.symbol,
            field=entry.field,
            path=entry.path,
            start_date=start,
            end_date=end,
        )
        if cached is not None:
            logger.debug(
                "fetch_from_cache: symbol=%s, rows=%d",
                entry.my_name,
                len(cached),
            )
            return cached

    # Fetch from source
    source = self._registry.get(entry.source)

    # Build kwargs for source
    kwargs: dict[str, str] = {}
    if entry.field is not None:
        kwargs["field"] = entry.field
    if entry.path is not None:
        kwargs["path"] = entry.path

    logger.debug(
        "fetch_from_source: symbol=%s, source=%s, range=%s/%s",
        entry.my_name,
        entry.source,
        start,
        end,
    )

    df = source.fetch(entry.symbol, start, end, **kwargs)

    # Store in cache
    if use_cache:
        self._cache.put(
            source=entry.source,
            symbol=entry.symbol,
            field=entry.field,
            path=entry.path,
            start_date=start,
            end_date=end,
            data=df,
        )

    return df
```

**Step 5.4: Update `_assemble_dataframe` to handle any column name**

In `src/metapyle/client.py`, update `_assemble_dataframe`:

```python
def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Assemble individual DataFrames into a wide DataFrame.

    Renames source columns to my_name from catalog.

    Parameters
    ----------
    dfs : dict[str, pd.DataFrame]
        Dictionary mapping my_name to DataFrames.

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with columns named by my_name.
    """
    if not dfs:
        return pd.DataFrame()

    # Rename first column to my_name and concatenate
    renamed: list[pd.DataFrame] = []
    for my_name, df in dfs.items():
        # Take first column regardless of name, rename to my_name
        col = df.columns[0]
        renamed.append(df[[col]].rename(columns={col: my_name}))

    result = pd.concat(renamed, axis=1)
    return result
```

**Step 5.5: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_client.py::test_client_get_renames_to_my_name -v`
Expected: PASS

**Step 5.6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): pass path to source, rename to my_name"
```

---

## Task 6: Update `get_raw()` with `path` Parameter

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `tests/unit/test_client.py`

**Step 6.1: Write failing test for get_raw() with path**

Add to `tests/unit/test_client.py`:

```python
def test_client_get_raw_with_path(tmp_path, mocker):
    """Client.get_raw() accepts path parameter for localfile."""
    # Create test CSV
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "date,GDP_US\n"
        "2024-01-01,100.0\n"
        "2024-01-02,101.0\n"
    )

    # Create minimal catalog (required for Client init)
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text("""
- my_name: dummy
  source: localfile
  symbol: dummy
  path: /dummy
""")

    client = Client(catalog=str(catalog_path), cache_enabled=False)
    df = client.get_raw(
        source="localfile",
        symbol="GDP_US",
        start="2024-01-01",
        end="2024-01-02",
        path=str(csv_path),
    )

    # get_raw returns original column name
    assert "GDP_US" in df.columns
    assert "value" not in df.columns
    client.close()


def test_client_get_raw_bloomberg_returns_symbol_field(tmp_path, mocker):
    """Client.get_raw() for Bloomberg returns symbol_field column name."""
    # Mock Bloomberg
    mock_blp = mocker.MagicMock()
    mock_blp.bdh.return_value = pd.DataFrame(
        {("SPX Index", "PX_LAST"): [100.0, 101.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    mocker.patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp)

    # Create minimal catalog
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text("""
- my_name: dummy
  source: bloomberg
  symbol: dummy
""")

    client = Client(catalog=str(catalog_path), cache_enabled=False)
    df = client.get_raw(
        source="bloomberg",
        symbol="SPX Index",
        start="2024-01-01",
        end="2024-01-02",
        field="PX_LAST",
    )

    assert "SPX Index_PX_LAST" in df.columns
    assert "value" not in df.columns
    client.close()
```

**Step 6.2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_client.py::test_client_get_raw_with_path -v`
Expected: FAIL (missing path parameter)

**Step 6.3: Update get_raw() signature**

In `src/metapyle/client.py`, update `get_raw`:

```python
def get_raw(
    self,
    source: str,
    symbol: str,
    start: str,
    end: str,
    *,
    field: str | None = None,
    path: str | None = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    Fetch data directly from a source, bypassing the catalog.

    Useful for ad-hoc queries or testing new data series.

    Parameters
    ----------
    source : str
        Name of registered source adapter.
    symbol : str
        Source-specific symbol identifier.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str
        End date in ISO format (YYYY-MM-DD).
    field : str | None, optional
        Source-specific field (e.g., "PX_LAST" for Bloomberg).
    path : str | None, optional
        File path for file-based sources (e.g., localfile).
    use_cache : bool, optional
        If False, bypass cache. Default True.

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and source-specific column name.

    Raises
    ------
    UnknownSourceError
        If source is not registered.
    FetchError
        If data retrieval fails.
    """
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
    source_adapter = self._registry.get(source)
    kwargs: dict[str, str] = {}
    if field is not None:
        kwargs["field"] = field
    if path is not None:
        kwargs["path"] = path

    logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
    df = source_adapter.fetch(symbol, start, end, **kwargs)

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

**Step 6.4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_client.py::test_client_get_raw_with_path tests/unit/test_client.py::test_client_get_raw_bloomberg_returns_symbol_field -v`
Expected: PASS

**Step 6.5: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): add path parameter to get_raw()"
```

---

## Task 7: Update Existing Tests for New Signatures

**Files:**
- Modify: `tests/unit/test_client.py`

**Step 7.1: Update cache-related client tests**

Update all client tests that use cache to pass `path=None`:

```python
# In test_client_uses_cache, test_client_get_raw_uses_cache, etc.
# The cache.get and cache.put calls now have path parameter
# Most tests should work with path=None for non-localfile sources
```

**Step 7.2: Run full test suite**

Run: `python -m pytest tests/unit/ -v`
Expected: All PASS (excluding pyarrow-dependent tests)

**Step 7.3: Commit**

```bash
git add tests/unit/
git commit -m "test: update tests for new cache and source signatures"
```

---

## Task 8: Final Integration Test

**Step 8.1: Run full test suite**

Run: `python -m pytest tests/ -v`

**Step 8.2: Run type checker**

Run: `python -m mypy src/metapyle/`

**Step 8.3: Run linter**

Run: `ruff check src/ tests/`

**Step 8.4: Fix any issues and commit**

```bash
git add -A
git commit -m "chore: fix linting and type issues"
```

---

## Summary of Changes

| File | Changes |
|------|---------|
| `src/metapyle/catalog.py` | Add `path` field to `CatalogEntry` |
| `src/metapyle/cache.py` | Add `path` to cache key, update schema |
| `src/metapyle/sources/localfile.py` | `symbol` = column name, `path` = file path |
| `src/metapyle/sources/bloomberg.py` | Return `symbol_field` column name |
| `src/metapyle/client.py` | Pass `path`, rename to `my_name`, add `path` to `get_raw()` |
| `tests/unit/test_*.py` | Update all tests for new signatures |
