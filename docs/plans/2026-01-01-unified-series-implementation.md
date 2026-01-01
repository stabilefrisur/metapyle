# Macrobond Unified Series Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Add support for Macrobond's `get_unified_series()` function which converts multiple series to a common frequency and calendar with server-side alignment.

**Architecture:** Extend `BaseSource.fetch()` signature to accept `**kwargs`, which pass through from `Client.get()` to source adapters. MacrobondSource uses `unified` kwarg to switch between `get_series()` and `get_unified_series()` APIs. Hardcoded defaults for unified series; power users override via macrobond enum kwargs.

**Tech Stack:** Python 3.12+, pandas, macrobond_data_api, pytest

---

## Task 1: Add `**kwargs` to BaseSource.fetch() Signature

**Files:**
- Modify: [src/metapyle/sources/base.py](src/metapyle/sources/base.py#L59-L83)
- Test: [tests/unit/test_sources_base.py](tests/unit/test_sources_base.py)

**Step 1: Write the failing test**

Add test to `tests/unit/test_sources_base.py`:

```python
class TestBaseSourceKwargs:
    """Tests for **kwargs support in BaseSource."""

    def test_fetch_accepts_kwargs(self) -> None:
        """BaseSource.fetch() signature accepts **kwargs."""
        from collections.abc import Sequence
        from typing import Any
        
        from metapyle.sources.base import BaseSource, FetchRequest
        
        class TestSource(BaseSource):
            def fetch(
                self,
                requests: Sequence[FetchRequest],
                start: str,
                end: str,
                **kwargs: Any,
            ) -> pd.DataFrame:
                # Store kwargs for verification
                self.received_kwargs = kwargs
                return pd.DataFrame()
            
            def get_metadata(self, symbol: str) -> dict[str, Any]:
                return {}
        
        source = TestSource()
        requests = [FetchRequest(symbol="TEST")]
        source.fetch(requests, "2024-01-01", "2024-01-02", unified=True, currency="EUR")
        
        assert source.received_kwargs == {"unified": True, "currency": "EUR"}
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_base.py::TestBaseSourceKwargs::test_fetch_accepts_kwargs -v`
Expected: PASS (signature change is compatible — test validates pattern)

**Step 3: Update BaseSource.fetch() abstract method signature**

In `src/metapyle/sources/base.py`, update the `fetch` abstract method:

```python
@abstractmethod
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
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
    **kwargs : Any
        Source-specific keyword arguments. Passed through from Client.get().
        Most sources ignore these; MacrobondSource uses them for unified series.

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

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_base.py::TestBaseSourceKwargs -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/base.py tests/unit/test_sources_base.py
git commit -m "feat(sources): add **kwargs to BaseSource.fetch() signature"
```

---

## Task 2: Update BloombergSource to Accept **kwargs

**Files:**
- Modify: [src/metapyle/sources/bloomberg.py](src/metapyle/sources/bloomberg.py#L52-L100)
- Test: [tests/unit/test_sources_bloomberg.py](tests/unit/test_sources_bloomberg.py)

**Step 1: Write the failing test**

Add test to `tests/unit/test_sources_bloomberg.py`:

```python
class TestBloombergSourceKwargs:
    """Tests for **kwargs handling in BloombergSource."""

    def test_fetch_ignores_kwargs(self, source: BloombergSource, mocker: Any) -> None:
        """BloombergSource.fetch() accepts and ignores **kwargs."""
        mock_blp = mocker.MagicMock()
        mock_blp.bdh.return_value = pd.DataFrame(
            {("SPX Index", "PX_LAST"): [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )
        mocker.patch("metapyle.sources.bloomberg._get_blp", return_value=mock_blp)

        requests = [FetchRequest(symbol="SPX Index", field="PX_LAST")]
        # Pass kwargs that should be ignored
        df = source.fetch(requests, "2024-01-01", "2024-01-01", unified=True, currency="EUR")

        assert not df.empty
        mock_blp.bdh.assert_called_once()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_bloomberg.py::TestBloombergSourceKwargs::test_fetch_ignores_kwargs -v`
Expected: FAIL with TypeError about unexpected keyword arguments

**Step 3: Update BloombergSource.fetch() signature**

In `src/metapyle/sources/bloomberg.py`, add `**kwargs: Any` to fetch():

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
) -> pd.DataFrame:
```

The method body stays the same — kwargs are accepted but ignored.

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_bloomberg.py::TestBloombergSourceKwargs -v`
Expected: PASS

**Step 5: Run all Bloomberg tests**

Run: `pytest tests/unit/test_sources_bloomberg.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/sources/bloomberg.py tests/unit/test_sources_bloomberg.py
git commit -m "feat(bloomberg): accept **kwargs in fetch() (ignored)"
```

---

## Task 3: Update GSQuantSource to Accept **kwargs

**Files:**
- Modify: [src/metapyle/sources/gsquant.py](src/metapyle/sources/gsquant.py)
- Test: [tests/unit/test_sources_gsquant.py](tests/unit/test_sources_gsquant.py)

**Step 1: Write the failing test**

Add test to `tests/unit/test_sources_gsquant.py`:

```python
class TestGSQuantSourceKwargs:
    """Tests for **kwargs handling in GSQuantSource."""

    def test_fetch_ignores_kwargs(self, source: GSQuantSource, mocker: Any) -> None:
        """GSQuantSource.fetch() accepts and ignores **kwargs."""
        # Mock the Dataset class
        mock_dataset_class = mocker.MagicMock()
        mock_dataset_instance = mocker.MagicMock()
        mock_dataset_class.return_value = mock_dataset_instance
        mock_dataset_instance.get_data.return_value = pd.DataFrame(
            {"value": [100.0]},
            index=pd.to_datetime(["2024-01-01"]),
        )
        mocker.patch("metapyle.sources.gsquant._get_dataset_class", return_value=mock_dataset_class)

        requests = [FetchRequest(symbol="DATASET_ID", field="column")]
        # Pass kwargs that should be ignored
        df = source.fetch(requests, "2024-01-01", "2024-01-01", unified=True, currency="EUR")

        assert not df.empty
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantSourceKwargs::test_fetch_ignores_kwargs -v`
Expected: FAIL with TypeError about unexpected keyword arguments

**Step 3: Update GSQuantSource.fetch() signature**

In `src/metapyle/sources/gsquant.py`, add `**kwargs: Any` to fetch():

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
) -> pd.DataFrame:
```

The method body stays the same — kwargs are accepted but ignored.

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantSourceKwargs -v`
Expected: PASS

**Step 5: Run all GSQuant tests**

Run: `pytest tests/unit/test_sources_gsquant.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/sources/gsquant.py tests/unit/test_sources_gsquant.py
git commit -m "feat(gsquant): accept **kwargs in fetch() (ignored)"
```

---

## Task 4: Update LocalFileSource to Accept **kwargs

**Files:**
- Modify: [src/metapyle/sources/localfile.py](src/metapyle/sources/localfile.py)
- Test: [tests/unit/test_sources_localfile.py](tests/unit/test_sources_localfile.py)

**Step 1: Write the failing test**

Add test to `tests/unit/test_sources_localfile.py`:

```python
class TestLocalFileSourceKwargs:
    """Tests for **kwargs handling in LocalFileSource."""

    def test_fetch_ignores_kwargs(self, tmp_path: Path) -> None:
        """LocalFileSource.fetch() accepts and ignores **kwargs."""
        from metapyle.sources.localfile import LocalFileSource
        
        # Create test CSV
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("date,value\n2024-01-01,100.0\n")
        
        source = LocalFileSource()
        requests = [FetchRequest(symbol="value", path=str(csv_path))]
        # Pass kwargs that should be ignored
        df = source.fetch(requests, "2024-01-01", "2024-01-01", unified=True, currency="EUR")

        assert not df.empty
        assert "value" in df.columns
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_localfile.py::TestLocalFileSourceKwargs::test_fetch_ignores_kwargs -v`
Expected: FAIL with TypeError about unexpected keyword arguments

**Step 3: Update LocalFileSource.fetch() signature**

In `src/metapyle/sources/localfile.py`, add `**kwargs: Any` to fetch():

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
) -> pd.DataFrame:
```

The method body stays the same — kwargs are accepted but ignored.

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_localfile.py::TestLocalFileSourceKwargs -v`
Expected: PASS

**Step 5: Run all LocalFile tests**

Run: `pytest tests/unit/test_sources_localfile.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/sources/localfile.py tests/unit/test_sources_localfile.py
git commit -m "feat(localfile): accept **kwargs in fetch() (ignored)"
```

---

## Task 5: Update MacrobondSource to Accept **kwargs (No Unified Yet)

**Files:**
- Modify: [src/metapyle/sources/macrobond.py](src/metapyle/sources/macrobond.py#L43-L115)
- Test: [tests/unit/test_sources_macrobond.py](tests/unit/test_sources_macrobond.py)

**Step 1: Write the failing test**

Add test to `tests/unit/test_sources_macrobond.py`:

```python
class TestMacrobondSourceKwargs:
    """Tests for **kwargs handling in MacrobondSource."""

    def test_fetch_accepts_kwargs(self, source: MacrobondSource) -> None:
        """MacrobondSource.fetch() accepts **kwargs without error."""
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
            # Pass kwargs - should be accepted (unified=False uses existing behavior)
            df = source.fetch(requests, "2024-01-01", "2024-01-02", unified=False, currency="EUR")

            assert list(df.columns) == ["usgdp"]
            mock_mda.get_series.assert_called_once_with(["usgdp"])
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceKwargs::test_fetch_accepts_kwargs -v`
Expected: FAIL with TypeError about unexpected keyword arguments

**Step 3: Update MacrobondSource.fetch() signature**

In `src/metapyle/sources/macrobond.py`, add `**kwargs: Any` to fetch():

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
) -> pd.DataFrame:
```

For now, kwargs are accepted but ignored. Unified logic comes in Task 7.

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceKwargs -v`
Expected: PASS

**Step 5: Run all Macrobond tests**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/sources/macrobond.py tests/unit/test_sources_macrobond.py
git commit -m "feat(macrobond): accept **kwargs in fetch() (prep for unified)"
```

---

## Task 6: Update Client to Pass **kwargs to Sources

**Files:**
- Modify: [src/metapyle/client.py](src/metapyle/client.py#L64-L175)
- Test: [tests/unit/test_client.py](tests/unit/test_client.py)

**Step 1: Write the failing test**

Add test to `tests/unit/test_client.py`. First add a new mock source that captures kwargs:

```python
# Add near top of file with other mock sources
_captured_kwargs: dict[str, Any] = {}


@register_source("mock_kwargs_capture")
class MockKwargsCaptureSource(BaseSource):
    """Mock source that captures **kwargs from fetch()."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Capture kwargs and return mock data."""
        _captured_kwargs.clear()
        _captured_kwargs.update(kwargs)
        
        dates = pd.date_range(start, end, freq="D")
        result = pd.DataFrame(index=dates)
        for req in requests:
            result[req.symbol] = list(range(len(dates)))
        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Return mock metadata."""
        return {"symbol": symbol}


class TestClientKwargsPassthrough:
    """Tests for Client passing **kwargs to sources."""

    def test_get_passes_kwargs_to_source(self, tmp_path: Path) -> None:
        """Client.get() passes **kwargs to source.fetch()."""
        yaml_content = """
- my_name: test_unified
  source: mock_kwargs_capture
  symbol: TEST_SYMBOL
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        client = Client(catalog=yaml_file, cache_enabled=False)
        client.get(
            ["test_unified"],
            start="2024-01-01",
            end="2024-01-02",
            unified=True,
            currency="EUR",
        )

        assert _captured_kwargs.get("unified") is True
        assert _captured_kwargs.get("currency") == "EUR"

        client.close()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::TestClientKwargsPassthrough::test_get_passes_kwargs_to_source -v`
Expected: FAIL with TypeError - `get()` got unexpected keyword argument 'unified'

**Step 3: Update Client.get() signature**

In `src/metapyle/client.py`, update `get()` signature:

```python
def get(
    self,
    names: list[str],
    start: str,
    end: str | None = None,
    *,
    frequency: str | None = None,
    output_format: str = "wide",
    use_cache: bool = True,
    unified: bool = False,
    **kwargs: Any,
) -> pd.DataFrame:
```

Update docstring to include:

```python
    """
    ...
    unified : bool, optional
        If True, use server-side alignment for Macrobond data via
        get_unified_series(). Other sources ignore this parameter.
        Default is False.
    **kwargs : Any
        Additional keyword arguments passed to source adapters.
        For Macrobond unified series, these override default settings
        (e.g., frequency, currency). Use macrobond_data_api enums directly.
    ...
    """
```

**Step 4: Update _fetch_from_source() to accept and pass kwargs**

```python
def _fetch_from_source(
    self,
    source_name: str,
    requests: list[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
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
    **kwargs : Any
        Source-specific keyword arguments.

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

    return source.fetch(requests, start, end, **kwargs)
```

**Step 5: Update get() to pass kwargs to _fetch_from_source()**

In the batch fetch loop inside `get()`, pass kwargs:

```python
# Batch fetch from source
result_df = self._fetch_from_source(
    source_name, requests, start, end, unified=unified, **kwargs
)
```

**Step 6: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py::TestClientKwargsPassthrough -v`
Expected: PASS

**Step 7: Run all client tests**

Run: `pytest tests/unit/test_client.py -v`
Expected: All PASS

**Step 8: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): pass unified and **kwargs to source adapters"
```

---

## Task 7: Implement MacrobondSource Unified Series Logic

**Files:**
- Modify: [src/metapyle/sources/macrobond.py](src/metapyle/sources/macrobond.py#L43-L115)
- Test: [tests/unit/test_sources_macrobond.py](tests/unit/test_sources_macrobond.py)

This is the core feature. MacrobondSource will route to either `_fetch_regular()` (existing get_series) or `_fetch_unified()` (new get_unified_series) based on the `unified` kwarg.

**Step 1: Write the failing test for unified=True**

Add test to `tests/unit/test_sources_macrobond.py`:

```python
class TestMacrobondSourceUnified:
    """Tests for unified series support in MacrobondSource."""

    def test_unified_calls_get_unified_series(self, source: MacrobondSource) -> None:
        """When unified=True, fetch() calls get_unified_series()."""
        # Mock the unified series result
        mock_result = MagicMock()
        mock_result.to_pd_data_frame.return_value = pd.DataFrame(
            {
                "usgdp": [100.0, 101.0],
                "gbgdp": [200.0, 201.0],
            },
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )

        with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
            mock_mda = MagicMock()
            mock_mda.get_unified_series.return_value = mock_result
            mock_get_mda.return_value = mock_mda

            requests = [
                FetchRequest(symbol="usgdp"),
                FetchRequest(symbol="gbgdp"),
            ]
            df = source.fetch(requests, "2024-01-01", "2024-01-02", unified=True)

            # Verify get_unified_series was called (not get_series)
            mock_mda.get_unified_series.assert_called_once()
            assert mock_mda.get_series.call_count == 0
            
            assert "usgdp" in df.columns
            assert "gbgdp" in df.columns

    def test_unified_false_calls_get_series(self, source: MacrobondSource) -> None:
        """When unified=False, fetch() calls get_series() (existing behavior)."""
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
            df = source.fetch(requests, "2024-01-01", "2024-01-02", unified=False)

            # Verify get_series was called (not get_unified_series)
            mock_mda.get_series.assert_called_once_with(["usgdp"])
            assert mock_mda.get_unified_series.call_count == 0

    def test_unified_default_is_false(self, source: MacrobondSource) -> None:
        """When unified not specified, defaults to False (get_series)."""
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
            # No unified kwarg
            df = source.fetch(requests, "2024-01-01", "2024-01-02")

            mock_mda.get_series.assert_called_once()
            assert mock_mda.get_unified_series.call_count == 0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceUnified::test_unified_calls_get_unified_series -v`
Expected: FAIL (get_unified_series not called because logic doesn't exist yet)

**Step 3: Implement unified routing in MacrobondSource**

Refactor `src/metapyle/sources/macrobond.py`:

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
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
    **kwargs : Any
        unified : bool - If True, use get_unified_series() with server-side
            alignment. Defaults to False.
        Other kwargs passed to get_unified_series() when unified=True
        (e.g., frequency, currency, weekdays, calendar_merge_mode).

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

    # Extract unified flag; remaining kwargs go to get_unified_series
    unified = kwargs.pop("unified", False)

    if unified:
        return self._fetch_unified(mda, requests, start, end, **kwargs)
    else:
        return self._fetch_regular(mda, requests, start, end)
```

**Step 4: Extract existing logic to _fetch_regular()**

```python
def _fetch_regular(
    self,
    mda: Any,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
) -> pd.DataFrame:
    """Fetch using get_series() - existing behavior."""
    symbols = [req.symbol for req in requests]

    logger.debug(
        "fetch_start: symbols=%s, start=%s, end=%s, mode=regular",
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

    # Normalize index name
    result.index.name = "date"

    # Ensure UTC timezone
    if result.index.tz is None:
        result.index = result.index.tz_localize("UTC")
    else:
        result.index = result.index.tz_convert("UTC")

    if result.empty:
        logger.warning(
            "fetch_no_data_in_range: symbols=%s, start=%s, end=%s",
            symbols,
            start,
            end,
        )
        raise NoDataError(f"No data in date range {start} to {end}")

    logger.info(
        "fetch_complete: symbols=%s, rows=%d, mode=regular",
        symbols,
        len(result),
    )
    return result
```

**Step 5: Run test to verify extraction doesn't break existing behavior**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All PASS (existing tests still work with refactored code)

**Step 6: Commit refactoring**

```bash
git add src/metapyle/sources/macrobond.py
git commit -m "refactor(macrobond): extract _fetch_regular() method"
```

---

## Task 8: Implement _fetch_unified() Method

**Files:**
- Modify: [src/metapyle/sources/macrobond.py](src/metapyle/sources/macrobond.py)
- Test: [tests/unit/test_sources_macrobond.py](tests/unit/test_sources_macrobond.py)

**Step 1: Write test for hardcoded defaults**

Add test to `tests/unit/test_sources_macrobond.py`:

```python
def test_unified_uses_hardcoded_defaults(self, source: MacrobondSource) -> None:
    """Unified fetch uses hardcoded defaults for common settings."""
    mock_result = MagicMock()
    mock_result.to_pd_data_frame.return_value = pd.DataFrame(
        {"usgdp": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
        mock_mda = MagicMock()
        mock_mda.get_unified_series.return_value = mock_result
        mock_get_mda.return_value = mock_mda

        requests = [FetchRequest(symbol="usgdp")]
        source.fetch(requests, "2024-01-01", "2024-01-02", unified=True)

        # Verify get_unified_series was called with symbols and kwargs
        call_args = mock_mda.get_unified_series.call_args
        
        # First positional arg should be unpacked symbols
        assert call_args.args == ("usgdp",)
        
        # Check that default kwargs were passed
        call_kwargs = call_args.kwargs
        assert "frequency" in call_kwargs
        assert "weekdays" in call_kwargs
        assert "calendar_merge_mode" in call_kwargs
        assert "currency" in call_kwargs
        assert call_kwargs["currency"] == "USD"
        assert "start_point" in call_kwargs
        assert "end_point" in call_kwargs
```

**Step 2: Write test for kwargs override**

```python
def test_unified_kwargs_override_defaults(self, source: MacrobondSource) -> None:
    """User kwargs override hardcoded defaults."""
    mock_result = MagicMock()
    mock_result.to_pd_data_frame.return_value = pd.DataFrame(
        {"usgdp": [100.0]},
        index=pd.to_datetime(["2024-01-01"]),
    )

    with patch("metapyle.sources.macrobond._get_mda") as mock_get_mda:
        mock_mda = MagicMock()
        mock_mda.get_unified_series.return_value = mock_result
        mock_get_mda.return_value = mock_mda

        requests = [FetchRequest(symbol="usgdp")]
        # Override currency default
        source.fetch(requests, "2024-01-01", "2024-01-02", unified=True, currency="EUR")

        call_kwargs = mock_mda.get_unified_series.call_args.kwargs
        # User override should take precedence
        assert call_kwargs["currency"] == "EUR"
```

**Step 3: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceUnified -v`
Expected: FAIL (no _fetch_unified method yet)

**Step 4: Implement _fetch_unified()**

Add to `src/metapyle/sources/macrobond.py`:

```python
def _fetch_unified(
    self,
    mda: Any,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    **kwargs: Any,
) -> pd.DataFrame:
    """Fetch using get_unified_series() with server-side alignment."""
    from macrobond_data_api.common.enums import (
        CalendarMergeMode,
        SeriesFrequency,
        SeriesWeekdays,
    )
    from macrobond_data_api.common.types import StartOrEndPoint

    symbols = [req.symbol for req in requests]

    logger.debug(
        "fetch_start: symbols=%s, start=%s, end=%s, mode=unified",
        symbols,
        start,
        end,
    )

    # Hardcoded defaults
    unified_kwargs: dict[str, Any] = {
        "frequency": SeriesFrequency.DAILY,
        "weekdays": SeriesWeekdays.MONDAY_TO_FRIDAY,
        "calendar_merge_mode": CalendarMergeMode.AVAILABLE_IN_ALL,
        "currency": "USD",
        "start_point": StartOrEndPoint(start),
        "end_point": StartOrEndPoint(end),
    }
    # User overrides take precedence
    unified_kwargs.update(kwargs)

    try:
        result = mda.get_unified_series(*symbols, **unified_kwargs)
    except Exception as e:
        logger.error("fetch_failed: symbols=%s, error=%s, mode=unified", symbols, str(e))
        raise FetchError(f"Macrobond unified API error: {e}") from e

    # Convert to DataFrame
    df = result.to_pd_data_frame()

    if df.empty:
        logger.warning("fetch_empty: symbols=%s, mode=unified", symbols)
        raise NoDataError(f"No unified data returned for {symbols}")

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # Normalize index name
    df.index.name = "date"

    # Ensure UTC timezone
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    logger.info(
        "fetch_complete: symbols=%s, rows=%d, mode=unified",
        symbols,
        len(df),
    )
    return df
```

**Step 5: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceUnified -v`
Expected: All PASS

**Step 6: Run all Macrobond unit tests**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All PASS

**Step 7: Commit**

```bash
git add src/metapyle/sources/macrobond.py tests/unit/test_sources_macrobond.py
git commit -m "feat(macrobond): implement _fetch_unified() with hardcoded defaults"
```

---

## Task 9: Handle Macrobond Import Errors for Unified Enums

**Files:**
- Modify: [src/metapyle/sources/macrobond.py](src/metapyle/sources/macrobond.py)
- Test: [tests/unit/test_sources_macrobond.py](tests/unit/test_sources_macrobond.py)

The enum imports in `_fetch_unified()` will fail if macrobond_data_api is not installed. We need graceful handling.

**Step 1: Write the failing test**

```python
def test_unified_mda_not_available(self, source: MacrobondSource) -> None:
    """Raise FetchError when unified=True but mda not installed."""
    with patch("metapyle.sources.macrobond._get_mda", return_value=None):
        requests = [FetchRequest(symbol="usgdp")]
        with pytest.raises(FetchError, match="macrobond"):
            source.fetch(requests, "2024-01-01", "2024-01-02", unified=True)
```

**Step 2: Run test to verify behavior**

Run: `pytest tests/unit/test_sources_macrobond.py::TestMacrobondSourceUnified::test_unified_mda_not_available -v`
Expected: PASS (the mda check happens before unified logic)

**Step 3: Run all tests to confirm no regression**

Run: `pytest tests/unit/test_sources_macrobond.py -v`
Expected: All PASS

**Step 4: Commit if any changes needed**

```bash
git add tests/unit/test_sources_macrobond.py
git commit -m "test(macrobond): add unified error handling tests"
```

---

## Task 10: Bypass Cache for Macrobond Unified Requests

**Files:**
- Modify: [src/metapyle/client.py](src/metapyle/client.py#L90-L130)
- Test: [tests/unit/test_client.py](tests/unit/test_client.py)

When `unified=True`, macrobond entries should skip cache because the server-side transformation depends on all symbols together.

**Step 1: Write the failing test**

Add test to `tests/unit/test_client.py`:

```python
class TestClientUnifiedCache:
    """Tests for cache behavior with unified=True."""

    def test_unified_bypasses_cache_for_macrobond(self, tmp_path: Path) -> None:
        """When unified=True, macrobond entries skip cache."""
        # Create catalog with macrobond entry
        yaml_content = """
- my_name: test_mb
  source: mock_kwargs_capture
  symbol: TEST_MB
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        cache_path = tmp_path / "cache.db"
        client = Client(catalog=yaml_file, cache_path=str(cache_path), cache_enabled=True)

        # First fetch with unified=True
        client.get(["test_mb"], start="2024-01-01", end="2024-01-02", unified=True)
        
        # unified=True should trigger fetch (not use cache)
        assert _captured_kwargs.get("unified") is True

        # Clear captured kwargs
        _captured_kwargs.clear()

        # Second fetch with unified=True should NOT hit cache - should fetch again
        client.get(["test_mb"], start="2024-01-01", end="2024-01-02", unified=True)
        
        # Should have fetched again (kwargs captured = fetch happened)
        assert _captured_kwargs.get("unified") is True

        client.close()

    def test_unified_false_uses_cache(self, tmp_path: Path) -> None:
        """When unified=False, normal caching applies."""
        yaml_content = """
- my_name: test_cached
  source: mock_kwargs_capture
  symbol: TEST_CACHED
"""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(yaml_content)

        cache_path = tmp_path / "cache.db"
        client = Client(catalog=yaml_file, cache_path=str(cache_path), cache_enabled=True)

        # First fetch (populates cache)
        client.get(["test_cached"], start="2024-01-01", end="2024-01-02", unified=False)
        
        # Clear captured kwargs
        _captured_kwargs.clear()

        # Second fetch should use cache (no fetch - kwargs not captured)
        client.get(["test_cached"], start="2024-01-01", end="2024-01-02", unified=False)
        
        # If cache was used, no fetch happened, so kwargs should be empty
        assert _captured_kwargs == {}

        client.close()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::TestClientUnifiedCache::test_unified_bypasses_cache_for_macrobond -v`
Expected: FAIL (second fetch still uses cache because unified cache bypass not implemented)

**Step 3: Update Client.get() to bypass cache when unified=True**

In `src/metapyle/client.py`, modify the cache lookup section in `get()`:

```python
# Collect cached and uncached entries
dfs: dict[str, pd.DataFrame] = {}
uncached_entries: list[CatalogEntry] = []

for entry in entries:
    # Skip cache for unified macrobond requests (server-side transformation)
    if unified and entry.source == "macrobond":
        logger.debug(
            "cache_bypass_unified: symbol=%s",
            entry.my_name,
        )
        uncached_entries.append(entry)
        continue

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
            dfs[entry.my_name] = cached
            continue

    uncached_entries.append(entry)
```

Also skip caching the result when unified:

```python
# Cache the individual column (skip for unified macrobond)
if use_cache and not (unified and entry.source == "macrobond"):
    self._cache.put(
        source=entry.source,
        symbol=entry.symbol,
        field=entry.field,
        path=entry.path,
        start_date=start,
        end_date=end,
        data=col_df,
    )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py::TestClientUnifiedCache -v`
Expected: All PASS

**Step 5: Run all client tests**

Run: `pytest tests/unit/test_client.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): bypass cache for macrobond unified requests"
```

---

## Task 11: Update Client.get() Docstring

**Files:**
- Modify: [src/metapyle/client.py](src/metapyle/client.py#L64-L105)

**Step 1: Update docstring**

Update the docstring in `Client.get()` to document new parameters:

```python
def get(
    self,
    names: list[str],
    start: str,
    end: str | None = None,
    *,
    frequency: str | None = None,
    output_format: str = "wide",
    use_cache: bool = True,
    unified: bool = False,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Fetch time-series data for multiple catalog names.

    Parameters
    ----------
    names : list[str]
        List of catalog names to fetch.
    start : str
        Start date in ISO format (YYYY-MM-DD).
    end : str | None, optional
        End date in ISO format (YYYY-MM-DD). Defaults to today.
    frequency : str | None, optional
        Pandas frequency string for alignment (e.g., "D", "ME", "QE").
        If omitted, data is returned as-is with a warning if indexes
        don't align.
    output_format : str, optional
        Output format: "wide" (default) or "long".
        Wide: DatetimeIndex, one column per name.
        Long: Columns [date, symbol, value], one row per observation.
    use_cache : bool, optional
        Whether to use cached data. Default is True.
    unified : bool, optional
        If True, use Macrobond's server-side alignment via get_unified_series().
        This converts all series to a common frequency and calendar.
        Caching is bypassed for macrobond entries when unified=True.
        Other sources ignore this parameter. Default is False.
    **kwargs : Any
        Additional keyword arguments passed to source adapters.
        For Macrobond unified series (unified=True), these override defaults:
        - frequency: SeriesFrequency enum (default: DAILY)
        - weekdays: SeriesWeekdays enum (default: MONDAY_TO_FRIDAY)
        - calendar_merge_mode: CalendarMergeMode enum (default: AVAILABLE_IN_ALL)
        - currency: str (default: "USD")
        Import enums directly from macrobond_data_api.common.enums.

    Returns
    -------
    pd.DataFrame
        DataFrame in the requested format.

    Raises
    ------
    NameNotFoundError
        If any name is not in the catalog.
    FetchError
        If data retrieval fails for any symbol.
    ValueError
        If frequency is an invalid pandas frequency string, or
        output_format is not "wide" or "long".

    Examples
    --------
    >>> # Basic unified series fetch
    >>> df = client.get(["gdp_us", "gdp_eu"], start, end, unified=True)

    >>> # Power user with custom settings
    >>> from macrobond_data_api.common.enums import SeriesFrequency
    >>> df = client.get(["gdp_us"], start, end, unified=True,
    ...                 frequency=SeriesFrequency.WEEKLY, currency="EUR")
    """
```

**Step 2: Run tests to verify no breakage**

Run: `pytest tests/unit/test_client.py -v`
Expected: All PASS

**Step 3: Commit**

```bash
git add src/metapyle/client.py
git commit -m "docs(client): document unified and **kwargs parameters"
```

---

## Task 12: Add Integration Test for Unified Series

**Files:**
- Modify: [tests/integration/test_macrobond.py](tests/integration/test_macrobond.py)

**Step 1: Add unified series integration test**

Add test class to `tests/integration/test_macrobond.py`:

```python
class TestMacrobondUnifiedSeries:
    """Test unified series fetch from Macrobond."""

    def test_unified_single_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch single series with unified=True."""
        df = macrobond_client.get(
            ["sp500_mb"],
            start=test_start,
            end=test_end,
            unified=True,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_unified_multiple_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch multiple series with unified=True (server-side alignment)."""
        df = macrobond_client.get(
            ["sp500_mb", "us_gdp_mb"],  # daily + quarterly
            start=test_start,
            end=test_end,
            unified=True,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert "us_gdp_mb" in df.columns
        # Both series should have same index (unified)
        assert len(df) > 0

    def test_unified_with_currency_override(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch unified series with currency override."""
        df = macrobond_client.get(
            ["sp500_mb"],
            start=test_start,
            end=test_end,
            unified=True,
            currency="EUR",
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
```

**Step 2: Run integration test (requires Macrobond credentials)**

Run: `pytest tests/integration/test_macrobond.py::TestMacrobondUnifiedSeries -v -m macrobond`
Expected: All PASS (when run with Macrobond API access)

**Step 3: Commit**

```bash
git add tests/integration/test_macrobond.py
git commit -m "test(integration): add unified series tests for macrobond"
```

---

## Task 13: Update User Guide Documentation

**Files:**
- Modify: [docs/user-guide.md](docs/user-guide.md)

**Step 1: Add unified series section**

Add section to `docs/user-guide.md`:

```markdown
## Macrobond Unified Series

When fetching data from Macrobond, you can use server-side alignment via `get_unified_series()`. This is useful when you need multiple series with different frequencies aligned to a common calendar.

### Basic Usage

```python
# Server-side alignment with hardcoded defaults
df = client.get(["gdp_us", "sp500"], start="2020-01-01", end="2024-12-31", unified=True)
```

Default settings when `unified=True`:
- **frequency**: Daily (`SeriesFrequency.DAILY`)
- **weekdays**: Monday to Friday (`SeriesWeekdays.MONDAY_TO_FRIDAY`)
- **calendar_merge_mode**: Available in all (`CalendarMergeMode.AVAILABLE_IN_ALL`)
- **currency**: USD

### Power User Overrides

Import Macrobond enums directly to customize alignment:

```python
from macrobond_data_api.common.enums import SeriesFrequency, SeriesWeekdays

df = client.get(
    ["gdp_us", "sp500"],
    start="2020-01-01",
    end="2024-12-31",
    unified=True,
    frequency=SeriesFrequency.WEEKLY,
    weekdays=SeriesWeekdays.FULL_WEEK,
    currency="EUR",
)
```

### Caching Behavior

When `unified=True`, caching is bypassed for Macrobond entries. This is because unified transformations are server-side and depend on all symbols together. Other sources in a mixed-source request still use caching normally.

### Mixed Source Requests

When mixing Macrobond (unified) with other sources:

```python
# Macrobond entries use unified API, Bloomberg uses standard fetch
df = client.get(
    ["gdp_us_mb", "sp500_bb"],  # mb=macrobond, bb=bloomberg
    start="2020-01-01",
    end="2024-12-31",
    unified=True,
)
```

The `unified` parameter only affects Macrobond sources; other sources ignore it.
```

**Step 2: Run documentation spell check (if applicable)**

Run: Manual review of documentation

**Step 3: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: add unified series section to user guide"
```

---

## Task 14: Run Full Test Suite and Quality Checks

**Files:** None (validation only)

**Step 1: Run full unit test suite**

Run: `pytest tests/unit/ -v`
Expected: All PASS

**Step 2: Run ruff linter**

Run: `ruff check src/ tests/`
Expected: No errors

**Step 3: Run ruff formatter check**

Run: `ruff format --check src/ tests/`
Expected: All files formatted correctly

**Step 4: Run mypy type checker**

Run: `mypy src/`
Expected: No errors

**Step 5: Fix any issues found**

If any checks fail, fix the issues and re-run.

**Step 6: Final commit**

```bash
git add -A
git commit -m "chore: fix linting and type issues"
```

---

## Summary

This plan implements Macrobond unified series support in 14 tasks:

| Task | Description |
|------|-------------|
| 1 | Add `**kwargs` to BaseSource.fetch() |
| 2 | Update BloombergSource to accept kwargs |
| 3 | Update GSQuantSource to accept kwargs |
| 4 | Update LocalFileSource to accept kwargs |
| 5 | Update MacrobondSource to accept kwargs |
| 6 | Update Client to pass kwargs to sources |
| 7 | Implement MacrobondSource unified routing |
| 8 | Implement `_fetch_unified()` method |
| 9 | Handle enum import errors gracefully |
| 10 | Bypass cache for unified macrobond requests |
| 11 | Update Client.get() docstring |
| 12 | Add integration tests |
| 13 | Update user guide documentation |
| 14 | Run full test suite and quality checks |

**Estimated time:** 2-3 hours

**Key files modified:**
- `src/metapyle/sources/base.py`
- `src/metapyle/sources/bloomberg.py`
- `src/metapyle/sources/gsquant.py`
- `src/metapyle/sources/localfile.py`
- `src/metapyle/sources/macrobond.py`
- `src/metapyle/client.py`
- `tests/unit/test_sources_*.py`
- `tests/unit/test_client.py`
- `tests/integration/test_macrobond.py`
- `docs/user-guide.md`

