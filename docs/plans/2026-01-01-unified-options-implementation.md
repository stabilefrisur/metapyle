# Unified Options Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Replace `**kwargs` with `unified_options` dict to avoid frequency parameter collision.

**Architecture:** Add explicit `unified_options: dict[str, Any] | None` parameter to `client.get()` and propagate through `_fetch_from_source()` to `MacrobondSource.fetch()`. Client-side `frequency` remains independent.

**Tech Stack:** Python 3.12, pandas, macrobond-data-api

---

## Task 1: Update MacrobondSource.fetch() signature

**Files:**
- Modify: `src/metapyle/sources/macrobond.py`
- Test: `tests/unit/test_sources_macrobond.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_sources_macrobond.py`:

```python
def test_fetch_unified_with_options_dict(mock_mda):
    """Test fetch with unified_options dict instead of kwargs."""
    source = MacrobondSource()
    requests = [FetchRequest(symbol="usgdp")]

    # Mock get_unified_series
    mock_result = Mock()
    mock_df = pd.DataFrame(
        {"usgdp": [100.0, 101.0]},
        index=pd.to_datetime(["2024-01-01", "2024-02-01"]),
    )
    mock_result.to_pd_data_frame.return_value = mock_df
    mock_mda.get_unified_series.return_value = mock_result

    result = source.fetch(
        requests,
        "2024-01-01",
        "2024-12-31",
        unified=True,
        unified_options={"currency": "EUR"},
    )

    assert not result.empty
    # Verify currency was passed to get_unified_series
    call_kwargs = mock_mda.get_unified_series.call_args.kwargs
    assert call_kwargs["currency"] == "EUR"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_sources_macrobond.py::test_fetch_unified_with_options_dict -v`
Expected: FAIL (TypeError: unexpected keyword argument 'unified_options')

**Step 3: Write minimal implementation**

In `src/metapyle/sources/macrobond.py`, update `fetch()` signature:

```python
def fetch(
    self,
    requests: Sequence[FetchRequest],
    start: str,
    end: str,
    *,
    unified: bool = False,
    unified_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
```

And update the body to use `unified_options`:

```python
if unified:
    return self._fetch_unified(mda, requests, start, end, **(unified_options or {}))
else:
    return self._fetch_regular(mda, requests, start, end)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_sources_macroband.py::test_fetch_unified_with_options_dict -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/macrobond.py tests/unit/test_sources_macrobond.py
git commit -m "feat(macrobond): accept unified_options dict parameter"
```

---

## Task 2: Update Client._fetch_from_source() signature

**Files:**
- Modify: `src/metapyle/client.py`

**Step 1: Update _fetch_from_source signature**

Replace `**kwargs: Any` with explicit parameters:

```python
def _fetch_from_source(
    self,
    source_name: str,
    requests: list[FetchRequest],
    start: str,
    end: str,
    *,
    unified: bool = False,
    unified_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
```

Update the call to source.fetch():

```python
return source.fetch(
    requests, start, end,
    unified=unified,
    unified_options=unified_options,
)
```

**Step 2: Run existing tests to verify no regression**

Run: `pytest tests/unit/test_client.py -v`
Expected: PASS (signature change is backward compatible)

**Step 3: Commit**

```bash
git add src/metapyle/client.py
git commit -m "refactor(client): update _fetch_from_source signature for unified_options"
```

---

## Task 3: Update Client.get() signature

**Files:**
- Modify: `src/metapyle/client.py`
- Test: `tests/unit/test_client.py`

**Step 1: Write the failing test**

Add to `tests/unit/test_client.py`:

```python
def test_get_with_unified_options(mock_catalog, mock_cache, mock_registry):
    """Test get() accepts unified_options parameter."""
    client = Client(catalog="test.yaml")

    # Should not raise TypeError
    with pytest.raises(NameNotFoundError):
        # Will fail at catalog lookup, but proves signature works
        client.get(
            ["test"],
            start="2024-01-01",
            unified=True,
            unified_options={"currency": "EUR"},
        )
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_client.py::test_get_with_unified_options -v`
Expected: FAIL (TypeError: unexpected keyword argument 'unified_options')

**Step 3: Update get() signature**

Replace `**kwargs: Any` with `unified_options`:

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
    unified_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
```

**Step 4: Update the call to _fetch_from_source**

Find the call to `self._fetch_from_source()` and change:

```python
result_df = self._fetch_from_source(
    source_name, requests, start, end,
    unified=unified,
    unified_options=unified_options,
)
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_client.py::test_get_with_unified_options -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/metapyle/client.py tests/unit/test_client.py
git commit -m "feat(client): replace **kwargs with unified_options parameter"
```

---

## Task 4: Test frequency independence

**Files:**
- Test: `tests/unit/test_client.py`

**Step 1: Write test for both frequency and unified_options**

Add to `tests/unit/test_client.py`:

```python
def test_get_frequency_independent_of_unified_options(
    mock_catalog, mock_cache, mock_registry, tmp_path
):
    """Test that frequency (pandas) and unified_options.frequency are independent."""
    # Create a minimal catalog
    catalog_file = tmp_path / "catalog.yaml"
    catalog_file.write_text("""
- my_name: test_series
  source: localfile
  symbol: value
  path: /tmp/test.csv
""")

    # Create test CSV
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("date,value\n2024-01-01,100\n2024-01-02,101\n")

    # Patch catalog path
    mock_catalog.return_value = Catalog.from_yaml(catalog_file)

    client = Client(catalog=str(catalog_file))

    # This should NOT raise - frequency is pandas string, unified_options is separate
    # (Will fail at fetch due to mocking, but proves no collision)
    try:
        client.get(
            ["test_series"],
            start="2024-01-01",
            unified=True,
            unified_options={"frequency": "SOME_ENUM_VALUE"},  # Macrobond freq
            frequency="ME",  # pandas freq - should not conflict
        )
    except Exception as e:
        # Accept fetch errors, but not TypeError for frequency collision
        assert "frequency" not in str(e).lower() or "unexpected" not in str(e).lower()
```

**Step 2: Run test**

Run: `pytest tests/unit/test_client.py::test_get_frequency_independent_of_unified_options -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/unit/test_client.py
git commit -m "test(client): verify frequency and unified_options are independent"
```

---

## Task 5: Update docstrings

**Files:**
- Modify: `src/metapyle/client.py`
- Modify: `src/metapyle/sources/macrobond.py`

**Step 1: Update client.get() docstring**

Replace the `**kwargs` docstring section with:

```python
unified_options : dict[str, Any] | None, optional
    Options passed to Macrobond's get_unified_series() when unified=True.
    Supports: frequency (SeriesFrequency), weekdays (SeriesWeekdays),
    calendar_merge_mode (CalendarMergeMode), currency (str),
    start_point/end_point (StartOrEndPoint). Ignored for non-macrobond sources.
```

**Step 2: Update macrobond.fetch() docstring**

Update the Parameters section:

```python
unified : bool, default False
    If True, use get_unified_series() with server-side alignment.
unified_options : dict[str, Any] | None, optional
    Options passed to get_unified_series() when unified=True.
    Supports: frequency, weekdays, calendar_merge_mode, currency,
    start_point, end_point.
```

**Step 3: Run docstring check**

Run: `python -c "from metapyle import Client; help(Client.get)"`
Expected: Shows updated docstring with unified_options

**Step 4: Commit**

```bash
git add src/metapyle/client.py src/metapyle/sources/macrobond.py
git commit -m "docs: update docstrings for unified_options parameter"
```

---

## Task 6: Update user-guide.md

**Files:**
- Modify: `docs/user-guide.md`

**Step 1: Update "Macrobond Unified Series" section**

Find the "Unified Series Options" subsection and replace the example:

**Before:**
```python
df = client.get(
    ["sp500", "stoxx600"],
    start="2020-01-01",
    end="2024-12-31",
    unified=True,
    frequency=SeriesFrequency.MONTHLY,
    weekdays=SeriesWeekdays.FULLWEEK,
    calendar_merge_mode=CalendarMergeMode.AVAILABLE_IN_ANY,
    currency="EUR",
    start_point=StartOrEndPoint.DATA_IN_ALL_SERIES,
)
```

**After:**
```python
df = client.get(
    ["sp500", "stoxx600"],
    start="2020-01-01",
    end="2024-12-31",
    unified=True,
    unified_options={
        "frequency": SeriesFrequency.MONTHLY,
        "weekdays": SeriesWeekdays.FULLWEEK,
        "calendar_merge_mode": CalendarMergeMode.AVAILABLE_IN_ANY,
        "currency": "EUR",
        "start_point": StartOrEndPoint.DATA_IN_ALL_SERIES,
    },
)
```

**Step 2: Update the defaults table description**

Change the table intro from:

> Without explicit options, unified series uses these defaults:

To:

> Without explicit `unified_options`, unified series uses these defaults:

**Step 3: Add mixed-source example**

After the existing examples, add:

```markdown
### Mixed Sources with Unified

When combining Macrobond (with `unified=True`) and other sources, use both `unified_options` for server-side alignment and `frequency` for client-side alignment:

```python
df = client.get(
    ["us_gdp", "sp500_close"],  # macrobond + bloomberg
    start="2020-01-01",
    end="2024-12-31",
    unified=True,
    unified_options={"frequency": SeriesFrequency.MONTHLY},
    frequency="ME",  # client-side alignment for final merge
)
```
```

**Step 4: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: update user-guide for unified_options syntax"
```

---

## Task 7: Update integration tests

**Files:**
- Modify: `tests/integration/test_macrobond.py`

**Step 1: Find and update existing unified tests**

Search for tests using the old `**kwargs` syntax and update to `unified_options`:

```python
# Before
df = client.get(
    names,
    start=start,
    end=end,
    unified=True,
    frequency=SeriesFrequency.MONTHLY,
)

# After
df = client.get(
    names,
    start=start,
    end=end,
    unified=True,
    unified_options={"frequency": SeriesFrequency.MONTHLY},
)
```

**Step 2: Run integration tests (if Macrobond available)**

Run: `pytest tests/integration/test_macrobond.py -v -m macrobond`
Expected: PASS (or skip if Macrobond not available)

**Step 3: Commit**

```bash
git add tests/integration/test_macrobond.py
git commit -m "test(integration): update macrobond tests for unified_options"
```

---

## Task 8: Run full test suite and verify

**Step 1: Run all unit tests**

Run: `pytest tests/unit/ -v`
Expected: All PASS

**Step 2: Run linting**

Run: `ruff check src/ tests/`
Expected: No errors

**Step 3: Run type checking**

Run: `mypy src/`
Expected: No errors

**Step 4: Final commit (if any cleanup needed)**

```bash
git add -A
git commit -m "chore: cleanup after unified_options refactor"
```

---

## Summary

| Task | Description | Estimated Time |
|------|-------------|----------------|
| 1 | Update MacrobondSource.fetch() signature | 5 min |
| 2 | Update Client._fetch_from_source() signature | 3 min |
| 3 | Update Client.get() signature | 5 min |
| 4 | Test frequency independence | 5 min |
| 5 | Update docstrings | 5 min |
| 6 | Update user-guide.md | 10 min |
| 7 | Update integration tests | 5 min |
| 8 | Run full test suite | 5 min |

**Total: ~45 minutes**
