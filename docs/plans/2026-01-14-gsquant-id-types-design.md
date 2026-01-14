# GS Quant Support for All ID Types - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend gsquant adapter to support all GS Marquee identifier types via the params field.

**Architecture:** Extract `id_type` from params (default `assetId`), pass dynamically to gs_quant API, use id_type as pivot column name.

**Tech Stack:** Python, gs-quant, pytest

---

GitHub Issue: #1

## Problem

The gsquant adapter currently hardcodes `bbid` (Bloomberg ID) as the only supported identifier type. Users need to query GS Marquee datasets using other identifier types such as `assetId` (GS Marquee unique identifier), `cusip`, `isin`, `sedol`, `ric`, and `ticker`.

## Solution

Use the existing `params` field in catalog entries to specify `id_type`. The adapter will read this parameter and pass it dynamically to the gs_quant `Dataset.get_data()` API. When `id_type` is not specified, it defaults to `assetId`.

## Supported Identifier Types

| ID Type | Description |
|---------|-------------|
| `assetId` | GS Marquee Asset ID (default) |
| `bbid` | Bloomberg ID |
| `gsid` | GS Internal ID |
| `cusip` | CUSIP |
| `isin` | ISIN |
| `sedol` | SEDOL |
| `ric` | Reuters Instrument Code |
| `ticker` | Exchange ticker symbol |

Reference: https://marquee.gs.com/s/data-services/catalog

## Catalog Entry Examples

```yaml
- my_name: CDX IG 5y
  source: gsquant
  symbol: MANJG63BKCNEHYW3
  field: CDS_INDICES_LEVELS_V1_INTERNAL::spread
  params:
    id_type: assetId  # defaults to assetId (GS Marquee identifier)

- my_name: iTraxx Xover 5y
  source: gsquant
  symbol: ITRXEXE
  field: CDS_INDICES_LEVELS_V1_INTERNAL::spread
  params:
    id_type: bbid

- my_name: SPX 1m 1y ATM Swaption Vol
  source: gsquant
  symbol: SPX
  field: SWAPTION_VOL::atmVol
  params:
    id_type: bbid
    tenor: 1y
    expirationTenor: 1m
```

## Implementation Changes

### src/metapyle/sources/gsquant.py

1. Extract `id_type` from `merged_params`, defaulting to `assetId`
2. Pass identifier dynamically to `get_data()`: `**{id_type: symbols}`
3. Use `id_type` as the pivot table column name

Current code (line 200):
```python
data = ds.get_data(start, end, bbid=symbols, **merged_params)
```

New code:
```python
id_type = merged_params.pop("id_type", "assetId")
data = ds.get_data(start, end, **{id_type: symbols}, **merged_params)
```

Current pivot (line 217):
```python
columns=["bbid"]
```

New pivot:
```python
columns=[id_type]
```

### docs/user-guide.md

Add documentation for the `id_type` parameter in the gsquant section:
- Supported identifier types table
- Example catalog entries
- Link to GS Marquee data catalog

---

## Implementation Tasks

### Task 1: Update Unit Tests for Default id_type

**Files:**
- Modify: `tests/unit/test_sources_gsquant.py`

**Step 1: Update mock data to use assetId instead of bbid**

The existing tests use `bbid` in mock DataFrames. Update all mock data to use `assetId` as the column name since that's the new default.

In `test_fetch_single_request` (line 105-111), change:
```python
mock_dataset_instance.get_data.return_value = pd.DataFrame(
    {
        "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        "assetId": ["EURUSD", "EURUSD"],
        "impliedVolatility": [0.08, 0.085],
    }
)
```

In `test_fetch_with_params` (line 136-142), change:
```python
mock_dataset_instance.get_data.return_value = pd.DataFrame(
    {
        "date": pd.to_datetime(["2024-01-01"]),
        "assetId": ["EURUSD"],
        "impliedVolatility": [0.08],
    }
)
```

In `test_fetch_multiple_symbols_same_dataset` (line 173-179), change:
```python
mock_dataset_instance.get_data.return_value = pd.DataFrame(
    {
        "date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"]),
        "assetId": ["EURUSD", "USDJPY", "EURUSD", "USDJPY"],
        "impliedVolatility": [0.08, 0.10, 0.085, 0.105],
    }
)
```

Also update the assertion at line 197:
```python
assert set(call_kwargs["assetId"]) == {"EURUSD", "USDJPY"}
```

In `test_fetch_multiple_datasets` (lines 209-224), change both DataFrames:
```python
if dataset_id == "FXIMPLIEDVOL":
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "assetId": ["EURUSD"],
            "impliedVolatility": [0.08],
        }
    )
else:  # FXSPOT
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "assetId": ["EURUSD"],
            "spot": [1.10],
        }
    )
```

In `test_fetch_ignores_kwargs` (lines 374-380), change:
```python
mock_dataset_instance.get_data.return_value = pd.DataFrame(
    {
        "date": pd.to_datetime(["2024-01-01"]),
        "assetId": ["EURUSD"],
        "impliedVolatility": [0.08],
    }
)
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sources_gsquant.py -v`
Expected: Multiple FAIL (tests expect `bbid` but adapter still uses `bbid`, so mocks with `assetId` will fail)

---

### Task 2: Implement gsquant Adapter Changes

**Files:**
- Modify: `src/metapyle/sources/gsquant.py:181-217`

**Step 1: Extract id_type from merged_params**

At line 196 (after merging params), add extraction of id_type:
```python
# Extract id_type from params, default to assetId
id_type = merged_params.pop("id_type", "assetId")
```

**Step 2: Update get_data call to use dynamic id_type**

Change line 200 from:
```python
data = ds.get_data(start, end, bbid=symbols, **merged_params)
```

To:
```python
data = ds.get_data(start, end, **{id_type: symbols}, **merged_params)
```

**Step 3: Update pivot table column**

Change line 217 from:
```python
columns=["bbid"],
```

To:
```python
columns=[id_type],
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_gsquant.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/metapyle/sources/gsquant.py tests/unit/test_sources_gsquant.py
git commit -m "feat(gsquant): support all GS Marquee identifier types

- Extract id_type from params, default to assetId
- Pass id_type dynamically to Dataset.get_data()
- Use id_type as pivot column name

Closes #1"
```

---

### Task 3: Add Tests for Custom id_type

**Files:**
- Modify: `tests/unit/test_sources_gsquant.py`

**Step 1: Add test for explicit id_type in params**

Add new test class after `TestGSQuantSourceKwargs`:

```python
class TestGSQuantIdType:
    """Tests for id_type parameter handling."""

    def test_fetch_with_explicit_id_type_bbid(self) -> None:
        """fetch uses id_type from params when specified."""
        from metapyle.sources.gsquant import GSQuantSource

        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "bbid": ["SPX"],
                "impliedVolatility": [0.15],
            }
        )

        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)

        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}

            source = GSQuantSource()
            request = FetchRequest(
                symbol="SPX",
                field="FXIMPLIEDVOL::impliedVolatility",
                params={"id_type": "bbid"},
            )

            df = source.fetch([request], "2024-01-01", "2024-01-01")

        # Verify bbid was passed to get_data
        call_kwargs = mock_dataset_instance.get_data.call_args[1]
        assert "bbid" in call_kwargs
        assert call_kwargs["bbid"] == ["SPX"]
        assert "id_type" not in call_kwargs  # Should be popped

        # Result column should exist
        assert "SPX::FXIMPLIEDVOL::impliedVolatility" in df.columns

    def test_fetch_with_id_type_cusip(self) -> None:
        """fetch supports cusip as id_type."""
        from metapyle.sources.gsquant import GSQuantSource

        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "cusip": ["037833100"],
                "closePrice": [185.50],
            }
        )

        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)

        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}

            source = GSQuantSource()
            request = FetchRequest(
                symbol="037833100",
                field="TREOD::closePrice",
                params={"id_type": "cusip"},
            )

            df = source.fetch([request], "2024-01-01", "2024-01-01")

        # Verify cusip was passed to get_data
        call_kwargs = mock_dataset_instance.get_data.call_args[1]
        assert "cusip" in call_kwargs
        assert call_kwargs["cusip"] == ["037833100"]

    def test_fetch_default_id_type_is_assetId(self) -> None:
        """fetch defaults to assetId when id_type not specified."""
        from metapyle.sources.gsquant import GSQuantSource

        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "assetId": ["MA4B66MW5E27UAL9SUX"],
                "spread": [50.5],
            }
        )

        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)

        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}

            source = GSQuantSource()
            request = FetchRequest(
                symbol="MA4B66MW5E27UAL9SUX",
                field="CDS_INDICES::spread",
                # No id_type in params
            )

            df = source.fetch([request], "2024-01-01", "2024-01-01")

        # Verify assetId was used by default
        call_kwargs = mock_dataset_instance.get_data.call_args[1]
        assert "assetId" in call_kwargs
        assert call_kwargs["assetId"] == ["MA4B66MW5E27UAL9SUX"]

    def test_fetch_id_type_with_other_params(self) -> None:
        """fetch passes id_type alongside other params."""
        from metapyle.sources.gsquant import GSQuantSource

        mock_dataset_instance = MagicMock()
        mock_dataset_instance.get_data.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "bbid": ["SPX"],
                "atmVol": [0.18],
            }
        )

        mock_dataset_class = MagicMock(return_value=mock_dataset_instance)

        with patch("metapyle.sources.gsquant._get_gsquant") as mock_get:
            mock_get.return_value = {"Dataset": mock_dataset_class, "GsSession": MagicMock()}

            source = GSQuantSource()
            request = FetchRequest(
                symbol="SPX",
                field="SWAPTION_VOL::atmVol",
                params={"id_type": "bbid", "tenor": "1y", "expirationTenor": "1m"},
            )

            source.fetch([request], "2024-01-01", "2024-01-01")

        # Verify bbid and other params were passed
        call_kwargs = mock_dataset_instance.get_data.call_args[1]
        assert call_kwargs["bbid"] == ["SPX"]
        assert call_kwargs["tenor"] == "1y"
        assert call_kwargs["expirationTenor"] == "1m"
        assert "id_type" not in call_kwargs  # Should be popped
```

**Step 2: Run tests to verify they pass**

Run: `pytest tests/unit/test_sources_gsquant.py::TestGSQuantIdType -v`
Expected: All PASS

**Step 3: Run full test suite**

Run: `pytest tests/unit/test_sources_gsquant.py -v`
Expected: All PASS

**Step 4: Commit**

```bash
git add tests/unit/test_sources_gsquant.py
git commit -m "test(gsquant): add tests for id_type parameter"
```

---

### Task 4: Update Documentation

**Files:**
- Modify: `docs/user-guide.md:883-902`

**Step 1: Replace the GS Quant section**

Replace lines 883-902 with:

```markdown
### GS Quant (`gsquant`)

Fetches data from GS Marquee platform via the `gs-quant` library.

**Requirements:**
- GS Quant session authenticated (call `GsSession.use()` before fetching)

**Symbol format:** Asset identifier (format depends on `id_type` parameter)

**Field format:** `dataset_id::value_column` (e.g., `FXIMPLIEDVOL::impliedVolatility`)

**Identifier Types:**

The `id_type` parameter in `params` specifies how to identify assets. Defaults to `assetId`.

| ID Type | Description |
|---------|-------------|
| `assetId` | GS Marquee Asset ID (default) |
| `bbid` | Bloomberg ID |
| `gsid` | GS Internal ID |
| `cusip` | CUSIP |
| `isin` | ISIN |
| `sedol` | SEDOL |
| `ric` | Reuters Instrument Code |
| `ticker` | Exchange ticker symbol |

Browse available datasets and their supported identifiers at the [GS Marquee Data Catalog](https://marquee.gs.com/s/data-services/catalog).

**Examples:**

```yaml
# Using GS Marquee Asset ID (default)
- my_name: cdx_ig_5y
  source: gsquant
  symbol: MANJG63BKCNEHYW3
  field: CDS_INDICES_LEVELS_V1_INTERNAL::spread
  params:
    id_type: assetId  # optional, this is the default

# Using Bloomberg ID
- my_name: itraxx_xover_5y
  source: gsquant
  symbol: ITRXEXE
  field: CDS_INDICES_LEVELS_V1_INTERNAL::spread
  params:
    id_type: bbid

# With additional query parameters
- my_name: spx_swaption_vol
  source: gsquant
  symbol: SPX
  field: SWAPTION_VOL::atmVol
  params:
    id_type: bbid
    tenor: 1y
    expirationTenor: 1m
```
```

**Step 2: Verify documentation renders correctly**

Review the markdown formatting is correct.

**Step 3: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: add id_type documentation for gsquant source"
```

---

### Task 5: Final Verification

**Step 1: Run full test suite**

Run: `pytest`
Expected: All PASS

**Step 2: Run linting**

Run: `ruff check .`
Expected: No errors

**Step 3: Run type checking**

Run: `mypy src/`
Expected: No errors

**Step 4: Create final commit if any fixes needed**

If any fixes were needed, commit them.

---

## Summary of Files Changed

| File | Change |
|------|--------|
| `src/metapyle/sources/gsquant.py` | Extract id_type from params, use dynamically in API call and pivot |
| `tests/unit/test_sources_gsquant.py` | Update mocks to use assetId, add TestGSQuantIdType class |
| `docs/user-guide.md` | Add id_type documentation with table and examples |
