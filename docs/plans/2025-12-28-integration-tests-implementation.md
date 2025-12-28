# Integration Tests Implementation Plan

> **REQUIRED SUB-SKILL:** Read and follow the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Add integration tests for Bloomberg and Macrobond sources that users run to verify their installation works.

**Architecture:** pytest-based integration tests with YAML catalog fixtures, pytest markers for selective execution, and `--run-private` flag for private series. Tests organized by source (bloomberg, macrobond, cross-source).

**Tech Stack:** pytest, pytest markers, YAML fixtures

---

## Task 1: Add pytest markers to pyproject.toml

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add new markers**

Add `bloomberg`, `macrobond`, and `private` markers to the existing `[tool.pytest.ini_options]` section:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = ["-v", "--strict-markers", "--tb=short"]
markers = [
    "integration: integration tests requiring external credentials",
    "bloomberg: tests requiring Bloomberg access",
    "macrobond: tests requiring Macrobond access",
    "private: tests requiring private/in-house series (skipped by default)",
]
```

**Step 2: Verify configuration**

Run: `pytest --markers | grep -E "(integration|bloomberg|macrobond|private)"`

Expected: All four markers listed with descriptions.

**Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "test(integration): add pytest markers for integration tests"
```

---

## Task 2: Create Bloomberg catalog fixture

**Files:**
- Create: `tests/integration/fixtures/bloomberg.yaml`

**Step 1: Create the fixture file**

```yaml
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  description: S&P 500 closing price
  unit: points

- my_name: sp500_volume
  source: bloomberg
  symbol: SPX Index
  field: PX_VOLUME
  description: S&P 500 trading volume

- my_name: us_gdp
  source: bloomberg
  symbol: GDP CUR$ Index
  description: US GDP in current dollars
  unit: USD billions

- my_name: us_cpi_yoy
  source: bloomberg
  symbol: CPI YOY Index
  description: US CPI year-over-year change
  unit: percent
```

**Step 2: Verify YAML is valid**

Run: `python -c "import yaml; yaml.safe_load(open('tests/integration/fixtures/bloomberg.yaml'))"`

Expected: No error.

**Step 3: Commit**

```bash
git add tests/integration/fixtures/bloomberg.yaml
git commit -m "test(integration): add bloomberg catalog fixture"
```

---

## Task 3: Create Macrobond catalog fixture

**Files:**
- Create: `tests/integration/fixtures/macrobond.yaml`

**Step 1: Create the fixture file**

```yaml
- my_name: sp500_mb
  source: macrobond
  symbol: ih:bl:spx index
  description: S&P 500 Index
  unit: points

- my_name: us_gdp_mb
  source: macrobond
  symbol: usnaac0169
  description: US GDP constant prices SA AR
  unit: USD trillions

- my_name: cmbs_bbb
  source: macrobond
  symbol: ih:mb:priv:xsa_spread_cmbs_bbb
  description: CMBS BBB spread
  unit: basis points
```

**Step 2: Verify YAML is valid**

Run: `python -c "import yaml; yaml.safe_load(open('tests/integration/fixtures/macrobond.yaml'))"`

Expected: No error.

**Step 3: Commit**

```bash
git add tests/integration/fixtures/macrobond.yaml
git commit -m "test(integration): add macrobond catalog fixture"
```

---

## Task 4: Create combined catalog fixture

**Files:**
- Create: `tests/integration/fixtures/combined.yaml`

**Step 1: Create the fixture file**

Merge both Bloomberg and Macrobond entries:

```yaml
# Bloomberg
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  description: S&P 500 closing price
  unit: points

- my_name: sp500_volume
  source: bloomberg
  symbol: SPX Index
  field: PX_VOLUME
  description: S&P 500 trading volume

- my_name: us_gdp
  source: bloomberg
  symbol: GDP CUR$ Index
  description: US GDP in current dollars
  unit: USD billions

- my_name: us_cpi_yoy
  source: bloomberg
  symbol: CPI YOY Index
  description: US CPI year-over-year change
  unit: percent

# Macrobond
- my_name: sp500_mb
  source: macrobond
  symbol: ih:bl:spx index
  description: S&P 500 Index
  unit: points

- my_name: us_gdp_mb
  source: macrobond
  symbol: usnaac0169
  description: US GDP constant prices SA AR
  unit: USD trillions

- my_name: cmbs_bbb
  source: macrobond
  symbol: ih:mb:priv:xsa_spread_cmbs_bbb
  description: CMBS BBB spread
  unit: basis points
```

**Step 2: Verify YAML is valid**

Run: `python -c "import yaml; yaml.safe_load(open('tests/integration/fixtures/combined.yaml'))"`

Expected: No error.

**Step 3: Commit**

```bash
git add tests/integration/fixtures/combined.yaml
git commit -m "test(integration): add combined catalog fixture"
```

---

## Task 5: Create integration conftest.py with fixtures and hooks

**Files:**
- Create: `tests/integration/conftest.py`

**Step 1: Create conftest with pytest hooks and fixtures**

```python
"""Integration test configuration and fixtures."""

from pathlib import Path

import pytest

from metapyle import Client

# Fixture paths
FIXTURES_DIR = Path(__file__).parent / "fixtures"
BLOOMBERG_CATALOG = FIXTURES_DIR / "bloomberg.yaml"
MACROBOND_CATALOG = FIXTURES_DIR / "macrobond.yaml"
COMBINED_CATALOG = FIXTURES_DIR / "combined.yaml"

# Test date range (guaranteed to have data)
TEST_START = "2024-01-01"
TEST_END = "2024-06-30"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --run-private option for private series tests."""
    parser.addoption(
        "--run-private",
        action="store_true",
        default=False,
        help="Run tests that require private/in-house series",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip private tests unless --run-private is passed."""
    if config.getoption("--run-private"):
        return

    skip_private = pytest.mark.skip(reason="Need --run-private option to run")
    for item in items:
        if "private" in item.keywords:
            item.add_marker(skip_private)


@pytest.fixture
def bloomberg_client() -> Client:
    """Client configured with Bloomberg catalog."""
    return Client(catalog=str(BLOOMBERG_CATALOG), cache_enabled=False)


@pytest.fixture
def macrobond_client() -> Client:
    """Client configured with Macrobond catalog."""
    return Client(catalog=str(MACROBOND_CATALOG), cache_enabled=False)


@pytest.fixture
def combined_client() -> Client:
    """Client configured with combined catalog (both sources)."""
    return Client(catalog=str(COMBINED_CATALOG), cache_enabled=False)


@pytest.fixture
def test_start() -> str:
    """Test start date."""
    return TEST_START


@pytest.fixture
def test_end() -> str:
    """Test end date."""
    return TEST_END
```

**Step 2: Verify imports work**

Run: `python -c "from tests.integration.conftest import BLOOMBERG_CATALOG; print(BLOOMBERG_CATALOG)"`

Expected: Path to bloomberg.yaml printed.

**Step 3: Commit**

```bash
git add tests/integration/conftest.py
git commit -m "test(integration): add conftest with fixtures and --run-private hook"
```

---

## Task 6: Create Bloomberg test file - single series

**Files:**
- Create: `tests/integration/test_bloomberg.py`

**Step 1: Create test file with single series test**

```python
"""Integration tests for Bloomberg source."""

import pandas as pd
import pytest

from metapyle import Client

pytestmark = [pytest.mark.integration, pytest.mark.bloomberg]


class TestBloombergSingleSeries:
    """Test single series fetch from Bloomberg."""

    def test_single_series(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close and verify DataFrame structure."""
        df = bloomberg_client.get(["sp500_close"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) > 0
```

**Step 2: Run test (requires Bloomberg)**

Run: `pytest tests/integration/test_bloomberg.py::TestBloombergSingleSeries::test_single_series -v`

Expected: PASS (if Bloomberg is available).

**Step 3: Commit**

```bash
git add tests/integration/test_bloomberg.py
git commit -m "test(integration): add bloomberg single series test"
```

---

## Task 7: Add Bloomberg multiple fields test

**Files:**
- Modify: `tests/integration/test_bloomberg.py`

**Step 1: Add test for multiple fields of same symbol**

Append to the test file:

```python
class TestBloombergMultipleFields:
    """Test multiple fields of same symbol."""

    def test_multiple_fields_same_symbol(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close and sp500_volume (same symbol, different fields)."""
        df = bloomberg_client.get(
            ["sp500_close", "sp500_volume"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "sp500_volume" in df.columns
        assert len(df.columns) == 2
```

**Step 2: Run test**

Run: `pytest tests/integration/test_bloomberg.py::TestBloombergMultipleFields -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_bloomberg.py
git commit -m "test(integration): add bloomberg multiple fields test"
```

---

## Task 8: Add Bloomberg frequency alignment test

**Files:**
- Modify: `tests/integration/test_bloomberg.py`

**Step 1: Add frequency alignment test**

Append to the test file:

```python
class TestBloombergFrequencyAlignment:
    """Test frequency alignment with Bloomberg data."""

    def test_frequency_alignment(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch daily and monthly data with alignment to month-end."""
        df = bloomberg_client.get(
            ["sp500_close", "us_cpi_yoy"],  # daily + monthly
            start=test_start,
            end=test_end,
            frequency="ME",  # month-end
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "us_cpi_yoy" in df.columns
        # Month-end frequency should have ~6 rows for Jan-Jun
        assert len(df) >= 5
        assert len(df) <= 7
```

**Step 2: Run test**

Run: `pytest tests/integration/test_bloomberg.py::TestBloombergFrequencyAlignment -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_bloomberg.py
git commit -m "test(integration): add bloomberg frequency alignment test"
```

---

## Task 9: Add Bloomberg get_raw and get_metadata tests

**Files:**
- Modify: `tests/integration/test_bloomberg.py`

**Step 1: Add get_raw and get_metadata tests**

Append to the test file:

```python
class TestBloombergRawAndMetadata:
    """Test get_raw and get_metadata for Bloomberg."""

    def test_get_raw(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Ad-hoc query using get_raw."""
        df = bloomberg_client.get_raw(
            source="bloomberg",
            symbol="SPX Index",
            field="PX_LAST",
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "SPX Index::PX_LAST" in df.columns

    def test_get_metadata(
        self,
        bloomberg_client: Client,
    ) -> None:
        """Verify metadata retrieval."""
        meta = bloomberg_client.get_metadata("sp500_close")

        assert isinstance(meta, dict)
        assert meta["my_name"] == "sp500_close"
        assert meta["source"] == "bloomberg"
        assert meta["symbol"] == "SPX Index"
```

**Step 2: Run tests**

Run: `pytest tests/integration/test_bloomberg.py::TestBloombergRawAndMetadata -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_bloomberg.py
git commit -m "test(integration): add bloomberg get_raw and get_metadata tests"
```

---

## Task 10: Add Bloomberg cache and recent data tests

**Files:**
- Modify: `tests/integration/test_bloomberg.py`

**Step 1: Add cache hit and recent data tests**

Append to the test file:

```python
import time
from datetime import datetime, timedelta


class TestBloombergCacheAndRecent:
    """Test caching and recent data access."""

    def test_cache_hit(
        self,
        test_start: str,
        test_end: str,
        tmp_path: pytest.TempPathFactory,
    ) -> None:
        """Fetch twice and verify cache is used."""
        cache_path = tmp_path / "test_cache.db"
        client = Client(
            catalog=str(BLOOMBERG_CATALOG),
            cache_enabled=True,
            cache_path=str(cache_path),
        )

        # First fetch (cache miss)
        start_time = time.time()
        df1 = client.get(["sp500_close"], start=test_start, end=test_end)
        first_duration = time.time() - start_time

        # Second fetch (cache hit - should be faster)
        start_time = time.time()
        df2 = client.get(["sp500_close"], start=test_start, end=test_end)
        second_duration = time.time() - start_time

        assert df1.equals(df2)
        # Cache hit should be significantly faster
        assert second_duration < first_duration

        client.close()

    def test_recent_data(
        self,
        bloomberg_client: Client,
    ) -> None:
        """Fetch last 7 days to confirm credentials are current."""
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        df = bloomberg_client.get(["sp500_close"], start=start, end=end)

        assert isinstance(df, pd.DataFrame)
        # May be empty on weekends/holidays, but should not error
```

Note: Add import for `BLOOMBERG_CATALOG` at the top of the file:

```python
from tests.integration.conftest import BLOOMBERG_CATALOG
```

**Step 2: Run tests**

Run: `pytest tests/integration/test_bloomberg.py::TestBloombergCacheAndRecent -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_bloomberg.py
git commit -m "test(integration): add bloomberg cache and recent data tests"
```

---

## Task 11: Create Macrobond test file - single series

**Files:**
- Create: `tests/integration/test_macrobond.py`

**Step 1: Create test file with single series test**

```python
"""Integration tests for Macrobond source."""

import pandas as pd
import pytest

from metapyle import Client
from tests.integration.conftest import MACROBOND_CATALOG

pytestmark = [pytest.mark.integration, pytest.mark.macrobond]


class TestMacrobondSingleSeries:
    """Test single series fetch from Macrobond."""

    def test_single_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_mb and verify DataFrame structure."""
        df = macrobond_client.get(["sp500_mb"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) > 0
```

**Step 2: Run test (requires Macrobond)**

Run: `pytest tests/integration/test_macrobond.py::TestMacrobondSingleSeries -v`

Expected: PASS (if Macrobond is available).

**Step 3: Commit**

```bash
git add tests/integration/test_macrobond.py
git commit -m "test(integration): add macrobond single series test"
```

---

## Task 12: Add Macrobond frequency alignment tests (client and unified)

**Files:**
- Modify: `tests/integration/test_macrobond.py`

**Step 1: Add client-side and server-side alignment tests**

Append to the test file:

```python
class TestMacrobondFrequencyAlignment:
    """Test frequency alignment with Macrobond data."""

    def test_frequency_alignment_client(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch daily and quarterly data with client-side alignment."""
        df = macrobond_client.get(
            ["sp500_mb", "us_gdp_mb"],  # daily + quarterly
            start=test_start,
            end=test_end,
            frequency="ME",  # month-end alignment
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_mb" in df.columns
        assert "us_gdp_mb" in df.columns

    def test_frequency_alignment_unified(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch via get_raw with unified=True for server-side alignment."""
        df = macrobond_client.get_raw(
            source="macrobond",
            symbol="usnaac0169",
            start=test_start,
            end=test_end,
            unified=True,
            frequency="Monthly",
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
```

**Step 2: Run tests**

Run: `pytest tests/integration/test_macrobond.py::TestMacrobondFrequencyAlignment -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_macrobond.py
git commit -m "test(integration): add macrobond frequency alignment tests"
```

---

## Task 13: Add Macrobond get_raw, get_metadata, and private series tests

**Files:**
- Modify: `tests/integration/test_macrobond.py`

**Step 1: Add get_raw, get_metadata, and private series tests**

Append to the test file:

```python
class TestMacrobondRawAndMetadata:
    """Test get_raw and get_metadata for Macrobond."""

    def test_get_raw(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Ad-hoc query using get_raw."""
        df = macrobond_client.get_raw(
            source="macrobond",
            symbol="usgdp",
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_metadata(
        self,
        macrobond_client: Client,
    ) -> None:
        """Verify metadata retrieval."""
        meta = macrobond_client.get_metadata("sp500_mb")

        assert isinstance(meta, dict)
        assert meta["my_name"] == "sp500_mb"
        assert meta["source"] == "macrobond"


@pytest.mark.private
class TestMacrobondPrivateSeries:
    """Test private/in-house Macrobond series."""

    def test_private_series(
        self,
        macrobond_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch private cmbs_bbb series."""
        df = macrobond_client.get(["cmbs_bbb"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "cmbs_bbb" in df.columns
```

**Step 2: Run tests (excluding private)**

Run: `pytest tests/integration/test_macrobond.py -v --ignore-glob="*private*" -k "not private"`

Expected: PASS for non-private tests.

**Step 3: Commit**

```bash
git add tests/integration/test_macrobond.py
git commit -m "test(integration): add macrobond get_raw, metadata, and private series tests"
```

---

## Task 14: Add Macrobond cache test

**Files:**
- Modify: `tests/integration/test_macrobond.py`

**Step 1: Add cache hit test**

Append to the test file:

```python
import time


class TestMacrobondCache:
    """Test caching for Macrobond."""

    def test_cache_hit(
        self,
        test_start: str,
        test_end: str,
        tmp_path: pytest.TempPathFactory,
    ) -> None:
        """Fetch twice and verify cache is used."""
        cache_path = tmp_path / "test_cache.db"
        client = Client(
            catalog=str(MACROBOND_CATALOG),
            cache_enabled=True,
            cache_path=str(cache_path),
        )

        # First fetch (cache miss)
        start_time = time.time()
        df1 = client.get(["sp500_mb"], start=test_start, end=test_end)
        first_duration = time.time() - start_time

        # Second fetch (cache hit - should be faster)
        start_time = time.time()
        df2 = client.get(["sp500_mb"], start=test_start, end=test_end)
        second_duration = time.time() - start_time

        assert df1.equals(df2)
        # Cache hit should be significantly faster
        assert second_duration < first_duration

        client.close()
```

**Step 2: Run test**

Run: `pytest tests/integration/test_macrobond.py::TestMacrobondCache -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_macrobond.py
git commit -m "test(integration): add macrobond cache test"
```

---

## Task 15: Create cross-source test file

**Files:**
- Create: `tests/integration/test_cross_source.py`

**Step 1: Create test file with cross-source tests**

```python
"""Integration tests for cross-source queries."""

import pandas as pd
import pytest

from metapyle import Client
from tests.integration.conftest import COMBINED_CATALOG

pytestmark = [pytest.mark.integration, pytest.mark.bloomberg, pytest.mark.macrobond]


class TestCrossSourceSameFrequency:
    """Test cross-source queries with same frequency."""

    def test_cross_source_same_frequency(
        self,
        combined_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close (BBG) and sp500_mb (MB) - both daily."""
        df = combined_client.get(
            ["sp500_close", "sp500_mb"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "sp500_mb" in df.columns


class TestCrossSourceDifferentFrequency:
    """Test cross-source queries with different frequencies."""

    def test_cross_source_different_frequency(
        self,
        combined_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close (daily) and us_gdp_mb (quarterly) - outer join."""
        df = combined_client.get(
            ["sp500_close", "us_gdp_mb"],
            start=test_start,
            end=test_end,
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "us_gdp_mb" in df.columns

    def test_cross_source_aligned(
        self,
        combined_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch with frequency alignment to business daily."""
        df = combined_client.get(
            ["sp500_close", "us_gdp_mb"],
            start=test_start,
            end=test_end,
            frequency="B",  # business daily
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert "us_gdp_mb" in df.columns
        # Both columns should have values (forward-filled for quarterly)
        assert df["sp500_close"].notna().any()
        assert df["us_gdp_mb"].notna().any()
```

**Step 2: Run tests (requires both Bloomberg and Macrobond)**

Run: `pytest tests/integration/test_cross_source.py -v`

Expected: PASS.

**Step 3: Commit**

```bash
git add tests/integration/test_cross_source.py
git commit -m "test(integration): add cross-source tests"
```

---

## Task 16: Add documentation to user guide

**Files:**
- Modify: `docs/user-guide.md`

**Step 1: Add "Verifying Your Setup" section after Installation**

Insert after the "Verify Installation" subsection (around line 50):

```markdown
### Running Integration Tests

After installing metapyle, run integration tests to verify your data source connections work correctly.

**All integration tests** (requires both Bloomberg and Macrobond):

```bash
pytest -m integration
```

**Single source only:**

```bash
# Bloomberg only
pytest -m bloomberg

# Macrobond only
pytest -m macrobond
```

**What's tested:**

- Single and batch fetches from each source
- Frequency alignment (client-side and server-side for Macrobond)
- Caching behavior
- Metadata retrieval
- Cross-source queries

**Testing private/in-house series:**

Some tests use private Macrobond series and are skipped by default. To include them:

```bash
pytest -m integration --run-private
```

To add your own private series tests:
1. Add entries to `tests/integration/fixtures/macrobond.yaml`
2. Create tests marked with `@pytest.mark.private`
```

**Step 2: Verify markdown renders correctly**

Open the user guide and verify the new section appears correctly.

**Step 3: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: add integration testing section to user guide"
```

---

## Task 17: Final verification

**Step 1: Run all unit tests to ensure no regressions**

Run: `pytest tests/unit/ -v`

Expected: All tests PASS.

**Step 2: Run ruff check**

Run: `ruff check tests/integration/`

Expected: No errors.

**Step 3: Run mypy**

Run: `mypy tests/integration/`

Expected: No errors.

**Step 4: Run integration tests (if Bloomberg and Macrobond available)**

Run: `pytest -m integration -v`

Expected: All tests PASS.

**Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "test(integration): fix any linting/typing issues"
```

---

## Summary

| Task | Description |
|------|-------------|
| 1 | Add pytest markers to pyproject.toml |
| 2 | Create Bloomberg catalog fixture |
| 3 | Create Macrobond catalog fixture |
| 4 | Create combined catalog fixture |
| 5 | Create integration conftest.py |
| 6-10 | Bloomberg tests (single, multi-field, alignment, raw, metadata, cache, recent) |
| 11-14 | Macrobond tests (single, alignment client/unified, raw, metadata, private, cache) |
| 15 | Cross-source tests |
| 16 | User guide documentation |
| 17 | Final verification |

