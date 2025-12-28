# Integration Tests Design

## Overview

Add integration tests that verify Bloomberg and Macrobond connections work correctly. Users run these after installation to confirm their data source access. Tests use pytest markers for selective execution.

## Goals

1. **Verify installation** - Users confirm their Bloomberg/Macrobond credentials and connections work
2. **Feature coverage** - Test key features: single fetch, batch fetch, frequency alignment, caching, metadata, cross-source queries

## Non-Goals

- LocalFile source testing (already covered by unit tests)
- CLI command for verification (users run pytest directly)
- CI automation (these require live credentials)

## File Structure

```
tests/integration/
├── fixtures/
│   ├── bloomberg.yaml      # Bloomberg catalog entries
│   ├── macrobond.yaml      # Macrobond catalog entries
│   └── combined.yaml       # Both sources for cross-source tests
├── conftest.py             # Shared fixtures and pytest hooks
├── test_bloomberg.py       # Bloomberg feature tests
├── test_macrobond.py       # Macrobond feature tests
└── test_cross_source.py    # Cross-source tests
```

## Catalog Fixtures

### bloomberg.yaml

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

### macrobond.yaml

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

### combined.yaml

Merges both bloomberg.yaml and macrobond.yaml entries.

## pytest Configuration

### Markers (pyproject.toml)

```toml
[tool.pytest.ini_options]
markers = [
    "integration: integration tests requiring external credentials",
    "bloomberg: tests requiring Bloomberg access",
    "macrobond: tests requiring Macrobond access",
    "private: tests requiring private/in-house series (skipped by default)"
]
```

### Private Series Hook (conftest.py)

Skip `@pytest.mark.private` tests unless `--run-private` flag is passed:

```python
def pytest_addoption(parser):
    parser.addoption("--run-private", action="store_true", help="Run private series tests")

def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-private"):
        skip_private = pytest.mark.skip(reason="Need --run-private to run")
        for item in items:
            if "private" in item.keywords:
                item.add_marker(skip_private)
```

## Test Cases

### test_bloomberg.py

| Test | Description |
|------|-------------|
| `test_single_series` | Fetch `sp500_close`, verify DataFrame with data |
| `test_multiple_fields_same_symbol` | Fetch `sp500_close` + `sp500_volume`, verify 2 columns |
| `test_frequency_alignment` | Fetch `sp500_close` (daily) + `us_cpi_yoy` (monthly) with `frequency="ME"` |
| `test_get_raw` | Ad-hoc query: `SPX Index` with `PX_LAST` |
| `test_get_metadata` | Verify metadata retrieval for `sp500_close` |
| `test_cache_hit` | Fetch twice, verify second uses cache |
| `test_recent_data` | Fetch last 7 days - confirms credentials current |

### test_macrobond.py

| Test | Description |
|------|-------------|
| `test_single_series` | Fetch `sp500_mb`, verify DataFrame with data |
| `test_frequency_alignment_client` | Fetch `sp500_mb` + `us_gdp_mb` with client-side alignment |
| `test_frequency_alignment_unified` | Fetch via `get_raw()` with `unified=True` - server-side alignment |
| `test_get_raw` | Ad-hoc query: `usgdp` |
| `test_get_metadata` | Verify metadata retrieval for `sp500_mb` |
| `test_private_series` | Fetch `cmbs_bbb` - marked `@pytest.mark.private` |
| `test_cache_hit` | Fetch twice, verify cache works |

### test_cross_source.py

| Test | Description |
|------|-------------|
| `test_cross_source_same_frequency` | `sp500_close` (BBG) + `sp500_mb` (MB) - both daily |
| `test_cross_source_different_frequency` | `sp500_close` (daily) + `us_gdp_mb` (quarterly) - outer join |
| `test_cross_source_aligned` | Same as above with `frequency="B"` - aligned output |

## Running Tests

```bash
# All integration tests (requires both Bloomberg and Macrobond)
pytest -m integration

# Single source
pytest -m bloomberg
pytest -m macrobond

# Include private series tests
pytest -m integration --run-private
```

## Documentation

Add "Verifying Your Setup" section to user-guide.md explaining:
- How to run integration tests
- What's tested
- How to include private series tests
- How to add custom private series tests

## Test Date Range

Use a fixed historical range that's guaranteed to have data:
- Start: `2024-01-01`
- End: `2024-06-30`

Exception: `test_recent_data` uses last 7 days to verify credentials are current.
