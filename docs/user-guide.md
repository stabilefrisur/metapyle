# Metapyle User Guide

> A unified interface for querying financial time-series data from multiple sources using human-readable catalog names.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Catalog Configuration](#catalog-configuration)
5. [Querying Data](#querying-data)
6. [Frequency Alignment](#frequency-alignment)
7. [Caching](#caching)
8. [Data Sources](#data-sources)
9. [Error Handling](#error-handling)
10. [Architecture Overview](#architecture-overview)

---

## Overview

Metapyle lets you query financial time-series data from multiple sources—Bloomberg, local files, and more—using simple, memorable names instead of source-specific identifiers.

### The Problem

Financial data lives in many places: Bloomberg terminals, internal APIs, CSV exports, vendor platforms. Each source has its own:

- Ticker syntax (`SPX Index` vs `^SPX` vs `SP500`)
- Authentication method
- API quirks and response formats

This makes research code brittle, hard to share, and full of source-specific boilerplate.

### The Solution

Metapyle introduces a **catalog**—a YAML file that maps human-readable names to source-specific details:

```yaml
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
```

Now your analysis code uses `sp500_close` everywhere. If the underlying source changes, you update the catalog once—not every script.

### Key Benefits

- **Unified interface** – Same `client.get()` call regardless of data source
- **Catalog-based naming** – Human-readable names your team agrees on
- **Automatic caching** – SQLite cache reduces redundant API calls
- **Frequency alignment** – Resample daily data to monthly (or vice versa) automatically
- **Fail-fast errors** – Clear error messages, no silent partial failures

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Client** | Main entry point. Load a catalog, query data. |
| **Catalog** | YAML file mapping names → source details |
| **Entry** | Single row in the catalog (one data series) |
| **Source** | Data provider adapter (Bloomberg, local file, etc.) |

### Supported Sources

| Source | Description | Status |
|--------|-------------|--------|
| `bloomberg` | Bloomberg Terminal via xbbg | Available |
| `localfile` | CSV and Parquet files | Available |
| More | Additional adapters in development | Coming soon |

---

## Installation

### Requirements

- Python 3.12 or higher
- pandas >= 2.0.0

### Basic Installation

```bash
pip install metapyle
```

### With Bloomberg Support

If you need Bloomberg data access, install with the `bloomberg` extra:

```bash
pip install metapyle[bloomberg]
```

This installs the `xbbg` package. You'll also need:
- Bloomberg Terminal running on your machine, OR
- Access to a Bloomberg Server API (B-PIPE)

### Verify Installation

```python
from metapyle import Client
print("metapyle installed successfully")
```

---

## Quick Start

This example shows the typical workflow: create a catalog, initialize a client, and query data.

### Step 1: Create a Catalog File

Create `catalog.yaml`:

```yaml
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST

- my_name: gdp_forecast
  source: localfile
  symbol: GDP_FORECAST       # column name in file
  path: /data/forecasts.csv  # file path
```

See [Catalog Configuration](#catalog-configuration) for full details.

### Step 2: Query Data

```python
from metapyle import Client

# Initialize client with your catalog
client = Client(catalog="catalog.yaml")

# Fetch a single series
df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31")
print(df.head())
#             sp500_close
# 2024-01-02      4742.83
# 2024-01-03      4704.81
# ...

# Fetch multiple series (same frequency)
df = client.get(
    ["sp500_close", "nasdaq_close"],
    start="2024-01-01",
    end="2024-12-31"
)

# Always close the client when done (or use context manager)
client.close()
```

### Step 3: Using Context Manager (Recommended)

```python
from metapyle import Client

with Client(catalog="catalog.yaml") as client:
    df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31")
    # Client automatically closes when exiting the block
```

---

## Catalog Configuration

The catalog is a YAML file that maps human-readable names to source-specific details. This is the heart of metapyle—define your data series once, use them everywhere.

### Entry Fields

| Field | Required | Description |
|-------|----------|-------------|
| `my_name` | Yes | Unique identifier you'll use in code |
| `source` | Yes | Data source adapter (`bloomberg`, `localfile`) |
| `symbol` | Yes | Source-specific identifier (column name for `localfile`) |
| `field` | No | Source-specific field (e.g., `PX_LAST` for Bloomberg) |
| `path` | No | File path for `localfile` source |
| `description` | No | Human-readable description |
| `unit` | No | Unit of measurement |

### Naming Convention

Use `lower_case_with_underscores` for `my_name`:

```yaml
# ✅ Good
- my_name: sp500_close
- my_name: us_gdp_nominal
- my_name: eur_usd_spot

# ❌ Avoid
- my_name: SP500_CLOSE      # uppercase
- my_name: sp500Close       # camelCase
- my_name: sp500-close      # hyphens
```

### Complete Example

```yaml
# Equity indices
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

# Macro data from Bloomberg
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

# Local file data
- my_name: internal_forecast
  source: localfile
  symbol: GDP_FORECAST       # column name in file
  path: /data/forecasts.csv  # file path
  description: Internal GDP forecast
```

### Organizing Large Catalogs

For large catalogs, split entries across multiple YAML files by asset class or theme:

```
catalogs/
├── equities.yaml
├── fixed_income.yaml
├── macro.yaml
└── fx.yaml
```

Load multiple catalog files by passing a list:

```python
client = Client(catalog=[
    "catalogs/equities.yaml",
    "catalogs/macro.yaml",
])
```

All entries are merged. If the same `my_name` appears in multiple files, metapyle raises a `DuplicateNameError`.

### Validation

Metapyle validates your catalog on load:

- **Missing required fields** → `CatalogValidationError`
- **Duplicate `my_name`** → `DuplicateNameError`
- **Unknown source** → `UnknownSourceError`

If validation fails, you'll get a clear error message pointing to the problem.

---

## Querying Data

### Basic Queries with `get()`

The `get()` method fetches data using catalog names. Pass symbols as a **list**, even for a single series:

```python
from metapyle import Client

with Client(catalog="catalog.yaml") as client:
    # Single series (note: still a list)
    df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31")
    
    # Multiple series (must have same frequency, or specify frequency param)
    df = client.get(
        ["sp500_close", "nasdaq_close"],
        start="2024-01-01",
        end="2024-12-31"
    )
```

### Return Format

`get()` returns a **wide DataFrame** with:
- **Index**: `DatetimeIndex` with dates
- **Columns**: One column per symbol, named by `my_name`

```python
df = client.get(["sp500_close", "nasdaq_close"], start="2024-01-01", end="2024-03-31")
print(df)
#             sp500_close  nasdaq_close
# 2024-01-02      4742.83      14765.94
# 2024-01-03      4704.81      14592.21
# ...
```

### Date Format

Dates must be in **ISO format** (`YYYY-MM-DD`):

```python
# ✅ Correct
df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31")

# ❌ Will not work
df = client.get(["sp500_close"], start="01/01/2024", end="12/31/2024")
```

### Ad-hoc Queries with `get_raw()`

Use `get_raw()` for one-off queries that bypass the catalog—useful for exploring new data or quick tests:

```python
# Bloomberg: returns column named 'symbol_field'
df = client.get_raw(
    source="bloomberg",
    symbol="AAPL US Equity",
    field="PX_LAST",
    start="2024-01-01",
    end="2024-12-31"
)
print(df.head())
#             AAPL US Equity_PX_LAST
# 2024-01-02                   185.64
# 2024-01-03                   184.25

# LocalFile: returns column with original name from file
df = client.get_raw(
    source="localfile",
    symbol="GDP_US",           # column name to extract
    path="/data/macro.csv",   # file path
    start="2024-01-01",
    end="2024-12-31"
)
print(df.head())
#             GDP_US
# 2024-01-02  27956.0
# 2024-01-03  27956.0
```

`get_raw()` returns a DataFrame with the source's original column name:

- **Bloomberg**: `symbol_field` (e.g., `AAPL US Equity_PX_LAST`)
- **LocalFile**: column name as-is from the file (e.g., `GDP_US`)

### Inspecting Metadata

Use `get_metadata()` to see details about a catalog entry:

```python
meta = client.get_metadata("sp500_close")
print(meta)
# {
#     'my_name': 'sp500_close',
#     'source': 'bloomberg',
#     'symbol': 'SPX Index',
#     'frequency': 'B',  # inferred from data (pandas alias, or None if irregular)
#     'field': 'PX_LAST',
#     'description': 'S&P 500 closing price',
#     'unit': 'points',
#     ...
# }
```

### Mixing Frequencies

When fetching multiple series with different frequencies, metapyle logs a warning and merges them using an outer join (which may produce NaN values). To avoid this, specify a `frequency` parameter to align all series. See [Frequency Alignment](#frequency-alignment) for details.

```python
# ⚠️ Warning logged: different frequencies merged with outer join (may have NaNs)
df = client.get(["sp500_close", "us_gdp"], start="2024-01-01", end="2024-12-31")

# ✅ Better: align everything to month-end
df = client.get(
    ["sp500_close", "us_gdp"],  # daily + quarterly
    start="2024-01-01",
    end="2024-12-31",
    frequency="ME"  # pandas month-end frequency
)
```

---

## Frequency Alignment

When combining data series with different native frequencies, metapyle can automatically resample them to a common frequency.

### Supported Frequencies

The `frequency` parameter accepts any valid [pandas frequency alias](https://pandas.pydata.org/docs/user_guide/timeseries.html#offset-aliases). Common options:

| Frequency | Alias | Example Dates |
|-----------|-------|---------------|
| Daily | `D` | 2024-01-02, 2024-01-03, ... |
| Business daily | `B` | 2024-01-02, 2024-01-03, ... (weekdays only) |
| Weekly | `W` | 2024-01-07, 2024-01-14, ... |
| Month-end | `ME` | 2024-01-31, 2024-02-29, ... |
| Business month-end | `BME` | 2024-01-31, 2024-02-29, ... (last business day) |
| Quarter-end | `QE` | 2024-03-31, 2024-06-30, ... |
| Year-end | `YE` | 2024-12-31, 2025-12-31, ... |

Invalid frequency strings cause pandas to raise a `ValueError`.

### How Resampling Works

**Downsampling** (e.g., daily → monthly):
- Takes the **last value** of each period
- Example: Monthly value = last trading day's close

**Upsampling** (e.g., quarterly → monthly):
- **Forward-fills** values until the next data point
- Example: Q1 GDP value appears for Jan, Feb, Mar

### Example

```python
# sp500_close: daily data, us_gdp: quarterly data
df = client.get(
    ["sp500_close", "us_gdp"],
    start="2024-01-01",
    end="2024-06-30",
    frequency="ME"  # month-end
)

print(df)
#             sp500_close    us_gdp
# 2024-01-31      4845.65   27956.0  # Q1 GDP forward-filled
# 2024-02-29      5096.27   27956.0
# 2024-03-31      5254.35   27956.0  # Q1 GDP (actual release month)
# 2024-04-30      5035.69   28279.0  # Q2 GDP forward-filled
# ...
```

### Data Interpretation Warning

Forward-filled values represent the **most recent available data point** carried forward until the next value appears. Be aware:

- Forward-filled GDP in January shows the *previous* quarter's value, not Q1
- This is not interpolation or estimation—it's simply repeating the last known value
- **Publication lag**: Economic data like GDP and CPI are typically published weeks or months after the period they refer to. Q1 GDP isn't available until late April. Metapyle does not track publication dates—if you need point-in-time accuracy, you'll need to handle that separately.

---

## Caching

Metapyle includes an SQLite-based cache to reduce redundant API calls. This is especially useful when working with APIs, where repeated queries can be slow.

### How It Works

- Cache is **enabled by default**
- Data is stored in a local SQLite database
- Cache key: `(source, symbol, field, path, start_date, end_date)`
- If you request a date range that's a subset of a cached range, the cached data is filtered and returned

### Default Cache Location

By default, the cache is stored at `./cache/data_cache.db` (relative to your current working directory). The directory is created automatically if it doesn't exist.

You can customize the cache location:

```python
# Custom cache path
client = Client(catalog="catalog.yaml", cache_path="/path/to/my_cache.db")
```

Or via environment variable:

```bash
# Linux/macOS
export METAPYLE_CACHE_PATH=/path/to/my_cache.db

# Windows
set METAPYLE_CACHE_PATH=C:\path\to\my_cache.db
```

The `cache_path` parameter takes precedence over the environment variable.

### Disabling Cache

Disable caching entirely when initializing the client:

```python
# No caching at all
client = Client(catalog="catalog.yaml", cache_enabled=False)
```

Or bypass cache for a single query:

```python
# Cache enabled globally, but skip for this query
df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31", use_cache=False)
```

### Clearing Cache

Clear cached data when you need fresh data or want to free disk space:

```python
# Clear cache for a specific symbol
client.clear_cache(symbol="sp500_close")

# Clear all cached data
client.clear_cache()
```

### Cache Behavior Notes

- Cache stores raw data **before** frequency alignment—resampling happens on retrieval
- Cache misses are logged at DEBUG level

---

## Data Sources

Metapyle supports multiple data sources through adapters. Each source has its own requirements and symbol format.

### Bloomberg (`bloomberg`)

Fetches data from Bloomberg Terminal via the `xbbg` library.

**Requirements:**
- `pip install metapyle[bloomberg]`
- Bloomberg Terminal running locally, OR Bloomberg Server API (B-PIPE) access

**Symbol format:** Standard Bloomberg tickers (`SPX Index`, `AAPL US Equity`, `EURUSD Curncy`)

**Field:** Bloomberg field name (default: `PX_LAST`). Common: `PX_LAST`, `PX_OPEN`, `PX_HIGH`, `PX_LOW`, `PX_VOLUME`

```yaml
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
```

### Local File (`localfile`)

Reads time-series data from CSV or Parquet files.

**Symbol:** Column name to extract (case-sensitive)

**Path:** File path (absolute or relative)

**File format:** First column = dates (index), remaining columns = data

```csv
date,GDP_US,CPI_US
2024-01-02,27956.0,308.4
2024-01-03,27956.0,308.5
```

```yaml
- my_name: gdp_us
  source: localfile
  symbol: GDP_US            # column name
  path: /data/macro.csv     # file path

- my_name: cpi_us
  source: localfile
  symbol: CPI_US            # same file, different column
  path: /data/macro.csv
```

---

## Error Handling

Metapyle uses a fail-fast approach: errors are raised immediately with clear messages. No partial results, no silent failures.

### Common Errors

| Exception | When It Occurs |
|-----------|----------------|
| `CatalogValidationError` | YAML malformed or missing required fields |
| `DuplicateNameError` | Same `my_name` in multiple entries |
| `SymbolNotFoundError` | Requested name not in catalog |
| `UnknownSourceError` | Unknown source name |
| `FetchError` | Data retrieval failed (API error, file/column not found) |
| `NoDataError` | Source returned empty data |
| `ValueError` | Invalid pandas frequency string |

All exceptions inherit from `MetapyleError`:

```python
from metapyle import Client, MetapyleError

try:
    with Client(catalog="catalog.yaml") as client:
        df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31")
except MetapyleError as e:
    print(f"Metapyle error: {e}")
```

---

## Architecture Overview

This section provides a high-level view of how metapyle works. You don't need to understand this to use the library, but it helps explain what's happening under the hood.

### Component Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                      Your Code                               │
│   client.get(["sp500_close", "us_gdp"], ...)                 │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│                       Client                                 │
│  Resolves names → Caching → Fetching → Alignment → Assemble  │
└──────┬──────────────────┬──────────────────┬─────────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐
│   Catalog    │  │    Cache     │  │        Sources           │
│  YAML files  │  │  SQLite DB   │  │  Bloomberg │ LocalFile   │
└──────────────┘  └──────────────┘  └──────────────────────────┘
```

### What Happens When You Call `get()`

1. **Resolve names** → Look up `my_name` in catalog
2. **For each symbol:** Check cache → fetch from source if miss → store in cache
3. **Apply alignment** → Resample if `frequency` parameter provided
4. **Assemble DataFrame** → Wide format with columns named by `my_name`
