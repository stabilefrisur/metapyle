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
  frequency: daily
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

Create a file called `catalog.yaml`:

```yaml
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  frequency: daily
  description: S&P 500 closing price

- my_name: us_gdp
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  description: US GDP in current dollars
  unit: USD billions
```

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
| `symbol` | Yes | Source-specific identifier |
| `frequency` | Yes | Data frequency: `daily`, `weekly`, `monthly`, `quarterly`, `annual` |
| `field` | No | Source-specific field (e.g., `PX_LAST` for Bloomberg) |
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
  frequency: daily
  description: S&P 500 closing price
  unit: points

- my_name: sp500_volume
  source: bloomberg
  symbol: SPX Index
  field: PX_VOLUME
  frequency: daily
  description: S&P 500 trading volume

# Macro data
- my_name: us_gdp
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  description: US GDP in current dollars
  unit: USD billions

- my_name: us_cpi_yoy
  source: bloomberg
  symbol: CPI YOY Index
  frequency: monthly
  description: US CPI year-over-year change
  unit: percent
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
- **Invalid frequency** → `CatalogValidationError`
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
df = client.get_raw(
    source="bloomberg",
    symbol="AAPL US Equity",
    field="PX_LAST",
    start="2024-01-01",
    end="2024-12-31"
)
```

`get_raw()` returns a DataFrame with a single `value` column:

```python
#             value
# 2024-01-02  185.64
# 2024-01-03  184.25
```

### Inspecting Metadata

Use `get_metadata()` to see details about a catalog entry:

```python
meta = client.get_metadata("sp500_close")
print(meta)
# {
#     'my_name': 'sp500_close',
#     'source': 'bloomberg',
#     'symbol': 'SPX Index',
#     'frequency': 'daily',
#     'field': 'PX_LAST',
#     'description': 'S&P 500 closing price',
#     'unit': 'points',
#     ...
# }
```

### Bypassing Cache

By default, queries use the cache. To force a fresh fetch:

```python
# Bypass cache for this query
df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31", use_cache=False)

# Also works with get_raw()
df = client.get_raw(
    source="bloomberg",
    symbol="SPX Index",
    start="2024-01-01",
    end="2024-12-31",
    use_cache=False
)
```

### Mixing Frequencies

When fetching multiple series with different frequencies, you must specify a target `frequency` parameter. Without it, metapyle raises a `FrequencyMismatchError`. See [Frequency Alignment](#frequency-alignment) for details.

```python
# ❌ Raises FrequencyMismatchError: different frequencies without alignment
df = client.get(["sp500_close", "us_gdp"], start="2024-01-01", end="2024-12-31")

# ✅ Works: align everything to monthly
df = client.get(
    ["sp500_close", "us_gdp"],  # daily + quarterly
    start="2024-01-01",
    end="2024-12-31",
    frequency="monthly"
)
```

---

## Frequency Alignment

When combining data series with different native frequencies, metapyle can automatically resample them to a common frequency.

### Supported Frequencies

| Frequency | Value | Example Dates |
|-----------|-------|---------------|
| Daily | `daily` | 2024-01-02, 2024-01-03, ... |
| Weekly | `weekly` | 2024-01-07, 2024-01-14, ... |
| Monthly | `monthly` | 2024-01-31, 2024-02-29, ... |
| Quarterly | `quarterly` | 2024-03-31, 2024-06-30, ... |
| Annual | `annual` | 2024-12-31, 2025-12-31, ... |

### How Resampling Works

**Downsampling** (e.g., daily → monthly):
- Takes the **last value** of each period
- Example: Monthly value = last trading day's close

**Upsampling** (e.g., quarterly → monthly):
- **Forward-fills** values until the next data point
- Example: Q1 GDP value appears for Jan, Feb, Mar

### When to Use Alignment

Use the `frequency` parameter when:
- Combining series with different native frequencies
- You need output at a specific frequency for your analysis

```python
# Combine daily equity prices with quarterly GDP
df = client.get(
    ["sp500_close", "us_gdp"],
    start="2024-01-01",
    end="2024-12-31",
    frequency="monthly"
)
```

### Example: Daily + Quarterly → Monthly

```python
# sp500_close: daily data
# us_gdp: quarterly data
df = client.get(
    ["sp500_close", "us_gdp"],
    start="2024-01-01",
    end="2024-06-30",
    frequency="monthly"
)

print(df)
#             sp500_close    us_gdp
# 2024-01-31      4845.65   27956.0  # Q1 GDP forward-filled
# 2024-02-29      5096.27   27956.0  # Q1 GDP forward-filled
# 2024-03-31      5254.35   27956.0  # Q1 GDP (actual release month)
# 2024-04-30      5035.69   28279.0  # Q2 GDP forward-filled
# 2024-05-31      5277.51   28279.0  # Q2 GDP forward-filled
# 2024-06-30      5460.48   28279.0  # Q2 GDP (actual release month)
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
- Cache key: `(source, symbol, field, start_date, end_date)`
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

- Cache stores raw data **before** frequency alignment
- If you request the same data with different `frequency` parameters, the raw data is fetched from cache and resampled
- Cache misses are logged at DEBUG level—enable logging to diagnose cache behavior

---

## Data Sources

Metapyle supports multiple data sources through adapters. Each source has its own requirements and symbol format.

### Bloomberg (`bloomberg`)

Fetches data from Bloomberg Terminal via the `xbbg` library.

**Requirements:**
- `pip install metapyle[bloomberg]`
- Bloomberg Terminal running locally, OR
- Bloomberg Server API (B-PIPE) access

**Symbol format:** Standard Bloomberg tickers

| Type | Example |
|------|---------|
| Index | `SPX Index` |
| Equity | `AAPL US Equity` |
| Currency | `EURUSD Curncy` |
| Economic | `GDP CUR$ Index` |

**Field parameter:** Bloomberg field name (default: `PX_LAST`)

Common fields:
- `PX_LAST` – Last price
- `PX_OPEN`, `PX_HIGH`, `PX_LOW` – OHLC prices
- `PX_VOLUME` – Volume
- `CHG_PCT_1D` – 1-day percent change

**Catalog example:**

```yaml
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  frequency: daily

- my_name: aapl_volume
  source: bloomberg
  symbol: AAPL US Equity
  field: PX_VOLUME
  frequency: daily
```

### Local File (`localfile`)

Reads time-series data from CSV or Parquet files.

**Requirements:** None (included in base install)

**Symbol format:** File path (absolute or relative)

**Supported formats:**
- `.csv` – First column must be parseable as dates (used as index)
- `.parquet` – Must have a date column or DatetimeIndex

**File format requirements:**

CSV files should look like:
```csv
date,value
2024-01-02,4742.83
2024-01-03,4704.81
2024-01-04,4688.68
```

Or with a named column (will be renamed to `value`):
```csv
date,close_price
2024-01-02,4742.83
2024-01-03,4704.81
```

**Catalog example:**

```yaml
- my_name: internal_forecast
  source: localfile
  symbol: /data/forecasts/gdp_forecast.csv
  frequency: quarterly
  description: Internal GDP forecast

- my_name: proprietary_index
  source: localfile
  symbol: ./data/prop_index.parquet
  frequency: daily
```

---

## Error Handling

Metapyle uses a fail-fast approach: errors are raised immediately with clear messages. No partial results, no silent failures.

### Exception Hierarchy

All metapyle exceptions inherit from `MetapyleError`, so you can catch everything with a single handler:

```python
from metapyle import Client, MetapyleError

try:
    with Client(catalog="catalog.yaml") as client:
        df = client.get(["sp500_close"], start="2024-01-01", end="2024-12-31")
except MetapyleError as e:
    print(f"Metapyle error: {e}")
```

### Common Errors

| Exception | When It Occurs | What To Do |
|-----------|----------------|------------|
| `CatalogValidationError` | Catalog YAML is malformed or missing required fields | Check YAML syntax and required fields |
| `DuplicateNameError` | Same `my_name` appears in multiple catalog entries | Use unique names across all catalog files |
| `SymbolNotFoundError` | Requested name not in catalog | Check spelling, verify entry exists in catalog |
| `UnknownSourceError` | Catalog references unregistered source | Check source name spelling (`bloomberg`, `localfile`) |
| `FrequencyMismatchError` | Multiple series have different frequencies without alignment | Add `frequency` parameter to align |
| `FetchError` | Data retrieval failed (API error, file not found) | Check source availability, credentials, file path |
| `NoDataError` | Source returned empty data for the request | Verify symbol exists and date range has data |

### Example Error Messages

**Symbol not found:**
```
SymbolNotFoundError: Symbol not found in catalog: spx_close. 
Available: sp500_close, nasdaq_close, us_gdp, us_cpi_yoy, eur_usd...
```

**Frequency mismatch:**
```
FrequencyMismatchError: Symbols have different frequencies: 
sp500_close=daily, us_gdp=quarterly. 
Specify a frequency parameter for alignment.
```

**File not found (localfile source):**
```
FetchError: File not found: /data/missing_file.csv
```

### Catching Specific Errors

```python
from metapyle import (
    Client,
    SymbolNotFoundError,
    FrequencyMismatchError,
    FetchError,
)

with Client(catalog="catalog.yaml") as client:
    try:
        df = client.get(["sp500_close", "us_gdp"], start="2024-01-01", end="2024-12-31")
    except SymbolNotFoundError as e:
        print(f"Check your symbol names: {e}")
    except FrequencyMismatchError:
        # Retry with frequency alignment
        df = client.get(
            ["sp500_close", "us_gdp"],
            start="2024-01-01",
            end="2024-12-31",
            frequency="monthly"
        )
    except FetchError as e:
        print(f"Data fetch failed: {e}")
```

---

## Architecture Overview

This section provides a high-level view of how metapyle works. You don't need to understand this to use the library, but it helps explain what's happening under the hood.

### Component Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                      Your Code                               │
│                                                              │
│   client.get(["sp500_close", "us_gdp"], ...)                 │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│                       Client                                 │
│                                                              │
│  • Resolves catalog names to source details                  │
│  • Coordinates caching and fetching                          │
│  • Applies frequency alignment                               │
│  • Assembles final DataFrame                                 │
└──────┬──────────────────┬──────────────────┬─────────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐
│   Catalog    │  │    Cache     │  │        Sources           │
│              │  │              │  │                          │
│ YAML files   │  │ SQLite DB    │  │ ┌──────────┐ ┌─────────┐ │
│ with name    │  │ stores       │  │ │Bloomberg │ │Localfile│ │
│ mappings     │  │ fetched data │  │ └──────────┘ └─────────┘ │
└──────────────┘  └──────────────┘  └──────────────────────────┘
```

### Components

**Client**  
The main entry point you interact with. It loads your catalog, manages the cache, and coordinates data fetching from sources. When you call `get()`, the client handles everything and returns a clean DataFrame.

**Catalog**  
A collection of entries loaded from one or more YAML files. Each entry maps a `my_name` to source-specific details (symbol, field, frequency). The catalog is validated on load—if there are errors, you'll know immediately.

**Cache**  
An SQLite database that stores previously fetched data. When you request data, the client checks the cache first. If the data exists (and covers your date range), it's returned without hitting the source. This speeds up repeated queries and reduces API calls.

**Sources**  
Adapters that know how to fetch data from specific providers. Each source (Bloomberg, local file, etc.) implements the same interface, so the client can treat them uniformly. Sources are registered by name and instantiated on demand.

### What Happens When You Call `get()`

1. **Resolve names** → Client looks up each `my_name` in the catalog to get source details
2. **Check frequencies** → If series have different frequencies and no alignment specified, raise error
3. **For each symbol:**
   - Check cache for existing data
   - If cache miss, fetch from source
   - Store fetched data in cache
4. **Apply alignment** → If `frequency` parameter provided, resample each series
5. **Assemble DataFrame** → Combine all series into a wide DataFrame with columns named by `my_name`
6. **Return** → You get a clean pandas DataFrame ready for analysis
