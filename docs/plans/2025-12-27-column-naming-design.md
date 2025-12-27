# Column Naming Redesign

## Problem

Column naming loses information — sources rename to `value`, losing the original column name. This is especially problematic for:

- **`get_raw()`**: Returns `value` instead of meaningful source-specific names
- **LocalFile**: The original CSV/Parquet column name is discarded

Additionally, LocalFile's current interface uses `symbol` as a file path, which is unintuitive. It should use `symbol` as the column name to extract.

## Solution

### 1. Sources Return Original Column Names

Sources stop normalizing to `value` and instead return meaningful column names:

- **LocalFile**: Returns column as-is from the file (e.g., `GDP_US`)
- **Bloomberg**: Returns `symbol_field` (e.g., `SPX Index_PX_LAST`)

### 2. Add `path` Field to CatalogEntry

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    my_name: str
    source: str
    symbol: str         # For localfile: column name to extract
    field: str | None = None
    path: str | None = None  # For localfile: file path
    description: str | None = None
    unit: str | None = None
```

### 3. LocalFile Interface Changes

- `symbol` = column name to extract from file
- `path` = file path (passed via kwargs from catalog or `get_raw()`)

```python
def fetch(
    self,
    symbol: str,        # column name to extract
    start: str,
    end: str,
    *,
    path: str | None = None,  # file path (required)
    **kwargs: Any,
) -> pd.DataFrame:
```

### 4. Client Behavior

- **`get()`**: Renames source column → `my_name` in `_assemble_dataframe()`
- **`get_raw()`**: Passes through source's original column name

### 5. `get_raw()` Signature Update

```python
def get_raw(
    self,
    source: str,
    symbol: str,
    start: str,
    end: str,
    *,
    field: str | None = None,
    path: str | None = None,  # new
    use_cache: bool = True,
) -> pd.DataFrame:
```

## Data Flow

### `get()` Flow (catalog-based)

```
Client.get(["gdp_us"])
  → Catalog.get("gdp_us") → CatalogEntry(symbol="GDP_US", path="/data/macro.csv")
  → LocalFileSource.fetch("GDP_US", path="/data/macro.csv")
  → Returns DataFrame with column "GDP_US"
  → _assemble_dataframe() renames "GDP_US" → "gdp_us" (my_name)
  → Final DataFrame has column "gdp_us"
```

### `get_raw()` Flow (bypass catalog)

```
Client.get_raw(source="localfile", symbol="GDP_US", path="/data/macro.csv")
  → LocalFileSource.fetch("GDP_US", path="/data/macro.csv")
  → Returns DataFrame with column "GDP_US"
  → Passed through as-is
```

### `get_raw()` for Bloomberg

```
Client.get_raw(source="bloomberg", symbol="SPX Index", field="PX_LAST")
  → BloombergSource.fetch("SPX Index", field="PX_LAST")
  → Returns DataFrame with column "SPX Index_PX_LAST"
  → Passed through as-is
```

## Catalog YAML Example

```yaml
- my_name: gdp_us
  source: localfile
  symbol: GDP_US
  path: /data/macro.csv
  description: US Gross Domestic Product

- my_name: spx_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  description: S&P 500 closing price
```

## Cache Changes

Cache key becomes `(source, symbol, field, path, start_date, end_date)`:

```sql
CREATE TABLE cache_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    field TEXT,
    path TEXT,  -- new
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, symbol, field, path, start_date, end_date)
)
```

**Migration strategy**: Drop and recreate (simple, cache is ephemeral).

## Component Changes Summary

| Component | Change |
|-----------|--------|
| `CatalogEntry` | Add `path: str \| None = None` field |
| `LocalFileSource.fetch()` | `symbol` = column name, `path` = file path (from kwargs), returns column with original name |
| `BloombergSource.fetch()` | Returns column `symbol_field` instead of `value` |
| `Client.get()` | `_assemble_dataframe()` renames source column → `my_name` |
| `Client.get_raw()` | Add `path` parameter, passes through source column name |
| `Client._fetch_symbol()` | Pass `path` from `CatalogEntry` to source |
| `Cache` | Add `path` to key; drop+recreate on schema change |

## Error Handling

### LocalFileSource

- **`path` is None** → `FetchError("path is required for localfile source")`
- **Column not found** → `FetchError(f"Column '{symbol}' not found in {path}")`
- **File not found** → `FetchError(f"File not found: {path}")`

Source validates its own requirements (self-contained, clear error messages).
