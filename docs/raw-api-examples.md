# Raw API Examples

> Minimal, functional examples showing the underlying API calls that metapyle abstracts.

This document shows the raw API calls for each data source that metapyle supports. Use these examples to understand what happens under the hood, or for testing and debugging outside of metapyle.

**Note:** Authentication is assumed to be handled externally (e.g., Bloomberg Terminal running, Macrobond credentials configured, GS Quant session initialized).

| Source | Library | Install |
|--------|---------|---------|
| Bloomberg | `xbbg` | `pip install xbbg` |
| Macrobond | `macrobond_data_api` | `pip install macrobond-data-api` |
| GS Quant | `gs_quant` | `pip install gs-quant` |
| LocalFile | `pandas` | `pip install pandas` |

---

## Bloomberg (xbbg)

xbbg is a Python wrapper around Bloomberg's `blpapi`. Metapyle uses `blp.bdh()` for historical data.

### Single Ticker, Single Field

```python
from xbbg import blp

df = blp.bdh("SPX Index", "PX_LAST", "2024-01-01", "2024-01-31")
```

```text
            SPX Index
              PX_LAST
2024-01-02    4742.83
2024-01-03    4704.81
2024-01-04    4688.68
```

### Multiple Tickers

```python
df = blp.bdh(["SPX Index", "AAPL US Equity"], "PX_LAST", "2024-01-01", "2024-01-31")
```

```text
            SPX Index  AAPL US Equity
              PX_LAST         PX_LAST
2024-01-02    4742.83          185.64
2024-01-03    4704.81          184.25
```

### Multiple Fields

```python
df = blp.bdh("SPX Index", ["PX_LAST", "PX_VOLUME"], "2024-01-01", "2024-01-31")
```

```text
            SPX Index              
              PX_LAST    PX_VOLUME
2024-01-02    4742.83     3.89e+09
2024-01-03    4704.81     4.12e+09
```

### Return Format

- Returns a pandas DataFrame with DatetimeIndex
- Columns are a MultiIndex: `(ticker, field)`
- Single ticker/field returns simplified columns

---

## Macrobond (macrobond_data_api)

Macrobond Data API provides access to Macrobond's economic and financial database.

### Single Series

```python
import macrobond_data_api as mda

series_list = mda.get_series(["usgdp"])
series = series_list[0]

df = series.values_to_pd_data_frame()
```

```text
         date         value
0  1947-01-01  2.033000e+11
1  1947-04-01  2.039000e+11
2  1947-07-01  2.043000e+11
```

### Multiple Series

```python
series_list = mda.get_series(["usgdp", "gbcpi"])

for series in series_list:
    df = series.values_to_pd_data_frame()
    print(f"{series.primary_name}: {len(df)} rows")
```

```text
usgdp: 312 rows
gbcpi: 1056 rows
```

### Converting to Wide DataFrame

```python
import pandas as pd

series_list = mda.get_series(["usgdp", "gbcpi"])

dfs = []
for series in series_list:
    df = series.values_to_pd_data_frame()
    df.index = pd.to_datetime(df["date"])
    df = df[["value"]].rename(columns={"value": series.primary_name})
    dfs.append(df)

result = dfs[0].join(dfs[1:], how="outer")
```

```text
              usgdp   gbcpi
1947-01-01  2.03e+11    NaN
1947-04-01  2.04e+11    NaN
...
1988-01-01  5.11e+12   66.1
```

### Return Format

- `get_series()` returns a list of `Series` objects
- Each series has `primary_name`, `is_error`, `error_message`
- `values_to_pd_data_frame()` returns DataFrame with `date` and `value` columns
- Full history returned; date filtering is done client-side

---

## GS Quant (gs_quant)

GS Quant provides access to Goldman Sachs Marquee datasets. Requires an authenticated session.

### Basic Fetch

```python
from gs_quant.data import Dataset
from gs_quant.session import GsSession

GsSession.use()

ds = Dataset("FXIMPLIEDVOL")
df = ds.get_data("2024-01-01", "2024-01-31", bbid=["EURUSD"])
```

```text
         date   bbid  tenor deltaStrike  impliedVolatility
0  2024-01-02  EURUSD    1m          DN               6.45
1  2024-01-02  EURUSD    3m          DN               6.82
2  2024-01-02  EURUSD    6m          DN               7.15
3  2024-01-03  EURUSD    1m          DN               6.51
```

### With Additional Parameters

```python
df = ds.get_data(
    "2024-01-01", "2024-01-31",
    bbid=["EURUSD"],
    tenor="3m",
    deltaStrike="DN"
)
```

```text
         date   bbid  tenor deltaStrike  impliedVolatility
0  2024-01-02  EURUSD    3m          DN               6.82
1  2024-01-03  EURUSD    3m          DN               6.89
2  2024-01-04  EURUSD    3m          DN               6.75
```

### Multiple Symbols

```python
df = ds.get_data(
    "2024-01-01", "2024-01-31",
    bbid=["EURUSD", "USDJPY"],
    tenor="1m",
    deltaStrike="DN"
)
```

```text
         date   bbid  tenor deltaStrike  impliedVolatility
0  2024-01-02  EURUSD    1m          DN               6.45
1  2024-01-02  USDJPY    1m          DN               8.92
2  2024-01-03  EURUSD    1m          DN               6.51
```

### Pivoting to Wide Format

```python
import pandas as pd

pivoted = pd.pivot_table(
    df,
    values="impliedVolatility",
    index=["date"],
    columns=["bbid"]
)
```

```text
              EURUSD  USDJPY
date
2024-01-02      6.45    8.92
2024-01-03      6.51    8.87
2024-01-04      6.38    8.79
```

### Return Format

- `get_data()` returns a long-format DataFrame
- Columns include `date`, `bbid`, and dataset-specific fields
- Use `pivot_table()` to convert to wide format for time-series analysis

---

## LocalFile (pandas)

Standard pandas file I/O for CSV and Parquet files with time-series data.

### Reading CSV

```python
import pandas as pd

df = pd.read_csv("data.csv", index_col=0, parse_dates=True)
```

```text
            GDP_US   CPI_US
2024-01-02  27956.0   308.4
2024-01-03  27956.0   308.5
2024-01-04  27960.0   308.6
```

### Reading Parquet

```python
df = pd.read_parquet("data.parquet")

# Set date column as index if needed
df = df.set_index("date")
df.index = pd.to_datetime(df.index)
```

```text
            GDP_US   CPI_US
2024-01-02  27956.0   308.4
2024-01-03  27956.0   308.5
2024-01-04  27960.0   308.6
```

### Expected File Format

CSV files should have dates in the first column:

```text
date,GDP_US,CPI_US
2024-01-02,27956.0,308.4
2024-01-03,27956.0,308.5
2024-01-04,27960.0,308.6
```
