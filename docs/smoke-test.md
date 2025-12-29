# Smoke Test: Verify Data Source Connections

Quick verification script to test your metapyle data source connections without running the full test suite.

---

## Prerequisites

- Python 3.12+
- metapyle installed (`pip install metapyle`)
- Source-specific requirements:
  - **Bloomberg**: Bloomberg Terminal running locally, OR B-PIPE access
  - **Macrobond**: Macrobond desktop app installed, OR Web API credentials
  - **GS Quant**: Authenticated session (`GsSession.use()` called)
  - **LocalFile**: A CSV or Parquet file with time-series data

---

## The Script

Copy this script and **uncomment the sections** for the sources you want to test:

```python
"""
Metapyle Smoke Test
Uncomment the sources you have access to and run this script.
"""
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from metapyle import Client

# Calculate date range: last 3 years (for low-frequency series)
end = datetime.now().strftime("%Y-%m-%d")
start = (datetime.now() - timedelta(days=3 * 365)).strftime("%Y-%m-%d")


def test_source(name: str, catalog_yaml: str) -> None:
    """Test a source using a temporary catalog file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(catalog_yaml)
        catalog_path = Path(f.name)
    try:
        with Client(catalog=catalog_path) as client:
            df = client.get([name], start=start, end=end)
        assert not df.empty, "DataFrame is empty"
        assert len(df.columns) == 1, f"Expected 1 column, got {len(df.columns)}"
        print(f"✓ {name}: {len(df)} rows")
        print(df.head(3).to_string())
        print("...")
        print(df.tail(3).to_string())
        print()
    finally:
        catalog_path.unlink()


print(f"Testing metapyle connections ({start} to {end})")
print("=" * 50)


# === BLOOMBERG ===
# Uncomment to test Bloomberg connection
# Requires: Bloomberg Terminal running or B-PIPE access
#
# test_source("bloomberg", """
# - my_name: spx_ind_bb
#   source: bloomberg
#   symbol: SPX Index
#   field: PX_LAST
# """)


# === MACROBOND ===
# Uncomment to test Macrobond connection
# Requires: Macrobond desktop app or Web API credentials
#
# test_source("macrobond", """
# - my_name: us_gdp_mb
#   source: macrobond
#   symbol: usnaac0169
# """)


# === GS QUANT ===
# Uncomment to test GS Quant connection
# Requires: Authenticated GsSession (call GsSession.use() first)
#
# from gs_quant.session import GsSession, Environment
# GsSession.use(Environment.PROD, client_id="YOUR_ID", client_secret="YOUR_SECRET")
#
# test_source("gsquant", """
# - my_name: spx_ivol_gs
#   source: gsquant
#   symbol: SPX
#   field: SWAPTION_VOL::atmVol
#   params:
#     tenor: 1y
#     expirationTenor: 1m
# """)


# === LOCALFILE ===
# Uncomment and edit to test local file reading
# Edit the path and symbol (column name) to match your file
#
# test_source("localfile", """
# - my_name: your_col
#   source: localfile
#   symbol: YOUR_COLUMN_NAME
#   path: /path/to/your/file.csv
# """)


print("=" * 50)
print("Smoke test complete")
```

---

## Understanding Results

### Success Output

```
Testing metapyle connections (2021-12-29 to 2024-12-29)
==================================================
✓ bloomberg: 756 rows
            bloomberg
2021-12-29    4778.73
2021-12-30    4778.73
2021-12-31    4766.18
...
            bloomberg
2024-12-26    5974.65
2024-12-27    5970.84
2024-12-29    5906.94

✓ macrobond: 12 rows
            macrobond
2022-03-31      24740
2022-06-30      25249
2022-09-30      25724
...
            macrobond
2024-03-31      28278
2024-06-30      28631
2024-09-30      29349

==================================================
Smoke test complete
```

- **Row count** varies by source and data frequency (daily vs quarterly)

### Failure Output

```
✗ Bloomberg: cannot find Bloomberg API (blpapi)
✗ Macrobond: Failed to connect to Macrobond
```

See [Troubleshooting](#troubleshooting) for common fixes.

---

## Troubleshooting

### Bloomberg

| Error | Cause | Fix |
|-------|-------|-----|
| `cannot find Bloomberg API (blpapi)` | blpapi not installed | `pip install blpapi` (requires Bloomberg C++ SDK) |
| `Connection refused` | Terminal not running | Start Bloomberg Terminal |
| `Invalid security` | Bad ticker | Verify ticker in Bloomberg Terminal |

### Macrobond

| Error | Cause | Fix |
|-------|-------|-----|
| `Failed to connect to Macrobond` | Desktop app not running | Start Macrobond application |
| `Series not found` | Invalid series name | Verify series in Macrobond app |
| `Authentication failed` | Web API credentials invalid | Check `MACROBOND_*` environment variables |

### GS Quant

| Error | Cause | Fix |
|-------|-------|-----|
| `Session not initialized` | No active session | Call `GsSession.use()` before fetching |
| `Unauthorized` | Invalid credentials | Verify client_id and client_secret |
| `Dataset not found` | Invalid field | Check dataset ID in GS Marquee |

### LocalFile

| Error | Cause | Fix |
|-------|-------|-----|
| `File not found` | Wrong path | Use absolute path, verify file exists |
| `Column not found` | Wrong symbol | Check column names in your file |
| `Could not parse dates` | Bad date format | Ensure first column is parseable dates |

---

## Next Steps

If the smoke test passes, you're ready to use metapyle! See the [User Guide](user-guide.md) for:

- Creating catalog files
- Querying multiple series
- Frequency alignment
- Caching options
