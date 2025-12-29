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

# Calculate date range: last 30 days
end = datetime.now().strftime("%Y-%m-%d")
start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")


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
    finally:
        catalog_path.unlink()


print(f"Testing metapyle connections ({start} to {end})")
print("=" * 50)


# === BLOOMBERG ===
# Uncomment to test Bloomberg connection
# Requires: Bloomberg Terminal running or B-PIPE access
#
# test_source("bloomberg", """
# - my_name: bloomberg
#   source: bloomberg
#   symbol: SPX Index
#   field: PX_LAST
# """)


# === MACROBOND ===
# Uncomment to test Macrobond connection
# Requires: Macrobond desktop app or Web API credentials
#
# test_source("macrobond", """
# - my_name: macrobond
#   source: macrobond
#   symbol: usgdp
# """)


# === GS QUANT ===
# Uncomment to test GS Quant connection
# Requires: Authenticated GsSession (call GsSession.use() first)
#
# from gs_quant.session import GsSession, Environment
# GsSession.use(Environment.PROD, client_id="YOUR_ID", client_secret="YOUR_SECRET")
#
# test_source("gsquant", """
# - my_name: gsquant
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
# - my_name: localfile
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
Testing metapyle connections (2024-12-01 to 2024-12-29)
==================================================
✓ bloomberg: 20 rows
✓ macrobond: 1 rows
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
