# Metapyle

[![PyPI](https://img.shields.io/pypi/v/metapyle)](https://pypi.org/project/metapyle/)
[![Python](https://img.shields.io/pypi/pyversions/metapyle)](https://pypi.org/project/metapyle/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Early-stage research framework** — Not for production use.

A unified interface for querying financial time-series data from multiple sources.

Requires Python 3.12+

## Overview

Financial data lives in many places—Bloomberg terminals, internal APIs, CSV exports—each with its own ticker syntax, authentication, and quirks. Metapyle provides a YAML-based catalog that maps human-readable names to source-specific details, giving you a single `client.get()` interface regardless of where the data comes from.

Currently supports Bloomberg (via xbbg), Macrobond (via macrobond-data-api), and local files (CSV/Parquet).

## Installation

```bash
uv add metapyle
```

Or with pip:

```bash
pip install metapyle
```

> **Note:** Bloomberg requires a Bloomberg Terminal. Macrobond requires the desktop app or Web API credentials.

## Quick Start

**1. Create a catalog file (`catalog.yaml`):**

```yaml
# Bloomberg
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST

# Macrobond
- my_name: us_gdp
  source: macrobond
  symbol: usnaac0169

# Local file
- my_name: gdp_forecast
  source: localfile
  symbol: GDP_FORECAST    # column name in the file
  path: /data/macro.csv   # file path
```

**2. Query data:**

```python
from metapyle import Client

with Client(catalog="catalog.yaml") as client:
    # end defaults to today
    df = client.get(["sp500_close", "us_gdp"], start="2024-01-01")
```

## Documentation

See the [User Guide](docs/user-guide.md) for complete documentation, including:

- Catalog configuration
- Creating catalogs from CSV
- Frequency alignment
- Caching
- Available data sources
- Error handling

## License

MIT License - see [LICENSE](LICENSE) for details.
