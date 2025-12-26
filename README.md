# Metapyle

Unified interface for querying financial time-series data from multiple sources using human-readable catalog names.

## Features

- **Catalog-based queries**: Use human-readable names instead of source-specific symbols
- **Multi-source support**: Bloomberg (xbbg), local files (CSV/Parquet), and custom adapters
- **Fail-fast error handling**: No partial results, immediate actionable errors
- **Optional caching**: SQLite-based cache to reduce redundant API calls
- **Frequency alignment**: Automatic upsampling/downsampling with explicit control
- **Type-safe**: Full type hints with mypy support

## Installation

```bash
# Core functionality
pip install metapyle

# With Bloomberg support
pip install metapyle[bloomberg]
```

## Quick Start

```python
from metapyle import Client

# Initialize with catalog
client = Client(catalog="catalogs/financial.yaml")

# Query data by catalog names
df = client.get(["GDP_US", "CPI_EU"], start="2020-01-01", end="2024-12-31")

# Ad-hoc queries bypassing catalog
df = client.get_raw(
    source="bloomberg",
    symbol="SPX Index",
    field="PX_LAST",
    start="2020-01-01",
    end="2024-12-31"
)
```

## Catalog Example

```yaml
- my_name: GDP_US
  source: bloomberg
  symbol: GDP CUR$ Index
  frequency: quarterly
  description: US Gross Domestic Product
  unit: USD billions

- my_name: SPX_CLOSE
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  frequency: daily
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev,bloomberg]"

# Run tests
pytest                        # Unit tests only
pytest -m integration         # Integration tests (requires credentials)

# Type checking
mypy src/metapyle

# Linting and formatting
ruff check src/ tests/
ruff format src/ tests/
```

## Requirements

- Python 3.12+
- pandas >= 2.0.0
- pyyaml >= 6.0

## Documentation

See [docs/plans/2025-12-25-metapyle-design.md](docs/plans/2025-12-25-metapyle-design.md) for detailed design documentation.

## License

[Add license information]
