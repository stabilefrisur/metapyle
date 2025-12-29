# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- GS Quant data source via gs-quant integration (optional dependency)
  - Fetch data from GS Marquee platform using Bloomberg IDs (bbid)
  - Field format `dataset_id::value_column` (e.g., `FXIMPLIEDVOL::impliedVolatility`)
  - Support for `params` field to pass additional query parameters (tenor, deltaStrike, etc.)
- `params` field in `CatalogEntry` for source-specific parameters
- `params` field in `FetchRequest` for passing parameters to source adapters
- Catalog CSV import/export tools:
  - `Catalog.csv_template()` generates blank or source-specific CSV templates
  - `Catalog.from_csv()` loads catalog entries from CSV files
  - `Catalog.to_csv()` exports catalog to CSV format
  - `Catalog.to_yaml()` exports catalog to YAML format
- Integration test infrastructure with pytest markers (`integration`, `bloomberg`, `macrobond`, `gsquant`)
- `--run-private` pytest flag for tests using private/in-house series

### Fixed

- Skip integration tests gracefully when services unavailable

## [0.1.1] - 2025-12-28

### Added

- Macrobond data source via macrobond-data-api integration (optional dependency)
  - `fetch()` with raw mode using `get_one_series`
  - `fetch()` with unified mode using `get_unified_series` for frequency/currency alignment
  - `get_metadata()` for retrieving series metadata
- `FetchRequest` dataclass exported in public API for custom source implementations
- `make_column_name()` utility function for consistent column naming across sources

### Changed

- `end` parameter in `get()` and `get_raw()` is now optional, defaulting to today's date
- **BREAKING:** `BaseSource.fetch()` now accepts `Sequence[FetchRequest]` instead of individual symbol parameters, enabling batch fetching for improved performance
- **BREAKING:** Bloomberg source column naming changed from `symbol_field` to `symbol::field` format (e.g., `AAPL US Equity::PX_LAST`)
- Internal: Client groups multi-symbol requests by source and performs batch fetching to reduce API calls

## [0.1.0] - 2025-12-27

### Added

- Unified `Client` class with `get()` method for fetching time-series data by catalog name
- `get_raw()` method for ad-hoc queries without catalog entries
- `get_metadata()` method for retrieving symbol metadata from sources
- Context manager support for automatic resource cleanup
- YAML-based catalog system mapping human-readable names to source-specific symbols
- `path` field in catalog entries for localfile source file paths
- `path` parameter in `get_raw()` for ad-hoc localfile queries
- Source validation at client initialization
- SQLite-based caching layer for offline access and reduced API calls
- Frequency alignment support using pandas resampling
- Bloomberg data source via xbbg integration (optional dependency)
- Local file source supporting CSV and Parquet formats
- Extensible source architecture with `BaseSource` ABC and registry
- Custom exception hierarchy for clear error handling
- Comprehensive type hints throughout (Python 3.12+)
- User guide documentation

[Unreleased]: https://github.com/stabilefrisur/metapyle/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/stabilefrisur/metapyle/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/stabilefrisur/metapyle/releases/tag/v0.1.0
