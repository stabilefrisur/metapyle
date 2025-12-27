# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Macrobond data source via macrobond-data-api integration (optional dependency)
  - `fetch()` with raw mode using `get_one_series`
  - `fetch()` with unified mode using `get_unified_series` for frequency/currency alignment
  - `get_metadata()` for retrieving series metadata

### Changed

- `end` parameter in `get()` and `get_raw()` is now optional, defaulting to today's date

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

[Unreleased]: https://github.com/stabilefrisur/metapyle/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/stabilefrisur/metapyle/releases/tag/v0.1.0
