# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add `path` field to catalog entries for localfile source file paths
- Add `path` parameter to `get_raw()` for ad-hoc localfile queries

### Changed

- **BREAKING:** LocalFile source: `symbol` is now the column name to extract, `path` is the file path
- **BREAKING:** `get_raw()` returns source-specific column names instead of `value`:
  - Bloomberg: `symbol_field` (e.g., `SPX Index_PX_LAST`)
  - LocalFile: original column name from file
- Cache key now includes `path` for localfile entries

## [0.1.0] - 2025-12-26

### Added

- Unified `Client` class with `get()` method for fetching time-series data by catalog name
- `get_raw()` method for ad-hoc queries without catalog entries
- `get_metadata()` method for retrieving symbol metadata from sources
- Context manager support for automatic resource cleanup
- YAML-based catalog system mapping human-readable names to source-specific symbols
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
