# Catalog CSV Tools Design

> **Date:** 2025-12-28  
> **Status:** Approved

## Overview

Add CSV import/export capabilities to the `Catalog` class, making it easier to create and maintain catalog files. Users can work in spreadsheets (Excel, Google Sheets) and convert to YAML.

## Requirements

- **Scale:** Support catalogs from 5 to 100+ entries
- **Input sources:** Spreadsheets, existing configs, copy-paste from various sources
- **Workflows supported:**
  - One-time bulk import from CSV
  - Incremental additions via CSV
  - Template-driven creation (download template, fill, convert)
  - Bidirectional editing (YAML ↔ CSV roundtrip)
- **No interactive/guided mode** – users want efficiency, not hand-holding
- **Validation:** Report all errors at once, then fail (no fix-one-at-a-time)
- **Interface:** Python API on `Catalog` class (no CLI)

## API Surface

### New Methods

```python
from pathlib import Path
from typing import Self

class Catalog:
    # Existing (signature updated to accept Path)
    @classmethod
    def from_yaml(cls, paths: str | Path | list[str | Path]) -> Self:
        """Load catalog entries from one or more YAML files."""
    
    # New: CSV import
    @classmethod
    def from_csv(cls, paths: str | Path | list[str | Path]) -> Self:
        """
        Load catalog from CSV file(s).
        
        Validates all rows and raises CatalogValidationError with all errors
        if any validation fails.
        """
    
    # New: CSV export
    def to_csv(self, path: str | Path) -> None:
        """Export catalog entries to CSV file."""
    
    # New: YAML export
    def to_yaml(self, path: str | Path) -> None:
        """Export catalog entries to YAML file."""
    
    # New: Template generation
    @staticmethod
    def csv_template(source: str | None = None, path: str | Path | None = None) -> str:
        """
        Generate CSV template with headers.
        
        Parameters
        ----------
        source : str | None
            If provided, generates source-specific template with relevant
            columns only. Valid: "bloomberg", "localfile", "macrobond".
            If None, includes all columns.
        path : str | Path | None
            If provided, writes template to file.
        
        Returns
        -------
        str
            Template string (header row + optional example row).
        
        Notes
        -----
        - Without source: header row only (all columns)
        - With source: header row + example row with source column pre-filled
        """
```

### CSV Column Mapping

CSV columns map 1:1 to `CatalogEntry` fields:

| CSV Header | Required | Notes |
|------------|----------|-------|
| `my_name` | Yes | Unique identifier |
| `source` | Yes | bloomberg, localfile, macrobond |
| `symbol` | Yes | Source-specific ticker/column |
| `field` | No | Bloomberg field (e.g., PX_LAST) |
| `path` | No | File path for localfile |
| `description` | No | Human-readable description |
| `unit` | No | Unit of measurement |

Empty cells for optional fields become `None`.

## Validation & Error Handling

### Validation Strategy

`from_csv()` validates **all rows** before raising, then reports all errors at once:

```python
CatalogValidationError: 3 errors in entries.csv:
  Row 2: Missing required field 'symbol'
  Row 5: Missing required field 'source'
  Row 8: Duplicate my_name 'sp500_close' (first seen row 3)
```

### Validation Rules

1. **Required fields** – `my_name`, `source`, `symbol` must be non-empty
2. **No duplicates** – `my_name` must be unique within the file
3. **Valid headers** – unrecognized columns are ignored (allows extra metadata)
4. **Cross-file duplicates** – when loading multiple CSVs, duplicates across files also caught

### Permissive Parsing

- **Extra columns ignored** – users can keep notes/metadata columns
- **Whitespace trimmed** – leading/trailing whitespace stripped from all values
- **Case preserved** – no automatic lowercasing (source names are case-sensitive)

## Template Generation

### Template Output by Source

**Without source (generic):**
```csv
my_name,source,symbol,field,path,description,unit
```

**With source="bloomberg":**
```csv
my_name,source,symbol,field,description,unit
,bloomberg,,,,
```

**With source="localfile":**
```csv
my_name,source,symbol,path,description,unit
,localfile,,,,
```

**With source="macrobond":**
```csv
my_name,source,symbol,description,unit
,macrobond,,,,
```

Only the `source` column is pre-filled in the example row.

### Column Selection per Source

| Source | Columns |
|--------|---------|
| `None` (generic) | my_name, source, symbol, field, path, description, unit |
| `bloomberg` | my_name, source, symbol, field, description, unit |
| `localfile` | my_name, source, symbol, path, description, unit |
| `macrobond` | my_name, source, symbol, description, unit |

## Export Format

### to_csv()

- Writes all columns (even if `None` for all entries)
- Column order: my_name, source, symbol, field, path, description, unit
- `None` values written as empty strings
- UTF-8 encoding

### to_yaml()

- Matches existing `from_yaml` expected format (list of dicts)
- Omits `None` fields for cleaner output
- Preserves entry order from internal dict

### Roundtrip Guarantee

`from_csv` → `to_yaml` → `from_yaml` → `to_csv` produces equivalent data (field values identical, order may differ).

## Implementation Details

### Dependencies

- **csv** (stdlib) – for reading/writing CSV
- **yaml** (already a dependency) – for `to_yaml()` output

No new dependencies needed.

### Error Aggregation

```python
@dataclass
class _ValidationError:
    row: int
    message: str

# Collect all errors, then raise single exception with full report
errors: list[_ValidationError] = []
# ... validation loop ...
if errors:
    raise CatalogValidationError(_format_errors(errors, source_file))
```

## Testing Strategy

### Unit Tests for from_csv()

- Load single CSV file
- Load multiple CSV files
- Missing required fields → collect all errors, raise once
- Duplicate `my_name` within file
- Duplicate `my_name` across files
- Extra columns ignored
- Whitespace trimmed
- Empty optional fields → `None`
- Accepts both `str` and `Path`

### Unit Tests for to_csv()

- Export entries to CSV
- `None` values → empty strings
- Column order consistent
- Accepts both `str` and `Path`

### Unit Tests for to_yaml()

- Export entries to YAML
- `None` fields omitted
- Valid YAML output (parseable by `from_yaml`)
- Accepts both `str` and `Path`

### Unit Tests for csv_template()

- No source → all columns, no example row
- source="bloomberg" → relevant columns + example row with source
- source="localfile" → relevant columns + example row with source
- source="macrobond" → relevant columns + example row with source
- With path → writes file
- Without path → returns string only

### Roundtrip Tests

- `from_csv` → `to_yaml` → `from_yaml` → `to_csv` produces equivalent data
- `from_yaml` → `to_csv` → `from_csv` → `to_yaml` produces equivalent data

### Test Fixtures

Use `tmp_path` pytest fixture for all file operations.

## Documentation Updates

### User Guide Additions

Add "Catalog Tools" section covering:

1. Creating catalogs from CSV – `Catalog.from_csv()` workflow
2. Generating templates – `Catalog.csv_template()` usage
3. Exporting catalogs – `to_csv()` and `to_yaml()` for editing/sharing
4. Example workflow – generate template → fill in Excel → convert to YAML

### Public API

No new exports needed – methods are on `Catalog` class which is already exported.

## Out of Scope

- CLI interface (may add later)
- Excel native format (.xlsx) support
- Interactive/guided mode
- Schema validation against registered sources (existing `validate_sources()` handles this)
