# Source Attribute Validation Design

## Problem

When users accidentally add a `field` value to a macrobond catalog entry, the column matching logic fails silently. The macrobond API ignores the field parameter and returns columns named by symbol only. The client's column matching first tries `symbol::field`, doesn't find it, falls back to `symbol`, and succeeds—but the user never learns their catalog is incorrect.

## Solution

Implement fail-fast validation at catalog load time. Each source has specific rules about which attributes (`field`, `path`, `params`) are required, forbidden, or optional.

## Validation Matrix

| Source    | field     | path      | params  |
|-----------|-----------|-----------|---------|
| bloomberg | REQUIRED  | FORBIDDEN | IGNORED |
| gsquant   | REQUIRED  | FORBIDDEN | ALLOWED |
| macrobond | FORBIDDEN | FORBIDDEN | IGNORED |
| localfile | FORBIDDEN | REQUIRED  | IGNORED |

- **REQUIRED**: Must be non-None
- **FORBIDDEN**: Must be None (error if set)
- **ALLOWED**: Optional, used if present
- **IGNORED**: Optional, not validated

## Error Messages

Actionable messages that identify the entry and attribute:

```
Macrobond entry 'us_gdp' has 'field' set, but macrobond does not use field. Remove it.
Localfile entry 'sp500' requires 'path' but none provided.
Bloomberg entry 'spx' has 'path' set, but bloomberg does not use path. Remove it.
```

## Implementation

### Data Structure

```python
from enum import StrEnum, auto

class AttrRule(StrEnum):
    REQUIRED = auto()
    FORBIDDEN = auto()
    ALLOWED = auto()
    IGNORED = auto()

_SOURCE_VALIDATION: dict[str, dict[str, AttrRule]] = {
    "bloomberg": {"field": AttrRule.REQUIRED, "path": AttrRule.FORBIDDEN},
    "gsquant": {"field": AttrRule.REQUIRED, "path": AttrRule.FORBIDDEN, "params": AttrRule.ALLOWED},
    "macrobond": {"field": AttrRule.FORBIDDEN, "path": AttrRule.FORBIDDEN},
    "localfile": {"field": AttrRule.FORBIDDEN, "path": AttrRule.REQUIRED},
}
```

### Helper Function

```python
def _validate_source_attributes(
    entry: CatalogEntry,
    source_file: str | Path,
) -> str | None:
    """
    Validate entry attributes against source-specific rules.

    Returns error message if validation fails, None if valid.
    Unknown sources are not validated (handled by validate_sources()).
    """
```

### Integration Points

1. **YAML loader** (`_parse_entry`): Call helper after creating entry, raise immediately on error
2. **CSV loader** (`from_csv`): Call helper after creating entry, collect errors for batch reporting

### Changes to Existing Code

- Remove Bloomberg-specific validation in `_parse_entry()` (replaced by general validation)
- Add `_SOURCE_VALIDATION` dict
- Add `_validate_source_attributes()` helper
- Call helper in both loaders

## Testing

### YAML Loader Tests

- `test_from_yaml_rejects_macrobond_with_field`
- `test_from_yaml_rejects_macrobond_with_path`
- `test_from_yaml_rejects_bloomberg_with_path`
- `test_from_yaml_rejects_localfile_with_field`
- `test_from_yaml_rejects_localfile_without_path`
- `test_from_yaml_rejects_gsquant_with_path`

### CSV Loader Tests

- `test_from_csv_collects_multiple_validation_errors`

### Assertions

Tests verify error messages contain:
- Entry name (`my_name`)
- Problematic attribute name
- Source name

## Decisions

1. **Fail-fast vs warn**: Fail-fast chosen for immediate, actionable feedback
2. **Scope**: Full source-attribute validation (not just macrobond+field)
3. **Error handling**: Match existing behavior—YAML fails fast, CSV collects all errors
4. **Unknown sources**: Silently allowed; `validate_sources()` handles unknown source detection
