"""Exception hierarchy for metapyle.

All metapyle exceptions inherit from MetapyleError for easy catching.
"""

__all__ = [
    "MetapyleError",
    "CatalogError",
    "FetchError",
    "CatalogValidationError",
    "DuplicateNameError",
    "UnknownSourceError",
    "SymbolNotFoundError",
    "NoDataError",
]


class MetapyleError(Exception):
    """Base exception for all metapyle errors."""


class CatalogError(MetapyleError):
    """Catalog-related errors (validation, lookup, duplicates)."""


class FetchError(MetapyleError):
    """Data fetching errors."""


class CatalogValidationError(CatalogError):
    """Raised when catalog YAML is malformed or missing required fields."""


class DuplicateNameError(CatalogError):
    """Raised when the same my_name appears in multiple catalog entries."""


class UnknownSourceError(CatalogError):
    """Raised when a catalog references a source that is not registered."""


class SymbolNotFoundError(CatalogError):
    """Raised when a queried name is not found in the catalog."""


class NoDataError(FetchError):
    """Raised when an adapter returns empty data."""
