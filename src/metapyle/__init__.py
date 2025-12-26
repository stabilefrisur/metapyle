"""Metapyle - A unified interface for querying financial time-series data."""

from .exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
    FrequencyMismatchError,
    MetapyleError,
    NoDataError,
    SymbolNotFoundError,
    UnknownSourceError,
)

__all__ = [
    "MetapyleError",
    "CatalogError",
    "FetchError",
    "FrequencyMismatchError",
    "CatalogValidationError",
    "DuplicateNameError",
    "UnknownSourceError",
    "SymbolNotFoundError",
    "NoDataError",
]
