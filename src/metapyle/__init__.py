"""Metapyle - A unified interface for querying financial time-series data."""

from .client import Client
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
    "Client",
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
