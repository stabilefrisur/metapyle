"""Metapyle - Unified interface for financial time-series data."""

__version__ = "0.1.0"

from metapyle.client import Client
from metapyle.exceptions import (
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
from metapyle.sources import BaseSource, register_source

__all__ = [
    "__version__",
    "Client",
    "BaseSource",
    "register_source",
    "MetapyleError",
    "CatalogError",
    "CatalogValidationError",
    "DuplicateNameError",
    "FetchError",
    "FrequencyMismatchError",
    "NoDataError",
    "SymbolNotFoundError",
    "UnknownSourceError",
]
