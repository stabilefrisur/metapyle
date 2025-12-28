"""Metapyle - Unified interface for financial time-series data."""

from importlib.metadata import version

__version__ = version("metapyle")

from metapyle.client import Client
from metapyle.exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
    MetapyleError,
    NoDataError,
    SymbolNotFoundError,
    UnknownSourceError,
)
from metapyle.sources import BaseSource, FetchRequest, register_source

__all__ = [
    "__version__",
    "Client",
    "BaseSource",
    "FetchRequest",
    "register_source",
    "MetapyleError",
    "CatalogError",
    "CatalogValidationError",
    "DuplicateNameError",
    "FetchError",
    "NoDataError",
    "SymbolNotFoundError",
    "UnknownSourceError",
]
