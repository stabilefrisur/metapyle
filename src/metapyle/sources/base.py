"""Base source interface and registry."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import pandas as pd

from metapyle.exceptions import UnknownSourceError


class BaseSource(ABC):
    """Abstract base class for all data source adapters.

    Subclasses must implement `fetch` and `get_metadata` methods.
    """

    @abstractmethod
    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data for a symbol.

        Parameters
        ----------
        symbol : str
            Source-specific identifier (e.g., "SPX Index" for Bloomberg).
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        **kwargs : Any
            Source-specific parameters (e.g., field for Bloomberg).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column with source-specific
            name (e.g., the symbol name for localfile, field name for Bloomberg).

        Raises
        ------
        NoDataError
            If no data is returned for the symbol.
        FetchError
            If data retrieval fails.
        """
        pass

    @abstractmethod
    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a symbol.

        Parameters
        ----------
        symbol : str
            Source-specific identifier.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary (description, unit, frequency, etc.).
        """
        pass


class SourceRegistry:
    """Registry for source adapters.

    Manages registration and retrieval of source adapters by name.
    Caches instantiated adapters for reuse.
    """

    def __init__(self) -> None:
        self._sources: dict[str, type[BaseSource]] = {}
        self._instances: dict[str, BaseSource] = {}

    def register(self, name: str, source_class: type[BaseSource]) -> None:
        """
        Register a source adapter class.

        Parameters
        ----------
        name : str
            Unique name for the source (e.g., "bloomberg", "localfile").
        source_class : type[BaseSource]
            The source adapter class to register.
        """
        self._sources[name] = source_class

    def get(self, name: str) -> BaseSource:
        """
        Get a source adapter instance by name.

        Parameters
        ----------
        name : str
            Name of the registered source.

        Returns
        -------
        BaseSource
            Instantiated source adapter.

        Raises
        ------
        UnknownSourceError
            If no source is registered with the given name.
        """
        if name not in self._sources:
            raise UnknownSourceError(
                f"Unknown source: {name}. "
                f"Available sources: {', '.join(self._sources.keys()) or 'none'}"
            )

        if name not in self._instances:
            self._instances[name] = self._sources[name]()

        return self._instances[name]

    def list_sources(self) -> list[str]:
        """
        List all registered source names.

        Returns
        -------
        list[str]
            List of registered source names.
        """
        return list(self._sources.keys())


# Global registry instance
_global_registry = SourceRegistry()


def register_source(name: str) -> Callable[[type[BaseSource]], type[BaseSource]]:
    """
    Decorator to register a source adapter class.

    Parameters
    ----------
    name : str
        Unique name for the source.

    Returns
    -------
    Callable
        Decorator function.

    Examples
    --------
    >>> @register_source("custom")
    ... class CustomSource(BaseSource):
    ...     def fetch(self, symbol, start, end, **kwargs):
    ...         ...
    ...     def get_metadata(self, symbol):
    ...         ...
    """

    def decorator(cls: type[BaseSource]) -> type[BaseSource]:
        _global_registry.register(name, cls)
        return cls

    return decorator
