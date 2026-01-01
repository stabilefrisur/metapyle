"""Base source interface and registry."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from metapyle.exceptions import UnknownSourceError

__all__ = ["BaseSource", "FetchRequest", "make_column_name", "register_source"]


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchRequest:
    """
    Single request within a batch fetch.

    Parameters
    ----------
    symbol : str
        Source-specific identifier.
    field : str | None
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    path : str | None
        File path for localfile source.
    params : dict[str, Any] | None
        Additional source-specific parameters (e.g., tenor, deltaStrike).
    """

    symbol: str
    field: str | None = None
    path: str | None = None
    params: dict[str, Any] | None = None


def make_column_name(symbol: str, field: str | None) -> str:
    """
    Generate consistent column name for source output.

    Parameters
    ----------
    symbol : str
        The symbol identifier.
    field : str | None
        Optional field name.

    Returns
    -------
    str
        "symbol::field" if field present, otherwise "symbol".
    """
    return f"{symbol}::{field}" if field else symbol


class BaseSource(ABC):
    """Abstract base class for all data source adapters.

    Subclasses must implement `fetch` and `get_metadata` methods.
    """

    @abstractmethod
    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data for one or more symbols.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        **kwargs : Any
            Source-specific keyword arguments. Passed through from Client.get().
            Most sources ignore these; MacrobondSource uses them for unified series.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and one column per request.
            Column naming: "symbol::field" if field present, otherwise "symbol".

        Raises
        ------
        NoDataError
            If no data is returned for any symbol.
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
