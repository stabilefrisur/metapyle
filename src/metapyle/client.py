"""Client for querying financial time-series data."""

import logging
from typing import Any, Self

import pandas as pd

from metapyle.cache import Cache
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.sources.base import SourceRegistry, _global_registry

__all__ = ["Client"]

logger = logging.getLogger(__name__)


class Client:
    """
    Client for querying financial time-series data.

    Provides a unified interface for fetching data from multiple sources
    using a catalog for name mapping and optional caching.

    Parameters
    ----------
    catalog : str | list[str]
        Path or list of paths to YAML catalog files.
    cache_path : str | None, optional
        Path to SQLite cache database. If None, uses default path.
    cache_enabled : bool, optional
        Whether caching is enabled. Default is True.

    Examples
    --------
    >>> client = Client(catalog="catalog.yaml")
    >>> df = client.get(["GDP_US", "CPI_EU"], start="2020-01-01", end="2024-12-31")
    """

    def __init__(
        self,
        catalog: str | list[str],
        *,
        cache_path: str | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self._registry: SourceRegistry = _global_registry
        self._catalog = Catalog.from_yaml(catalog)
        self._catalog.validate_sources(self._registry)
        self._cache = Cache(path=cache_path, enabled=cache_enabled)

        logger.info(
            "client_initialized: catalog_entries=%d, cache_enabled=%s",
            len(self._catalog),
            cache_enabled,
        )

    def get(
        self,
        symbols: list[str],
        start: str,
        end: str,
        *,
        frequency: str | None = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch time-series data for multiple symbols.

        Parameters
        ----------
        symbols : list[str]
            List of catalog names to fetch.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        frequency : str | None, optional
            Pandas frequency string for alignment (e.g., "D", "ME", "QE").
            If omitted, data is returned as-is with a warning if indexes
            don't align.
        use_cache : bool, optional
            Whether to use cached data. Default is True.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with DatetimeIndex and columns named by catalog names.

        Raises
        ------
        SymbolNotFoundError
            If any symbol is not in the catalog.
        FetchError
            If data retrieval fails for any symbol.
        ValueError
            If frequency is an invalid pandas frequency string.
        """
        # Resolve entries (raises SymbolNotFoundError if not found)
        entries = [self._catalog.get(symbol) for symbol in symbols]

        if frequency is not None:
            logger.info(
                "frequency_alignment_requested: target=%s, symbols=%d",
                frequency,
                len(symbols),
            )

        # Fetch data for each symbol
        dfs: dict[str, pd.DataFrame] = {}
        for entry in entries:
            df = self._fetch_symbol(entry, start, end, use_cache)

            # Apply frequency alignment if requested
            if frequency is not None:
                from metapyle.processing import align_to_frequency

                logger.debug(
                    "aligning_symbol: symbol=%s, target_frequency=%s",
                    entry.my_name,
                    frequency,
                )
                df = align_to_frequency(df, frequency)

            dfs[entry.my_name] = df

        # Check index alignment if no frequency specified
        if frequency is None:
            self._check_index_alignment(dfs)

        # Assemble into wide DataFrame
        return self._assemble_dataframe(dfs)

    def _check_index_alignment(self, dfs: dict[str, pd.DataFrame]) -> None:
        """
        Warn if series have misaligned indexes.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping symbol names to DataFrames.
        """
        if len(dfs) <= 1:
            return

        # Infer frequency for each series
        freqs = {name: pd.infer_freq(df.index) for name, df in dfs.items()}

        # Check for mismatches
        unique_freqs = set(freqs.values())

        if len(unique_freqs) > 1:
            # Different frequencies (including None for irregular)
            freq_summary = ", ".join(
                f"{name}={freq or 'irregular'}" for name, freq in freqs.items()
            )
            logger.warning(
                "index_mismatch: Series have different frequencies: %s. "
                "Outer join may produce NaN values. Consider specifying frequency parameter.",
                freq_summary,
            )
        elif unique_freqs == {None}:
            # All irregular — check if indexes actually match
            indexes = list(dfs.values())
            first_idx = indexes[0].index
            if not all(df.index.equals(first_idx) for df in indexes[1:]):
                logger.warning(
                    "index_mismatch: Irregular series have different dates. "
                    "Outer join may produce NaN values. Consider specifying frequency parameter.",
                )

    def _fetch_symbol(
        self,
        entry: CatalogEntry,
        start: str,
        end: str,
        use_cache: bool,
    ) -> pd.DataFrame:
        """
        Fetch data for a single symbol, using cache if available.

        Parameters
        ----------
        entry : CatalogEntry
            Catalog entry for the symbol.
        start : str
            Start date in ISO format.
        end : str
            End date in ISO format.
        use_cache : bool
            Whether to use cached data.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.
        """
        # Try cache first
        if use_cache:
            cached = self._cache.get(
                source=entry.source,
                symbol=entry.symbol,
                field=entry.field,
                path=entry.path,
                start_date=start,
                end_date=end,
            )
            if cached is not None:
                logger.debug(
                    "fetch_from_cache: symbol=%s, rows=%d",
                    entry.my_name,
                    len(cached),
                )
                return cached

        # Fetch from source
        source = self._registry.get(entry.source)

        # Build kwargs for source
        kwargs: dict[str, str] = {}
        if entry.field is not None:
            kwargs["field"] = entry.field
        if entry.path is not None:
            kwargs["path"] = entry.path

        logger.debug(
            "fetch_from_source: symbol=%s, source=%s, range=%s/%s",
            entry.my_name,
            entry.source,
            start,
            end,
        )

        df = source.fetch(entry.symbol, start, end, **kwargs)

        # Store in cache
        if use_cache:
            self._cache.put(
                source=entry.source,
                symbol=entry.symbol,
                field=entry.field,
                path=entry.path,
                start_date=start,
                end_date=end,
                data=df,
            )

        return df

    def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Assemble individual DataFrames into a wide DataFrame.

        Renames source columns to my_name from catalog.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping my_name to DataFrames.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by my_name.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename first column to my_name and concatenate
        renamed: list[pd.DataFrame] = []
        for my_name, df in dfs.items():
            # Take first column regardless of name, rename to my_name
            col = df.columns[0]
            renamed.append(df[[col]].rename(columns={col: my_name}))

        result = pd.concat(renamed, axis=1)
        return result

    def clear_cache(self, *, symbol: str | None = None) -> None:
        """
        Clear cached data.

        Parameters
        ----------
        symbol : str | None, optional
            If provided, only clear cache for this catalog symbol.
            If None, clears all cached data.
        """
        if symbol is not None:
            entry = self._catalog.get(symbol)
            self._cache.clear(source=entry.source, symbol=entry.symbol)
            logger.info("cache_cleared: symbol=%s", symbol)
        else:
            self._cache.clear()
            logger.info("cache_cleared: all")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a catalog symbol.

        Frequency is inferred from source metadata if available, otherwise None.

        Parameters
        ----------
        symbol : str
            Catalog name.

        Returns
        -------
        dict[str, Any]
            Combined metadata from catalog entry and source adapter.

        Raises
        ------
        SymbolNotFoundError
            If symbol not in catalog.
        """
        entry = self._catalog.get(symbol)
        source = self._registry.get(entry.source)

        # Get source-specific metadata
        source_meta = source.get_metadata(entry.symbol)

        logger.debug(
            "get_metadata: symbol=%s, source=%s",
            symbol,
            entry.source,
        )

        # Infer frequency from source metadata or return None
        inferred_freq = source_meta.get("frequency")

        # Combine with catalog info (catalog takes precedence)
        return {
            **source_meta,
            "my_name": entry.my_name,
            "source": entry.source,
            "symbol": entry.symbol,
            "frequency": inferred_freq,
            "field": entry.field,
            "description": entry.description,
            "unit": entry.unit,
        }

    def get_raw(
        self,
        source: str,
        symbol: str,
        start: str,
        end: str,
        *,
        field: str | None = None,
        path: str | None = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch data directly from a source, bypassing the catalog.

        Useful for ad-hoc queries or testing new data series.

        Parameters
        ----------
        source : str
            Name of registered source adapter.
        symbol : str
            Source-specific symbol identifier.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        field : str | None, optional
            Source-specific field (e.g., "PX_LAST" for Bloomberg).
        path : str | None, optional
            Path to local file for localfile source.
        use_cache : bool, optional
            If False, bypass cache. Default True.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and source-specific column name.

        Raises
        ------
        UnknownSourceError
            If source is not registered.
        FetchError
            If data retrieval fails.
        """
        # Try cache first
        if use_cache:
            cached = self._cache.get(
                source=source,
                symbol=symbol,
                field=field,
                path=path,
                start_date=start,
                end_date=end,
            )
            if cached is not None:
                logger.debug("get_raw_from_cache: source=%s, symbol=%s", source, symbol)
                return cached

        # Fetch from source
        source_adapter = self._registry.get(source)
        kwargs: dict[str, str] = {}
        if field is not None:
            kwargs["field"] = field
        if path is not None:
            kwargs["path"] = path

        logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
        df = source_adapter.fetch(symbol, start, end, **kwargs)

        # Store in cache
        if use_cache:
            self._cache.put(
                source=source,
                symbol=symbol,
                field=field,
                path=path,
                start_date=start,
                end_date=end,
                data=df,
            )

        return df

    def close(self) -> None:
        """
        Close the cache connection.

        Should be called when the client is no longer needed to release
        database resources. Alternatively, use the client as a context manager.

        Examples
        --------
        >>> client = Client(catalog="catalog.yaml")
        >>> try:
        ...     df = client.get(["GDP"], start="2020-01-01", end="2024-12-31")
        ... finally:
        ...     client.close()
        """
        self._cache.close()
        logger.debug("client_closed")

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit context manager, closing cache connection."""
        self.close()
