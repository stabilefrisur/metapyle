"""Client for querying financial time-series data."""

import logging
from typing import Any

import pandas as pd

from metapyle.cache import Cache
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.exceptions import FrequencyMismatchError
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
            Alignment frequency. If omitted, all symbols must have the same
            native frequency.
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
        FrequencyMismatchError
            If symbols have different frequencies and no alignment frequency
            is specified.
        FetchError
            If data retrieval fails for any symbol.
        """
        # Resolve entries (raises SymbolNotFoundError if not found)
        entries = [self._catalog.get(symbol) for symbol in symbols]

        # Check frequency compatibility if no alignment specified
        if frequency is None:
            self._check_frequency_compatibility(entries)
        else:
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
                # Lazy import to avoid circular imports
                from metapyle.processing import align_to_frequency

                logger.debug(
                    "aligning_symbol: symbol=%s, target_frequency=%s",
                    entry.my_name,
                    frequency,
                )
                df = align_to_frequency(df, frequency)

            dfs[entry.my_name] = df

        # Assemble into wide DataFrame
        return self._assemble_dataframe(dfs)

    def _check_frequency_compatibility(self, entries: list[CatalogEntry]) -> None:
        """
        Check that all entries have the same frequency.

        Parameters
        ----------
        entries : list[CatalogEntry]
            List of catalog entries to check.

        Raises
        ------
        FrequencyMismatchError
            If entries have different frequencies.
        """
        if len(entries) <= 1:
            return

        frequencies = {entry.frequency for entry in entries}
        if len(frequencies) > 1:
            freq_list = ", ".join(f"{entry.my_name}={entry.frequency}" for entry in entries)
            raise FrequencyMismatchError(
                f"Symbols have different frequencies: {freq_list}. "
                "Specify a frequency parameter for alignment."
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
                start_date=start,
                end_date=end,
                data=df,
            )

        return df

    def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Assemble individual DataFrames into a wide DataFrame.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping symbol names to DataFrames.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by symbol names.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename 'value' column to symbol name and concatenate
        renamed: list[pd.DataFrame] = []
        for name, df in dfs.items():
            if "value" in df.columns:
                renamed.append(df[["value"]].rename(columns={"value": name}))
            else:
                # If no 'value' column, use first column
                col = df.columns[0]
                renamed.append(df[[col]].rename(columns={col: name}))

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

        # Combine with catalog info (catalog takes precedence)
        return {
            **source_meta,
            "my_name": entry.my_name,
            "source": entry.source,
            "symbol": entry.symbol,
            "frequency": entry.frequency.value,
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
        use_cache : bool, optional
            If False, bypass cache. Default True.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and 'value' column.

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

        logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
        df = source_adapter.fetch(symbol, start, end, **kwargs)

        # Store in cache
        if use_cache:
            self._cache.put(
                source=source,
                symbol=symbol,
                field=field,
                start_date=start,
                end_date=end,
                data=df,
            )

        return df
