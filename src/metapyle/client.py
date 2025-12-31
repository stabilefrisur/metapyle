"""Client for querying financial time-series data."""

import datetime
import logging
from itertools import groupby
from pathlib import Path
from typing import Any, Self

import pandas as pd

from metapyle.cache import Cache
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.sources.base import FetchRequest, SourceRegistry, _global_registry, make_column_name

__all__ = ["Client"]

logger = logging.getLogger(__name__)


class Client:
    """
    Client for querying financial time-series data.

    Provides a unified interface for fetching data from multiple sources
    using a catalog for name mapping and optional caching.

    Parameters
    ----------
    catalog : str | Path | list[str | Path]
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
        catalog: str | Path | list[str | Path],
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
        end: str | None = None,
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
        end : str | None, optional
            End date in ISO format (YYYY-MM-DD). Defaults to today.
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
        # Default end to today if not specified
        if end is None:
            end = datetime.date.today().isoformat()

        # Resolve entries (raises SymbolNotFoundError if not found)
        entries = [self._catalog.get(symbol) for symbol in symbols]

        if frequency is not None:
            logger.info(
                "frequency_alignment_requested: target=%s, symbols=%d",
                frequency,
                len(symbols),
            )

        # Collect cached and uncached entries
        dfs: dict[str, pd.DataFrame] = {}
        uncached_entries: list[CatalogEntry] = []

        for entry in entries:
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
                    dfs[entry.my_name] = cached
                    continue

            uncached_entries.append(entry)

        # Batch fetch uncached entries grouped by source
        if uncached_entries:
            # Sort by source for groupby
            sorted_entries = sorted(uncached_entries, key=lambda e: e.source)

            for source_name, group in groupby(sorted_entries, key=lambda e: e.source):
                group_entries = list(group)

                # Build FetchRequest list for this source
                requests = [
                    FetchRequest(
                        symbol=e.symbol,
                        field=e.field,
                        path=e.path,
                        params=e.params,
                    )
                    for e in group_entries
                ]

                # Batch fetch from source
                result_df = self._fetch_from_source(source_name, requests, start, end)

                # Split result and cache each column
                for entry in group_entries:
                    col_name = make_column_name(entry.symbol, entry.field)
                    if col_name in result_df.columns:
                        col_df = result_df[[col_name]]

                        # Cache the individual column
                        if use_cache:
                            self._cache.put(
                                source=entry.source,
                                symbol=entry.symbol,
                                field=entry.field,
                                path=entry.path,
                                start_date=start,
                                end_date=end,
                                data=col_df,
                            )

                        dfs[entry.my_name] = col_df

        # Apply frequency alignment if requested
        if frequency is not None:
            from metapyle.processing import align_to_frequency

            for my_name in dfs:
                logger.debug(
                    "aligning_symbol: symbol=%s, target_frequency=%s",
                    my_name,
                    frequency,
                )
                dfs[my_name] = align_to_frequency(dfs[my_name], frequency)

        # Check index alignment if no frequency specified
        if frequency is None:
            self._check_index_alignment(dfs)

        # Assemble into wide DataFrame
        return self._assemble_dataframe(dfs, symbols)

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

    def _fetch_from_source(
        self,
        source_name: str,
        requests: list[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch data from a source for multiple requests.

        Parameters
        ----------
        source_name : str
            Name of the source adapter.
        requests : list[FetchRequest]
            Fetch requests for this source.
        start : str
            Start date.
        end : str
            End date.

        Returns
        -------
        pd.DataFrame
            DataFrame with one column per request.
        """
        source = self._registry.get(source_name)

        logger.debug(
            "fetch_from_source: source=%s, requests=%d, range=%s/%s",
            source_name,
            len(requests),
            start,
            end,
        )

        return source.fetch(requests, start, end)

    def _assemble_dataframe(
        self, dfs: dict[str, pd.DataFrame], names: list[str]
    ) -> pd.DataFrame:
        """
        Assemble individual DataFrames into a wide DataFrame.

        Renames source columns to my_name from catalog and preserves
        the order specified by names.

        Parameters
        ----------
        dfs : dict[str, pd.DataFrame]
            Dictionary mapping my_name to DataFrames.
        names : list[str]
            Original input symbol names, used to preserve column order.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with columns named by my_name in input order.
        """
        if not dfs:
            return pd.DataFrame()

        # Rename first column to my_name and concatenate
        renamed: list[pd.DataFrame] = []
        for my_name, df in dfs.items():
            # Take first column regardless of name, rename to my_name
            col = df.columns[0]
            renamed.append(df[[col]].rename(columns={col: my_name}))

        combined = pd.concat(renamed, axis=1)

        # Preserve input order
        ordered_cols = [name for name in names if name in combined.columns]
        return combined[ordered_cols]

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
        end: str | None = None,
        *,
        field: str | None = None,
        path: str | None = None,
        params: dict[str, Any] | None = None,
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
        end : str | None, optional
            End date in ISO format (YYYY-MM-DD). Defaults to today.
        field : str | None, optional
            Source-specific field (e.g., "PX_LAST" for Bloomberg).
        path : str | None, optional
            Path to local file for localfile source.
        params : dict[str, Any] | None, optional
            Source-specific parameters (e.g., Macrobond unified options).
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
        # Default end to today if not specified
        if end is None:
            end = datetime.date.today().isoformat()

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
        request = FetchRequest(symbol=symbol, field=field, path=path, params=params)

        logger.debug("get_raw_from_source: source=%s, symbol=%s", source, symbol)
        df = source_adapter.fetch([request], start, end)

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
