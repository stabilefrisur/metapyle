"""Client for querying financial time-series data."""

import datetime
import logging
from itertools import groupby
from pathlib import Path
from typing import Any, Self

import pandas as pd

from metapyle.cache import Cache
from metapyle.catalog import Catalog, CatalogEntry
from metapyle.sources.base import (
    FetchRequest,
    SourceRegistry,
    _global_registry,
    make_column_name,
    normalize_dataframe,
)

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
        names: list[str],
        start: str,
        end: str | None = None,
        *,
        frequency: str | None = None,
        output_format: str = "wide",
        use_cache: bool = True,
        unified: bool = False,
        unified_options: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch time-series data for multiple catalog names.

        Parameters
        ----------
        names : list[str]
            List of catalog names to fetch.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str | None, optional
            End date in ISO format (YYYY-MM-DD). Defaults to today.
        frequency : str | None, optional
            Pandas frequency string for alignment (e.g., "D", "ME", "QE").
            If omitted, data is returned as-is with a warning if indexes
            don't align.
        output_format : str, optional
            Output format: "wide" (default) or "long".
            Wide: DatetimeIndex, one column per name.
            Long: Columns [date, symbol, value], one row per observation.
        use_cache : bool, optional
            Whether to use cached data. Default is True.
        unified : bool, default False
            If True, use Macrobond's get_unified_series() for server-side alignment.
            Only affects macrobond sources; other sources ignore this parameter.
            When True, cache is bypassed since the transformation depends on all
            symbols together.
        unified_options : dict[str, Any] | None, optional
            Options passed to source adapters for unified series. For macrobond
            with unified=True, supports: frequency (SeriesFrequency), weekdays
            (SeriesWeekdays), calendar_merge_mode (CalendarMergeMode), currency
            (str), start_point/end_point (StartOrEndPoint). Unused by other sources.

        Returns
        -------
        pd.DataFrame
            DataFrame in the requested format.

        Raises
        ------
        NameNotFoundError
            If any name is not in the catalog.
        FetchError
            If data retrieval fails for any symbol.
        ValueError
            If frequency is an invalid pandas frequency string, or
            output_format is not "wide" or "long".

        Examples
        --------
        >>> client = Client(catalog="catalog.yaml")
        >>> df = client.get(["GDP_US", "CPI_EU"], start="2020-01-01", end="2024-12-31")

        >>> # Unified series with custom options
        >>> df = client.get(
        ...     ["gdp_us", "gdp_eu"],
        ...     start="2020-01-01",
        ...     end="2024-12-31",
        ...     unified=True,
        ...     unified_options={"frequency": SeriesFrequency.QUARTERLY},
        ... )
        """
        # Default end to today if not specified
        if end is None:
            end = datetime.date.today().isoformat()

        # Resolve entries (raises NameNotFoundError if not found)
        entries = [self._catalog.get(name) for name in names]

        if frequency is not None:
            logger.info(
                "frequency_alignment_requested: target=%s, names=%d",
                frequency,
                len(names),
            )

        # Collect cached and uncached entries
        dfs: dict[str, pd.DataFrame] = {}
        uncached_entries: list[CatalogEntry] = []

        for entry in entries:
            # Skip cache for unified macrobond requests (server-side transformation)
            if unified and entry.source == "macrobond":
                logger.debug(
                    "cache_bypass_unified: symbol=%s",
                    entry.my_name,
                )
                uncached_entries.append(entry)
                continue

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
                result_df = self._fetch_from_source(
                    source_name,
                    requests,
                    start,
                    end,
                    unified=unified,
                    unified_options=unified_options,
                )

                # Build case-insensitive lookup map for sources that normalize case
                lower_to_actual = {col.lower(): col for col in result_df.columns}

                # Split result and cache each column
                for entry in group_entries:
                    # Try with field first (e.g., Bloomberg)
                    col_name = make_column_name(entry.symbol, entry.field)

                    # Fallback 1: symbol-only if field column not found
                    # (e.g., Macrobond ignores field parameter)
                    if col_name not in result_df.columns:
                        col_name = make_column_name(entry.symbol, None)

                    # Fallback 2: case-insensitive match
                    # (e.g., Macrobond normalizes case in response)
                    if col_name not in result_df.columns:
                        actual_col = lower_to_actual.get(col_name.lower())
                        if actual_col is not None:
                            col_name = actual_col

                    if col_name in result_df.columns:
                        col_df = result_df[[col_name]]

                        # Cache the column (skip for unified macrobond - server-side transform)
                        skip_cache = unified and entry.source == "macrobond"
                        if use_cache and not skip_cache:
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
                    else:
                        logger.warning(
                            "column_not_found: my_name=%s, tried=%s and %s, available=%s",
                            entry.my_name,
                            make_column_name(entry.symbol, entry.field),
                            make_column_name(entry.symbol, None),
                            list(result_df.columns),
                        )

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
        result = self._assemble_dataframe(dfs, names)

        # Convert to long format if requested
        if output_format == "long":
            from metapyle.processing import wide_to_long

            return wide_to_long(result)
        elif output_format != "wide":
            raise ValueError(f"output_format must be 'wide' or 'long', got '{output_format}'")

        return result

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
        *,
        unified: bool = False,
        unified_options: dict[str, Any] | None = None,
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
        unified : bool, default False
            Whether to use unified series API (macrobond only).
        unified_options : dict[str, Any] | None, optional
            Options for unified series (frequency, currency, etc.).

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

        return source.fetch(
            requests,
            start,
            end,
            unified=unified,
            unified_options=unified_options,
        )

    def _assemble_dataframe(self, dfs: dict[str, pd.DataFrame], names: list[str]) -> pd.DataFrame:
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

        # Rename first column to my_name and normalize each DataFrame
        renamed: list[pd.DataFrame] = []
        for my_name, df in dfs.items():
            # Take first column regardless of name, rename to my_name
            col = df.columns[0]
            df_renamed = df[[col]].rename(columns={col: my_name})
            # Defensive normalization (sources should already normalize,
            # but this ensures safe concat even with misbehaving sources)
            renamed.append(normalize_dataframe(df_renamed))

        combined = pd.concat(renamed, axis=1)

        # Preserve input order
        ordered_cols = [name for name in names if name in combined.columns]
        return combined[ordered_cols]

    def clear_cache(self, *, source: str | None = None) -> None:
        """
        Clear cached data.

        Parameters
        ----------
        source : str | None, optional
            If provided, only clear cache for this data source (e.g., "bloomberg").
            If None, clears all cached data.
        """
        self._cache.clear(source=source)
        if source is not None:
            logger.info("cache_cleared: source=%s", source)
        else:
            logger.info("cache_cleared: all")

    def list_cached(self) -> list[dict[str, str | None]]:
        """
        List all cached entries.

        Returns
        -------
        list[dict[str, str | None]]
            List of dicts with keys: source, symbol, field, path, start_date, end_date.

        Examples
        --------
        >>> with Client(catalog="catalog.yaml") as client:
        ...     df = client.get(["sp500"], start="2024-01-01", end="2024-06-30")
        ...     for entry in client.list_cached():
        ...         print(f"{entry['source']}/{entry['symbol']}")
        bloomberg/SPX Index
        """
        return self._cache.list_cached_entries()

    def get_metadata(self, name: str) -> dict[str, Any]:
        """
        Retrieve metadata for a catalog entry.

        Frequency is inferred from source metadata if available, otherwise None.

        Parameters
        ----------
        name : str
            Catalog name.

        Returns
        -------
        dict[str, Any]
            Combined metadata from catalog entry and source adapter.

        Raises
        ------
        NameNotFoundError
            If name not in catalog.
        """
        entry = self._catalog.get(name)
        source = self._registry.get(entry.source)

        # Get source-specific metadata
        source_meta = source.get_metadata(entry.symbol)

        logger.debug(
            "get_metadata: name=%s, source=%s",
            name,
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
