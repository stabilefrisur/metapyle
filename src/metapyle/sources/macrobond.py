"""Macrobond source adapter using macrobond_data_api library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["MacrobondSource"]

logger = logging.getLogger(__name__)

_MDA_AVAILABLE: bool | None = None
_mda_module: Any = None


def _get_mda() -> Any:
    """Lazy import of macrobond_data_api module."""
    global _MDA_AVAILABLE, _mda_module

    if _MDA_AVAILABLE is None:
        try:
            import macrobond_data_api as mda

            _mda_module = mda
            _MDA_AVAILABLE = True
        except (ImportError, Exception):
            _mda_module = None
            _MDA_AVAILABLE = False

    return _mda_module


@register_source("macrobond")
class MacrobondSource(BaseSource):
    """Source adapter for Macrobond data via macrobond_data_api.

    Uses get_series for batch fetching of multiple series in a single call.
    Supports unified=True kwarg to use get_unified_series() for server-side alignment.
    """

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        *,
        unified: bool = False,
        unified_options: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from Macrobond.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests. Field and path are ignored.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        unified : bool, optional
            If True, use get_unified_series() with server-side alignment.
            Defaults to False.
        unified_options : dict[str, Any] | None, optional
            Options passed to get_unified_series() when unified=True
            (e.g., frequency, currency, weekdays, calendar_merge_mode).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and columns named by symbol.

        Raises
        ------
        FetchError
            If macrobond_data_api not available or API call fails.
        NoDataError
            If no data returned or no data in date range.
        """
        if not requests:
            return pd.DataFrame()

        mda = _get_mda()
        if mda is None:
            logger.error("fetch_failed: reason=mda_not_installed")
            raise FetchError(
                "macrobond_data_api package is not installed. "
                "Install with: pip install macrobond-data-api"
            )

        if unified:
            return self._fetch_unified(mda, requests, start, end, **(unified_options or {}))
        else:
            return self._fetch_regular(mda, requests, start, end)

    def _fetch_regular(
        self,
        mda: Any,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Fetch using get_series() - existing behavior."""
        symbols = [req.symbol for req in requests]

        logger.debug(
            "fetch_start: symbols=%s, start=%s, end=%s, mode=regular",
            symbols,
            start,
            end,
        )

        try:
            series_list = mda.get_series(symbols)
        except Exception as e:
            logger.error("fetch_failed: symbols=%s, error=%s", symbols, str(e))
            raise FetchError(f"Macrobond API error: {e}") from e

        # Check for errors in any series
        for series in series_list:
            if series.is_error:
                logger.error(
                    "fetch_failed: symbol=%s, error=%s",
                    series.primary_name,
                    series.error_message,
                )
                raise FetchError(
                    f"Macrobond error for {series.primary_name}: {series.error_message}"
                )

        # Convert each series to DataFrame and merge
        dfs: list[pd.DataFrame] = []
        for series in series_list:
            df = series.values_to_pd_data_frame()
            df.index = pd.to_datetime(df["date"])
            df = df[["value"]].rename(columns={"value": series.primary_name})
            dfs.append(df)

        if not dfs:
            logger.warning("fetch_empty: symbols=%s", symbols)
            raise NoDataError(f"No data returned for {symbols}")

        # Merge all series on index
        result = dfs[0]
        for df in dfs[1:]:
            result = result.join(df, how="outer")

        # Filter by date range
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (result.index >= start_dt) & (result.index <= end_dt)
        result = result.loc[mask]

        # Normalize index name
        result.index.name = "date"

        # Ensure UTC timezone
        if result.index.tz is None:
            result.index = result.index.tz_localize("UTC")
        else:
            result.index = result.index.tz_convert("UTC")

        if result.empty:
            logger.warning(
                "fetch_no_data_in_range: symbols=%s, start=%s, end=%s",
                symbols,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end}")

        logger.info(
            "fetch_complete: symbols=%s, rows=%d, mode=regular",
            symbols,
            len(result),
        )
        return result

    def _fetch_unified(
        self,
        mda: Any,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Fetch using get_unified_series() with server-side alignment."""
        from macrobond_data_api.common.enums import (  # type: ignore[attr-defined]
            CalendarMergeMode,
            SeriesFrequency,
            SeriesWeekdays,
        )
        from macrobond_data_api.common.types import StartOrEndPoint  # type: ignore[attr-defined]

        symbols = [req.symbol for req in requests]

        logger.debug(
            "fetch_start: symbols=%s, start=%s, end=%s, mode=unified",
            symbols,
            start,
            end,
        )

        # Hardcoded defaults
        unified_kwargs: dict[str, Any] = {
            "frequency": SeriesFrequency.DAILY,
            "weekdays": SeriesWeekdays.MONDAY_TO_FRIDAY,
            "calendar_merge_mode": CalendarMergeMode.AVAILABLE_IN_ALL,
            "currency": "USD",
            "start_point": StartOrEndPoint(start, None),
            "end_point": StartOrEndPoint(end, None),
        }
        # User overrides take precedence
        unified_kwargs.update(kwargs)

        try:
            result = mda.get_unified_series(*symbols, **unified_kwargs)
        except Exception as e:
            logger.error("fetch_failed: symbols=%s, error=%s, mode=unified", symbols, str(e))
            raise FetchError(f"Macrobond unified API error: {e}") from e

        # Convert to DataFrame
        df = result.to_pd_data_frame()

        if df.empty:
            logger.warning("fetch_empty: symbols=%s, mode=unified", symbols)
            raise NoDataError(f"No unified data returned for {symbols}")

        # Ensure index is DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # Normalize index name
        df.index.name = "date"

        # Ensure UTC timezone
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        logger.info(
            "fetch_complete: symbols=%s, rows=%d, mode=unified",
            symbols,
            len(df),
        )
        return df

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a Macrobond symbol."""
        mda = _get_mda()
        if mda is None:
            logger.error("get_metadata_failed: symbol=%s, reason=mda_not_installed", symbol)
            raise FetchError(
                "macrobond_data_api package is not installed. "
                "Install with: pip install macrobond-data-api"
            )

        logger.debug("get_metadata: symbol=%s", symbol)

        try:
            entity = mda.get_one_entity(symbol)
            metadata = dict(entity.metadata)
        except Exception as e:
            logger.error("get_metadata_failed: symbol=%s, error=%s", symbol, str(e))
            raise FetchError(f"Failed to get metadata for {symbol}: {e}") from e

        logger.info("get_metadata_complete: symbol=%s", symbol)
        return metadata
