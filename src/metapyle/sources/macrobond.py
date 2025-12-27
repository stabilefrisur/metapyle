"""Macrobond source adapter using macrobond_data_api library."""

import logging
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, register_source

__all__ = ["MacrobondSource"]

logger = logging.getLogger(__name__)

# Lazy import of macrobond_data_api to avoid import-time errors
_MDA_AVAILABLE: bool | None = None
_mda_module: Any = None


def _get_mda() -> Any:
    """Lazy import of macrobond_data_api module.

    Returns
    -------
    Any
        The macrobond_data_api module, or None if not available.
    """
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

    Uses macrobond_data_api for data retrieval. Automatically detects
    whether to use ComClient (desktop app) or WebClient (API credentials).

    Examples
    --------
    >>> source = MacrobondSource()
    >>> df = source.fetch("usgdp", "2020-01-01", "2024-12-31")
    """

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        unified: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from Macrobond.

        Parameters
        ----------
        symbol : str
            Macrobond series name (e.g., "usgdp", "gbcpi").
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        unified : bool, optional
            If True, use get_unified_series with kwargs pass-through.
            If False (default), use get_one_series.
        **kwargs : Any
            Additional parameters passed to get_unified_series when unified=True.
            E.g., frequency, currency, calendar_merge_mode.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named by symbol.

        Raises
        ------
        FetchError
            If macrobond_data_api is not available or API call fails.
        NoDataError
            If no data is returned for the symbol.
        """
        mda = _get_mda()
        if mda is None:
            logger.error("fetch_failed: symbol=%s, reason=mda_not_installed", symbol)
            raise FetchError(
                "macrobond_data_api package is not installed. "
                "Install with: pip install macrobond-data-api"
            )

        logger.debug(
            "fetch_start: symbol=%s, start=%s, end=%s, unified=%s",
            symbol,
            start,
            end,
            unified,
        )

        try:
            if unified:
                df = self._fetch_unified(mda, symbol, **kwargs)
            else:
                df = self._fetch_raw(mda, symbol)
        except (FetchError, NoDataError, NotImplementedError):
            raise
        except Exception as e:
            logger.error("fetch_failed: symbol=%s, error=%s", symbol, str(e))
            raise FetchError(f"Macrobond API error for {symbol}: {e}") from e

        if df.empty:
            logger.warning("fetch_empty: symbol=%s", symbol)
            raise NoDataError(f"No data returned for {symbol}")

        # Filter by date range
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (df.index >= start_dt) & (df.index <= end_dt)
        df_filtered = df.loc[mask]

        if df_filtered.empty:
            logger.warning(
                "fetch_no_data_in_range: symbol=%s, start=%s, end=%s",
                symbol,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end} for {symbol}")

        logger.info("fetch_complete: symbol=%s, rows=%d", symbol, len(df_filtered))
        return df_filtered

    def _fetch_raw(self, mda: Any, symbol: str) -> pd.DataFrame:
        """Fetch using get_one_series."""
        series = mda.get_one_series(symbol)
        df = series.values_to_pd_data_frame()

        # Convert to proper DataFrame structure
        df.index = pd.to_datetime(df["date"])
        df = df[["value"]].rename(columns={"value": symbol})
        return df

    def _fetch_unified(self, mda: Any, symbol: str, **kwargs: Any) -> pd.DataFrame:
        """Fetch using get_unified_series with kwargs pass-through."""
        result = mda.get_unified_series(symbol, **kwargs)
        df = result.to_pd_data_frame()

        # First column is typically date, rest are values
        if len(df.columns) < 2:
            # Malformed response - return empty DataFrame with proper structure
            return pd.DataFrame(columns=[symbol])

        df.index = pd.to_datetime(df.iloc[:, 0])
        df = df.iloc[:, 1:2]
        df.columns = [symbol]
        return df

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a Macrobond symbol.

        Parameters
        ----------
        symbol : str
            Macrobond series name.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary containing series information.

        Raises
        ------
        FetchError
            If macrobond_data_api is not available or API call fails.
        """
        raise NotImplementedError
