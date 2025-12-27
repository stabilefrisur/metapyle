"""Bloomberg source adapter using xbbg library."""

import logging
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, register_source

__all__ = ["BloombergSource"]

logger = logging.getLogger(__name__)

# Lazy import of xbbg to avoid import-time errors when blpapi is not installed.
# xbbg internally uses pytest.importorskip which causes test collection issues.
_XBBG_AVAILABLE: bool | None = None
_blp_module: Any = None


def _get_blp() -> Any:
    """Lazy import of xbbg.blp module.

    Returns
    -------
    Any
        The blp module from xbbg, or None if not available.
    """
    global _XBBG_AVAILABLE, _blp_module

    if _XBBG_AVAILABLE is None:
        try:
            from xbbg import blp

            _blp_module = blp
            _XBBG_AVAILABLE = True
        except (ImportError, Exception):
            _blp_module = None
            _XBBG_AVAILABLE = False

    return _blp_module


@register_source("bloomberg")
class BloombergSource(BaseSource):
    """Source adapter for Bloomberg data via xbbg.

    Uses xbbg.blp.bdh for historical data retrieval. Requires the xbbg
    package to be installed and a Bloomberg Terminal connection.

    Examples
    --------
    >>> source = BloombergSource()
    >>> df = source.fetch("SPX Index", "2024-01-01", "2024-01-31")
    """

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        field: str = "PX_LAST",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch historical data from Bloomberg.

        Parameters
        ----------
        symbol : str
            Bloomberg ticker (e.g., "SPX Index", "AAPL US Equity").
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        field : str, optional
            Bloomberg field to fetch, by default "PX_LAST".
        **kwargs : Any
            Additional parameters passed to blp.bdh.

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named 'symbol_field'
            (e.g., 'SPX Index_PX_LAST').

        Raises
        ------
        FetchError
            If xbbg is not available or API call fails.
        NoDataError
            If no data is returned for the symbol.
        """
        blp = _get_blp()
        if blp is None:
            logger.error("fetch_failed: symbol=%s, reason=xbbg_not_installed", symbol)
            raise FetchError("xbbg package is not installed. Install with: pip install xbbg")

        logger.debug(
            "fetch_start: symbol=%s, start=%s, end=%s, field=%s",
            symbol,
            start,
            end,
            field,
        )

        try:
            df = blp.bdh(symbol, field, start, end, **kwargs)
        except Exception as e:
            logger.error("fetch_failed: symbol=%s, error=%s", symbol, str(e))
            raise FetchError(f"Bloomberg API error for {symbol}: {e}") from e

        if df.empty:
            logger.warning("fetch_empty: symbol=%s, field=%s", symbol, field)
            raise NoDataError(f"No data returned for {symbol} with field {field}")

        # Construct column name as symbol_field (e.g., "SPX Index_PX_LAST")
        col_name = f"{symbol}_{field}"

        # Convert MultiIndex columns from bdh response to symbol_field column
        # bdh returns DataFrame with MultiIndex columns: (ticker, field)
        if isinstance(df.columns, pd.MultiIndex):
            # Extract the first column's data
            df = df.iloc[:, 0].to_frame(name=col_name)
        elif len(df.columns) == 1:
            df = df.rename(columns={df.columns[0]: col_name})

        # Ensure DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        logger.info("fetch_complete: symbol=%s, rows=%d", symbol, len(df))
        return df

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a Bloomberg symbol.

        Parameters
        ----------
        symbol : str
            Bloomberg ticker.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary containing source and symbol information.
        """
        logger.debug("get_metadata: symbol=%s", symbol)

        # Trigger lazy import to determine availability
        _get_blp()

        return {
            "source": "bloomberg",
            "symbol": symbol,
            "xbbg_available": _XBBG_AVAILABLE if _XBBG_AVAILABLE is not None else False,
        }
