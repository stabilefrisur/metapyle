"""Bloomberg source adapter using xbbg library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    make_column_name,
    register_source,
)

__all__ = ["BloombergSource"]

logger = logging.getLogger(__name__)

_XBBG_AVAILABLE: bool | None = None
_blp_module: Any = None


def _get_blp() -> Any:
    """Lazy import of xbbg.blp module."""
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

    Uses xbbg.blp.bdh for historical data retrieval. Supports batch
    fetching of multiple tickers and fields in a single API call.
    """

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch historical data from Bloomberg.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests. Field defaults to PX_LAST if not specified.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and columns named "symbol::field".

        Raises
        ------
        FetchError
            If xbbg not available or API call fails.
        NoDataError
            If no data returned.
        """
        if not requests:
            return pd.DataFrame()

        blp = _get_blp()
        if blp is None:
            logger.error("fetch_failed: reason=xbbg_not_installed")
            raise FetchError("xbbg package is not installed. Install with: pip install xbbg")

        # Collect unique tickers and fields
        tickers = list(dict.fromkeys(req.symbol for req in requests))
        fields = list(dict.fromkeys(req.field or "PX_LAST" for req in requests))

        logger.debug(
            "fetch_start: tickers=%s, fields=%s, start=%s, end=%s",
            tickers,
            fields,
            start,
            end,
        )

        try:
            df = blp.bdh(tickers, fields, start, end)
        except Exception as e:
            logger.error("fetch_failed: error=%s", str(e))
            raise FetchError(f"Bloomberg API error: {e}") from e

        if df.empty:
            logger.warning("fetch_empty: tickers=%s, fields=%s", tickers, fields)
            raise NoDataError(f"No data returned for {tickers} with fields {fields}")

        # Ensure DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # Rename columns from MultiIndex (ticker, field) to "ticker::field"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [make_column_name(ticker, field) for ticker, field in df.columns]
        else:
            # Single ticker/field case
            req = requests[0]
            field = req.field or "PX_LAST"
            df.columns = [make_column_name(req.symbol, field)]

        # Filter to only requested symbol::field combinations
        requested_cols = [make_column_name(req.symbol, req.field or "PX_LAST") for req in requests]
        df = df[[c for c in requested_cols if c in df.columns]]

        # Normalize index name
        df.index.name = "date"

        # Ensure UTC timezone
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        logger.info(
            "fetch_complete: tickers=%s, fields=%s, rows=%d",
            tickers,
            fields,
            len(df),
        )
        return df

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a Bloomberg symbol."""
        logger.debug("get_metadata: symbol=%s", symbol)
        _get_blp()

        return {
            "source": "bloomberg",
            "symbol": symbol,
            "xbbg_available": _XBBG_AVAILABLE or False,
        }
