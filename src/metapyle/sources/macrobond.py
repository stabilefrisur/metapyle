"""Macrobond source adapter using macrobond_data_api library."""

import logging
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError  # noqa: F401
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
        raise NotImplementedError

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
