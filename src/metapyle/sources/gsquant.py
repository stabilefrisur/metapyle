"""gs-quant source adapter using gs_quant library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["GSQuantSource"]

logger = logging.getLogger(__name__)

_GSQUANT_AVAILABLE: bool | None = None
_gsquant_modules: dict[str, Any] = {}


def _get_gsquant() -> dict[str, Any]:
    """Lazy import of gs_quant modules."""
    global _GSQUANT_AVAILABLE, _gsquant_modules

    if _GSQUANT_AVAILABLE is None:
        try:
            from gs_quant.data import Dataset
            from gs_quant.session import GsSession

            _gsquant_modules = {"Dataset": Dataset, "GsSession": GsSession}
            _GSQUANT_AVAILABLE = True
        except (ImportError, Exception):
            _gsquant_modules = {}
            _GSQUANT_AVAILABLE = False

    return _gsquant_modules


@register_source("gsquant")
class GSQuantSource(BaseSource):
    """Source adapter for Goldman Sachs Marquee data via gs-quant."""

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch data from gs-quant datasets.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            Fetch requests with field format "dataset_id::value_column".
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and symbol columns.

        Raises
        ------
        FetchError
            If gs-quant not available or API call fails.
        NoDataError
            If no data returned.
        """
        if not requests:
            return pd.DataFrame()

        gs = _get_gsquant()
        if not gs:
            logger.error("fetch_failed: reason=gsquant_not_installed")
            raise FetchError(
                "gs-quant package is not installed. Install with: pip install gs-quant"
            )

        raise NotImplementedError("GSQuantSource.fetch not yet implemented")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a gs-quant symbol."""
        _get_gsquant()
        return {
            "source": "gsquant",
            "symbol": symbol,
            "gsquant_available": _GSQUANT_AVAILABLE or False,
        }
