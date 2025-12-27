"""Macrobond source adapter using macrobond_data_api library."""

import logging
from typing import Any

import pandas as pd  # noqa: F401  # Used in Task 2

from metapyle.exceptions import FetchError, NoDataError  # noqa: F401  # Used in Task 2
from metapyle.sources.base import BaseSource, register_source  # noqa: F401  # Used in Task 2

# __all__ will be populated when MacrobondSource class is added in Task 2
__all__: list[str] = []

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
