"""Catalog system for mapping human-readable names to data sources."""

from dataclasses import dataclass
from enum import StrEnum, auto

__all__ = ["CatalogEntry", "Frequency"]


class Frequency(StrEnum):
    """Supported data frequencies."""

    DAILY = auto()
    WEEKLY = auto()
    MONTHLY = auto()
    QUARTERLY = auto()
    ANNUAL = auto()


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogEntry:
    """
    A single catalog entry mapping a name to a data source.

    Parameters
    ----------
    my_name : str
        Unique human-readable identifier for this data series.
    source : str
        Name of the registered source adapter (e.g., "bloomberg").
    symbol : str
        Source-specific identifier (e.g., "SPX Index").
    frequency : Frequency
        Data frequency (daily, weekly, monthly, quarterly, annual).
    field : str | None, optional
        Source-specific field name (e.g., "PX_LAST" for Bloomberg).
    description : str | None, optional
        Human-readable description of the data series.
    unit : str | None, optional
        Unit of measurement (e.g., "USD billions", "points").
    """

    my_name: str
    source: str
    symbol: str
    frequency: Frequency
    field: str | None = None
    description: str | None = None
    unit: str | None = None
