"""gs-quant source adapter using gs_quant library."""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, make_column_name, register_source

__all__ = ["GSQuantSource"]

logger = logging.getLogger(__name__)

_GSQUANT_AVAILABLE: bool | None = None
_gsquant_modules: dict[str, Any] = {}


def _parse_field(field: str) -> tuple[str, str]:
    """
    Parse field into dataset_id and value_column.

    Parameters
    ----------
    field : str
        Field in format "dataset_id::value_column".

    Returns
    -------
    tuple[str, str]
        (dataset_id, value_column)

    Raises
    ------
    ValueError
        If field format is invalid.
    """
    if "::" not in field:
        raise ValueError(f"Invalid field format: '{field}'. Expected 'dataset_id::value_column'")

    parts = field.split("::", 1)
    dataset_id, value_column = parts[0], parts[1]

    if not dataset_id or not value_column:
        raise ValueError(
            f"Invalid field format: '{field}'. Both dataset_id and value_column required"
        )

    return dataset_id, value_column


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
            If gs-quant not available, field format invalid, or API call fails.
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

        Dataset = gs["Dataset"]

        # Group requests by dataset_id
        groups: dict[str, list[FetchRequest]] = {}
        value_columns: dict[str, str] = {}

        for req in requests:
            if not req.field:
                raise FetchError(
                    "gsquant source requires field in format 'dataset_id::value_column'"
                )

            try:
                dataset_id, value_column = _parse_field(req.field)
            except ValueError as e:
                raise FetchError(str(e)) from e

            if dataset_id not in groups:
                groups[dataset_id] = []
                value_columns[dataset_id] = value_column
            elif value_columns[dataset_id] != value_column:
                raise FetchError(
                    f"Cannot batch requests with different value columns for same dataset: "
                    f"{value_columns[dataset_id]} vs {value_column}"
                )

            groups[dataset_id].append(req)

        # Fetch each dataset group
        result_dfs: list[pd.DataFrame] = []

        for dataset_id, group_requests in groups.items():
            symbols = [req.symbol for req in group_requests]
            value_column = value_columns[dataset_id]

            # Merge params from all requests
            merged_params: dict[str, Any] = {}
            for req in group_requests:
                if req.params:
                    merged_params.update(req.params)

            logger.debug(
                "fetch_start: dataset=%s, symbols=%s, params=%s",
                dataset_id,
                symbols,
                merged_params,
            )

            try:
                ds = Dataset(dataset_id)
                data = ds.get_data(start, end, bbid=symbols, **merged_params)
            except (FetchError, NoDataError):
                raise
            except Exception as e:
                logger.error("fetch_failed: dataset=%s, error=%s", dataset_id, str(e))
                raise FetchError(f"gs-quant API error for {dataset_id}: {e}") from e

            if data.empty:
                logger.warning("fetch_empty: dataset=%s, symbols=%s", dataset_id, symbols)
                raise NoDataError(f"No data returned for {symbols} from {dataset_id}")

            # Pivot to wide format
            pivoted = pd.pivot_table(
                data,
                values=value_column,
                index=["date"],
                columns=["bbid"],
            )

            # Ensure DatetimeIndex
            if not isinstance(pivoted.index, pd.DatetimeIndex):
                pivoted.index = pd.to_datetime(pivoted.index)

            # Rename columns to include field for uniqueness across datasets
            # Build a mapping from symbol to full column name
            field_str = f"{dataset_id}::{value_column}"
            rename_map = {symbol: make_column_name(symbol, field_str) for symbol in pivoted.columns}
            pivoted = pivoted.rename(columns=rename_map)

            result_dfs.append(pivoted)

        # Merge all results
        if not result_dfs:
            return pd.DataFrame()

        result = result_dfs[0]
        for df in result_dfs[1:]:
            result = result.join(df, how="outer")

        # Normalize index name
        result.index.name = "date"

        # Ensure UTC timezone
        if result.index.tz is None:
            result.index = result.index.tz_localize("UTC")
        else:
            result.index = result.index.tz_convert("UTC")

        logger.info(
            "fetch_complete: columns=%s, rows=%d",
            list(result.columns),
            len(result),
        )
        return result

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Return metadata for a symbol.

        Parameters
        ----------
        symbol : str
            Symbol to retrieve metadata for.

        Returns
        -------
        dict[str, Any]
            Empty dict (gs-quant metadata requires session and is complex).
        """
        return {}
