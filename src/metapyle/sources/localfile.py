"""Local file source adapter for CSV and Parquet files."""

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, FetchRequest, register_source

__all__ = ["LocalFileSource"]

logger = logging.getLogger(__name__)


@register_source("localfile")
class LocalFileSource(BaseSource):
    """Source adapter for reading local CSV and Parquet files.

    All requests in a batch must reference the same file path.
    The symbol parameter is the column name to extract from the file.
    """

    def fetch(
        self,
        requests: Sequence[FetchRequest],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from a local file.

        Parameters
        ----------
        requests : Sequence[FetchRequest]
            One or more fetch requests. All must have same path.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and one column per request.

        Raises
        ------
        FetchError
            If path missing, paths differ, file not found, or column not found.
        NoDataError
            If file is empty or no data in date range.
        """
        if not requests:
            return pd.DataFrame()

        # Validate all requests have same path
        paths = {req.path for req in requests}
        if None in paths:
            logger.error("fetch_failed: reason=path_not_provided")
            raise FetchError("path is required for localfile source")
        if len(paths) > 1:
            logger.error("fetch_failed: reason=different_paths")
            raise FetchError("All requests must reference the same path")

        # Type narrowing: path is guaranteed non-None after validation above
        path = requests[0].path
        if path is None:  # pragma: no cover - validated above
            raise FetchError("path is required for localfile source")
        file_path = Path(path)
        symbols = [req.symbol for req in requests]

        logger.debug(
            "fetch_start: path=%s, symbols=%s, start=%s, end=%s",
            path,
            symbols,
            start,
            end,
        )

        if not file_path.exists():
            logger.error("fetch_failed: path=%s, reason=file_not_found", path)
            raise FetchError(f"File not found: {path}")

        try:
            df = self._read_file(file_path)
        except FetchError:
            raise
        except Exception as e:
            logger.error("fetch_failed: path=%s, reason=%s", path, str(e))
            raise FetchError(f"Failed to read file: {path}") from e

        if df.empty:
            logger.warning("fetch_empty: path=%s, reason=empty_file", path)
            raise NoDataError(f"File is empty: {path}")

        # Check all requested columns exist
        missing = [s for s in symbols if s not in df.columns]
        if missing:
            available = ", ".join(str(c) for c in df.columns)
            logger.error("fetch_failed: path=%s, missing=%s", path, missing)
            raise FetchError(f"Column(s) {missing} not found in {path}. Available: {available}")

        # Extract requested columns
        df = df[symbols]

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.error("fetch_failed: path=%s, reason=invalid_datetime_index", path)
                raise FetchError(f"Cannot convert index to datetime: {path}") from e

        # Filter by date range
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (df.index >= start_dt) & (df.index <= end_dt)
        df_filtered = df.loc[mask]

        if df_filtered.empty:
            logger.warning(
                "fetch_no_data_in_range: path=%s, start=%s, end=%s",
                path,
                start,
                end,
            )
            raise NoDataError(f"No data in date range {start} to {end}: {path}")

        # Normalize index name
        df_filtered.index.name = "date"

        # Ensure UTC timezone
        if df_filtered.index.tz is None:
            df_filtered.index = df_filtered.index.tz_localize("UTC")
        else:
            df_filtered.index = df_filtered.index.tz_convert("UTC")

        logger.info(
            "fetch_complete: path=%s, symbols=%s, rows=%d",
            path,
            symbols,
            len(df_filtered),
        )
        return df_filtered

    def _read_file(self, path: Path) -> pd.DataFrame:
        """Read a file based on its extension."""
        suffix = path.suffix.lower()

        if suffix == ".csv":
            logger.debug("reading_csv: path=%s", path)
            return pd.read_csv(path, index_col=0, parse_dates=True)
        elif suffix == ".parquet":
            logger.debug("reading_parquet: path=%s", path)
            df = pd.read_parquet(path)
            if not isinstance(df.index, pd.DatetimeIndex):
                date_cols = [c for c in df.columns if c.lower() in ("date", "datetime", "time")]
                if date_cols:
                    df = df.set_index(date_cols[0])
                    df.index = pd.to_datetime(df.index)
            return df
        else:
            raise FetchError(f"Unsupported file extension: {suffix}")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """Retrieve metadata for a local file."""
        path = Path(symbol)
        logger.debug("get_metadata: path=%s", symbol)

        metadata: dict[str, Any] = {
            "source": "localfile",
            "path": str(path.absolute()),
            "filename": path.name,
            "extension": path.suffix.lower(),
        }

        if path.exists():
            metadata["exists"] = True
            metadata["size_bytes"] = path.stat().st_size
        else:
            metadata["exists"] = False

        return metadata
