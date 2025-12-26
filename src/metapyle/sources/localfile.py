"""Local file source adapter for CSV and Parquet files."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from metapyle.exceptions import FetchError, NoDataError
from metapyle.sources.base import BaseSource, register_source

__all__ = ["LocalFileSource"]

logger = logging.getLogger(__name__)


@register_source("localfile")
class LocalFileSource(BaseSource):
    """Source adapter for reading local CSV and Parquet files.

    The symbol parameter is treated as a file path. Supports CSV files
    (with date index) and Parquet files.

    Examples
    --------
    >>> source = LocalFileSource()
    >>> df = source.fetch("/path/to/data.csv", "2020-01-01", "2020-12-31")
    """

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from a local file.

        Parameters
        ----------
        symbol : str
            Path to the data file (CSV or Parquet).
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        **kwargs : Any
            Additional parameters (currently unused).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named 'value'.

        Raises
        ------
        FetchError
            If file not found or read fails.
        NoDataError
            If file is empty or no data in date range.
        """
        path = Path(symbol)
        logger.debug("fetch_start: path=%s, start=%s, end=%s", symbol, start, end)

        if not path.exists():
            logger.error("fetch_failed: path=%s, reason=file_not_found", symbol)
            raise FetchError(f"File not found: {symbol}")

        try:
            df = self._read_file(path)
        except Exception as e:
            logger.error("fetch_failed: path=%s, reason=%s", symbol, str(e))
            raise FetchError(f"Failed to read file: {symbol}") from e

        if df.empty:
            logger.warning("fetch_empty: path=%s, reason=empty_file", symbol)
            raise NoDataError(f"File is empty: {symbol}")

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.error("fetch_failed: path=%s, reason=invalid_datetime_index", symbol)
                raise FetchError(f"Cannot convert index to datetime: {symbol}") from e

        # Rename single column to 'value' if needed
        if len(df.columns) == 1 and df.columns[0] != "value":
            original_col = df.columns[0]
            df = df.rename(columns={original_col: "value"})
            logger.debug("column_renamed: path=%s, from=%s, to=value", symbol, original_col)

        # Filter by date range using boolean indexing for type safety
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (df.index >= start_dt) & (df.index <= end_dt)
        df_filtered = df.loc[mask]

        if df_filtered.empty:
            logger.warning("fetch_no_data_in_range: path=%s, start=%s, end=%s", symbol, start, end)
            raise NoDataError(f"No data in date range {start} to {end}: {symbol}")

        logger.info("fetch_complete: path=%s, rows=%d", symbol, len(df_filtered))
        return df_filtered

    def _read_file(self, path: Path) -> pd.DataFrame:
        """
        Read a file based on its extension.

        Parameters
        ----------
        path : Path
            Path to the file.

        Returns
        -------
        pd.DataFrame
            DataFrame read from the file.

        Raises
        ------
        FetchError
            If file extension is not supported.
        """
        suffix = path.suffix.lower()

        if suffix == ".csv":
            logger.debug("reading_csv: path=%s", path)
            return pd.read_csv(path, index_col=0, parse_dates=True)
        elif suffix == ".parquet":
            logger.debug("reading_parquet: path=%s", path)
            df = pd.read_parquet(path)
            # Parquet files may have date column, set as index if not already
            if not isinstance(df.index, pd.DatetimeIndex):
                # Look for common date column names
                date_cols = [c for c in df.columns if c.lower() in ("date", "datetime", "time")]
                if date_cols:
                    df = df.set_index(date_cols[0])
                    df.index = pd.to_datetime(df.index)
            return df
        else:
            raise FetchError(f"Unsupported file extension: {suffix}")

    def get_metadata(self, symbol: str) -> dict[str, Any]:
        """
        Retrieve metadata for a local file.

        Parameters
        ----------
        symbol : str
            Path to the data file.

        Returns
        -------
        dict[str, Any]
            Metadata dictionary containing file information.
        """
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
