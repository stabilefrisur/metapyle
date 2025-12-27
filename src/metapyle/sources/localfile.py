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

    The symbol parameter is the column name to extract from the file.
    The path to the file is provided via the `path` keyword argument.

    Examples
    --------
    >>> source = LocalFileSource()
    >>> df = source.fetch("GDP_US", "2020-01-01", "2020-12-31", path="/path/to/data.csv")
    """

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        path: str | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Fetch time-series data from a local file.

        Parameters
        ----------
        symbol : str
            Column name to extract from the file.
        start : str
            Start date in ISO format (YYYY-MM-DD).
        end : str
            End date in ISO format (YYYY-MM-DD).
        path : str | None
            Path to the data file (CSV or Parquet). Required.
        **kwargs : Any
            Additional parameters (currently unused).

        Returns
        -------
        pd.DataFrame
            DataFrame with DatetimeIndex and single column named by symbol.

        Raises
        ------
        FetchError
            If path not provided, file not found, column not found, or read fails.
        NoDataError
            If file is empty or no data in date range.
        """
        if path is None:
            logger.error("fetch_failed: symbol=%s, reason=path_not_provided", symbol)
            raise FetchError("path is required for localfile source")

        file_path = Path(path)
        logger.debug("fetch_start: path=%s, symbol=%s, start=%s, end=%s", path, symbol, start, end)

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

        # Check if requested column exists
        if symbol not in df.columns:
            available = ", ".join(str(c) for c in df.columns)
            logger.error("fetch_failed: path=%s, symbol=%s, reason=column_not_found", path, symbol)
            raise FetchError(f"Column '{symbol}' not found in {path}. Available: {available}")

        # Extract only the requested column
        df = df[[symbol]]

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.error("fetch_failed: path=%s, reason=invalid_datetime_index", path)
                raise FetchError(f"Cannot convert index to datetime: {path}") from e

        # Filter by date range using boolean indexing for type safety
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        mask = (df.index >= start_dt) & (df.index <= end_dt)
        df_filtered = df.loc[mask]

        if df_filtered.empty:
            logger.warning("fetch_no_data_in_range: path=%s, start=%s, end=%s", path, start, end)
            raise NoDataError(f"No data in date range {start} to {end}: {path}")

        logger.info("fetch_complete: path=%s, symbol=%s, rows=%d", path, symbol, len(df_filtered))
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
