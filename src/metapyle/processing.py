"""Processing utilities for frequency alignment and data transformation."""

import logging

import pandas as pd

__all__ = ["align_to_frequency", "flatten_to_tall"]

logger = logging.getLogger(__name__)


def align_to_frequency(
    df: pd.DataFrame,
    target_frequency: str,
) -> pd.DataFrame:
    """
    Resample a DataFrame to a target frequency.

    Downsampling (e.g., daily to monthly) takes the last value of each period.
    Upsampling (e.g., monthly to daily) forward-fills values.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a DatetimeIndex to resample.
    target_frequency : str
        Target pandas frequency string (e.g., "D", "B", "ME", "QE", "YE").

    Returns
    -------
    pd.DataFrame
        Resampled DataFrame aligned to the target frequency.

    Raises
    ------
    ValueError
        If the frequency string is not valid for pandas.

    Examples
    --------
    >>> import pandas as pd
    >>> dates = pd.date_range("2024-01-01", periods=90, freq="D")
    >>> df = pd.DataFrame({"value": range(90)}, index=dates)
    >>> aligned = align_to_frequency(df, "ME")
    >>> len(aligned)
    3
    """
    logger.debug(
        "aligning_frequency: rows=%d, target=%s",
        len(df),
        target_frequency,
    )

    # Resample using last value for downsampling, forward-fill for upsampling
    resampled = df.resample(target_frequency).last()

    # Forward-fill NaN values (handles upsampling)
    resampled = resampled.ffill()

    logger.debug("alignment_complete: input_rows=%d, output_rows=%d", len(df), len(resampled))
    return resampled


def flatten_to_tall(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    symbol_col: str = "symbol",
    value_col: str = "value",
) -> pd.DataFrame:
    """
    Convert wide DataFrame to tall/long format.

    Wide format has one column per symbol. Tall format has columns for
    date, symbol, and value - one row per observation.

    Parameters
    ----------
    df : pd.DataFrame
        Wide DataFrame with DatetimeIndex and one column per symbol.
    date_col : str, optional
        Name for the date column. Default is "date".
    symbol_col : str, optional
        Name for the symbol column. Default is "symbol".
    value_col : str, optional
        Name for the value column. Default is "value".

    Returns
    -------
    pd.DataFrame
        Tall DataFrame with columns: [date_col, symbol_col, value_col].

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame(
    ...     {"SPX": [100, 101], "VIX": [15, 16]},
    ...     index=pd.date_range("2024-01-01", periods=2, freq="D"),
    ... )
    >>> tall = flatten_to_tall(df)
    >>> tall
            date symbol  value
    0 2024-01-01    SPX  100.0
    1 2024-01-02    SPX  101.0
    2 2024-01-01    VIX   15.0
    3 2024-01-02    VIX   16.0
    """
    logger.debug("flatten_to_tall: wide_shape=%s", df.shape)

    if df.empty:
        return pd.DataFrame(columns=[date_col, symbol_col, value_col])

    # Reset index to get date as column
    df_reset = df.reset_index()
    date_column = df_reset.columns[0]  # First column is the former index

    # Melt from wide to tall
    tall = pd.melt(
        df_reset,
        id_vars=[date_column],
        var_name=symbol_col,
        value_name=value_col,
    )

    # Rename date column
    tall = tall.rename(columns={date_column: date_col})

    # Sort by symbol then date
    tall = tall.sort_values([symbol_col, date_col]).reset_index(drop=True)

    logger.debug("flatten_to_tall_complete: tall_shape=%s", tall.shape)
    return tall
