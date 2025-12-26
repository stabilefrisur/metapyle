"""Processing utilities for frequency alignment and data transformation."""

import logging

import pandas as pd

from metapyle.catalog import Frequency

__all__ = ["FREQUENCY_MAP", "align_to_frequency", "get_pandas_frequency"]

logger = logging.getLogger(__name__)


FREQUENCY_MAP: dict[str | Frequency, str] = {
    Frequency.DAILY: "D",
    Frequency.WEEKLY: "W",
    Frequency.MONTHLY: "ME",
    Frequency.QUARTERLY: "QE",
    Frequency.ANNUAL: "YE",
    "daily": "D",
    "weekly": "W",
    "monthly": "ME",
    "quarterly": "QE",
    "annual": "YE",
}


def get_pandas_frequency(frequency: Frequency | str) -> str:
    """
    Map a Frequency enum or string to a pandas frequency string.

    Parameters
    ----------
    frequency : Frequency | str
        The frequency to map. Can be a Frequency enum member or a lowercase
        string like "daily", "weekly", etc.

    Returns
    -------
    str
        The corresponding pandas frequency string (e.g., "D", "ME", "QE").

    Raises
    ------
    ValueError
        If the frequency is not recognized.

    Examples
    --------
    >>> get_pandas_frequency(Frequency.DAILY)
    'D'
    >>> get_pandas_frequency("monthly")
    'ME'
    """
    if frequency not in FREQUENCY_MAP:
        raise ValueError(
            f"Unknown frequency: {frequency}. "
            f"Valid values: {', '.join(str(k) for k in FREQUENCY_MAP)}"
        )
    logger.debug("frequency_mapped: input=%s, output=%s", frequency, FREQUENCY_MAP[frequency])
    return FREQUENCY_MAP[frequency]


def align_to_frequency(
    df: pd.DataFrame,
    target_frequency: Frequency | str,
) -> pd.DataFrame:
    """
    Resample a DataFrame to a target frequency.

    Downsampling (e.g., daily to monthly) takes the last value of each period.
    Upsampling (e.g., monthly to daily) forward-fills values.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a DatetimeIndex to resample.
    target_frequency : Frequency | str
        Target frequency for alignment.

    Returns
    -------
    pd.DataFrame
        Resampled DataFrame aligned to the target frequency.

    Raises
    ------
    ValueError
        If the frequency is not recognized.

    Examples
    --------
    >>> import pandas as pd
    >>> dates = pd.date_range("2024-01-01", periods=90, freq="D")
    >>> df = pd.DataFrame({"value": range(90)}, index=dates)
    >>> aligned = align_to_frequency(df, Frequency.MONTHLY)
    >>> len(aligned)
    3
    """
    pandas_freq = get_pandas_frequency(target_frequency)
    logger.debug(
        "aligning_frequency: rows=%d, target=%s, pandas_freq=%s",
        len(df),
        target_frequency,
        pandas_freq,
    )

    # Resample using last value for downsampling, forward-fill for upsampling
    resampled = df.resample(pandas_freq).last()

    # Forward-fill NaN values (handles upsampling)
    resampled = resampled.ffill()

    logger.debug("alignment_complete: input_rows=%d, output_rows=%d", len(df), len(resampled))
    return resampled
