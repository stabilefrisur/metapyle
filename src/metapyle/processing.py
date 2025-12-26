"""Processing utilities for frequency alignment and data transformation."""

import logging

import pandas as pd

__all__ = ["align_to_frequency"]

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
