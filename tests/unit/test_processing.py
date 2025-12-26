"""Unit tests for processing module."""

import pandas as pd
import pytest

from metapyle.catalog import Frequency
from metapyle.processing import FREQUENCY_MAP, align_to_frequency, get_pandas_frequency


def test_get_pandas_frequency_mapping() -> None:
    """get_pandas_frequency maps Frequency enum values correctly."""
    assert get_pandas_frequency(Frequency.DAILY) == "D"
    assert get_pandas_frequency(Frequency.WEEKLY) == "W"
    assert get_pandas_frequency(Frequency.MONTHLY) == "ME"
    assert get_pandas_frequency(Frequency.QUARTERLY) == "QE"
    assert get_pandas_frequency(Frequency.ANNUAL) == "YE"


def test_get_pandas_frequency_from_string() -> None:
    """get_pandas_frequency maps string values correctly."""
    assert get_pandas_frequency("daily") == "D"
    assert get_pandas_frequency("weekly") == "W"
    assert get_pandas_frequency("monthly") == "ME"
    assert get_pandas_frequency("quarterly") == "QE"
    assert get_pandas_frequency("annual") == "YE"


def test_get_pandas_frequency_invalid() -> None:
    """get_pandas_frequency raises ValueError for unknown frequency."""
    with pytest.raises(ValueError, match="Unknown frequency"):
        get_pandas_frequency("biweekly")  # type: ignore[arg-type]


def test_align_to_frequency_downsample() -> None:
    """align_to_frequency downsamples daily data to monthly using last value."""
    dates = pd.date_range("2024-01-01", periods=90, freq="D")
    df = pd.DataFrame({"value": range(90)}, index=dates)

    result = align_to_frequency(df, Frequency.MONTHLY)

    # Should have 3 months: Jan, Feb, Mar
    assert len(result) == 3

    # Last value of January (index 30, value 30 for Jan 31)
    # Jan has 31 days, so indices 0-30 (values 0-30), last is 30
    assert result.iloc[0]["value"] == 30

    # Last value of February (index 59 for Feb 29 in 2024, value 59)
    # Feb 2024 has 29 days, so Jan(31) + Feb(29) = 60 days, indices 0-59
    assert result.iloc[1]["value"] == 59

    # Last value of March (index 89 for Mar 30, value 89)
    assert result.iloc[2]["value"] == 89


def test_align_to_frequency_upsample() -> None:
    """align_to_frequency upsamples monthly data to daily using forward-fill."""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    df = pd.DataFrame({"value": [100, 200, 300]}, index=dates)

    result = align_to_frequency(df, Frequency.DAILY)

    # First day should be Jan 31 with value 100
    assert result.iloc[0]["value"] == 100

    # February values should be forward-filled from 100 until Feb 29
    # Then 200 from Feb 29 onwards
    feb_values = result.loc["2024-02"]
    assert (feb_values["value"].iloc[:-1] == 100).all()  # Days before Feb 29
    assert feb_values.iloc[-1]["value"] == 200  # Feb 29

    # March values should have 200 forward-filled until Mar 31
    mar_values = result.loc["2024-03"]
    assert (mar_values["value"].iloc[:-1] == 200).all()  # Days before Mar 31
    assert mar_values.iloc[-1]["value"] == 300  # Mar 31


def test_align_to_frequency_no_change() -> None:
    """align_to_frequency with same frequency returns equivalent data."""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    df = pd.DataFrame({"value": [100, 200, 300]}, index=dates)

    result = align_to_frequency(df, Frequency.MONTHLY)

    # Same number of rows
    assert len(result) == 3

    # Same values
    assert list(result["value"]) == [100, 200, 300]


def test_align_to_frequency_quarterly_to_daily() -> None:
    """align_to_frequency can upsample quarterly to daily."""
    dates = pd.date_range("2024-03-31", periods=2, freq="QE")
    df = pd.DataFrame({"value": [1000, 2000]}, index=dates)

    result = align_to_frequency(df, Frequency.DAILY)

    # Q1 2024 ends Mar 31, Q2 ends Jun 30
    # Should have daily data from Mar 31 to Jun 30

    # First value should be 1000 (Q1 end)
    assert result.iloc[0]["value"] == 1000

    # Last value should be 2000 (Q2 end)
    assert result.iloc[-1]["value"] == 2000

    # Values in between should be forward-filled
    apr_1_value = result.loc["2024-04-01"]["value"]
    assert apr_1_value == 1000  # Forward-filled from Q1


def test_frequency_map_completeness() -> None:
    """FREQUENCY_MAP contains all Frequency enum values and string equivalents."""
    for freq in Frequency:
        assert freq in FREQUENCY_MAP
        assert freq.value in FREQUENCY_MAP
