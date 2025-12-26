"""Unit tests for processing module."""

import pandas as pd
import pytest

from metapyle.processing import align_to_frequency


def test_align_to_frequency_downsample_to_monthly() -> None:
    """align_to_frequency downsamples daily data to monthly using last value."""
    dates = pd.date_range("2024-01-01", periods=90, freq="D")
    df = pd.DataFrame({"value": range(90)}, index=dates)

    result = align_to_frequency(df, "ME")

    # Should have 3 months: Jan, Feb, Mar
    assert len(result) == 3

    # Last value of January (index 30, value 30 for Jan 31)
    assert result.iloc[0]["value"] == 30

    # Last value of February (index 59 for Feb 29 in 2024, value 59)
    assert result.iloc[1]["value"] == 59

    # Last value of March (index 89 for Mar 30, value 89)
    assert result.iloc[2]["value"] == 89


def test_align_to_frequency_downsample_to_quarterly() -> None:
    """align_to_frequency downsamples daily data to quarterly."""
    dates = pd.date_range("2024-01-01", periods=180, freq="D")
    df = pd.DataFrame({"value": range(180)}, index=dates)

    result = align_to_frequency(df, "QE")

    # Should have 2 quarters: Q1 (ends Mar 31), Q2 (ends Jun 28)
    assert len(result) == 2


def test_align_to_frequency_upsample_to_daily() -> None:
    """align_to_frequency upsamples monthly data to daily using forward-fill."""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    df = pd.DataFrame({"value": [100, 200, 300]}, index=dates)

    result = align_to_frequency(df, "D")

    # First day should be Jan 31 with value 100
    assert result.iloc[0]["value"] == 100

    # February values should be forward-filled from 100 until Feb 29
    feb_values = result.loc["2024-02"]
    assert (feb_values["value"].iloc[:-1] == 100).all()
    assert feb_values.iloc[-1]["value"] == 200

    # March values should have 200 forward-filled until Mar 31
    mar_values = result.loc["2024-03"]
    assert (mar_values["value"].iloc[:-1] == 200).all()
    assert mar_values.iloc[-1]["value"] == 300


def test_align_to_frequency_business_day() -> None:
    """align_to_frequency supports business day frequency."""
    dates = pd.date_range("2024-01-01", periods=31, freq="D")
    df = pd.DataFrame({"value": range(31)}, index=dates)

    result = align_to_frequency(df, "B")

    # Business days only (excludes weekends)
    assert len(result) < len(df)
    # All result dates should be weekdays
    assert all(d.dayofweek < 5 for d in result.index)


def test_align_to_frequency_week_end() -> None:
    """align_to_frequency supports weekly frequency."""
    dates = pd.date_range("2024-01-01", periods=28, freq="D")
    df = pd.DataFrame({"value": range(28)}, index=dates)

    result = align_to_frequency(df, "W")

    # 4 weeks in 28 days
    assert len(result) == 4


def test_align_to_frequency_business_month_end() -> None:
    """align_to_frequency supports business month-end frequency."""
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    df = pd.DataFrame({"value": range(60)}, index=dates)

    result = align_to_frequency(df, "BME")

    # 2 business month-ends (Jan 31, Feb 29)
    assert len(result) == 2
    # All should be weekdays
    assert all(d.dayofweek < 5 for d in result.index)


def test_align_to_frequency_year_end() -> None:
    """align_to_frequency supports year-end frequency."""
    dates = pd.date_range("2023-01-01", periods=730, freq="D")
    df = pd.DataFrame({"value": range(730)}, index=dates)

    result = align_to_frequency(df, "YE")

    # 2 years
    assert len(result) == 2


def test_align_to_frequency_invalid_frequency_raises() -> None:
    """align_to_frequency raises ValueError for invalid pandas frequency."""
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    df = pd.DataFrame({"value": range(10)}, index=dates)

    with pytest.raises(ValueError):
        align_to_frequency(df, "INVALID_FREQ")


def test_align_to_frequency_same_frequency() -> None:
    """align_to_frequency with same frequency returns equivalent data."""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    df = pd.DataFrame({"value": [100, 200, 300]}, index=dates)

    result = align_to_frequency(df, "ME")

    assert len(result) == 3
    assert list(result["value"]) == [100, 200, 300]


def test_align_to_frequency_quarterly_to_daily() -> None:
    """align_to_frequency can upsample quarterly to daily."""
    dates = pd.date_range("2024-03-31", periods=2, freq="QE")
    df = pd.DataFrame({"value": [1000, 2000]}, index=dates)

    result = align_to_frequency(df, "D")

    # First value should be 1000 (Q1 end)
    assert result.iloc[0]["value"] == 1000

    # Last value should be 2000 (Q2 end)
    assert result.iloc[-1]["value"] == 2000

    # Values in between should be forward-filled
    apr_1_value = result.loc["2024-04-01"]["value"]
    assert apr_1_value == 1000
