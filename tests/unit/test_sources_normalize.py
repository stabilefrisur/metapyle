"""Tests for normalize_dataframe utility."""

import pandas as pd
import pytest

from metapyle.sources.base import normalize_dataframe


class TestNormalizeDataframe:
    """Tests for normalize_dataframe function."""

    def test_tz_naive_localized_to_utc(self) -> None:
        """Tz-naive index should be localized to UTC."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        )
        assert df.index.tz is None

        result = normalize_dataframe(df)

        assert result.index.tz is not None
        assert str(result.index.tz) == "UTC"

    def test_tz_aware_non_utc_converted_to_utc(self) -> None:
        """Tz-aware non-UTC index should be converted to UTC."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]).tz_localize(
                "America/New_York"
            ),
        )
        assert str(df.index.tz) == "America/New_York"

        result = normalize_dataframe(df)

        assert str(result.index.tz) == "UTC"

    def test_already_utc_unchanged(self) -> None:
        """Already UTC index should remain UTC (idempotent)."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]).tz_localize("UTC"),
        )

        result = normalize_dataframe(df)

        assert str(result.index.tz) == "UTC"
        # Values unchanged
        assert list(result["value"]) == [1, 2, 3]

    def test_index_name_set_to_date(self) -> None:
        """Index name should be set to 'date'."""
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        df.index.name = "timestamp"

        result = normalize_dataframe(df)

        assert result.index.name == "date"

    def test_non_datetime_index_converted(self) -> None:
        """String index should be converted to DatetimeIndex."""
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=["2024-01-01", "2024-01-02"],
        )
        assert not isinstance(df.index, pd.DatetimeIndex)

        result = normalize_dataframe(df)

        assert isinstance(result.index, pd.DatetimeIndex)
        assert str(result.index.tz) == "UTC"

    def test_invalid_index_raises_valueerror(self) -> None:
        """Non-convertible index should raise ValueError."""
        df = pd.DataFrame(
            {"value": [1, 2]},
            index=["not-a-date", "also-not-a-date"],
        )

        with pytest.raises(ValueError, match="Cannot convert index to DatetimeIndex"):
            normalize_dataframe(df)

    def test_returns_dataframe(self) -> None:
        """Should return a DataFrame for chaining."""
        df = pd.DataFrame(
            {"value": [1]},
            index=pd.to_datetime(["2024-01-01"]),
        )

        result = normalize_dataframe(df)

        assert isinstance(result, pd.DataFrame)
