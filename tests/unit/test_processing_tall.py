"""Tests for flatten_to_tall function."""

import pandas as pd
import pytest

from metapyle.processing import flatten_to_tall


class TestFlattenToTall:
    """Tests for flatten_to_tall."""

    def test_basic_flatten(self) -> None:
        """Basic wide to tall conversion."""
        df = pd.DataFrame(
            {
                "SPX": [100, 101, 102],
                "VIX": [15, 16, 17],
            },
            index=pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC"),
        )
        
        result = flatten_to_tall(df)
        
        assert len(result) == 6  # 3 dates * 2 symbols
        assert list(result.columns) == ["date", "symbol", "value"]
        assert set(result["symbol"]) == {"SPX", "VIX"}

    def test_preserves_data_values(self) -> None:
        """Values are correctly preserved in tall format."""
        df = pd.DataFrame(
            {
                "A": [1.0, 2.0],
                "B": [3.0, 4.0],
            },
            index=pd.date_range("2024-01-01", periods=2, freq="D", tz="UTC"),
        )
        
        result = flatten_to_tall(df)
        
        # Check specific values
        a_values = result[result["symbol"] == "A"]["value"].tolist()
        assert a_values == [1.0, 2.0]
        
        b_values = result[result["symbol"] == "B"]["value"].tolist()
        assert b_values == [3.0, 4.0]

    def test_custom_column_names(self) -> None:
        """Custom column names can be specified."""
        df = pd.DataFrame(
            {"X": [1, 2]},
            index=pd.date_range("2024-01-01", periods=2, freq="D", tz="UTC"),
        )
        
        result = flatten_to_tall(
            df,
            date_col="timestamp",
            symbol_col="ticker",
            value_col="price",
        )
        
        assert list(result.columns) == ["timestamp", "ticker", "price"]

    def test_empty_dataframe(self) -> None:
        """Empty DataFrame returns empty tall DataFrame."""
        df = pd.DataFrame(index=pd.DatetimeIndex([], name="date"))
        
        result = flatten_to_tall(df)
        
        assert len(result) == 0
        assert list(result.columns) == ["date", "symbol", "value"]
