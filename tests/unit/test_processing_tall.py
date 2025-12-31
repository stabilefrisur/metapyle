"""Tests for flatten_to_tall function."""

from pathlib import Path

import pandas as pd
import pytest

from metapyle import Client
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


class TestClientOutputFormat:
    """Tests for Client.get() output_format parameter."""

    def test_default_wide_format(self, tmp_path: Path) -> None:
        """Default output format is wide."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,a,b\n2024-01-01,1,2\n2024-01-02,3,4\n2024-01-03,5,6\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: alpha
  source: localfile
  symbol: a
  path: {csv_file}
- my_name: beta
  source: localfile
  symbol: b
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            df = client.get(["alpha", "beta"], start="2024-01-01", end="2024-01-03")

        # Wide format: columns are symbol names
        assert list(df.columns) == ["alpha", "beta"]
        assert len(df) == 3

    def test_tall_format(self, tmp_path: Path) -> None:
        """output_format='tall' returns melted DataFrame."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,a,b\n2024-01-01,1,2\n2024-01-02,3,4\n2024-01-03,5,6\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: alpha
  source: localfile
  symbol: a
  path: {csv_file}
- my_name: beta
  source: localfile
  symbol: b
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            df = client.get(
                ["alpha", "beta"],
                start="2024-01-01",
                end="2024-01-03",
                output_format="tall",
            )

        # Tall format: date, symbol, value columns
        assert list(df.columns) == ["date", "symbol", "value"]
        assert len(df) == 6  # 3 dates * 2 symbols
        assert set(df["symbol"]) == {"alpha", "beta"}

    def test_invalid_output_format_raises(self, tmp_path: Path) -> None:
        """Invalid output_format raises ValueError."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("date,a\n2024-01-01,1\n2024-01-02,2\n2024-01-03,3\n")

        catalog = tmp_path / "catalog.yaml"
        catalog.write_text(f"""
- my_name: test
  source: localfile
  symbol: a
  path: {csv_file}
""")

        with Client(catalog=catalog, cache_enabled=False) as client:
            with pytest.raises(ValueError, match="output_format must be"):
                client.get(["test"], start="2024-01-01", end="2024-01-03", output_format="invalid")
