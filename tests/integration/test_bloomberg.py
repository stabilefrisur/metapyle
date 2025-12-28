"""Integration tests for Bloomberg source."""

import pandas as pd
import pytest

from metapyle import Client

pytestmark = [pytest.mark.integration, pytest.mark.bloomberg]


class TestBloombergSingleSeries:
    """Test single series fetch from Bloomberg."""

    def test_single_series(
        self,
        bloomberg_client: Client,
        test_start: str,
        test_end: str,
    ) -> None:
        """Fetch sp500_close and verify DataFrame structure."""
        df = bloomberg_client.get(["sp500_close"], start=test_start, end=test_end)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "sp500_close" in df.columns
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) > 0
