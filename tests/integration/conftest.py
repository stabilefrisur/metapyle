"""Integration test configuration and fixtures."""

from pathlib import Path

import pytest

from metapyle import Client

# Fixture paths
FIXTURES_DIR = Path(__file__).parent / "fixtures"
BLOOMBERG_CATALOG = FIXTURES_DIR / "bloomberg.yaml"
MACROBOND_CATALOG = FIXTURES_DIR / "macrobond.yaml"
COMBINED_CATALOG = FIXTURES_DIR / "combined.yaml"

# Test date range (guaranteed to have data)
TEST_START = "2024-01-01"
TEST_END = "2024-06-30"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --run-private option for private series tests."""
    parser.addoption(
        "--run-private",
        action="store_true",
        default=False,
        help="Run tests that require private/in-house series",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip private tests unless --run-private is passed."""
    if config.getoption("--run-private"):
        return

    skip_private = pytest.mark.skip(reason="Need --run-private option to run")
    for item in items:
        if "private" in item.keywords:
            item.add_marker(skip_private)


@pytest.fixture
def bloomberg_client() -> Client:
    """Client configured with Bloomberg catalog."""
    return Client(catalog=str(BLOOMBERG_CATALOG), cache_enabled=False)


@pytest.fixture
def macrobond_client() -> Client:
    """Client configured with Macrobond catalog."""
    return Client(catalog=str(MACROBOND_CATALOG), cache_enabled=False)


@pytest.fixture
def combined_client() -> Client:
    """Client configured with combined catalog (both sources)."""
    return Client(catalog=str(COMBINED_CATALOG), cache_enabled=False)


@pytest.fixture
def test_start() -> str:
    """Test start date."""
    return TEST_START


@pytest.fixture
def test_end() -> str:
    """Test end date."""
    return TEST_END
