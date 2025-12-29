"""Integration test configuration and fixtures."""

from pathlib import Path

import pytest

from metapyle import Client

# Fixture paths
FIXTURES_DIR = Path(__file__).parent / "fixtures"
BLOOMBERG_CATALOG = FIXTURES_DIR / "bloomberg.yaml"
GSQUANT_CATALOG = FIXTURES_DIR / "gsquant.yaml"
MACROBOND_CATALOG = FIXTURES_DIR / "macrobond.yaml"
COMBINED_CATALOG = FIXTURES_DIR / "combined.yaml"

# Test date range (guaranteed to have data)
TEST_START = "2023-01-01"
TEST_END = "2024-06-30"


def _macrobond_available() -> bool:
    """Check if Macrobond API is available and connected."""
    try:
        import macrobond_data_api as mda

        # Try to make a lightweight API call to verify connection
        mda.get_one_entity("usgdp")
        return True
    except Exception:
        return False


def _gsquant_available() -> bool:
    """Check if gs-quant session is initialized."""
    try:
        from gs_quant.session import GsSession

        # Check if session is initialized (will raise if not)
        _ = GsSession.current
        return True
    except Exception:
        return False


# Check once at module load time
MACROBOND_AVAILABLE = _macrobond_available()
GSQUANT_AVAILABLE = _gsquant_available()


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --run-private option for private series tests."""
    parser.addoption(
        "--run-private",
        action="store_true",
        default=False,
        help="Run tests that require private/in-house series",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip private tests unless --run-private is passed, and skip unavailable sources."""
    if not config.getoption("--run-private"):
        skip_private = pytest.mark.skip(reason="Need --run-private option to run")
        for item in items:
            if "private" in item.keywords:
                item.add_marker(skip_private)

    # Skip Macrobond tests if Macrobond is not available
    if not MACROBOND_AVAILABLE:
        skip_macrobond = pytest.mark.skip(reason="Macrobond not available or not connected")
        for item in items:
            if "macrobond" in item.keywords:
                item.add_marker(skip_macrobond)

    # Skip gs-quant tests if GsSession is not initialized
    if not GSQUANT_AVAILABLE:
        skip_gsquant = pytest.mark.skip(reason="GsSession not initialized")
        for item in items:
            if "gsquant" in item.keywords:
                item.add_marker(skip_gsquant)


@pytest.fixture
def bloomberg_client() -> Client:
    """Client configured with Bloomberg catalog."""
    return Client(catalog=str(BLOOMBERG_CATALOG), cache_enabled=False)


@pytest.fixture
def gsquant_client() -> Client:
    """Client configured with gs-quant catalog."""
    return Client(catalog=str(GSQUANT_CATALOG), cache_enabled=False)


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
