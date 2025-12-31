"""Shared pytest fixtures and configuration."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --run-private option for private series tests."""
    parser.addoption(
        "--run-private",
        action="store_true",
        default=False,
        help="Run tests that require private/in-house series",
    )
