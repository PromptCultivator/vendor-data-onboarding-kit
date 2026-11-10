"""Pytest test configuration and shared fixtures."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register custom command-line options for golden report regeneration."""
    parser.addoption(
        "--update-goldens",
        action="store_true",
        default=False,
        help="Update golden reference files rather than asserting equality.",
    )


@pytest.fixture
def update_goldens(request: pytest.FixtureRequest) -> bool:
    """Fixture indicating whether golden files should be updated."""
    return bool(request.config.getoption("--update-goldens", default=False))
