"""Unit tests for exception hierarchy."""

import pytest

from metapyle.exceptions import (
    CatalogError,
    CatalogValidationError,
    DuplicateNameError,
    FetchError,
    MetapyleError,
    NameNotFoundError,
    NoDataError,
    UnknownSourceError,
)


def test_metapyle_error_is_exception() -> None:
    """MetapyleError should be a base exception."""
    error = MetapyleError("test message")
    assert isinstance(error, Exception)
    assert str(error) == "test message"


def test_catalog_error_inherits_from_metapyle_error() -> None:
    """CatalogError should inherit from MetapyleError."""
    error = CatalogError("catalog issue")
    assert isinstance(error, MetapyleError)
    assert isinstance(error, Exception)


def test_fetch_error_inherits_from_metapyle_error() -> None:
    """FetchError should inherit from MetapyleError."""
    error = FetchError("fetch failed")
    assert isinstance(error, MetapyleError)


def test_catalog_validation_error_inherits_from_catalog_error() -> None:
    """CatalogValidationError should inherit from CatalogError."""
    error = CatalogValidationError("invalid yaml")
    assert isinstance(error, CatalogError)


def test_duplicate_name_error_inherits_from_catalog_error() -> None:
    """DuplicateNameError should inherit from CatalogError."""
    error = DuplicateNameError("duplicate found")
    assert isinstance(error, CatalogError)


def test_unknown_source_error_inherits_from_catalog_error() -> None:
    """UnknownSourceError should inherit from CatalogError."""
    error = UnknownSourceError("unknown source")
    assert isinstance(error, CatalogError)


def test_name_not_found_error_inherits_from_catalog_error() -> None:
    """NameNotFoundError should inherit from CatalogError."""
    error = NameNotFoundError("name not found")
    assert isinstance(error, CatalogError)


def test_no_data_error_inherits_from_fetch_error() -> None:
    """NoDataError should inherit from FetchError."""
    error = NoDataError("no data returned")
    assert isinstance(error, FetchError)


@pytest.mark.parametrize(
    "exception_class",
    [
        MetapyleError,
        CatalogError,
        FetchError,
        CatalogValidationError,
        DuplicateNameError,
        UnknownSourceError,
        NameNotFoundError,
        NoDataError,
    ],
)
def test_all_exceptions_catchable_via_metapyle_error(exception_class: type) -> None:
    """All exceptions should be catchable via MetapyleError."""
    with pytest.raises(MetapyleError):
        raise exception_class("test")
