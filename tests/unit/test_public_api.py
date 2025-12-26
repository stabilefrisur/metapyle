"""Test that public API is correctly exported."""


def test_public_api_exports() -> None:
    """All public symbols should be importable from metapyle."""
    from metapyle import (
        BaseSource,
        CatalogError,
        CatalogValidationError,
        Client,
        DuplicateNameError,
        FetchError,
        MetapyleError,
        NoDataError,
        SymbolNotFoundError,
        UnknownSourceError,
        register_source,
    )

    assert Client is not None
    assert BaseSource is not None
    assert register_source is not None
    assert MetapyleError is not None
    assert CatalogError is not None
    assert CatalogValidationError is not None
    assert DuplicateNameError is not None
    assert FetchError is not None
    assert NoDataError is not None
    assert SymbolNotFoundError is not None
    assert UnknownSourceError is not None


def test_version_available() -> None:
    """Package version should be available."""
    import metapyle

    assert hasattr(metapyle, "__version__")
    assert metapyle.__version__ == "0.1.0"
