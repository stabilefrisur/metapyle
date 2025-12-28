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
        FetchRequest,
        MetapyleError,
        NoDataError,
        SymbolNotFoundError,
        UnknownSourceError,
        register_source,
    )

    assert Client is not None
    assert BaseSource is not None
    assert FetchRequest is not None
    assert register_source is not None
    assert MetapyleError is not None
    assert CatalogError is not None
    assert CatalogValidationError is not None
    assert DuplicateNameError is not None
    assert FetchError is not None
    assert NoDataError is not None
    assert SymbolNotFoundError is not None
    assert UnknownSourceError is not None


def test_frequency_mismatch_error_not_exported() -> None:
    """FrequencyMismatchError should no longer be exported."""
    import metapyle

    assert not hasattr(metapyle, "FrequencyMismatchError")


def test_frequency_enum_not_exported() -> None:
    """Frequency enum should no longer be exported."""
    import metapyle

    assert not hasattr(metapyle, "Frequency")


def test_version_available() -> None:
    """Package version should be available."""
    import metapyle

    assert hasattr(metapyle, "__version__")
    # Check version format (X.Y.Z) rather than hardcoded value
    assert metapyle.__version__  # Non-empty
    parts = metapyle.__version__.split(".")
    assert len(parts) == 3  # Major.Minor.Patch
    assert all(part.isdigit() for part in parts)
