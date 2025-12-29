"""Unit tests for BaseSource ABC and SourceRegistry."""

from collections.abc import Sequence

import pandas as pd
import pytest

from metapyle.exceptions import UnknownSourceError
from metapyle.sources.base import (
    BaseSource,
    FetchRequest,
    SourceRegistry,
    _global_registry,
    make_column_name,
    register_source,
)


def test_base_source_is_abstract() -> None:
    """BaseSource cannot be instantiated directly."""
    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        BaseSource()  # type: ignore[abstract]


def test_base_source_requires_fetch_method() -> None:
    """Subclass must implement fetch method."""

    class IncompleteSource(BaseSource):
        def get_metadata(self, symbol: str) -> dict:
            return {}  # pragma: no cover

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        IncompleteSource()  # type: ignore[abstract]


def test_base_source_requires_get_metadata_method() -> None:
    """Subclass must implement get_metadata method."""

    class IncompleteSource(BaseSource):
        def fetch(
            self,
            requests: Sequence[FetchRequest],
            start: str,
            end: str,
        ) -> pd.DataFrame:
            return pd.DataFrame()  # pragma: no cover

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        IncompleteSource()  # type: ignore[abstract]


def test_concrete_source_can_be_instantiated() -> None:
    """Concrete subclass with both methods can be instantiated."""

    class ConcreteSource(BaseSource):
        def fetch(
            self,
            requests: Sequence[FetchRequest],
            start: str,
            end: str,
        ) -> pd.DataFrame:
            return pd.DataFrame({"value": [1, 2, 3]})

        def get_metadata(self, symbol: str) -> dict:
            return {"description": "test"}

    source = ConcreteSource()
    assert source is not None


def test_source_registry_starts_empty() -> None:
    """SourceRegistry should start with no registered sources."""
    registry = SourceRegistry()
    assert registry.list_sources() == []


def test_source_registry_register_and_get() -> None:
    """SourceRegistry can register and retrieve sources."""

    class TestSource(BaseSource):
        def fetch(
            self,
            requests: Sequence[FetchRequest],
            start: str,
            end: str,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    registry = SourceRegistry()
    registry.register("test", TestSource)

    source = registry.get("test")
    assert isinstance(source, TestSource)


def test_source_registry_get_unknown_raises() -> None:
    """SourceRegistry raises UnknownSourceError for unknown source."""
    registry = SourceRegistry()

    with pytest.raises(UnknownSourceError, match="unknown_source"):
        registry.get("unknown_source")


def test_source_registry_list_sources() -> None:
    """SourceRegistry can list all registered sources."""

    class TestSource(BaseSource):
        def fetch(
            self,
            requests: Sequence[FetchRequest],
            start: str,
            end: str,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    registry = SourceRegistry()
    registry.register("alpha", TestSource)
    registry.register("beta", TestSource)

    sources = registry.list_sources()
    assert sorted(sources) == ["alpha", "beta"]


def test_register_source_decorator() -> None:
    """register_source decorator adds source to global registry."""

    @register_source("decorated_test")
    class DecoratedSource(BaseSource):
        def fetch(
            self,
            requests: Sequence[FetchRequest],
            start: str,
            end: str,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    # Verify it's in the global registry
    source = _global_registry.get("decorated_test")
    assert isinstance(source, DecoratedSource)


def test_source_registry_caches_instances() -> None:
    """SourceRegistry returns same instance on repeated get calls."""

    class TestSource(BaseSource):
        def fetch(
            self,
            requests: Sequence[FetchRequest],
            start: str,
            end: str,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def get_metadata(self, symbol: str) -> dict:
            return {}

    registry = SourceRegistry()
    registry.register("test", TestSource)

    source1 = registry.get("test")
    source2 = registry.get("test")
    assert source1 is source2


class TestMakeColumnName:
    """Tests for make_column_name utility."""

    def test_symbol_only(self) -> None:
        """Column name is symbol when no field."""
        result = make_column_name("usgdp", None)
        assert result == "usgdp"

    def test_symbol_with_field(self) -> None:
        """Column name is symbol::field when field present."""
        result = make_column_name("SPX Index", "PX_LAST")
        assert result == "SPX Index::PX_LAST"


class TestFetchRequest:
    """Tests for FetchRequest dataclass."""

    def test_symbol_only(self) -> None:
        """FetchRequest with only symbol."""
        req = FetchRequest(symbol="usgdp")
        assert req.symbol == "usgdp"
        assert req.field is None
        assert req.path is None

    def test_all_fields(self) -> None:
        """FetchRequest with all fields."""
        req = FetchRequest(symbol="GDP", field="value", path="/data/file.csv")
        assert req.symbol == "GDP"
        assert req.field == "value"
        assert req.path == "/data/file.csv"

    def test_frozen(self) -> None:
        """FetchRequest is immutable."""
        req = FetchRequest(symbol="test")
        with pytest.raises(AttributeError):
            req.symbol = "changed"  # type: ignore[misc]


class TestFetchRequestParams:
    """Tests for FetchRequest params field."""

    def test_fetch_request_with_params(self) -> None:
        """FetchRequest accepts params dict."""
        params = {"tenor": "3m", "deltaStrike": "DN"}
        request = FetchRequest(symbol="EURUSD", field="PX_LAST", params=params)

        assert request.symbol == "EURUSD"
        assert request.field == "PX_LAST"
        assert request.params == params

    def test_fetch_request_params_default_none(self) -> None:
        """FetchRequest params defaults to None."""
        request = FetchRequest(symbol="EURUSD")

        assert request.params is None

    def test_fetch_request_with_params_frozen(self) -> None:
        """FetchRequest with params is still frozen."""
        params = {"tenor": "3m"}
        request = FetchRequest(symbol="EURUSD", params=params)

        with pytest.raises(AttributeError):
            request.params = {}  # type: ignore[misc]
