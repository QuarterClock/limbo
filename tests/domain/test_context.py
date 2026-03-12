"""Tests for parsing/runtime context contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

import pytest

from limbo_core.application.context import (
    ConnectionNotFoundError,
    ParsingContext,
    ReferenceResolver,
    RuntimeContext,
)
from limbo_core.application.interfaces.connections import ConnectionBackend

if TYPE_CHECKING:
    from limbo_core.domain.entities import ConnectionBackendSpec


class _StubResolver(ReferenceResolver):
    """Resolver that echoes the reference back with a prefix."""

    def resolve(self, reference: str) -> str:
        return f"resolved:{reference}"


class _StubConnectionBackend(ConnectionBackend):
    """Minimal connection backend for testing context wiring."""

    @classmethod
    def from_spec(cls, spec: ConnectionBackendSpec) -> Self:
        return cls()

    def connect(self) -> None:
        return None


class TestRuntimeContextResolveReference:
    """Tests for RuntimeContext.resolve_reference."""

    def test_requires_resolver(self) -> None:
        """RuntimeContext raises when reference resolver is not configured."""
        context = RuntimeContext(generators=set(), paths={})
        with pytest.raises(
            RuntimeError, match="Reference resolver is not configured"
        ):
            context.resolve_reference("table.col")

    def test_delegates_to_resolver(self) -> None:
        """RuntimeContext delegates reference lookup to resolver."""
        context = RuntimeContext(
            generators=set(), paths={}, reference_resolver=_StubResolver()
        )
        assert context.resolve_reference("table.col") == "resolved:table.col"


class TestRuntimeContextGetConnection:
    """Tests for RuntimeContext.get_connection."""

    @pytest.fixture
    def backend(self) -> _StubConnectionBackend:
        """Provide a fresh stub connection backend."""
        return _StubConnectionBackend()

    def test_get_connection_returns_backend(
        self, backend: _StubConnectionBackend
    ) -> None:
        """Registered connection is returned by name."""
        context = RuntimeContext(
            generators=set(), paths={}, connections={"main_db": backend}
        )
        assert context.get_connection("main_db") is backend

    def test_get_connection_unknown_name_raises(self) -> None:
        """Unknown connection name raises ConnectionNotFoundError."""
        context = RuntimeContext(generators=set(), paths={})
        with pytest.raises(ConnectionNotFoundError):
            context.get_connection("missing")

    def test_get_connection_error_lists_available(
        self, backend: _StubConnectionBackend
    ) -> None:
        """Error message contains the names of available connections."""
        context = RuntimeContext(
            generators=set(),
            paths={},
            connections={"pg": backend, "redis": backend},
        )
        with pytest.raises(ConnectionNotFoundError, match="pg") as exc_info:
            context.get_connection("missing")
        assert "redis" in str(exc_info.value)


class TestConnectionNotFoundError:
    """Tests for the ConnectionNotFoundError exception."""

    def test_error_message_contains_name(self) -> None:
        """Error message includes the missing connection name."""
        err = ConnectionNotFoundError("my_db", available_connections=())
        assert "my_db" in str(err)

    def test_error_stores_connection_name(self) -> None:
        """Attribute ``connection_name`` preserves the requested name."""
        err = ConnectionNotFoundError("my_db", available_connections=())
        assert err.connection_name == "my_db"

    def test_error_stores_available_connections(self) -> None:
        """Attribute ``available_connections`` is a tuple of names."""
        err = ConnectionNotFoundError(
            "my_db", available_connections=["pg", "redis"]
        )
        assert err.available_connections == ("pg", "redis")


class TestParsingContext:
    """Tests for ParsingContext creation."""

    def test_parsing_context_stores_paths(self) -> None:
        """ParsingContext exposes the paths dictionary provided at init."""
        paths: dict[str, Any] = {"seeds": "/data/seeds"}
        ctx = ParsingContext(paths=paths)
        assert ctx.paths == paths
