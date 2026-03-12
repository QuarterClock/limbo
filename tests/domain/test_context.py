"""Tests for parsing/runtime context contracts."""

from __future__ import annotations

import pytest

from limbo_core.application.context import (
    ConnectionNotFoundError,
    RuntimeContext,
)
from limbo_core.application.interfaces import (
    GeneratorRegistryPort,
    ReferenceResolver,
)


class _StubResolver(ReferenceResolver):
    """Resolver that echoes the reference back with a prefix."""

    def resolve(self, reference: str) -> str:
        return f"resolved:{reference}"


class _StubRegistry(GeneratorRegistryPort):
    """Minimal generator registry stub."""

    def register(self, registration) -> None:  # pragma: no cover
        raise NotImplementedError

    def get_hooks(self) -> frozenset[str]:  # pragma: no cover
        return frozenset()

    def resolve(self, qualified_hook: str):  # pragma: no cover
        raise NotImplementedError

    def clear(self) -> None:  # pragma: no cover
        raise NotImplementedError


class TestRuntimeContextResolveReference:
    """Tests for RuntimeContext.resolve_reference."""

    def test_requires_resolver(self) -> None:
        """RuntimeContext raises when reference resolver is not configured."""
        context = RuntimeContext(generator_registry=_StubRegistry())
        with pytest.raises(
            RuntimeError, match="Reference resolver is not configured"
        ):
            context.resolve_reference("table.col")

    def test_delegates_to_resolver(self) -> None:
        """RuntimeContext delegates reference lookup to resolver."""
        context = RuntimeContext(
            generator_registry=_StubRegistry(),
            reference_resolver=_StubResolver(),
        )
        assert context.resolve_reference("table.col") == "resolved:table.col"


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
