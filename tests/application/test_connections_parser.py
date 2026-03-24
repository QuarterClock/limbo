"""Tests for connections_parser helpers."""

from __future__ import annotations

import pytest

from limbo_core.adapters.connections import ConnectionRegistry
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.application.parsers.common import ParseError
from limbo_core.application.parsers.connections_parser import (
    _parse_connections,
    _resolve_lookup_values,
)
from limbo_core.domain.entities import ConnectionBackendSpec
from limbo_core.errors import LimboValidationError
from limbo_core.plugins.builtin.connections import SQLAlchemyConnectionBackend
from limbo_core.plugins.builtin.value_readers import OsEnvReader


class _BoomOnCreateRegistry(ConnectionRegistry):
    """Registry whose ``create`` raises for coverage of error wrapping."""

    def create(self, backend_key: str, *, config=None):
        raise ValueError("create failed")


class _BoomLimboOnCreateRegistry(ConnectionRegistry):
    def create(self, backend_key: str, *, config=None):
        raise LimboValidationError("limbo create failed")


def test_parse_error_from_item_reraises() -> None:
    """Inner ``ParseError`` is not wrapped."""
    reg = ConnectionRegistry()
    reg.register("sqlalchemy", SQLAlchemyConnectionBackend)
    vr = ValueReaderRegistry()

    with pytest.raises(ParseError, match="expects a mapping"):
        _parse_connections(
            [123],
            path=("connections",),
            connection_registry=reg,
            value_reader_registry=vr,
        )


def test_value_error_from_create_wrapped() -> None:
    reg = _BoomOnCreateRegistry()
    reg.register("sqlalchemy", SQLAlchemyConnectionBackend)
    vr = ValueReaderRegistry()

    payload = [
        {
            "name": "main",
            "type": "sqlalchemy",
            "config": {
                "host": "h",
                "user": "u",
                "password": "p",
                "database": "d",
            },
        }
    ]
    with pytest.raises(ParseError, match="create failed"):
        _parse_connections(
            payload,
            path=("connections",),
            connection_registry=reg,
            value_reader_registry=vr,
        )


def test_limbo_validation_error_from_create_wrapped() -> None:
    reg = _BoomLimboOnCreateRegistry()
    reg.register("sqlalchemy", SQLAlchemyConnectionBackend)
    vr = ValueReaderRegistry()

    payload = [
        {
            "name": "main",
            "type": "sqlalchemy",
            "config": {
                "host": "h",
                "user": "u",
                "password": "p",
                "database": "d",
            },
        }
    ]
    with pytest.raises(ParseError, match="limbo create failed"):
        _parse_connections(
            payload,
            path=("connections",),
            connection_registry=reg,
            value_reader_registry=vr,
        )


def test_resolve_lookup_values_recurses_into_lists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vr = ValueReaderRegistry()
    vr.register("env", OsEnvReader)
    monkeypatch.setenv("NESTED_KEY", "resolved")

    payload = {
        "items": [{"value_from": {"reader": "env", "key": "NESTED_KEY"}}]
    }
    out = _resolve_lookup_values(payload, value_reader_registry=vr)
    assert out == {"items": ["resolved"]}


def test_value_error_from_parse_backend_spec_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*_a: object, **_kw: object) -> None:
        raise ValueError("bad spec parse")

    monkeypatch.setattr(
        "limbo_core.application.parsers.connections_parser.parse_backend_spec",
        _boom,
    )
    reg = ConnectionRegistry()
    vr = ValueReaderRegistry()
    with pytest.raises(ParseError, match="bad spec parse"):
        _parse_connections(
            [{"name": "n", "type": "t", "config": {}}],
            path=("connections",),
            connection_registry=reg,
            value_reader_registry=vr,
        )


def test_limbo_validation_error_from_parse_backend_spec_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*_a: object, **_kw: object) -> None:
        raise LimboValidationError("lv from spec")

    monkeypatch.setattr(
        "limbo_core.application.parsers.connections_parser.parse_backend_spec",
        _boom,
    )
    reg = ConnectionRegistry()
    vr = ValueReaderRegistry()
    with pytest.raises(ParseError, match="lv from spec"):
        _parse_connections(
            [{"name": "n", "type": "t", "config": {}}],
            path=("connections",),
            connection_registry=reg,
            value_reader_registry=vr,
        )


def test_parse_connections_preserves_connection_backend_spec() -> None:
    reg = ConnectionRegistry()
    reg.register("sqlalchemy", SQLAlchemyConnectionBackend)
    vr = ValueReaderRegistry()

    payload = [
        {
            "name": "main_db",
            "type": "sqlalchemy",
            "config": {
                "host": "localhost",
                "user": "u",
                "password": "p",
                "database": ":memory:",
            },
        }
    ]
    specs = _parse_connections(
        payload,
        path=("connections",),
        connection_registry=reg,
        value_reader_registry=vr,
    )
    assert len(specs) == 1
    assert isinstance(specs[0], ConnectionBackendSpec)
    assert specs[0].name == "main_db"
