"""Tests for backend spec parsing contract behavior."""

from __future__ import annotations

import pytest

from limbo_core.application.parsers.backends_parser import parse_backend_spec
from limbo_core.domain.entities import (
    ConnectionBackendSpec,
    PathBackendSpec,
    ValueReaderBackendSpec,
)
from limbo_core.validation import ValidationError


class TestBackendSpecParsing:
    """Tests for the generic parse_backend_spec contract."""

    def test_connection_spec_uses_common_pattern(self) -> None:
        """Connection specs follow the ``name/type/config`` contract."""
        spec = parse_backend_spec(
            {
                "name": "main_db",
                "type": "sqlalchemy",
                "config": {"host": "localhost"},
            },
            spec_cls=ConnectionBackendSpec,
        )
        assert spec.name == "main_db"
        assert spec.type == "sqlalchemy"
        assert spec.config == {"host": "localhost"}

    def test_connection_spec_merges_legacy_top_level_fields(self) -> None:
        """Top-level connection fields are normalized into ``config``."""
        spec = parse_backend_spec(
            {
                "name": "main_db",
                "type": "sqlalchemy",
                "host": "localhost",
                "database": "db",
            },
            spec_cls=ConnectionBackendSpec,
        )
        assert spec.config == {"host": "localhost", "database": "db"}

    def test_all_specs_share_same_contract_shape(self) -> None:
        """Connection/value/path specs parse with identical contract rules."""
        value_spec = parse_backend_spec(
            {"name": "runtime_env", "type": "env"},
            spec_cls=ValueReaderBackendSpec,
        )
        path_spec = parse_backend_spec(
            {"name": "localfs", "type": "file"}, spec_cls=PathBackendSpec
        )
        connection_spec = parse_backend_spec(
            {"name": "main_db", "type": "sqlalchemy"},
            spec_cls=ConnectionBackendSpec,
        )
        assert value_spec.config == {}
        assert path_spec.config == {}
        assert connection_spec.config == {}

    def test_rejects_missing_name(self) -> None:
        """Connection specs require a ``name`` string field."""
        with pytest.raises(ValidationError, match="`name` expects a string"):
            parse_backend_spec(
                {"type": "sqlalchemy"}, spec_cls=ConnectionBackendSpec
            )

    def test_rejects_invalid_type(self) -> None:
        """Connection specs require a string ``type`` field."""
        with pytest.raises(ValidationError, match="`type` expects a string"):
            parse_backend_spec(
                {"name": "main_db", "type": 1}, spec_cls=ConnectionBackendSpec
            )

    def test_rejects_invalid_config(self) -> None:
        """Connection specs require ``config`` to be a mapping when present."""
        with pytest.raises(ValidationError, match="`config` expects a mapping"):
            parse_backend_spec(
                {"name": "main_db", "type": "sqlalchemy", "config": "bad"},
                spec_cls=ConnectionBackendSpec,
            )
