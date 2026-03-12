"""Tests for SQLAlchemy connection backend."""

from unittest.mock import patch

import pytest

from limbo_core.adapters.connections.errors import MissingPackageError
from limbo_core.domain.entities import ConnectionBackendSpec
from limbo_core.plugins.builtin.connections import SQLAlchemyConnectionBackend
from limbo_core.validation import ValidationError


def _sqlalchemy_spec(**config: object) -> ConnectionBackendSpec:
    return ConnectionBackendSpec(
        name="main_db", type="sqlalchemy", config=dict(config)
    )


class TestSQLAlchemyConnectionBackendFromSpec:
    """Tests backend construction from generic connection specs."""

    def test_create_minimal_backend(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                host="localhost",
                user="user",
                password="secret",
                database="mydb",
            )
        )
        assert backend.dialect == "sqlite"
        assert backend.driver is None
        assert backend.host == "localhost"
        assert backend.port is None
        assert backend.user == "user"
        assert backend.password == "secret"
        assert backend.database == "mydb"
        assert backend.connection_args == {}

    def test_create_full_backend(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="postgresql",
                driver="psycopg2",
                host="db.example.com",
                port=5432,
                user="admin",
                password="p@ssw0rd!",
                database="production",
                connection_args={"connect_timeout": 10},
            )
        )
        assert backend.dialect == "postgresql"
        assert backend.driver == "psycopg2"
        assert backend.port == 5432
        assert backend.connection_args == {"connect_timeout": 10}

    def test_non_string_port_passthrough(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                host="localhost",
                port=3306,
                user="user",
                password="pass",
                database="db",
            )
        )
        assert backend.port == 3306

    def test_password_lookup_resolution(self) -> None:
        with pytest.raises(
            ValidationError, match=r"`password` expects a string"
        ):
            SQLAlchemyConnectionBackend.from_spec(
                _sqlalchemy_spec(
                    host="localhost",
                    user="user",
                    password={
                        "value_from": {"reader": "env", "key": "DB_PASSWORD"}
                    },
                    database="db",
                )
            )

    def test_required_string_field_rejects_invalid_type(self) -> None:
        with pytest.raises(ValidationError, match=r"`host` expects a string"):
            SQLAlchemyConnectionBackend.from_spec(
                _sqlalchemy_spec(
                    host=123, user="user", password="pass", database="db"
                )
            )

    def test_port_string_is_converted_to_int(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                host="localhost",
                user="user",
                password="pass",
                database="db",
                port="15432",
            )
        )
        assert backend.port == 15432

    def test_port_rejects_invalid_type(self) -> None:
        with pytest.raises(
            ValidationError, match=r"`port` expects int or string"
        ):
            SQLAlchemyConnectionBackend.from_spec(
                _sqlalchemy_spec(
                    host="localhost",
                    user="user",
                    password="pass",
                    database="db",
                    port=["not", "valid"],
                )
            )

    def test_connection_args_rejects_non_mapping(self) -> None:
        """`connection_args` must be a mapping when provided."""
        with pytest.raises(
            ValidationError,
            match="SQLAlchemyConnection: `connection_args` expects a mapping",
        ):
            SQLAlchemyConnectionBackend.from_spec(
                _sqlalchemy_spec(
                    host="localhost",
                    user="user",
                    password="pass",
                    database="db",
                    connection_args="not-a-mapping",
                )
            )

    def test_rejects_unknown_extra_config_fields(self) -> None:
        """Unexpected extra config keys are rejected explicitly."""
        with pytest.raises(
            ValidationError,
            match="SQLAlchemyConnection: unexpected config fields: extra",
        ):
            SQLAlchemyConnectionBackend.from_spec(
                _sqlalchemy_spec(
                    host="localhost",
                    user="user",
                    password="pass",
                    database="db",
                    extra="value",
                )
            )

    def test_rejects_unknown_type(self) -> None:
        with pytest.raises(ValidationError, match="expected type 'sqlalchemy'"):
            SQLAlchemyConnectionBackend.from_spec(
                ConnectionBackendSpec(
                    name="main_db",
                    type="other",
                    config={
                        "host": "localhost",
                        "user": "user",
                        "password": "pass",
                        "database": "db",
                    },
                )
            )


class TestSQLAlchemyConnectionBackendBuildUrl:
    """Tests for SQLAlchemy backend URL building."""

    def test_build_url_without_driver(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="postgresql",
                host="localhost",
                user="user",
                password="pass",
                database="mydb",
            )
        )
        url = backend._build_url()
        assert url.drivername == "postgresql"
        assert url.username == "user"
        assert url.host == "localhost"
        assert url.database == "mydb"

    def test_build_url_with_driver(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="postgresql",
                driver="asyncpg",
                host="localhost",
                user="user",
                password="pass",
                database="mydb",
            )
        )
        url = backend._build_url()
        assert url.drivername == "postgresql+asyncpg"

    def test_build_url_with_port(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="mysql",
                host="localhost",
                port=3306,
                user="user",
                password="pass",
                database="mydb",
            )
        )
        url = backend._build_url()
        assert url.port == 3306


class TestSQLAlchemyConnectionBackendConnect:
    """Tests for SQLAlchemy backend connect method."""

    def test_connect_creates_engine(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="sqlite",
                host="",
                user="",
                password="",
                database=":memory:",
            )
        )
        engine = backend.connect()

        from sqlalchemy import Engine

        assert isinstance(engine, Engine)

    def test_connect_with_connection_args(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="sqlite",
                host="",
                user="",
                password="",
                database=":memory:",
                connection_args={"check_same_thread": False},
            )
        )
        engine = backend.connect()
        assert engine is not None

    def test_connect_raises_when_sqlalchemy_is_missing(self) -> None:
        backend = SQLAlchemyConnectionBackend.from_spec(
            _sqlalchemy_spec(
                dialect="sqlite",
                host="",
                user="",
                password="",
                database=":memory:",
            )
        )
        with (
            patch.dict("sys.modules", {"sqlalchemy": None}),
            pytest.raises(MissingPackageError, match="sqlalchemy"),
        ):
            backend.connect()
