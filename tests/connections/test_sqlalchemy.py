"""Tests for SQLAlchemyConnection."""

import os
from unittest.mock import patch

from pydantic import SecretStr

from limbo_core.connections import SQLAlchemyConnection


class TestSQLAlchemyConnectionBasic:
    """Basic tests for SQLAlchemyConnection."""

    def test_create_minimal_connection(self) -> None:
        """Test creating a connection with minimal required fields."""
        conn = SQLAlchemyConnection(
            name="test_db",
            host="localhost",
            user="user",
            password="secret",
            database="mydb",
        )
        assert conn.name == "test_db"
        assert conn.dialect == "sqlite"  # Default
        assert conn.driver is None  # Default
        assert conn.host == "localhost"
        assert conn.port is None  # Default
        assert conn.user == "user"
        assert conn.password.get_secret_value() == "secret"
        assert conn.database == "mydb"
        assert conn.connection_args == {}

    def test_create_full_connection(self) -> None:
        """Test creating a connection with all fields."""
        conn = SQLAlchemyConnection(
            name="full_db",
            dialect="postgresql",
            driver="psycopg2",
            host="db.example.com",
            port=5432,
            user="admin",
            password="p@ssw0rd!",
            database="production",
            connection_args={"connect_timeout": 10},
        )
        assert conn.dialect == "postgresql"
        assert conn.driver == "psycopg2"
        assert conn.port == 5432
        assert conn.connection_args == {"connect_timeout": 10}

    def test_type_literal(self) -> None:
        """Test that type is always 'sqlalchemy'."""
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="user",
            password="pass",
            database="db",
        )
        assert conn.type == "sqlalchemy"


class TestSQLAlchemyConnectionInterpolation:
    """Tests for environment variable interpolation."""

    def test_interpolate_driver(self) -> None:
        """Test interpolating driver from environment."""
        with patch.dict(os.environ, {"DB_DRIVER": "asyncpg"}):
            conn = SQLAlchemyConnection(
                name="test",
                dialect="postgresql",
                driver="${env:DB_DRIVER}",
                host="localhost",
                user="user",
                password="pass",
                database="db",
            )
            assert conn.driver == "asyncpg"

    def test_interpolate_port_as_string(self) -> None:
        """Test interpolating port from environment variable."""
        with patch.dict(os.environ, {"DB_PORT": "5432"}):
            conn = SQLAlchemyConnection(
                name="test",
                host="localhost",
                port="${env:DB_PORT}",
                user="user",
                password="pass",
                database="db",
            )
            assert conn.port == 5432

    def test_interpolate_port_with_default_empty(self) -> None:
        """Test port interpolation with empty default."""
        # Remove env var if exists
        env = os.environ.copy()
        env.pop("NONEXISTENT_PORT", None)

        with patch.dict(os.environ, env, clear=True):
            conn = SQLAlchemyConnection(
                name="test",
                host="localhost",
                port="${env:NONEXISTENT_PORT:-}",
                user="user",
                password="pass",
                database="db",
            )
            assert conn.port is None

    def test_interpolate_dialect(self) -> None:
        """Test interpolating dialect from environment."""
        with patch.dict(os.environ, {"DB_DIALECT": "mysql"}):
            conn = SQLAlchemyConnection(
                name="test",
                dialect="${env:DB_DIALECT}",
                host="localhost",
                user="user",
                password="pass",
                database="db",
            )
            assert conn.dialect == "mysql"

    def test_non_string_port_passthrough(self) -> None:
        """Test that integer port is passed through without interpolation."""
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            port=3306,
            user="user",
            password="pass",
            database="db",
        )
        assert conn.port == 3306

    def test_non_string_values_passthrough(self) -> None:
        """Test that non-string values pass through validators."""
        # Password can be passed as SecretStr directly
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="user",
            password=SecretStr("direct_secret"),
            database="db",
        )
        assert conn.password.get_secret_value() == "direct_secret"


class TestSQLAlchemyConnectionBuildUrl:
    """Tests for URL building."""

    def test_build_url_without_driver(self) -> None:
        """Test building URL without driver specified."""
        conn = SQLAlchemyConnection(
            name="test",
            dialect="postgresql",
            host="localhost",
            user="user",
            password="pass",
            database="mydb",
        )
        url = conn._build_url()
        assert url.drivername == "postgresql"
        assert url.username == "user"
        assert url.host == "localhost"
        assert url.database == "mydb"

    def test_build_url_with_driver(self) -> None:
        """Test building URL with driver specified."""
        conn = SQLAlchemyConnection(
            name="test",
            dialect="postgresql",
            driver="asyncpg",
            host="localhost",
            user="user",
            password="pass",
            database="mydb",
        )
        url = conn._build_url()
        assert url.drivername == "postgresql+asyncpg"

    def test_build_url_with_port(self) -> None:
        """Test building URL with port specified."""
        conn = SQLAlchemyConnection(
            name="test",
            dialect="mysql",
            host="localhost",
            port=3306,
            user="user",
            password="pass",
            database="mydb",
        )
        url = conn._build_url()
        assert url.port == 3306

    def test_build_url_special_chars_in_password(self) -> None:
        """Test that special characters in password are handled."""
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="user",
            password="p@ss:word/with#special",
            database="mydb",
        )
        url = conn._build_url()
        # URL.create handles escaping
        assert url.password == "p@ss:word/with#special"


class TestSQLAlchemyConnectionConnect:
    """Tests for connect method."""

    def test_connect_creates_engine(self) -> None:
        """Test that connect creates a SQLAlchemy engine."""
        conn = SQLAlchemyConnection(
            name="test",
            dialect="sqlite",
            host="",
            user="",
            password="",
            database=":memory:",
        )
        engine = conn.connect()

        # Verify it's an engine
        from sqlalchemy import Engine

        assert isinstance(engine, Engine)

    def test_connect_with_connection_args(self) -> None:
        """Test that connect passes connection_args to create_engine."""
        conn = SQLAlchemyConnection(
            name="test",
            dialect="sqlite",
            host="",
            user="",
            password="",
            database=":memory:",
            connection_args={"check_same_thread": False},
        )
        engine = conn.connect()
        # Engine should be created successfully with args
        assert engine is not None
