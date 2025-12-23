"""Tests for SourceConfig and Context connection management."""

import pytest
from pydantic import ValidationError

from limbo_core.connections import SQLAlchemyConnection
from limbo_core.context import Context
from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.sources.config import SourceConfig


class TestSourceConfigConnectionValidation:
    """Test cases for SourceConfig connection validation."""

    def test_valid_connection(self, context_with_connection: Context) -> None:
        """Verify valid connection name is accepted."""
        config = SourceConfig.model_validate(
            {"connection": "main_db"}, context=context_with_connection
        )
        assert config.connection == "main_db"

    def test_invalid_connection_raises(
        self, context_with_connection: Context
    ) -> None:
        """Verify invalid connection name raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig.model_validate(
                {"connection": "nonexistent_db"},
                context=context_with_connection,
            )
        assert "nonexistent_db" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_get_connection(
        self,
        context_with_connection: Context,
        main_db_connection: SQLAlchemyConnection,
    ) -> None:
        """Verify get_connection returns the correct connection."""
        config = SourceConfig.model_validate(
            {"connection": "main_db"}, context=context_with_connection
        )
        resolved = config.get_connection(context_with_connection)
        assert resolved is main_db_connection

    def test_missing_context_raises(self) -> None:
        """Verify missing context raises ContextMissingError."""
        with pytest.raises(ContextMissingError):
            SourceConfig.model_validate({"connection": "main_db"})

    def test_with_optional_fields(
        self, context_with_connection: Context
    ) -> None:
        """Verify optional fields are parsed correctly."""
        config = SourceConfig.model_validate(
            {
                "connection": "main_db",
                "schema_name": "public",
                "table_name": "users",
            },
            context=context_with_connection,
        )
        assert config.connection == "main_db"
        assert config.schema_name == "public"
        assert config.table_name == "users"


class TestContextConnectionManagement:
    """Test cases for Context connection management."""

    @pytest.fixture
    def test_connection(self) -> SQLAlchemyConnection:
        """Create a test connection."""
        return SQLAlchemyConnection(
            name="test_db",
            host="localhost",
            user="user",
            password="pass",
            database="db",
        )

    def test_get_connection_exists(
        self, test_connection: SQLAlchemyConnection
    ) -> None:
        """Verify existing connection is returned."""
        context = Context(
            generators={}, paths={}, connections={"test_db": test_connection}
        )
        assert context.get_connection("test_db") is test_connection

    def test_get_connection_missing_raises(self) -> None:
        """Verify missing connection raises KeyError."""
        context = Context(generators={}, paths={}, connections={})
        with pytest.raises(KeyError) as exc_info:
            context.get_connection("missing_db")
        assert "missing_db" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_get_connection_shows_available(
        self, test_connection: SQLAlchemyConnection
    ) -> None:
        """Verify error message shows available connections."""
        context = Context(
            generators={},
            paths={},
            connections={"existing_db": test_connection},
        )
        with pytest.raises(KeyError) as exc_info:
            context.get_connection("wrong_name")
        assert "existing_db" in str(exc_info.value)
