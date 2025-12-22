import pytest
from pydantic import ValidationError

from limbo_core.connections import SQLAlchemyConnection
from limbo_core.context import Context
from limbo_core.yaml_schema.sources.config import SourceConfig


class TestSourceConfigConnectionValidation:
    @pytest.fixture
    def main_db_connection(self) -> SQLAlchemyConnection:
        return SQLAlchemyConnection(
            name="main_db",
            host="localhost",
            user="user",
            password="password",
            database="mydb",
        )

    @pytest.fixture
    def context_with_connection(
        self, main_db_connection: SQLAlchemyConnection
    ) -> Context:
        return Context(
            generators={}, paths={}, connections={"main_db": main_db_connection}
        )

    def test_source_config_valid_connection(
        self, context_with_connection: Context
    ) -> None:
        config = SourceConfig.model_validate(
            {"connection": "main_db"}, context=context_with_connection
        )
        assert config.connection == "main_db"

    def test_source_config_invalid_connection_raises(
        self, context_with_connection: Context
    ) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SourceConfig.model_validate(
                {"connection": "nonexistent_db"},
                context=context_with_connection,
            )
        assert "nonexistent_db" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_source_config_get_connection(
        self,
        context_with_connection: Context,
        main_db_connection: SQLAlchemyConnection,
    ) -> None:
        config = SourceConfig.model_validate(
            {"connection": "main_db"}, context=context_with_connection
        )
        resolved = config.get_connection(context_with_connection)
        assert resolved is main_db_connection

    def test_source_config_missing_context_raises(self) -> None:
        from limbo_core.errors import ContextMissingError

        with pytest.raises(ContextMissingError):
            SourceConfig.model_validate({"connection": "main_db"})

    def test_source_config_with_optional_fields(
        self, context_with_connection: Context
    ) -> None:
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
    def test_context_get_connection_exists(self) -> None:
        conn = SQLAlchemyConnection(
            name="test_db",
            host="localhost",
            user="user",
            password="pass",
            database="db",
        )
        context = Context(
            generators={}, paths={}, connections={"test_db": conn}
        )
        assert context.get_connection("test_db") is conn

    def test_context_get_connection_missing_raises(self) -> None:
        context = Context(generators={}, paths={}, connections={})
        with pytest.raises(KeyError) as exc_info:
            context.get_connection("missing_db")
        assert "missing_db" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_context_get_connection_shows_available(self) -> None:
        conn = SQLAlchemyConnection(
            name="existing_db",
            host="localhost",
            user="user",
            password="pass",
            database="db",
        )
        context = Context(
            generators={}, paths={}, connections={"existing_db": conn}
        )
        with pytest.raises(KeyError) as exc_info:
            context.get_connection("wrong_name")
        assert "existing_db" in str(exc_info.value)
