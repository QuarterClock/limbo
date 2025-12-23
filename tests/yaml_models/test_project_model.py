"""Tests for Project model validation."""

from pathlib import Path

import pytest

from limbo_core.connections import SQLAlchemyConnection
from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.project import Project
from limbo_core.yaml_schema.tables import TableColumn


class TestProjectModel:
    """Test cases for Project model construction and validation."""

    @pytest.fixture
    def seed_file(self, tmp_path: Path) -> Path:
        """Create a seed file for testing."""
        seed_path = tmp_path / "file.csv"
        seed_path.write_text("id,sex\n1,M\n")
        return seed_path

    @pytest.fixture
    def project_data(self) -> dict:
        """Provide minimal project data for testing."""
        return {
            "vars": {"llm": {"provider": "openai", "model": "gpt-4o-mini"}},
            "connections": [
                {
                    "type": "sqlalchemy",
                    "name": "main_db",
                    "dialect": "sqlite",
                    "host": "",
                    "user": "",
                    "password": "",
                    "database": ":memory:",
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "integer",
                            "generator": "primary_key.incrementing_id",
                        }
                    ],
                    "config": {},
                    "references": [
                        {
                            "type": "seed",
                            "name": "sex",
                            "relationship": "one_to_many",
                        }
                    ],
                }
            ],
            "seeds": [
                {
                    "name": "sex",
                    "columns": [
                        {"name": "id", "data_type": "integer"},
                        {"name": "sex", "data_type": "string"},
                    ],
                    "seed_file": {"path": "${path:this}/file.csv"},
                    "config": {},
                }
            ],
            "sources": [
                {
                    "name": "company",
                    "columns": [
                        {"name": "id", "data_type": "integer"},
                        {"name": "name", "data_type": "string"},
                    ],
                    "config": {"connection": "main_db"},
                }
            ],
        }

    @pytest.fixture
    def project_context(self, tmp_path: Path, seed_file: Path) -> Context:
        """Create a context for project validation."""
        main_db = SQLAlchemyConnection(
            name="main_db",
            dialect="sqlite",
            host="",
            user="",
            password="",
            database=":memory:",
        )
        return Context(
            generators={"primary_key.incrementing_id": object()},
            paths={"this": tmp_path},
            connections={"main_db": main_db},
        )

    def test_project_construct_minimal(
        self, project_data: dict, project_context: Context
    ) -> None:
        """Verify minimal project construction with all required components."""
        project = Project.model_validate(project_data, context=project_context)

        assert project.tables[0].name == "users"
        assert project.seeds[0].name == "sex"
        assert project.sources[0].name == "company"


class TestTableColumnWithContext:
    """Test cases for TableColumn validation with context."""

    @pytest.fixture
    def column_context(self) -> Context:
        """Create a context for column validation."""
        return Context(
            generators={"primary_key.incrementing_id": object()}, paths={}
        )

    def test_table_column_validates_with_context(
        self, column_context: Context
    ) -> None:
        """Verify TableColumn validation passes with proper context."""
        column = TableColumn.model_validate(
            {
                "name": "id",
                "data_type": DataType.INTEGER,
                "generator": "primary_key.incrementing_id",
            },
            context=column_context,
        )
        assert column.name == "id"
        assert column.data_type == DataType.INTEGER
        assert column.generator == "primary_key.incrementing_id"
