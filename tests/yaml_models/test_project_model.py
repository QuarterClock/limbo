from pathlib import Path

from limbo_core.connections import SQLAlchemyConnection
from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.project import Project
from limbo_core.yaml_schema.tables import TableColumn


def test_project_construct_minimal(tmp_path: Path) -> None:
    # Prepare a real seed file to satisfy path validation
    (tmp_path / "file.csv").write_text("id,sex\n1,M\n")
    # Create the connection object for the context
    main_db = SQLAlchemyConnection(
        name="main_db",
        dialect="sqlite",
        host="",
        user="",
        password="",
        database=":memory:",
    )
    data = {
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
    project = Project.model_validate(
        data,
        context=Context(
            generators={"primary_key.incrementing_id": object()},
            paths={"this": tmp_path},
            connections={"main_db": main_db},
        ),
    )
    # Validate a single column with context to ensure generator check passes
    TableColumn.model_validate(
        {
            "name": "id",
            "data_type": DataType.INTEGER,
            "generator": "primary_key.incrementing_id",
        },
        context=Context(
            generators={"primary_key.incrementing_id": object()}, paths={}
        ),
    )
    assert project.tables[0].name == "users"
    assert project.seeds[0].name == "sex"
    assert project.sources[0].name == "company"
