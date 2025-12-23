"""Base fixtures used across multiple test modules."""

from pathlib import Path

import pytest

from limbo_core.connections import SQLAlchemyConnection
from limbo_core.context import Context


@pytest.fixture
def base_context() -> Context:
    """Provide a basic context with common generators for testing."""
    return Context(
        generators={
            "pii.full_name": object(),
            "pii.email": object(),
            "pii.password": object(),
            "primary_key.incrementing_id": object(),
            "foreign_key.key": object(),
            "datetime.random": object(),
        },
        paths={},
    )


@pytest.fixture
def main_db_connection() -> SQLAlchemyConnection:
    """Provide a SQLAlchemy connection for testing."""
    return SQLAlchemyConnection(
        name="main_db",
        dialect="sqlite",
        host="localhost",
        user="user",
        password="password",
        database=":memory:",
    )


@pytest.fixture
def context_with_connection(
    base_context: Context, main_db_connection: SQLAlchemyConnection
) -> Context:
    """Provide a context with a database connection."""
    return Context(
        generators=base_context.generators,
        paths=base_context.paths,
        connections={"main_db": main_db_connection},
    )


@pytest.fixture
def tmp_project_dir(tmp_path: Path) -> Path:
    """Create a temporary project directory with seeds subfolder."""
    project_root = tmp_path / "proj"
    seeds_dir = project_root / "seeds"
    seeds_dir.mkdir(parents=True)
    (seeds_dir / "sex.csv").write_text("id,sex\n1,M\n2,F\n")
    return project_root


@pytest.fixture
def context_with_paths(tmp_project_dir: Path) -> Context:
    """Provide a context with path resolution capabilities."""
    return Context(
        generators={
            "pii.full_name": object(),
            "primary_key.incrementing_id": object(),
        },
        paths={"this": tmp_project_dir},
    )
