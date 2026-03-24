"""Tests for application-level project loading use case."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from limbo_core.adapters.connections import ConnectionRegistry
from limbo_core.adapters.generators import GeneratorRegistry
from limbo_core.adapters.persistence import (
    DataPersistenceRegistry,
    PathResolverRegistry,
)
from limbo_core.adapters.plugins import PluggyPluginLoader
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.application.context import ResolutionContext, RuntimeContext
from limbo_core.application.interfaces.generators import (
    Generator,
    GeneratorRegistration,
    GeneratorRegistryPort,
)
from limbo_core.application.parsers import ProjectParser
from limbo_core.application.services import (
    ProjectLoaderService,
    ProjectValidatorService,
)
from limbo_core.application.services.project_validator import (
    GeneratorNotFoundError,
    UnknownSourceConnectionError,
)
from limbo_core.plugins import PluginManager
from limbo_core.plugins.builtin.connections import SQLAlchemyConnectionBackend


class _StaticGeneratorRegistry(GeneratorRegistryPort):
    """Minimal generator registry stub for tests."""

    def __init__(self, hooks: set[str]) -> None:
        self._hooks = frozenset(hooks)

    def register(self, registration: GeneratorRegistration) -> None:
        raise NotImplementedError

    def get_hooks(self) -> frozenset[str]:
        return self._hooks

    def resolve(self, qualified_hook: str) -> tuple[type[Generator], str]:
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError


def _base_payload(generator_name: str = "gen.ok") -> dict[str, object]:
    return {
        "connections": [
            {
                "type": "sqlalchemy",
                "name": "main_db",
                "config": {
                    "host": "",
                    "user": "",
                    "password": "",
                    "database": ":memory:",
                },
            }
        ],
        "destinations": [{"name": "default", "type": "file"}],
        "tables": [
            {
                "name": "users",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "generator": generator_name,
                    }
                ],
                "config": {},
            }
        ],
        "seeds": [
            {
                "name": "sex",
                "columns": [{"name": "value", "data_type": "string"}],
                "seed_file": {
                    "path": {
                        "path_from": {
                            "backend": "file",
                            "base": "this",
                            "location": "seed.csv",
                        }
                    }
                },
                "config": {},
            }
        ],
        "sources": [
            {
                "name": "company",
                "columns": [{"name": "id", "data_type": "integer"}],
                "config": {"connection": "main_db"},
            }
        ],
    }


@pytest.fixture
def connection_registry() -> ConnectionRegistry:
    """Create a shared connection registry for loader and validator."""
    return ConnectionRegistry()


@pytest.fixture
def path_registry() -> PathResolverRegistry:
    """Create a shared path registry for loader and validator."""
    return PathResolverRegistry()


@pytest.fixture
def loader(
    connection_registry: ConnectionRegistry, path_registry: PathResolverRegistry
) -> ProjectLoaderService:
    """Create project loader service with default plugin loader."""
    value_reader_registry = ValueReaderRegistry()
    return ProjectLoaderService(
        plugin_loader=PluggyPluginLoader(
            manager=PluginManager(
                connection_registry=connection_registry,
                value_reader_registry=value_reader_registry,
                path_resolver_registry=path_registry,
                data_persistence_registry=DataPersistenceRegistry(
                    path_resolver_registry=path_registry
                ),
                generator_registry=GeneratorRegistry(),
            )
        ),
        parser=ProjectParser(
            connection_registry=connection_registry,
            value_reader_registry=value_reader_registry,
            path_resolver_registry=path_registry,
        ),
        validator=ProjectValidatorService(
            path_registry=path_registry, connection_registry=connection_registry
        ),
    )


class TestLoadProjectWithContext:
    """Tests that exercise project loading with a RuntimeContext."""

    def test_validates_runtime_context(
        self, loader: ProjectLoaderService, tmp_path: Path
    ) -> None:
        """Load validates seed file paths against runtime context."""
        (tmp_path / "seed.csv").write_text("value\nx\n")
        context = RuntimeContext(
            generator_registry=_StaticGeneratorRegistry({"gen.ok"})
        )
        res_ctx = ResolutionContext(source_dir=tmp_path)
        project = loader.load(
            _base_payload(), context=context, resolution_context=res_ctx
        )
        assert project.seeds[0].name == "sex"
        assert project.seeds[0].seed_file.path.backend == "file"

    def test_configures_connection_instances_in_registry(
        self,
        loader: ProjectLoaderService,
        connection_registry: ConnectionRegistry,
        tmp_path: Path,
    ) -> None:
        """Parsed project connections are configured in the registry."""
        (tmp_path / "seed.csv").write_text("value\nx\n")
        context = RuntimeContext(
            generator_registry=_StaticGeneratorRegistry({"gen.ok"})
        )
        res_ctx = ResolutionContext(source_dir=tmp_path)

        loader.load(
            _base_payload(), context=context, resolution_context=res_ctx
        )

        instances = connection_registry.get_instances()
        assert "main_db" in instances
        assert isinstance(instances["main_db"], SQLAlchemyConnectionBackend)

    def test_missing_generator_raises(
        self, loader: ProjectLoaderService, tmp_path: Path
    ) -> None:
        """Raise if a table references an unknown generator."""
        (tmp_path / "seed.csv").write_text("value\nx\n")
        context = RuntimeContext(
            generator_registry=_StaticGeneratorRegistry(set())
        )
        res_ctx = ResolutionContext(source_dir=tmp_path)
        with pytest.raises(GeneratorNotFoundError):
            loader.load(
                _base_payload(), context=context, resolution_context=res_ctx
            )

    def test_missing_source_connection_raises(
        self, loader: ProjectLoaderService, tmp_path: Path
    ) -> None:
        """Raise if a source references unknown runtime connection."""
        (tmp_path / "seed.csv").write_text("value\nx\n")
        payload = _base_payload()
        payload["sources"] = [
            {
                "name": "company",
                "columns": [{"name": "id", "data_type": "integer"}],
                "config": {"connection": "missing"},
            }
        ]
        context = RuntimeContext(
            generator_registry=_StaticGeneratorRegistry({"gen.ok"})
        )
        res_ctx = ResolutionContext(source_dir=tmp_path)
        with pytest.raises(UnknownSourceConnectionError):
            loader.load(payload, context=context, resolution_context=res_ctx)


class TestLoadProjectWithoutContext:
    """Tests that exercise parse-only mode (no RuntimeContext)."""

    def test_without_runtime_context(
        self, loader: ProjectLoaderService
    ) -> None:
        """Load succeeds in parse-only mode without context."""
        project = loader.load(_base_payload())
        assert project.tables[0].name == "users"
        assert project.seeds[0].seed_file.path is not None

    def test_resolves_connection_lookup_values(
        self, loader: ProjectLoaderService, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Connection fields using value_from are resolved before validation."""
        monkeypatch.setenv("DB_HOST", "db.example.com")
        payload = _base_payload()
        payload["connections"][0]["config"]["host"] = {  # type: ignore[index]
            "value_from": {"reader": "env", "key": "DB_HOST"}
        }

        project = loader.load(payload)
        assert project.connections[0].type == "sqlalchemy"
        assert project.connections[0].name == "main_db"
        assert project.connections[0].config["host"] == "db.example.com"

    def test_supports_project_backend_bindings(
        self,
        loader: ProjectLoaderService,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Backend bindings alias value readers and path backends."""
        monkeypatch.setenv("DB_HOST", "bound.example.com")
        (tmp_path / "seed.csv").write_text("value\nx\n")
        payload = _base_payload()
        payload["value_readers"] = [{"name": "runtime_env", "type": "env"}]
        payload["path_backends"] = [{"name": "localfs", "type": "file"}]
        payload["connections"][0]["config"]["host"] = {  # type: ignore[index]
            "value_from": {"reader": "runtime_env", "key": "DB_HOST"}
        }
        payload["seeds"][0]["seed_file"]["path"] = {  # type: ignore[index]
            "path_from": {
                "backend": "localfs",
                "base": "this",
                "location": "seed.csv",
            }
        }
        context = RuntimeContext(
            generator_registry=_StaticGeneratorRegistry({"gen.ok"})
        )
        res_ctx = ResolutionContext(source_dir=tmp_path)

        project = loader.load(
            payload, context=context, resolution_context=res_ctx
        )
        assert project.connections[0].config["host"] == "bound.example.com"
        assert project.seeds[0].seed_file.path.backend == "localfs"
