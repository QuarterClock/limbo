"""Composition root wiring adapters to use cases."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from limbo_core.adapters.connections import ConnectionRegistry
from limbo_core.adapters.generators import GeneratorRegistry
from limbo_core.adapters.persistence import (
    DataPersistenceRegistry,
    PathResolverRegistry,
)
from limbo_core.adapters.plugins import PluggyPluginLoader
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.application.parsers import ProjectParser
from limbo_core.application.services import (
    ProjectLoaderService,
    ProjectValidatorService,
)
from limbo_core.plugins import PluginManager

if TYPE_CHECKING:
    from limbo_core.application.context import ResolutionContext, RuntimeContext
    from limbo_core.domain.entities import Project


@dataclass(slots=True)
class Container:
    """Compose use cases with their concrete adapter implementations."""

    connection_registry: ConnectionRegistry = field(
        default_factory=ConnectionRegistry
    )
    value_reader_registry: ValueReaderRegistry = field(
        default_factory=ValueReaderRegistry
    )
    path_resolver_registry: PathResolverRegistry = field(
        default_factory=PathResolverRegistry
    )
    data_persistence_registry: DataPersistenceRegistry = field(
        default_factory=DataPersistenceRegistry
    )
    generator_registry: GeneratorRegistry = field(
        default_factory=GeneratorRegistry
    )
    plugin_manager: PluginManager = field(init=False)
    plugin_loader: PluggyPluginLoader = field(init=False)
    project_parser: ProjectParser = field(init=False)
    project_validator_service: ProjectValidatorService = field(init=False)
    project_loader_service: ProjectLoaderService = field(init=False)

    def __post_init__(self) -> None:
        """Instantiate services after dependencies are available."""
        self.plugin_manager = PluginManager(
            connection_registry=self.connection_registry,
            value_reader_registry=self.value_reader_registry,
            path_resolver_registry=self.path_resolver_registry,
            data_persistence_registry=self.data_persistence_registry,
            generator_registry=self.generator_registry,
        )
        self.plugin_loader = PluggyPluginLoader(manager=self.plugin_manager)
        self.project_parser = ProjectParser(
            connection_registry=self.connection_registry,
            value_reader_registry=self.value_reader_registry,
            path_resolver_registry=self.path_resolver_registry,
            data_persistence_registry=self.data_persistence_registry,
        )
        self.project_validator_service = ProjectValidatorService(
            path_registry=self.path_resolver_registry,
            connection_registry=self.connection_registry,
        )
        self.project_loader_service = ProjectLoaderService(
            plugin_loader=self.plugin_loader,
            parser=self.project_parser,
            validator=self.project_validator_service,
        )

    def load_project(
        self,
        payload: dict[str, Any],
        *,
        context: RuntimeContext | None = None,
        resolution_context: ResolutionContext | None = None,
    ) -> Project:
        """Load and validate a project using wired dependencies.

        Returns:
            Parsed and runtime-validated project model.
        """
        return self.project_loader_service.load(
            payload, context=context, resolution_context=resolution_context
        )


_default_container: Container | None = None


def get_container() -> Container:
    """Get a lazily created default container.

    Returns:
        Shared application container instance.
    """
    global _default_container
    if _default_container is None:
        _default_container = Container()
    return _default_container
