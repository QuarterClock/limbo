"""Project runtime validation service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from limbo_core.application.context import ConnectionNotFoundError
from limbo_core.domain.errors import DomainValidationError

if TYPE_CHECKING:
    from limbo_core.application.context import RuntimeContext
    from limbo_core.application.interfaces import PathResolverPort
    from limbo_core.domain.entities import Project


class GeneratorNotFoundError(DomainValidationError):
    """Raised when a configured generator does not exist in context."""

    def __init__(self, generator_name: str) -> None:
        """Initialize the GeneratorNotFoundError."""
        super().__init__(f"Generator {generator_name} is not in the context")


class UnknownSourceConnectionError(DomainValidationError):
    """Raised when a source references an unknown connection."""

    def __init__(self, connection_name: str) -> None:
        """Initialize the UnknownSourceConnectionError."""
        super().__init__(f"Connection '{connection_name}' not found in context")


@dataclass(slots=True)
class ProjectValidatorService:
    """Validate project references that require runtime context."""

    path_registry: PathResolverPort

    def validate(self, project: Project, *, context: RuntimeContext) -> Project:
        """Validate generators, connections, and seed paths.

        Returns:
            Same project instance after runtime validation.

        Raises:
            GeneratorNotFoundError: If a generator name is missing.
            UnknownSourceConnectionError: If a source connection is unknown.
        """
        for table in project.tables:
            for column in table.columns:
                if column.generator not in context.generators:
                    raise GeneratorNotFoundError(column.generator)

        for source in project.sources:
            try:
                context.get_connection(source.config.connection)
            except ConnectionNotFoundError as err:
                raise UnknownSourceConnectionError(err.connection_name) from err

        for seed in project.seeds:
            self.path_registry.resolve(seed.seed_file.path, paths=context.paths)

        return project
