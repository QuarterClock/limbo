"""Project payload parser facade.

Converts raw mappings into fully-typed domain entities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from limbo_core.domain.entities import (
    ConnectionBackendSpec,
    DestinationBackendSpec,
    PathBackendSpec,
    Project,
    SeedFile,
    SourceConfig,
    TableColumn,
    ValueReaderBackendSpec,
    ValueSpec,
)
from limbo_core.errors import LimboValidationError

from .backends_parser import (
    _parse_destinations,
    _parse_path_backends,
    _parse_value_reader_backends,
)
from .common import InvalidValueSpecError, ParseError, _expect_mapping
from .connections_parser import _parse_connections
from .seeds_parser import _parse_seed_file, _parse_seeds
from .sources_parser import _parse_source_config, _parse_sources
from .tables_parser import _parse_table_column, _parse_tables
from .value_spec_parser import parse_value_spec

if TYPE_CHECKING:
    from collections.abc import Sequence

    from limbo_core.application.interfaces import (
        ConnectionRegistryPort,
        DataPersistenceRegistryPort,
        PathResolverRegistryPort,
        ValueReaderRegistryPort,
    )


@dataclass(slots=True)
class ProjectParser:
    """Parse project payloads into domain entities."""

    connection_registry: ConnectionRegistryPort
    value_reader_registry: ValueReaderRegistryPort
    path_resolver_registry: PathResolverRegistryPort | None = None
    data_persistence_registry: DataPersistenceRegistryPort | None = None

    def parse(self, payload: dict[str, Any]) -> Project:
        """Parse a raw project payload into a typed project entity.

        Returns:
            Parsed project domain object.
        """
        root = _expect_mapping(payload, path=())
        raw_vars = root.get("vars")
        parsed_vars = self._parse_vars(raw_vars)

        self._reset_backend_bindings()
        value_readers = _parse_value_reader_backends(
            root.get("value_readers", []), path=("value_readers",)
        )
        path_backends = _parse_path_backends(
            root.get("path_backends", []), path=("path_backends",)
        )
        destinations = _parse_destinations(
            root.get("destinations"), path=("destinations",)
        )
        self._configure_backends(
            value_readers=value_readers,
            path_backends=path_backends,
            destinations=destinations,
        )

        connections = _parse_connections(
            root.get("connections", []),
            path=("connections",),
            connection_registry=self.connection_registry,
            value_reader_registry=self.value_reader_registry,
        )
        self._configure_connections(connections)
        tables = _parse_tables(root.get("tables"), path=("tables",))
        seeds = _parse_seeds(root.get("seeds", []), path=("seeds",))
        sources = _parse_sources(root.get("sources", []), path=("sources",))

        return Project(
            vars=parsed_vars,
            value_readers=value_readers,
            path_backends=path_backends,
            destinations=destinations,
            connections=connections,
            tables=tables,
            seeds=seeds,
            sources=sources,
        )

    def parse_table_column(self, payload: dict[str, Any]) -> TableColumn:
        """Parse one table column payload.

        Returns:
            Parsed table column entity.
        """
        return _parse_table_column(payload, path=("column",))

    def parse_source_config(self, payload: dict[str, Any]) -> SourceConfig:
        """Parse one source config payload.

        Returns:
            Parsed source config entity.
        """
        return _parse_source_config(payload, path=("config",))

    def parse_seed_file(self, payload: dict[str, Any]) -> SeedFile:
        """Parse one seed file payload.

        Returns:
            Parsed seed file entity.
        """
        return _parse_seed_file(payload, path=("seed_file",))

    @staticmethod
    def _parse_vars(raw: Any) -> dict[str, ValueSpec] | None:
        """Parse project vars into typed value specs.

        Returns:
            Mapping of variable names to typed value specs,
            or ``None`` when no vars are declared.

        Raises:
            ParseError: If the vars payload is invalid.
        """
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ParseError(path=("vars",), message="expects a mapping")
        parsed: dict[str, ValueSpec] = {}
        for key, value in raw.items():
            try:
                parsed[key] = parse_value_spec(value)
            except InvalidValueSpecError as err:
                raise ParseError(path=("vars", key), message=str(err)) from err
        return parsed

    def _configure_backends(
        self,
        *,
        value_readers: list[ValueReaderBackendSpec],
        path_backends: list[PathBackendSpec],
        destinations: list[DestinationBackendSpec],
    ) -> None:
        """Configure value-reader, path, and destination backend instances."""
        self._configure_registry(
            self.value_reader_registry, value_readers, "value_readers"
        )
        if self.path_resolver_registry is not None:
            self._configure_registry(
                self.path_resolver_registry, path_backends, "path_backends"
            )
        if self.data_persistence_registry is not None:
            self._configure_registry(
                self.data_persistence_registry, destinations, "destinations"
            )

    @staticmethod
    def _configure_registry(
        registry: Any, specs: Sequence[Any], section: str
    ) -> None:
        """Configure a single registry from parsed specs.

        Raises:
            ParseError: If a backend binding is invalid.
        """
        for idx, spec in enumerate(specs):
            try:
                registry.configure(spec)
            except (ValueError, LimboValidationError) as err:
                raise ParseError(path=(section, idx), message=str(err)) from err

    def _configure_connections(
        self, connections: list[ConnectionBackendSpec]
    ) -> None:
        """Configure named runtime connection backends from project specs.

        Raises:
            ParseError: If a connection binding configuration is invalid.
        """
        for idx, connection in enumerate(connections):
            try:
                self.connection_registry.configure(connection)
            except (ValueError, LimboValidationError) as err:
                raise ParseError(
                    path=("connections", idx), message=str(err)
                ) from err

    def _reset_backend_bindings(self) -> None:
        """Reset project-scoped backend bindings before each parse."""
        self.value_reader_registry.clear_instances()
        self.connection_registry.clear_instances()
        if self.path_resolver_registry is not None:
            self.path_resolver_registry.clear_instances()
        if self.data_persistence_registry is not None:
            self.data_persistence_registry.clear_instances()
