"""Builtin plugin implementation."""

from __future__ import annotations

from limbo_core.application.interfaces import (
    BackendRegistration,
    ConnectionBackend,
    DataPersistenceBackend,
    GeneratorRegistration,
    PathResolverBackend,
    ValueReaderBackend,
)
from limbo_core.plugins.builtin.persistence import (
    CsvFileDataPersistenceBackend,
    FilesystemPathResolver,
    JsonFileDataPersistenceBackend,
    JsonlFileDataPersistenceBackend,
    ParquetFileDataPersistenceBackend,
)
from limbo_core.plugins.markers import hookimpl

from .connections import SQLAlchemyConnectionBackend
from .value_readers import OsEnvReader


class BuiltinPlugin:
    """Built-in plugin providing default limbo functionality."""

    @hookimpl
    def limbo_register_connections(
        self,
    ) -> list[BackendRegistration[ConnectionBackend]]:
        """Register built-in connection types.

        Returns:
            List containing SQLAlchemyConnectionBackend.
        """
        return [
            BackendRegistration(
                key="sqlalchemy", backend_class=SQLAlchemyConnectionBackend
            )
        ]

    @hookimpl
    def limbo_register_value_readers(
        self,
    ) -> list[BackendRegistration[ValueReaderBackend]]:
        """Register built-in value readers.

        Returns:
            List with the default `env` value reader type.
        """
        return [BackendRegistration(key="env", backend_class=OsEnvReader)]

    @hookimpl
    def limbo_register_path_resolver_backends(
        self,
    ) -> list[BackendRegistration[PathResolverBackend]]:
        """Register built-in path resolver backends.

        Returns:
            List with the default filesystem resolver type.
        """
        return [
            BackendRegistration(
                key="file", backend_class=FilesystemPathResolver
            )
        ]

    @hookimpl
    def limbo_register_data_persistence_backends(
        self,
    ) -> list[BackendRegistration[DataPersistenceBackend]]:
        """Register built-in tabular data persistence backends.

        Returns:
            CSV, JSON, JSONL, and Parquet backends under fixed directories.
        """
        return [
            BackendRegistration(
                key="csv", backend_class=CsvFileDataPersistenceBackend
            ),
            BackendRegistration(
                key="json", backend_class=JsonFileDataPersistenceBackend
            ),
            BackendRegistration(
                key="jsonl", backend_class=JsonlFileDataPersistenceBackend
            ),
            BackendRegistration(
                key="parquet", backend_class=ParquetFileDataPersistenceBackend
            ),
        ]

    @hookimpl
    def limbo_register_generators(self) -> list[GeneratorRegistration]:
        """Register built-in generators.

        The core library does not provide a default implementation yet.

        Returns:
            Empty list - no built-in generators.
        """
        return []
