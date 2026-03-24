"""Builtin plugin implementation."""

from __future__ import annotations

from limbo_core.application.interfaces import (
    BackendRegistration,
    ConnectionBackend,
    GeneratorRegistration,
    PersistenceReadBackend,
    PersistenceWriteBackend,
    ValueReaderBackend,
)
from limbo_core.plugins.builtin.persistence import (
    CsvFilePersistenceWriteBackend,
    FilesystemReadBackend,
    JsonFilePersistenceWriteBackend,
    JsonlFilePersistenceWriteBackend,
    ParquetFilePersistenceWriteBackend,
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
    def limbo_register_path_backends(
        self,
    ) -> list[BackendRegistration[PersistenceReadBackend]]:
        """Register built-in persistence read backends.

        Returns:
            List with the default filesystem backend type.
        """
        return [
            BackendRegistration(key="file", backend_class=FilesystemReadBackend)
        ]

    @hookimpl
    def limbo_register_persistence_write_backends(
        self,
    ) -> list[BackendRegistration[PersistenceWriteBackend]]:
        """Register built-in tabular file persistence write backends.

        Returns:
            CSV, JSON, JSONL, and Parquet backends under fixed directories.
        """
        return [
            BackendRegistration(
                key="csv", backend_class=CsvFilePersistenceWriteBackend
            ),
            BackendRegistration(
                key="json", backend_class=JsonFilePersistenceWriteBackend
            ),
            BackendRegistration(
                key="jsonl", backend_class=JsonlFilePersistenceWriteBackend
            ),
            BackendRegistration(
                key="parquet", backend_class=ParquetFilePersistenceWriteBackend
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
