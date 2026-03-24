"""Application parser tests for payload-to-entity flow."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

import limbo_core.application.parsers.tables_parser as tables_parser_module
from limbo_core.adapters.connections import ConnectionRegistry
from limbo_core.adapters.persistence import (
    DataPersistenceRegistry,
    PathResolverRegistry,
)
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.application.parsers import ParseError, ProjectParser
from limbo_core.application.parsers.common import InvalidValueSpecError
from limbo_core.domain.entities import (
    DataType,
    LiteralValue,
    LookupValue,
    TableRelationship,
)
from limbo_core.domain.value_objects import (
    LocalFilesystemStorageRef,
    ResolvedStorageRef,
)
from limbo_core.errors import LimboValidationError
from limbo_core.plugins.builtin.connections import SQLAlchemyConnectionBackend
from limbo_core.plugins.builtin.persistence import FilesystemPathResolver
from limbo_core.plugins.builtin.value_readers import OsEnvReader

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch


@dataclass(slots=True)
class _StubWriteBackend(DataPersistenceBackend):
    """Minimal data persistence backend for parser tests."""

    store: dict[str, TabularBatch] = field(default_factory=dict)
    _root: Path = field(default_factory=lambda: Path("/__limbo_stub__"))

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        return LocalFilesystemStorageRef(
            backend="memory",
            uri=f"memory://{name}",
            local_path=self._root / name,
        )

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        self.store[ref.as_local_path().name] = data

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        return self.store[ref.as_local_path().name]

    def exists(self, ref: ResolvedStorageRef) -> bool:
        return ref.as_local_path().name in self.store

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        self.store.pop(ref.as_local_path().name, None)


@pytest.fixture
def project_parser() -> ProjectParser:
    """Create parser fixture local to parser tests."""
    return ProjectParser(
        connection_registry=ConnectionRegistry(),
        value_reader_registry=ValueReaderRegistry(),
    )


def _minimal_project_payload() -> dict[str, object]:
    return {
        "connections": [],
        "destinations": [{"name": "default", "type": "file"}],
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
            }
        ],
        "seeds": [
            {
                "name": "seed_users",
                "columns": [{"name": "id", "data_type": "integer"}],
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
                "name": "source_users",
                "columns": [{"name": "id", "data_type": "integer"}],
                "config": {"connection": "main"},
            }
        ],
    }


# -------------------------------------------------------------------
# Table column parsing
# -------------------------------------------------------------------


class TestParseTableColumn:
    """Tests for ProjectParser.parse_table_column."""

    def test_column_with_options(self, project_parser: ProjectParser) -> None:
        """Column payload parsing yields typed option values."""
        column = project_parser.parse_table_column({
            "name": "full_name",
            "data_type": DataType.STRING,
            "generator": "pii.full_name",
            "options": {"length": 5},
        })
        assert column.generator == "pii.full_name"
        assert column.options is not None
        length_opt = column.options["length"]
        assert isinstance(length_opt, LiteralValue)
        assert length_opt.value == 5

    def test_wraps_option_validation_error(
        self, project_parser: ProjectParser, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Value-spec parsing errors are wrapped as ParseError."""

        def _raise_error(_raw: object) -> object:
            raise InvalidValueSpecError("bad option")

        monkeypatch.setattr(
            tables_parser_module, "parse_value_spec", _raise_error
        )
        with pytest.raises(
            ParseError, match="column\\.options\\.length: bad option"
        ):
            project_parser.parse_table_column({
                "name": "full_name",
                "data_type": DataType.STRING,
                "generator": "pii.full_name",
                "options": {"length": 5},
            })

    def test_column_without_options(
        self, project_parser: ProjectParser
    ) -> None:
        """Column without options parses with options as None."""
        column = project_parser.parse_table_column({
            "name": "id",
            "data_type": "integer",
            "generator": "primary_key.incrementing_id",
        })
        assert column.name == "id"
        assert column.options is None


# -------------------------------------------------------------------
# Source config parsing
# -------------------------------------------------------------------


class TestParseSourceConfig:
    """Tests for ProjectParser.parse_source_config."""

    def test_optional_fields(self, project_parser: ProjectParser) -> None:
        """Source config parser handles optional names explicitly."""
        config = project_parser.parse_source_config({
            "connection": "main",
            "schema_name": "public",
            "table_name": "users",
        })
        assert config.connection == "main"
        assert config.schema_name == "public"
        assert config.table_name == "users"

    def test_rejects_non_bool_materialize(
        self, project_parser: ProjectParser
    ) -> None:
        """Source config materialize must be a bool."""
        with pytest.raises(ParseError, match="config\\.materialize"):
            project_parser.parse_source_config({
                "materialize": "yes",
                "connection": "main",
            })

    def test_rejects_non_string_schema_name(
        self, project_parser: ProjectParser
    ) -> None:
        """Source config schema_name must be string when provided."""
        with pytest.raises(ParseError, match="config\\.schema_name"):
            project_parser.parse_source_config({
                "connection": "main",
                "schema_name": 1,
            })

    def test_minimal_config(self, project_parser: ProjectParser) -> None:
        """Source config with only required connection field."""
        config = project_parser.parse_source_config({"connection": "main"})
        assert config.connection == "main"
        assert config.schema_name is None
        assert config.table_name is None


# -------------------------------------------------------------------
# Seed file parsing
# -------------------------------------------------------------------


class TestParseSeedFile:
    """Tests for ProjectParser.parse_seed_file."""

    def test_rejects_unsupported_type(
        self, project_parser: ProjectParser
    ) -> None:
        """Unsupported seed file types raise parse errors."""
        with pytest.raises(ParseError, match="seed_file\\.type"):
            project_parser.parse_seed_file({"type": "xml", "path": "file.csv"})

    def test_rejects_unsupported_compression(
        self, project_parser: ProjectParser
    ) -> None:
        """Unsupported seed compression values raise parse errors."""
        with pytest.raises(ParseError, match="seed_file\\.compression"):
            project_parser.parse_seed_file({
                "compression": "zip",
                "path": "file.csv",
            })

    def test_default_infer_type(self, project_parser: ProjectParser) -> None:
        """Seed file defaults to infer type when not specified."""
        seed_file = project_parser.parse_seed_file({"path": "file.csv"})
        assert seed_file.type == "infer"


# -------------------------------------------------------------------
# Table reference parsing
# -------------------------------------------------------------------


class TestParseTableReference:
    """Tests for table reference parsing within parse()."""

    def test_happy_path(self, project_parser: ProjectParser) -> None:
        """Table references are parsed into typed relationships."""
        payload = _minimal_project_payload()
        payload["tables"][0]["references"] = [  # type: ignore[index]
            {
                "type": "seed",
                "name": "seed_users",
                "relationship": "one_to_many",
            }
        ]
        project = project_parser.parse(payload)
        ref = project.tables[0].references[0]  # type: ignore[index]
        assert ref.type == "seed"
        assert ref.relationship == TableRelationship.ONE_TO_MANY

    def test_rejects_empty_references(
        self, project_parser: ProjectParser
    ) -> None:
        """Explicit references list cannot be empty."""
        payload = _minimal_project_payload()
        payload["tables"][0]["references"] = []  # type: ignore[index]
        with pytest.raises(ParseError, match="tables\\[0\\]\\.references"):
            project_parser.parse(payload)

    def test_rejects_invalid_relationship(
        self, project_parser: ProjectParser
    ) -> None:
        """Reference relationship must be a supported enum value."""
        payload = _minimal_project_payload()
        payload["tables"][0]["references"] = [  # type: ignore[index]
            {"type": "seed", "name": "seed_users", "relationship": "invalid"}
        ]
        with pytest.raises(
            ParseError, match="tables\\[0\\]\\.references\\[0\\]\\.relationship"
        ):
            project_parser.parse(payload)

    def test_rejects_invalid_reference_type(
        self, project_parser: ProjectParser
    ) -> None:
        """Table reference type must be one of supported values."""
        payload = _minimal_project_payload()
        payload["tables"][0]["references"] = [  # type: ignore[index]
            {
                "type": "view",
                "name": "seed_users",
                "relationship": "one_to_many",
            }
        ]
        with pytest.raises(
            ParseError, match="tables\\[0\\]\\.references\\[0\\]"
        ):
            project_parser.parse(payload)


# -------------------------------------------------------------------
# Full project parse validation
# -------------------------------------------------------------------


class TestParseProjectValidation:
    """Tests for full project-level parse validation."""

    def test_error_includes_precise_field_path(
        self, project_parser: ProjectParser
    ) -> None:
        """Nested parse failures include precise field paths."""
        payload = _minimal_project_payload()
        payload["tables"] = [
            {
                "name": "users",
                "columns": [{"name": "id", "data_type": "integer"}],
                "config": {},
            }
        ]
        with pytest.raises(ParseError) as err:
            project_parser.parse(payload)
        assert "tables[0].columns[0].generator" in str(err.value)

    def test_rejects_non_mapping_vars(
        self, project_parser: ProjectParser
    ) -> None:
        """Top-level vars must be a mapping."""
        payload = _minimal_project_payload()
        payload["vars"] = "bad"
        with pytest.raises(ParseError, match="vars: expects a mapping"):
            project_parser.parse(payload)

    def test_parses_vars_as_typed_value_specs(
        self, project_parser: ProjectParser
    ) -> None:
        """Vars entries are parsed through value spec typing."""
        payload = _minimal_project_payload()
        payload["vars"] = {
            "name": "hello",
            "count": 42,
            "rate": math.pi,
            "flag": True,
        }
        project = project_parser.parse(payload)
        assert project.vars is not None
        assert isinstance(project.vars["name"], LiteralValue)
        assert project.vars["name"].data_type == DataType.STRING
        assert project.vars["count"].data_type == DataType.INTEGER
        assert project.vars["rate"].data_type == DataType.FLOAT
        assert project.vars["flag"].data_type == DataType.BOOLEAN

    def test_parses_vars_with_lookup(
        self, project_parser: ProjectParser
    ) -> None:
        """Vars support value_from lookups."""
        payload = _minimal_project_payload()
        payload["vars"] = {
            "api_key": {
                "value_from": {
                    "reader": "env",
                    "key": "API_KEY",
                    "default": "fallback",
                }
            }
        }
        project = project_parser.parse(payload)
        assert project.vars is not None
        assert isinstance(project.vars["api_key"], LookupValue)
        assert project.vars["api_key"].reader == "env"
        assert project.vars["api_key"].default == "fallback"

    def test_vars_rejects_invalid_value_spec(
        self, project_parser: ProjectParser
    ) -> None:
        """Vars with unsupported mapping shape raise ParseError."""
        payload = _minimal_project_payload()
        payload["vars"] = {"bad": {"unknown_key": 1}}
        with pytest.raises(ParseError, match="vars\\.bad"):
            project_parser.parse(payload)

    def test_no_vars_returns_none(self, project_parser: ProjectParser) -> None:
        """Omitting vars produces None."""
        payload = _minimal_project_payload()
        payload.pop("vars", None)
        project = project_parser.parse(payload)
        assert project.vars is None

    def test_wraps_connection_validation_error(
        self, project_parser: ProjectParser
    ) -> None:
        """Connection validation failures include indexed path."""
        payload = _minimal_project_payload()
        payload["connections"] = [{"type": "unknown", "name": "x"}]
        with pytest.raises(ParseError) as err:
            project_parser.parse(payload)
        assert "connections[0]" in str(err.value)
        msg = str(err.value)
        assert "Unknown connection backend: unknown" in msg

    def test_rejects_unknown_data_type(
        self, project_parser: ProjectParser
    ) -> None:
        """Unknown artifact data types fail with precise path."""
        payload = _minimal_project_payload()
        payload["tables"][0]["columns"][0][  # type: ignore[index]
            "data_type"
        ] = "not_a_type"
        with pytest.raises(
            ParseError, match="tables\\[0\\]\\.columns\\[0\\]\\.data_type"
        ):
            project_parser.parse(payload)

    def test_rejects_non_mapping_root(
        self, project_parser: ProjectParser
    ) -> None:
        """Project parser requires mapping payload at root."""
        with pytest.raises(ParseError, match="expects a mapping"):
            project_parser.parse([])  # type: ignore[arg-type]

    def test_rejects_non_list_tables(
        self, project_parser: ProjectParser
    ) -> None:
        """Tables field must be provided as a list."""
        payload = _minimal_project_payload()
        payload["tables"] = {}
        with pytest.raises(ParseError, match="tables: expects a list"):
            project_parser.parse(payload)

    def test_rejects_empty_tables(self, project_parser: ProjectParser) -> None:
        """Tables list cannot be empty."""
        payload = _minimal_project_payload()
        payload["tables"] = []
        with pytest.raises(
            ParseError, match="tables: must have at least one item"
        ):
            project_parser.parse(payload)

    def test_rejects_empty_table_columns(
        self, project_parser: ProjectParser
    ) -> None:
        """Table column list cannot be empty."""
        payload = _minimal_project_payload()
        payload["tables"][0]["columns"] = []  # type: ignore[index]
        with pytest.raises(ParseError, match="tables\\[0\\]\\.columns"):
            project_parser.parse(payload)

    def test_accepts_empty_seeds(self, project_parser: ProjectParser) -> None:
        """Empty seeds list is accepted."""
        payload = _minimal_project_payload()
        payload["seeds"] = []
        project = project_parser.parse(payload)
        assert project.seeds == []

    def test_accepts_omitted_seeds(self, project_parser: ProjectParser) -> None:
        """Omitting seeds produces an empty list."""
        payload = _minimal_project_payload()
        payload.pop("seeds", None)
        project = project_parser.parse(payload)
        assert project.seeds == []

    def test_rejects_empty_seed_columns(
        self, project_parser: ProjectParser
    ) -> None:
        """Seed column list cannot be empty."""
        payload = _minimal_project_payload()
        payload["seeds"][0]["columns"] = []  # type: ignore[index]
        with pytest.raises(ParseError, match="seeds\\[0\\]\\.columns"):
            project_parser.parse(payload)

    def test_accepts_empty_sources(self, project_parser: ProjectParser) -> None:
        """Empty sources list is accepted."""
        payload = _minimal_project_payload()
        payload["sources"] = []
        project = project_parser.parse(payload)
        assert project.sources == []

    def test_accepts_omitted_sources(
        self, project_parser: ProjectParser
    ) -> None:
        """Omitting sources produces an empty list."""
        payload = _minimal_project_payload()
        payload.pop("sources", None)
        project = project_parser.parse(payload)
        assert project.sources == []

    def test_rejects_empty_source_columns(
        self, project_parser: ProjectParser
    ) -> None:
        """Source column list cannot be empty."""
        payload = _minimal_project_payload()
        payload["sources"][0]["columns"] = []  # type: ignore[index]
        with pytest.raises(ParseError, match="sources\\[0\\]\\.columns"):
            project_parser.parse(payload)

    def test_rejects_missing_destinations(
        self, project_parser: ProjectParser
    ) -> None:
        """Destinations must be present and non-empty."""
        payload = _minimal_project_payload()
        payload.pop("destinations", None)
        with pytest.raises(ParseError, match="destinations: expects a list"):
            project_parser.parse(payload)

    def test_rejects_empty_destinations(
        self, project_parser: ProjectParser
    ) -> None:
        """Destinations list cannot be empty."""
        payload = _minimal_project_payload()
        payload["destinations"] = []
        with pytest.raises(
            ParseError, match="destinations: must have at least one item"
        ):
            project_parser.parse(payload)

    def test_parses_multiple_tables(
        self, project_parser: ProjectParser
    ) -> None:
        """Multiple tables are parsed preserving order."""
        payload = _minimal_project_payload()
        second_table = {
            "name": "posts",
            "columns": [
                {
                    "name": "id",
                    "data_type": "integer",
                    "generator": "primary_key.incrementing_id",
                }
            ],
            "config": {},
        }
        tables = payload["tables"]
        assert isinstance(tables, list)
        first_table = tables[0]
        payload["tables"] = [first_table, second_table]
        project = project_parser.parse(payload)
        assert len(project.tables) == 2
        assert project.tables[0].name == "users"
        assert project.tables[1].name == "posts"


# -------------------------------------------------------------------
# Backend bindings
# -------------------------------------------------------------------


class TestParseBackendBindings:
    """Tests for project-level backend binding parsing."""

    def test_applies_bindings(self) -> None:
        """Parser configures named backend bindings."""
        path_reg = PathResolverRegistry()
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_resolver_registry=path_reg,
        )
        parser.value_reader_registry.register("env", OsEnvReader)
        path_reg.register("file", FilesystemPathResolver)
        payload = _minimal_project_payload()
        payload["value_readers"] = [{"name": "runtime_env", "type": "env"}]
        payload["path_backends"] = [{"name": "localfs", "type": "file"}]

        project = parser.parse(payload)

        assert project.value_readers[0].name == "runtime_env"
        assert project.path_backends[0].name == "localfs"
        vr_inst = parser.value_reader_registry.get_instances()
        pb_inst = path_reg.get_instances()
        assert "runtime_env" in vr_inst
        assert "localfs" in pb_inst

    def test_rejects_duplicate_backend_names(
        self, project_parser: ProjectParser
    ) -> None:
        """Backend binding names must be unique per category."""
        payload = _minimal_project_payload()
        payload["value_readers"] = [
            {"name": "runtime_env", "type": "env"},
            {"name": "runtime_env", "type": "env"},
        ]
        with pytest.raises(
            ParseError,
            match=("value_readers\\[1\\]\\.name: duplicate backend name"),
        ):
            project_parser.parse(payload)

    def test_resets_bindings_between_calls(self) -> None:
        """Project-scoped bindings do not leak across parse calls."""
        path_reg = PathResolverRegistry()
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_resolver_registry=path_reg,
        )
        parser.value_reader_registry.register("env", OsEnvReader)
        path_reg.register("file", FilesystemPathResolver)
        payload = _minimal_project_payload()
        payload["value_readers"] = [{"name": "runtime_env", "type": "env"}]
        payload["path_backends"] = [{"name": "localfs", "type": "file"}]
        parser.parse(payload)
        vr_inst = parser.value_reader_registry.get_instances()
        pb_inst = path_reg.get_instances()
        assert "runtime_env" in vr_inst
        assert "localfs" in pb_inst

        parser.parse(_minimal_project_payload())
        assert parser.value_reader_registry.get_instances() == {}
        assert path_reg.get_instances() == {}


# -------------------------------------------------------------------
# Destination bindings
# -------------------------------------------------------------------


class TestParseDestinationBindings:
    """Tests for project-level destination (write backend) binding parsing."""

    @pytest.fixture
    def parser_with_data_registry(self) -> ProjectParser:
        """Parser with data persistence registry pre-loaded."""
        data_registry = DataPersistenceRegistry()
        data_registry.register("memory", _StubWriteBackend)
        return ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            data_persistence_registry=data_registry,
        )

    def test_applies_destination_bindings(
        self, parser_with_data_registry: ProjectParser
    ) -> None:
        """Parser configures named destination backend bindings."""
        payload = _minimal_project_payload()
        payload["destinations"] = [{"name": "output", "type": "memory"}]

        project = parser_with_data_registry.parse(payload)

        assert len(project.destinations) == 1
        assert project.destinations[0].name == "output"
        assert project.destinations[0].type == "memory"
        data_reg = parser_with_data_registry.data_persistence_registry
        assert data_reg is not None
        dest_inst = data_reg.get_instances()
        assert "output" in dest_inst

    def test_rejects_duplicate_destination_names(
        self, parser_with_data_registry: ProjectParser
    ) -> None:
        """Destination names must be unique."""
        payload = _minimal_project_payload()
        payload["destinations"] = [
            {"name": "out", "type": "memory"},
            {"name": "out", "type": "memory"},
        ]
        with pytest.raises(
            ParseError, match=r"destinations\[1\]\.name: duplicate backend name"
        ):
            parser_with_data_registry.parse(payload)

    def test_resets_destination_bindings_between_calls(self) -> None:
        """Destination bindings do not leak across parse calls."""
        data_registry = DataPersistenceRegistry()
        data_registry.register("memory", _StubWriteBackend)
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            data_persistence_registry=data_registry,
        )
        payload = _minimal_project_payload()
        payload["destinations"] = [{"name": "output", "type": "memory"}]
        parser.parse(payload)
        assert "output" in data_registry.get_instances()

        reset_payload = _minimal_project_payload()
        reset_payload["destinations"] = [{"name": "other", "type": "memory"}]
        parser.parse(reset_payload)
        assert "output" not in data_registry.get_instances()

    def test_skips_configuration_when_no_data_registry(
        self, project_parser: ProjectParser
    ) -> None:
        """Destinations are parsed but not configured when no registry."""
        payload = _minimal_project_payload()
        payload["destinations"] = [{"name": "output", "type": "memory"}]
        project = project_parser.parse(payload)

        assert len(project.destinations) == 1
        assert project.destinations[0].name == "output"


class TestParseNullCollections:
    """Explicit JSON null for list sections."""

    def test_null_sources_becomes_empty(
        self, project_parser: ProjectParser
    ) -> None:
        payload = _minimal_project_payload()
        payload["sources"] = None
        project = project_parser.parse(payload)
        assert project.sources == []

    def test_null_seeds_becomes_empty(
        self, project_parser: ProjectParser
    ) -> None:
        payload = _minimal_project_payload()
        payload["seeds"] = None
        project = project_parser.parse(payload)
        assert project.seeds == []


class BoomConfigureConnectionRegistry(ConnectionRegistry):
    """``configure`` raises for testing ``ProjectParser`` error wrapping."""

    def configure(self, spec: object) -> None:
        raise ValueError("connection configure boom")


class TestConfigureBackendFailures:
    """Registry ``configure`` failures are wrapped as ``ParseError``."""

    def test_path_backend_configure_value_error_wrapped(self) -> None:
        path_reg = PathResolverRegistry()
        path_reg.register("file", FilesystemPathResolver)
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_resolver_registry=path_reg,
        )
        payload = _minimal_project_payload()
        payload["seeds"] = []
        payload["sources"] = []
        payload["path_backends"] = [{"name": "pb", "type": "file"}]
        with (
            patch.object(
                path_reg, "configure", side_effect=ValueError("path fail")
            ),
            pytest.raises(ParseError, match="path fail") as err,
        ):
            parser.parse(payload)
        assert err.value.path == ("path_backends", 0)

    def test_connection_configure_value_error_wrapped(self) -> None:
        conn_reg = BoomConfigureConnectionRegistry()
        conn_reg.register("sqlalchemy", SQLAlchemyConnectionBackend)
        parser = ProjectParser(
            connection_registry=conn_reg,
            value_reader_registry=ValueReaderRegistry(),
        )
        payload = _minimal_project_payload()
        payload["seeds"] = []
        payload["sources"] = []
        payload["connections"] = [
            {
                "name": "main",
                "type": "sqlalchemy",
                "config": {
                    "host": "",
                    "user": "",
                    "password": "",
                    "database": ":memory:",
                },
            }
        ]
        with pytest.raises(
            ParseError, match="connection configure boom"
        ) as err:
            parser.parse(payload)
        assert err.value.path == ("connections", 0)

    def test_connection_configure_limbo_validation_wrapped(self) -> None:
        class _LimboConn(ConnectionRegistry):
            def configure(self, spec: object) -> None:
                raise LimboValidationError("conn limbo")

        conn_reg = _LimboConn()
        conn_reg.register("sqlalchemy", SQLAlchemyConnectionBackend)
        parser = ProjectParser(
            connection_registry=conn_reg,
            value_reader_registry=ValueReaderRegistry(),
        )
        payload = _minimal_project_payload()
        payload["seeds"] = []
        payload["sources"] = []
        payload["connections"] = [
            {
                "name": "main",
                "type": "sqlalchemy",
                "config": {
                    "host": "",
                    "user": "",
                    "password": "",
                    "database": ":memory:",
                },
            }
        ]
        with pytest.raises(ParseError, match="conn limbo") as err:
            parser.parse(payload)
        assert err.value.path == ("connections", 0)

    def test_destination_configure_limbo_wrapped(self) -> None:
        data_reg = DataPersistenceRegistry()
        data_reg.register("memory", _StubWriteBackend)
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            data_persistence_registry=data_reg,
        )

        def _boom(_spec: object) -> None:
            raise LimboValidationError("dest limbo")

        payload = _minimal_project_payload()
        payload["destinations"] = [{"name": "output", "type": "memory"}]
        with (
            patch.object(data_reg, "configure", side_effect=_boom),
            pytest.raises(ParseError, match="dest limbo") as err,
        ):
            parser.parse(payload)
        assert err.value.path == ("destinations", 0)
