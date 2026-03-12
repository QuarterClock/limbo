"""Application parser tests for payload-to-entity flow."""

from __future__ import annotations

import math

import pytest

import limbo_core.application.parsers.tables_parser as tables_parser_module
from limbo_core.adapters.connections import ConnectionRegistry
from limbo_core.adapters.filesystem import PathBackendRegistry
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.application.parsers import ParseError, ProjectParser
from limbo_core.application.parsers.common import InvalidValueSpecError
from limbo_core.domain.entities import (
    DataType,
    LiteralValue,
    LookupValue,
    TableRelationship,
)
from limbo_core.plugins.builtin.path_backends import FilesystemPathBackend
from limbo_core.plugins.builtin.value_readers import OsEnvReader


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
        assert column.options["length"].value == 5

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

    def test_rejects_empty_seeds(self, project_parser: ProjectParser) -> None:
        """Seeds list cannot be empty."""
        payload = _minimal_project_payload()
        payload["seeds"] = []
        with pytest.raises(
            ParseError, match="seeds: must have at least one item"
        ):
            project_parser.parse(payload)

    def test_rejects_empty_seed_columns(
        self, project_parser: ProjectParser
    ) -> None:
        """Seed column list cannot be empty."""
        payload = _minimal_project_payload()
        payload["seeds"][0]["columns"] = []  # type: ignore[index]
        with pytest.raises(ParseError, match="seeds\\[0\\]\\.columns"):
            project_parser.parse(payload)

    def test_rejects_empty_sources(self, project_parser: ProjectParser) -> None:
        """Source list cannot be empty."""
        payload = _minimal_project_payload()
        payload["sources"] = []
        with pytest.raises(
            ParseError, match="sources: must have at least one item"
        ):
            project_parser.parse(payload)

    def test_rejects_empty_source_columns(
        self, project_parser: ProjectParser
    ) -> None:
        """Source column list cannot be empty."""
        payload = _minimal_project_payload()
        payload["sources"][0]["columns"] = []  # type: ignore[index]
        with pytest.raises(ParseError, match="sources\\[0\\]\\.columns"):
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
        payload["tables"] = [  # type: ignore[assignment]
            payload["tables"][0],  # type: ignore[index]
            second_table,
        ]
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
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_backend_registry=PathBackendRegistry(),
        )
        parser.value_reader_registry.register("env", OsEnvReader)
        parser.path_backend_registry.register("file", FilesystemPathBackend)
        payload = _minimal_project_payload()
        payload["value_readers"] = [{"name": "runtime_env", "type": "env"}]
        payload["path_backends"] = [{"name": "localfs", "type": "file"}]

        project = parser.parse(payload)

        assert project.value_readers[0].name == "runtime_env"
        assert project.path_backends[0].name == "localfs"
        vr_inst = parser.value_reader_registry.get_instances()
        pb_inst = parser.path_backend_registry.get_instances()
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
        parser = ProjectParser(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_backend_registry=PathBackendRegistry(),
        )
        parser.value_reader_registry.register("env", OsEnvReader)
        parser.path_backend_registry.register("file", FilesystemPathBackend)
        payload = _minimal_project_payload()
        payload["value_readers"] = [{"name": "runtime_env", "type": "env"}]
        payload["path_backends"] = [{"name": "localfs", "type": "file"}]
        parser.parse(payload)
        vr_inst = parser.value_reader_registry.get_instances()
        pb_inst = parser.path_backend_registry.get_instances()
        assert "runtime_env" in vr_inst
        assert "localfs" in pb_inst

        parser.parse(_minimal_project_payload())
        assert parser.value_reader_registry.get_instances() == {}
        assert parser.path_backend_registry.get_instances() == {}
