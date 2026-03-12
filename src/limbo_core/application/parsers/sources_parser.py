"""Source payload parsing helpers."""

from __future__ import annotations

from typing import Any

from limbo_core.domain.entities import Source, SourceColumn, SourceConfig

from .artifacts_parser import _parse_artifact_column
from .common import (
    ParseError,
    PathPart,
    _expect_bool,
    _expect_list,
    _expect_mapping,
    _expect_optional_str,
    _expect_str,
)


def _parse_sources(value: Any, *, path: tuple[PathPart, ...]) -> list[Source]:
    """Parse source payload list.

    Returns:
        Parsed source entities.

    Raises:
        ParseError: If list structure or source payloads are invalid.
    """
    payloads = _expect_list(value, path=path)
    parsed: list[Source] = []
    for idx, payload in enumerate(payloads):
        parsed.append(_parse_source(payload, path=(*path, idx)))
    if not parsed:
        raise ParseError(path=path, message="must have at least one item")
    return parsed


def _parse_source(value: Any, *, path: tuple[PathPart, ...]) -> Source:
    """Parse one source payload.

    Returns:
        Parsed source entity.
    """
    payload = _expect_mapping(value, path=path)
    name = _expect_str(payload.get("name"), path=(*path, "name"))
    description = _expect_optional_str(
        payload.get("description"), path=(*path, "description")
    )
    config = _parse_source_config(
        payload.get("config", {}), path=(*path, "config")
    )
    columns = _parse_source_columns(
        payload.get("columns"), path=(*path, "columns")
    )
    return Source(
        name=name, description=description, config=config, columns=columns
    )


def _parse_source_config(
    value: Any, *, path: tuple[PathPart, ...]
) -> SourceConfig:
    """Parse source config payload.

    Returns:
        Parsed source config.
    """
    payload = _expect_mapping(value, path=path)
    materialize = _expect_bool(
        payload.get("materialize", True), path=(*path, "materialize")
    )
    connection = _expect_str(
        payload.get("connection"), path=(*path, "connection")
    )
    schema_name = _expect_optional_str(
        payload.get("schema_name"), path=(*path, "schema_name")
    )
    table_name = _expect_optional_str(
        payload.get("table_name"), path=(*path, "table_name")
    )
    return SourceConfig(
        materialize=materialize,
        connection=connection,
        schema_name=schema_name,
        table_name=table_name,
    )


def _parse_source_columns(
    value: Any, *, path: tuple[PathPart, ...]
) -> list[SourceColumn]:
    """Parse source column payload list.

    Returns:
        Parsed source column entities.

    Raises:
        ParseError: If list structure or columns are invalid.
    """
    payloads = _expect_list(value, path=path)
    columns: list[SourceColumn] = []
    for idx, payload in enumerate(payloads):
        column_payload = _expect_mapping(payload, path=(*path, idx))
        base = _parse_artifact_column(column_payload, path=(*path, idx))
        columns.append(
            SourceColumn(
                name=base.name,
                description=base.description,
                data_type=base.data_type,
            )
        )
    if not columns:
        raise ParseError(path=path, message="must have at least one item")
    return columns
