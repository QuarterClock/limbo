"""Table payload parsing helpers."""

from __future__ import annotations

from typing import Any

from limbo_core.domain.entities import (
    Table,
    TableColumn,
    TableConfig,
    TableReference,
    TableRelationship,
    ValueSpec,
)

from .artifacts_parser import _parse_artifact_column
from .common import (
    InvalidValueSpecError,
    ParseError,
    PathPart,
    _expect_bool,
    _expect_list,
    _expect_mapping,
    _expect_optional_str,
    _expect_str,
)
from .value_spec_parser import parse_value_spec


def _parse_tables(value: Any, *, path: tuple[PathPart, ...]) -> list[Table]:
    """Parse table payload list.

    Returns:
        Parsed table entities.

    Raises:
        ParseError: If list structure or table payloads are invalid.
    """
    payloads = _expect_list(value, path=path)
    parsed: list[Table] = []
    for idx, payload in enumerate(payloads):
        parsed.append(_parse_table(payload, path=(*path, idx)))
    if not parsed:
        raise ParseError(path=path, message="must have at least one item")
    return parsed


def _parse_table(value: Any, *, path: tuple[PathPart, ...]) -> Table:
    """Parse one table payload.

    Returns:
        Parsed table entity.

    Raises:
        ParseError: If table payload fields are invalid.
    """
    payload = _expect_mapping(value, path=path)
    name = _expect_str(payload.get("name"), path=(*path, "name"))
    description = _expect_optional_str(
        payload.get("description"), path=(*path, "description")
    )
    config = _parse_table_config(
        payload.get("config", {}), path=(*path, "config")
    )
    columns = _parse_table_columns(
        payload.get("columns"), path=(*path, "columns")
    )

    raw_references = payload.get("references")
    references: list[TableReference] | None = None
    if raw_references is not None:
        references = _parse_table_references(
            raw_references, path=(*path, "references")
        )
        if not references:
            raise ParseError(
                path=(*path, "references"),
                message="must have at least one item",
            )

    return Table(
        name=name,
        description=description,
        config=config,
        columns=columns,
        references=references,
    )


def _parse_table_config(
    value: Any, *, path: tuple[PathPart, ...]
) -> TableConfig:
    """Parse table config payload.

    Returns:
        Parsed table config.
    """
    payload = _expect_mapping(value, path=path)
    materialize = payload.get("materialize", True)
    return TableConfig(
        materialize=_expect_bool(materialize, path=(*path, "materialize"))
    )


def _parse_table_columns(
    value: Any, *, path: tuple[PathPart, ...]
) -> list[TableColumn]:
    """Parse table column payload list.

    Returns:
        Parsed table column entities.

    Raises:
        ParseError: If list structure or column payloads are invalid.
    """
    payloads = _expect_list(value, path=path)
    columns: list[TableColumn] = []
    for idx, payload in enumerate(payloads):
        columns.append(_parse_table_column(payload, path=(*path, idx)))
    if not columns:
        raise ParseError(path=path, message="must have at least one item")
    return columns


def _parse_table_column(
    value: Any, *, path: tuple[PathPart, ...]
) -> TableColumn:
    """Parse one table column payload.

    Returns:
        Parsed table column entity.

    Raises:
        ParseError: If payload or option values are invalid.
    """
    payload = _expect_mapping(value, path=path)
    base = _parse_artifact_column(payload, path=path)
    generator = _expect_str(payload.get("generator"), path=(*path, "generator"))

    raw_options = payload.get("options")
    options: dict[str, ValueSpec] | None = None
    if raw_options is not None:
        options_payload = _expect_mapping(raw_options, path=(*path, "options"))
        options = {}
        for key, option_value in options_payload.items():
            option_path = (*path, "options", key)
            try:
                options[key] = parse_value_spec(option_value)
            except InvalidValueSpecError as err:
                raise ParseError(path=option_path, message=str(err)) from err

    return TableColumn(
        name=base.name,
        description=base.description,
        data_type=base.data_type,
        generator=generator,
        options=options,
    )


def _parse_table_references(
    value: Any, *, path: tuple[PathPart, ...]
) -> list[TableReference]:
    """Parse table reference payload list.

    Returns:
        Parsed table reference entities.
    """
    payloads = _expect_list(value, path=path)
    parsed: list[TableReference] = []
    for idx, payload in enumerate(payloads):
        parsed.append(_parse_table_reference(payload, path=(*path, idx)))
    return parsed


def _parse_table_reference(
    value: Any, *, path: tuple[PathPart, ...]
) -> TableReference:
    """Parse one table reference payload.

    Returns:
        Parsed table reference entity.

    Raises:
        ParseError: If payload fields are invalid.
    """
    payload = _expect_mapping(value, path=path)
    raw_type = payload.get("type")
    if raw_type not in {"table", "seed", "source"}:
        raise ParseError(
            path=(*path, "type"), message="expects one of: table, seed, source"
        )
    name = _expect_str(payload.get("name"), path=(*path, "name"))
    raw_relationship = payload.get("relationship")
    try:
        relationship = (
            raw_relationship
            if isinstance(raw_relationship, TableRelationship)
            else TableRelationship(str(raw_relationship))
        )
    except ValueError as err:
        raise ParseError(
            path=(*path, "relationship"),
            message=f"unknown table relationship: {raw_relationship}",
        ) from err
    return TableReference(type=raw_type, name=name, relationship=relationship)
