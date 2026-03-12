"""Artifact parsing helpers shared by table/seed/source parsers."""

from __future__ import annotations

from typing import Any

from limbo_core.domain.entities import ArtifactColumn, DataType

from .common import ParseError, PathPart, _expect_optional_str, _expect_str


def _parse_artifact_column(
    payload: dict[str, Any], *, path: tuple[PathPart, ...]
) -> ArtifactColumn:
    """Parse one artifact column payload.

    Returns:
        Parsed artifact column.

    Raises:
        ParseError: If payload fields are malformed.
    """
    name = _expect_str(payload.get("name"), path=(*path, "name"))
    description = _expect_optional_str(
        payload.get("description"), path=(*path, "description")
    )
    raw_data_type = payload.get("data_type")
    try:
        data_type = (
            raw_data_type
            if isinstance(raw_data_type, DataType)
            else DataType(str(raw_data_type))
        )
    except ValueError as err:
        raise ParseError(
            path=(*path, "data_type"),
            message=f"unknown data type: {raw_data_type}",
        ) from err
    return ArtifactColumn(
        name=name, description=description, data_type=data_type
    )
