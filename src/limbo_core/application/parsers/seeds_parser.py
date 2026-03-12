"""Seed payload parsing helpers."""

from __future__ import annotations

from typing import Any

from limbo_core.domain.entities import Seed, SeedColumn, SeedConfig, SeedFile

from .artifacts_parser import _parse_artifact_column
from .common import (
    InvalidPathSpecError,
    ParseError,
    PathPart,
    _expect_bool,
    _expect_list,
    _expect_mapping,
    _expect_optional_str,
    _expect_str,
)
from .path_spec_parser import parse_path_spec


def _parse_seeds(value: Any, *, path: tuple[PathPart, ...]) -> list[Seed]:
    """Parse seed payload list.

    Returns:
        Parsed seed entities (may be empty).
    """
    if value is None:
        return []
    payloads = _expect_list(value, path=path)
    parsed: list[Seed] = []
    for idx, payload in enumerate(payloads):
        parsed.append(_parse_seed(payload, path=(*path, idx)))
    return parsed


def _parse_seed(value: Any, *, path: tuple[PathPart, ...]) -> Seed:
    """Parse one seed payload.

    Returns:
        Parsed seed entity.
    """
    payload = _expect_mapping(value, path=path)
    name = _expect_str(payload.get("name"), path=(*path, "name"))
    description = _expect_optional_str(
        payload.get("description"), path=(*path, "description")
    )
    config = _parse_seed_config(
        payload.get("config", {}), path=(*path, "config")
    )
    columns = _parse_seed_columns(
        payload.get("columns"), path=(*path, "columns")
    )
    seed_file = _parse_seed_file(
        payload.get("seed_file"), path=(*path, "seed_file")
    )
    return Seed(
        name=name,
        description=description,
        config=config,
        columns=columns,
        seed_file=seed_file,
    )


def _parse_seed_config(value: Any, *, path: tuple[PathPart, ...]) -> SeedConfig:
    """Parse seed config payload.

    Returns:
        Parsed seed config.
    """
    payload = _expect_mapping(value, path=path)
    materialize = payload.get("materialize", True)
    return SeedConfig(
        materialize=_expect_bool(materialize, path=(*path, "materialize"))
    )


def _parse_seed_columns(
    value: Any, *, path: tuple[PathPart, ...]
) -> list[SeedColumn]:
    """Parse seed column payload list.

    Returns:
        Parsed seed column entities.

    Raises:
        ParseError: If list structure or columns are invalid.
    """
    payloads = _expect_list(value, path=path)
    columns: list[SeedColumn] = []
    for idx, payload in enumerate(payloads):
        column_payload = _expect_mapping(payload, path=(*path, idx))
        base = _parse_artifact_column(column_payload, path=(*path, idx))
        columns.append(
            SeedColumn(
                name=base.name,
                description=base.description,
                data_type=base.data_type,
            )
        )
    if not columns:
        raise ParseError(path=path, message="must have at least one item")
    return columns


def _parse_seed_file(value: Any, *, path: tuple[PathPart, ...]) -> SeedFile:
    """Parse seed file payload.

    Returns:
        Parsed seed file entity.

    Raises:
        ParseError: If seed file payload fields are invalid.
    """
    payload = _expect_mapping(value, path=path)
    file_type = payload.get("type", "infer")
    if file_type not in {"csv", "json", "parquet", "jsonl", "infer"}:
        raise ParseError(
            path=(*path, "type"),
            message=f"unsupported seed file type: {file_type}",
        )
    compression = payload.get("compression", "infer")
    if compression not in {"gzip", "brotli", "zstd", "infer"}:
        raise ParseError(
            path=(*path, "compression"),
            message=f"unsupported seed compression value: {compression}",
        )
    raw_path = payload.get("path")
    try:
        path_spec = parse_path_spec(raw_path)
    except InvalidPathSpecError as err:
        raise ParseError(path=(*path, "path"), message=str(err)) from err
    return SeedFile(type=file_type, compression=compression, path=path_spec)
