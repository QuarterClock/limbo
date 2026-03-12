"""Project backend specification parsing helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from limbo_core.domain.entities import PathBackendSpec, ValueReaderBackendSpec
from limbo_core.domain.entities.backends.backend_spec import BackendSpec
from limbo_core.validation import ValidationError

from .common import (
    ParseError,
    PathPart,
    _expect_list,
    _expect_mapping,
    check_duplicate_name,
)

_SpecT = TypeVar("_SpecT", bound=BackendSpec)


def parse_backend_spec(data: Any, *, spec_cls: type[_SpecT]) -> _SpecT:
    """Parse a raw mapping into a backend spec instance.

    Returns:
        Parsed backend specification.

    Raises:
        ValidationError: If payload shape is invalid.
    """
    if isinstance(data, spec_cls):
        return data

    if not isinstance(data, Mapping):
        raise ValidationError(
            f"{spec_cls.__name__} expects a mapping, got {type(data).__name__}"
        )
    payload = dict(data)

    name = payload.get("name")
    if not isinstance(name, str):
        raise ValidationError(f"{spec_cls.__name__}: `name` expects a string")

    backend_type = payload.get("type")
    if not isinstance(backend_type, str):
        raise ValidationError(f"{spec_cls.__name__}: `type` expects a string")

    raw_config = payload.get("config")
    extras = {
        key: value
        for key, value in payload.items()
        if key not in {"name", "type", "config"}
    }

    if raw_config is None:
        config = dict(extras)
    elif isinstance(raw_config, Mapping):
        config = dict(raw_config)
        config.update(extras)
    else:
        raise ValidationError(
            f"{spec_cls.__name__}: `config` expects a mapping when provided"
        )

    return spec_cls(name=name, type=backend_type, config=config)


def _parse_value_reader_backends(
    value: Any, *, path: tuple[PathPart, ...]
) -> list[ValueReaderBackendSpec]:
    """Parse value reader backend bindings.

    Returns:
        Parsed value reader backend specs.

    Raises:
        ParseError: If the value reader backend payload is invalid.
    """
    payloads = _expect_list(value, path=path)
    parsed: list[ValueReaderBackendSpec] = []
    seen_names: set[str] = set()
    for idx, payload in enumerate(payloads):
        item = _expect_mapping(payload, path=(*path, idx))
        try:
            spec = parse_backend_spec(item, spec_cls=ValueReaderBackendSpec)
        except ValidationError as err:
            raise ParseError(path=(*path, idx), message=str(err)) from err
        check_duplicate_name(spec.name, seen_names, path=(*path, idx))
        parsed.append(spec)
    return parsed


def _parse_path_backends(
    value: Any, *, path: tuple[PathPart, ...]
) -> list[PathBackendSpec]:
    """Parse path backend bindings.

    Returns:
        Parsed path backend specs.

    Raises:
        ParseError: If the path backend payload is invalid.
    """
    payloads = _expect_list(value, path=path)
    parsed: list[PathBackendSpec] = []
    seen_names: set[str] = set()
    for idx, payload in enumerate(payloads):
        item = _expect_mapping(payload, path=(*path, idx))
        try:
            spec = parse_backend_spec(item, spec_cls=PathBackendSpec)
        except ValidationError as err:
            raise ParseError(path=(*path, idx), message=str(err)) from err
        check_duplicate_name(spec.name, seen_names, path=(*path, idx))
        parsed.append(spec)
    return parsed
