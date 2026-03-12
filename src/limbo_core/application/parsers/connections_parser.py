"""Connection payload parsing helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from limbo_core.domain.entities import ConnectionBackendSpec
from limbo_core.errors import LimboValidationError

from .backends_parser import parse_backend_spec
from .common import (
    ParseError,
    PathPart,
    _expect_list,
    _expect_mapping,
    check_duplicate_name,
)
from .value_spec_parser import parse_lookup_value

if TYPE_CHECKING:
    from limbo_core.application.interfaces import (
        ConnectionRegistryPort,
        ValueReaderRegistryPort,
    )


def _parse_connections(
    value: Any,
    *,
    path: tuple[PathPart, ...],
    connection_registry: ConnectionRegistryPort,
    value_reader_registry: ValueReaderRegistryPort,
) -> list[ConnectionBackendSpec]:
    """Parse and validate connection payloads.

    Returns:
        Parsed connection entities.

    Raises:
        ParseError: If payload list or item validation fails.
    """
    payloads = _expect_list(value, path=path)
    parsed: list[ConnectionBackendSpec] = []
    seen_names: set[str] = set()
    for idx, payload in enumerate(payloads):
        item_path = (*path, idx)
        try:
            payload_mapping = _expect_mapping(payload, path=item_path)
            resolved_payload = _resolve_lookup_values(
                payload_mapping, value_reader_registry=value_reader_registry
            )
            spec = parse_backend_spec(
                resolved_payload, spec_cls=ConnectionBackendSpec
            )
        except ParseError:
            raise
        except ValueError as err:
            raise ParseError(path=item_path, message=str(err)) from err
        except LimboValidationError as err:
            raise ParseError(path=item_path, message=str(err)) from err

        check_duplicate_name(spec.name, seen_names, path=item_path)
        try:
            connection_registry.create(spec.type, config=spec.config)
            parsed.append(spec)
        except ValueError as err:
            raise ParseError(path=item_path, message=str(err)) from err
        except LimboValidationError as err:
            raise ParseError(path=item_path, message=str(err)) from err
    return parsed


def _resolve_lookup_values(
    payload: Any, *, value_reader_registry: ValueReaderRegistryPort
) -> Any:
    """Recursively resolve ``{value_from: ...}`` mappings in payload values.

    Returns:
        Payload with lookup mappings replaced by resolved scalar strings.
    """
    if isinstance(payload, Mapping):
        if "value_from" in payload:
            return value_reader_registry.resolve(parse_lookup_value(payload))
        return {
            key: _resolve_lookup_values(
                value, value_reader_registry=value_reader_registry
            )
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [
            _resolve_lookup_values(
                value, value_reader_registry=value_reader_registry
            )
            for value in payload
        ]
    return payload
