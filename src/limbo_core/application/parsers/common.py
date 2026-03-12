"""Shared parser helpers and error types."""

from __future__ import annotations

from typing import Any

from limbo_core.domain.errors import DomainValidationError
from limbo_core.validation import ValidationError


class InvalidPathSpecError(DomainValidationError):
    """Raised when a structured path payload is malformed."""

    def __init__(self, message: str) -> None:
        """Initialize the InvalidPathSpecError."""
        super().__init__(message)


class InvalidValueSpecError(DomainValidationError):
    """Raised when a structured value spec payload is invalid."""


PathPart = str | int


def _format_path(path: tuple[PathPart, ...]) -> str:
    rendered = ""
    for part in path:
        if isinstance(part, int):
            rendered += f"[{part}]"
            continue
        if rendered:
            rendered += f".{part}"
        else:
            rendered = part
    return rendered or "<root>"


class ParseError(ValidationError):
    """Raised when parsing raw payloads into domain entities fails."""

    def __init__(self, *, path: tuple[PathPart, ...], message: str) -> None:
        """Initialize parse error with structured path context."""
        self.path = path
        self.message = message
        super().__init__(f"{_format_path(path)}: {message}")


def _expect_mapping(
    value: Any, *, path: tuple[PathPart, ...]
) -> dict[str, Any]:
    """Validate and return mapping value.

    Returns:
        Mapping value.

    Raises:
        ParseError: If value is not a mapping.
    """
    if not isinstance(value, dict):
        raise ParseError(path=path, message="expects a mapping")
    return value


def _expect_list(value: Any, *, path: tuple[PathPart, ...]) -> list[Any]:
    """Validate and return list value.

    Returns:
        List value.

    Raises:
        ParseError: If value is not a list.
    """
    if not isinstance(value, list):
        raise ParseError(path=path, message="expects a list")
    return value


def _expect_str(value: Any, *, path: tuple[PathPart, ...]) -> str:
    """Validate and return string value.

    Returns:
        String value.

    Raises:
        ParseError: If value is not a string.
    """
    if not isinstance(value, str):
        raise ParseError(path=path, message="expects a string")
    return value


def _expect_optional_str(
    value: Any, *, path: tuple[PathPart, ...]
) -> str | None:
    """Validate and return optional string value.

    Returns:
        String value, or None.

    Raises:
        ParseError: If value is neither None nor a string.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise ParseError(path=path, message="expects a string")


def check_duplicate_name(
    name: str, seen_names: set[str], *, path: tuple[PathPart, ...]
) -> None:
    """Raise if a normalized name was already seen.

    Raises:
        ParseError: If name collides with an earlier entry.
    """
    normalized = name.strip().lower()
    if normalized in seen_names:
        raise ParseError(
            path=(*path, "name"), message=f"duplicate backend name: {name}"
        )
    seen_names.add(normalized)


def _expect_bool(value: Any, *, path: tuple[PathPart, ...]) -> bool:
    """Validate and return boolean value.

    Returns:
        Boolean value.

    Raises:
        ParseError: If value is not a boolean.
    """
    if not isinstance(value, bool):
        raise ParseError(path=path, message="expects a bool value")
    return value
