"""Path spec parsing helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from limbo_core.domain.entities import PathSpec

from .common import InvalidPathSpecError


def parse_path_spec(raw: Any) -> PathSpec:
    """Parse raw path payload into a structured path spec.

    Accepted forms:
      - ``"relative/path.csv"`` (shorthand local path)
      - ``{"path_from": {"backend": "file", "location": "seed.csv"}}``
      - ``{"path_from": {"backend": "file", "base": "this", ...}}``

    Returns:
        Parsed backend path specification.

    Raises:
        InvalidPathSpecError: If payload does not match expected shape.
    """
    if isinstance(raw, PathSpec):
        return raw
    if isinstance(raw, str):
        return PathSpec(backend="file", location=raw)
    if not isinstance(raw, Mapping):
        raise InvalidPathSpecError("path spec expects a mapping or string")

    source = raw.get("path_from", raw)
    if not isinstance(source, Mapping):
        raise InvalidPathSpecError("`path_from` expects a mapping")

    backend = source.get("backend")
    if not isinstance(backend, str):
        raise InvalidPathSpecError("`path_from.backend` expects a string")
    normalized_backend = backend.strip().lower()
    if not normalized_backend:
        raise InvalidPathSpecError("`path_from.backend` cannot be empty")

    location = source.get("location")
    if not isinstance(location, str):
        raise InvalidPathSpecError("`path_from.location` expects a string")

    base = source.get("base")
    if base is not None and not isinstance(base, str):
        raise InvalidPathSpecError(
            "`path_from.base` expects a string when provided"
        )

    return PathSpec(backend=normalized_backend, location=location, base=base)
