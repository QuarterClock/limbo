"""Filesystem path backend implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from limbo_core.application.interfaces import PathBackend
from limbo_core.application.parsers.common import InvalidPathSpecError
from limbo_core.domain.entities import PathSpec, ResolvedResource


@dataclass(slots=True)
class FilesystemPathBackend(PathBackend):
    """Resolve resources from local filesystem expressions."""

    cwd: Path = field(default_factory=Path.cwd)

    def resolve(
        self, path_spec: PathSpec, *, paths: dict[str, Any]
    ) -> ResolvedResource:
        """Resolve one local filesystem path spec.

        Returns:
            Filesystem-backed resolved resource.

        Raises:
            FileNotFoundError: If resolved path does not exist.
        """
        raw_path = Path(path_spec.location)
        if raw_path.is_absolute():
            return ResolvedResource(
                backend=path_spec.backend,
                uri=str(raw_path),
                local_path=raw_path,
            )

        base_path = self._resolve_base_path(path_spec.base, paths=paths)
        resolved = base_path / raw_path
        if not resolved.exists():
            raise FileNotFoundError(f"Path {resolved} does not exist")

        return ResolvedResource(
            backend=path_spec.backend, uri=str(resolved), local_path=resolved
        )

    def _resolve_base_path(
        self, base: str | None, *, paths: dict[str, Any]
    ) -> Path:
        """Resolve optional base alias from runtime context paths.

        Returns:
            Base path to use for local relative path resolution.

        Raises:
            InvalidPathSpecError: If base alias is empty or unknown.
        """
        if base is None:
            return self.cwd

        parts = base.split(".")
        if not parts or not parts[0]:
            raise InvalidPathSpecError("`path_from.base` cannot be empty")
        if parts[0] not in paths:
            raise InvalidPathSpecError(
                f"`path_from.base` unknown key: {parts[0]}"
            )

        value: Any = paths[parts[0]]
        for part in parts[1:]:
            value = getattr(value, part)
        return Path(value)
