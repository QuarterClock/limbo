"""Filesystem persistence read backend implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from limbo_core.application.interfaces import PersistenceReadBackend
from limbo_core.domain.entities import PathSpec, ResolvedResource


@dataclass(slots=True)
class FilesystemReadBackend(PersistenceReadBackend):
    """Resolve resources from local filesystem expressions."""

    cwd: Path = field(default_factory=Path.cwd)

    def resolve(
        self, path_spec: PathSpec, *, base: Any | None = None
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

        base_path = Path(base) if base is not None else self.cwd
        resolved = base_path / raw_path
        if not resolved.exists():
            raise FileNotFoundError(f"Path {resolved} does not exist")

        return ResolvedResource(
            backend=path_spec.backend, uri=str(resolved), local_path=resolved
        )
