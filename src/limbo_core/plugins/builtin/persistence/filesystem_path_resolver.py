"""Local filesystem path resolver implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from limbo_core.application.interfaces import PathResolverBackend
from limbo_core.domain.value_objects import LocalFilesystemStorageRef

if TYPE_CHECKING:
    from limbo_core.domain.entities import PathSpec


@dataclass(slots=True)
class FilesystemPathResolver(PathResolverBackend):
    """Resolve path specs to local filesystem storage references."""

    cwd: Path = field(default_factory=Path.cwd)

    def resolve(
        self,
        path_spec: PathSpec,
        *,
        base: Any | None = None,
        allow_missing: bool = False,
    ) -> LocalFilesystemStorageRef:
        """Resolve one local filesystem path spec.

        Returns:
            A ref pointing at the resolved path.

        Raises:
            FileNotFoundError: If a relative path does not exist under base and
                ``allow_missing`` is False.
        """
        raw_path = Path(path_spec.location)
        if raw_path.is_absolute():
            p = raw_path
            return LocalFilesystemStorageRef(
                backend=path_spec.backend, uri=str(p), local_path=p
            )

        base_path = Path(base) if base is not None else self.cwd
        resolved = base_path / raw_path
        if not allow_missing and not resolved.exists():
            raise FileNotFoundError(f"Path {resolved} does not exist")

        return LocalFilesystemStorageRef(
            backend=path_spec.backend, uri=str(resolved), local_path=resolved
        )
