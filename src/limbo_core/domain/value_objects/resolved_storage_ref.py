"""Resolved storage references for path resolution and tabular I/O."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


class ResolvedStorageRef(ABC):
    """Backend-agnostic handle for stored bytes (local path in phase A)."""

    @property
    @abstractmethod
    def backend(self) -> str:
        """Logical storage backend key from the original path spec."""
        ...

    @property
    @abstractmethod
    def uri(self) -> str:
        """Canonical URI or path string for this object."""
        ...

    @abstractmethod
    def exists(self) -> bool:
        """Return True if the object is present in storage."""
        ...

    @abstractmethod
    def unlink(self) -> None:
        """Remove the object if present (no-op or missing_ok semantics)."""
        ...

    @abstractmethod
    def as_local_path(self) -> Path:
        """Return a local filesystem path; raise if not filesystem-backed."""
        ...


class LocalFilesystemStorageRef(ResolvedStorageRef):
    """Filesystem-backed ref (local Path) with read/write/delete."""

    __slots__ = ("_backend", "_metadata", "_uri", "local_path")

    def __hash__(self) -> int:
        """Refuse hashing; metadata is mutable.

        Raises:
            TypeError: Always, because instances are not hashable.
        """
        raise TypeError(f"{type(self).__name__!r} objects are unhashable")

    def __init__(
        self,
        *,
        backend: str,
        uri: str,
        local_path: Path,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Build a ref for a concrete local file path."""
        self._backend = backend
        self._uri = uri
        self.local_path = local_path
        self._metadata = {} if metadata is None else dict(metadata)

    @property
    def backend(self) -> str:
        """Logical storage backend key from the original path spec."""
        return self._backend

    @property
    def uri(self) -> str:
        """Canonical URI or path string for this object."""
        return self._uri

    @property
    def metadata(self) -> dict[str, Any]:
        """Optional extra attributes for this ref."""
        return self._metadata

    def __eq__(self, other: object) -> bool:
        """Return True if the other ref denotes the same logical object."""
        if not isinstance(other, LocalFilesystemStorageRef):
            return NotImplemented
        return (
            self._backend == other._backend
            and self._uri == other._uri
            and self.local_path == other.local_path
            and self._metadata == other._metadata
        )

    def exists(self) -> bool:
        """Return True if the file exists."""
        return self.local_path.is_file()

    def unlink(self) -> None:
        """Remove the file if present."""
        self.local_path.unlink(missing_ok=True)

    def as_local_path(self) -> Path:
        """Return the backing filesystem path."""
        return self.local_path
