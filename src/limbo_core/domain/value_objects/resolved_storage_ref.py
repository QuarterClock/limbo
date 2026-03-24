"""Resolved storage references for path resolution and tabular I/O."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator  # noqa: TC003
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path  # noqa: TC003
from typing import Any, BinaryIO, Literal, TextIO


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

    @abstractmethod
    def read_bytes(self) -> bytes:
        """Read the entire object as bytes."""
        ...

    @abstractmethod
    def write_bytes(self, data: bytes) -> None:
        """Replace contents with ``data``; create parent dirs if needed."""
        ...

    @abstractmethod
    def open_binary(
        self, mode: Literal["rb", "wb", "ab"]
    ) -> AbstractContextManager[BinaryIO]:
        """Open a binary stream; use ``with ref.open_binary('wb') as f:``."""
        ...

    @abstractmethod
    def open_text(
        self,
        mode: Literal["r", "w", "a"],
        *,
        encoding: str,
        newline: str | None = None,
    ) -> AbstractContextManager[TextIO]:
        """Open a text stream; use ``with ref.open_text(...) as fh:``."""
        ...


class LocalFilesystemStorageRef(ResolvedStorageRef):
    """Filesystem-backed ref (local Path) with read/write/delete."""

    __slots__ = ("_backend", "_metadata", "_uri", "local_path")

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

    @staticmethod
    def _ensure_parent(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

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

    def read_bytes(self) -> bytes:
        """Read the entire file.

        Returns:
            Raw file bytes.
        """
        with self.open_binary("rb") as fh:
            return fh.read()

    def write_bytes(self, data: bytes) -> None:
        """Write bytes, creating parent directories if needed."""
        with self.open_binary("wb") as fh:
            fh.write(data)

    @contextmanager
    def open_binary(
        self, mode: Literal["rb", "wb", "ab"]
    ) -> Iterator[BinaryIO]:
        """Open the file in binary mode.

        Yields:
            An open binary stream, closed after the context exits.
        """
        if mode != "rb":
            self._ensure_parent(self.local_path)
        with self.local_path.open(mode) as fh:
            yield fh

    @contextmanager
    def open_text(
        self,
        mode: Literal["r", "w", "a"],
        *,
        encoding: str,
        newline: str | None = None,
    ) -> Iterator[TextIO]:
        """Open the file in text mode.

        Yields:
            An open text stream, closed after the context exits.
        """
        if mode != "r":
            self._ensure_parent(self.local_path)
        with self.local_path.open(
            mode, encoding=encoding, newline=newline
        ) as fh:
            yield fh
