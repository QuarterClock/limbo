"""JSON document tabular data persistence (orjson when installed)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.domain.value_objects import (
    LocalFilesystemStorageRef,
    ResolvedStorageRef,
)

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch

from .tabular_file_utils import (
    dump_json_bytes,
    ensure_parent_dir,
    load_json_document_from_bytes,
    safe_filename_stem,
    tabular_batch_from_json_document,
    tabular_batch_to_json_document,
)


@dataclass
class JsonFileDataPersistenceBackend(DataPersistenceBackend):
    """Read/write one JSON document per artifact (orjson when installed)."""

    directory: str | Path
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        """Coerce ``directory`` to a Path."""
        self.directory = Path(self.directory)

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        """Return a ref for ``name`` under this backend's directory."""
        path = Path(self.directory) / f"{safe_filename_stem(name)}.json"
        return LocalFilesystemStorageRef(
            backend="file", uri=str(path), local_path=path
        )

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        """Serialize ``data`` to the JSON file for ``ref``."""
        path = ref.as_local_path()
        ensure_parent_dir(path)
        doc = tabular_batch_to_json_document(data)
        path.write_bytes(dump_json_bytes(doc))

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        """Load a tabular batch from the JSON file for ``ref``.

        Returns:
            The decoded ``TabularBatch``.

        Raises:
            FileNotFoundError: If the file is missing.
        """
        path = ref.as_local_path()
        if not path.is_file():
            raise FileNotFoundError(path)
        doc = load_json_document_from_bytes(path.read_bytes())
        return tabular_batch_from_json_document(doc)

    def exists(self, ref: ResolvedStorageRef) -> bool:
        """Return True if the file for ``ref`` exists."""
        return ref.exists()

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        """Remove the file for ``ref`` if present."""
        ref.unlink()
