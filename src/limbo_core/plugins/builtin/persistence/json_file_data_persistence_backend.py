"""JSON document tabular data persistence (orjson when installed)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from limbo_core.application.interfaces.persistence import DataPersistenceBackend

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import ResolvedStorageRef, TabularBatch

from .tabular_file_utils import (
    dump_json_bytes,
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

    def storage_object_name(self, logical_name: str) -> str:
        """Return filename including ``.json`` suffix."""
        return f"{safe_filename_stem(logical_name)}.json"

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        """Serialize ``data`` to the JSON file for ``ref``."""
        doc = tabular_batch_to_json_document(data)
        payload = dump_json_bytes(doc)
        with ref.open_binary("wb") as out:
            out.write(payload)

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        """Load a tabular batch from the JSON file for ``ref``.

        Returns:
            The decoded ``TabularBatch``.

        Raises:
            FileNotFoundError: If the file is missing.
        """
        if not ref.exists():
            raise FileNotFoundError(ref.uri)
        with ref.open_binary("rb") as inp:
            raw = inp.read()
        doc = load_json_document_from_bytes(raw)
        return tabular_batch_from_json_document(doc)

    def exists(self, ref: ResolvedStorageRef) -> bool:
        """Return True if the file for ``ref`` exists."""
        return ref.exists()

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        """Remove the file for ``ref`` if present."""
        ref.unlink()
