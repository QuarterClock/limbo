"""JSON document tabular PersistenceWriteBackend (orjson when installed)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from limbo_core.application.interfaces.persistence import (
    PersistenceWriteBackend,
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
class JsonFilePersistenceWriteBackend(PersistenceWriteBackend):
    """Read/write one JSON document per artifact (orjson when installed)."""

    directory: str | Path
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        self.directory = Path(self.directory)

    def _path(self, name: str) -> Path:
        return Path(self.directory) / f"{safe_filename_stem(name)}.json"

    def save(self, name: str, data: TabularBatch) -> None:
        path = self._path(name)
        ensure_parent_dir(path)
        doc = tabular_batch_to_json_document(data)
        path.write_bytes(dump_json_bytes(doc))

    def load(self, name: str) -> TabularBatch:
        path = self._path(name)
        if not path.is_file():
            raise FileNotFoundError(path)
        doc = load_json_document_from_bytes(path.read_bytes())
        return tabular_batch_from_json_document(doc)

    def exists(self, name: str) -> bool:
        return self._path(name).is_file()

    def cleanup(self, name: str) -> None:
        p = self._path(name)
        if p.is_file():
            p.unlink()
