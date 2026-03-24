"""JSONL tabular PersistenceWriteBackend (orjson when installed)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from limbo_core.application.interfaces.persistence import (
    PersistenceWriteBackend,
)
from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import CellValue, TabularBatch

from .tabular_file_utils import (
    cell_from_json_value,
    cell_to_json_value,
    dump_json_bytes,
    ensure_parent_dir,
    load_json_document_from_bytes,
    safe_filename_stem,
    tabular_batch_from_json_document,
    tabular_batch_to_json_document,
)


@dataclass
class JsonlFilePersistenceWriteBackend(PersistenceWriteBackend):
    """Read/write newline-delimited JSON rows (orjson when installed).

    With zero data rows, writes a single envelope line (column_names + rows [])
    so columns are recoverable on load.
    """

    directory: str | Path
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        self.directory = Path(self.directory)

    def _path(self, name: str) -> Path:
        return Path(self.directory) / f"{safe_filename_stem(name)}.jsonl"

    def save(self, name: str, data: TabularBatch) -> None:
        path = self._path(name)
        ensure_parent_dir(path)
        if not data.rows:
            payload = dump_json_bytes(tabular_batch_to_json_document(data))
            path.write_bytes(payload + b"\n")
            return
        parts: list[bytes] = []
        for row in data.rows:
            doc = {k: cell_to_json_value(row[k]) for k in data.column_names}
            parts.append(dump_json_bytes(doc) + b"\n")
        path.write_bytes(b"".join(parts))

    def load(self, name: str) -> TabularBatch:
        path = self._path(name)
        if not path.is_file():
            raise FileNotFoundError(path)
        raw = path.read_bytes()
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if not lines:
            raise ValidationError("JSONL file is empty")
        first = load_json_document_from_bytes(lines[0])
        if "column_names" in first and "rows" in first:
            return tabular_batch_from_json_document(first)
        column_names: tuple[str, ...] = tuple(first.keys())
        parsed: list[dict[str, CellValue]] = [
            {k: cell_from_json_value(first[k]) for k in column_names}
        ]
        for i, line in enumerate(lines[1:], start=1):
            obj = load_json_document_from_bytes(line)
            if tuple(obj.keys()) != column_names:
                raise ValidationError(
                    f"JSONL row {i} keys do not match first row"
                )
            parsed.append({
                k: cell_from_json_value(obj[k]) for k in column_names
            })
        return TabularBatch(column_names=column_names, rows=tuple(parsed))

    def exists(self, name: str) -> bool:
        return self._path(name).is_file()

    def cleanup(self, name: str) -> None:
        p = self._path(name)
        if p.is_file():
            p.unlink()
