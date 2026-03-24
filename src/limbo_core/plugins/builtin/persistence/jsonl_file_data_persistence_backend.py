"""JSONL tabular data persistence (orjson when installed)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import (
    CellValue,
    LocalFilesystemStorageRef,
    ResolvedStorageRef,
    TabularBatch,
)

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
class JsonlFileDataPersistenceBackend(DataPersistenceBackend):
    """Read/write newline-delimited JSON rows (orjson when installed).

    With zero data rows, writes a single envelope line (column_names + rows [])
    so columns are recoverable on load.
    """

    directory: str | Path
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        """Coerce ``directory`` to a Path."""
        self.directory = Path(self.directory)

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        """Return a ref for ``name`` under this backend's directory."""
        path = Path(self.directory) / f"{safe_filename_stem(name)}.jsonl"
        return LocalFilesystemStorageRef(
            backend="file", uri=str(path), local_path=path
        )

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        """Serialize ``data`` to the JSONL file for ``ref``."""
        path = ref.as_local_path()
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

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        """Load a tabular batch from the JSONL file for ``ref``.

        Returns:
            The decoded ``TabularBatch``.

        Raises:
            FileNotFoundError: If the file is missing.
            ValidationError: If the file is empty or rows are inconsistent.
        """
        path = ref.as_local_path()
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

    def exists(self, ref: ResolvedStorageRef) -> bool:
        """Return True if the file for ``ref`` exists."""
        return ref.exists()

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        """Remove the file for ``ref`` if present."""
        ref.unlink()
