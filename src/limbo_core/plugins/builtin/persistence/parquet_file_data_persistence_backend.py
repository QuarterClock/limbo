"""Parquet tabular data persistence (PyArrow, Snappy)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.domain.value_objects import (
    LocalFilesystemStorageRef,
    ResolvedStorageRef,
    TabularBatch,
)

from .tabular_file_utils import (
    ensure_parent_dir,
    normalize_arrow_scalar,
    safe_filename_stem,
    try_import_pyarrow,
)


@dataclass
class ParquetFileDataPersistenceBackend(DataPersistenceBackend):
    """Read/write Parquet (Snappy) via PyArrow.

    ``encoding`` is accepted for config compatibility with other file backends
    but is not used for Parquet.
    """

    directory: str | Path
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        """Coerce ``directory`` to a Path."""
        self.directory = Path(self.directory)

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        """Return a ref for ``name`` under this backend's directory."""
        path = Path(self.directory) / f"{safe_filename_stem(name)}.parquet"
        return LocalFilesystemStorageRef(
            backend="file", uri=str(path), local_path=path
        )

    def _batch_from_pyarrow_rows(
        self, column_names: tuple[str, ...], rows: list[dict[str, Any]]
    ) -> TabularBatch:
        normalized = [
            {c: normalize_arrow_scalar(row.get(c)) for c in column_names}
            for row in rows
        ]
        return TabularBatch(column_names=column_names, rows=tuple(normalized))

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        """Serialize ``data`` to the Parquet file for ``ref`` (via PyArrow)."""
        pa = try_import_pyarrow()
        import pyarrow.parquet as pq

        path = ref.as_local_path()
        ensure_parent_dir(path)
        cols = {c: [row[c] for row in data.rows] for c in data.column_names}
        table = pa.Table.from_pydict(cols)
        pq.write_table(table, str(path), compression="snappy")

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        """Load a tabular batch from the Parquet file for ``ref``.

        Returns:
            The decoded ``TabularBatch``.

        Raises:
            FileNotFoundError: If the file is missing.
        """
        try_import_pyarrow()
        import pyarrow.parquet as pq

        path = ref.as_local_path()
        if not path.is_file():
            raise FileNotFoundError(path)
        table = pq.read_table(str(path))
        column_names = tuple(str(c) for c in table.column_names)
        return self._batch_from_pyarrow_rows(column_names, table.to_pylist())

    def exists(self, ref: ResolvedStorageRef) -> bool:
        """Return True if the file for ``ref`` exists."""
        return ref.exists()

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        """Remove the file for ``ref`` if present."""
        ref.unlink()
