"""Parquet tabular PersistenceWriteBackend (PyArrow, Snappy)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from limbo_core.application.interfaces.persistence import (
    PersistenceWriteBackend,
)
from limbo_core.domain.value_objects import TabularBatch

from .tabular_file_utils import (
    ensure_parent_dir,
    normalize_arrow_scalar,
    safe_filename_stem,
    try_import_pyarrow,
)


@dataclass
class ParquetFilePersistenceWriteBackend(PersistenceWriteBackend):
    """Read/write Parquet (Snappy) via PyArrow.

    ``encoding`` is accepted for config compatibility with other file backends
    but is not used for Parquet.
    """

    directory: str | Path
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        self.directory = Path(self.directory)

    def _path(self, name: str) -> Path:
        return Path(self.directory) / f"{safe_filename_stem(name)}.parquet"

    def _batch_from_pyarrow_rows(
        self, column_names: tuple[str, ...], rows: list[dict[str, Any]]
    ) -> TabularBatch:
        normalized = [
            {c: normalize_arrow_scalar(row.get(c)) for c in column_names}
            for row in rows
        ]
        return TabularBatch(column_names=column_names, rows=tuple(normalized))

    def save(self, name: str, data: TabularBatch) -> None:
        pa = try_import_pyarrow()
        import pyarrow.parquet as pq

        path = self._path(name)
        ensure_parent_dir(path)
        cols = {c: [row[c] for row in data.rows] for c in data.column_names}
        table = pa.Table.from_pydict(cols)
        pq.write_table(table, str(path), compression="snappy")

    def load(self, name: str) -> TabularBatch:
        try_import_pyarrow()
        import pyarrow.parquet as pq

        path = self._path(name)
        if not path.is_file():
            raise FileNotFoundError(path)
        table = pq.read_table(str(path))
        column_names = tuple(str(c) for c in table.column_names)
        return self._batch_from_pyarrow_rows(column_names, table.to_pylist())

    def exists(self, name: str) -> bool:
        return self._path(name).is_file()

    def cleanup(self, name: str) -> None:
        p = self._path(name)
        if p.is_file():
            p.unlink()
