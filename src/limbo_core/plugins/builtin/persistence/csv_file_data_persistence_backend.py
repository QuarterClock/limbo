"""CSV tabular data persistence (stdlib or optional PyArrow)."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
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
    ensure_parent_dir,
    normalize_arrow_scalar,
    safe_filename_stem,
    try_import_pyarrow,
)


@dataclass
class CsvFileDataPersistenceBackend(DataPersistenceBackend):
    """Read/write CSV under a fixed directory via storage refs.

    Config:
        directory: Output directory path.
        encoding: Text encoding (default utf-8).
        csv_engine: ``stdlib`` (default) or ``pyarrow`` for faster I/O.
    """

    directory: str | Path
    encoding: str = "utf-8"
    csv_engine: str = "stdlib"

    def __post_init__(self) -> None:
        """Coerce ``directory`` to a Path and normalize ``csv_engine``."""
        self.directory = Path(self.directory)
        self.csv_engine = self.csv_engine.strip().lower()

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        """Return a ref for ``name`` under this backend's directory."""
        path = Path(self.directory) / f"{safe_filename_stem(name)}.csv"
        return LocalFilesystemStorageRef(
            backend="file", uri=str(path), local_path=path
        )

    @staticmethod
    def _cell_as_text(value: CellValue) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        """Write ``data`` to the CSV file for ``ref``.

        Raises:
            ValidationError: If ``csv_engine`` is not ``stdlib`` or ``pyarrow``.
        """
        path = ref.as_local_path()
        ensure_parent_dir(path)
        if self.csv_engine == "pyarrow":
            pa = try_import_pyarrow()
            import pyarrow.csv as pacsv

            cols = {c: [row[c] for row in data.rows] for c in data.column_names}
            table = pa.Table.from_pydict(cols)
            pacsv.write_csv(table, str(path))
            return
        if self.csv_engine != "stdlib":
            raise ValidationError(
                "csv_engine must be 'stdlib' or 'pyarrow', "
                f"got {self.csv_engine!r}"
            )
        with path.open("w", newline="", encoding=self.encoding) as fh:
            writer = csv.DictWriter(
                fh, fieldnames=list(data.column_names), extrasaction="raise"
            )
            writer.writeheader()
            for row in data.rows:
                writer.writerow({
                    k: self._cell_as_text(row[k]) for k in data.column_names
                })

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        """Load a tabular batch from the CSV file for ``ref``.

        Returns:
            The decoded ``TabularBatch``.

        Raises:
            FileNotFoundError: If the file is missing.
            ValidationError: If the CSV has no header row.
        """
        path = ref.as_local_path()
        if not path.is_file():
            raise FileNotFoundError(path)
        if self.csv_engine == "pyarrow":
            try_import_pyarrow()
            import pyarrow.csv as pacsv

            table = pacsv.read_csv(str(path))
            column_names = tuple(str(c) for c in table.column_names)
            rows_py = table.to_pylist()
            normalized = [
                {c: normalize_arrow_scalar(r.get(c)) for c in column_names}
                for r in rows_py
            ]
            return TabularBatch(
                column_names=column_names, rows=tuple(normalized)
            )
        with path.open(encoding=self.encoding) as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                raise ValidationError("CSV has no header row")
            column_names = tuple(reader.fieldnames)
            rows: list[dict[str, CellValue]] = []
            for raw in reader:
                row_out: dict[str, CellValue] = {}
                for col in column_names:
                    cell = raw.get(col)
                    row_out[col] = None if cell is None or cell == "" else cell
                rows.append(row_out)
        return TabularBatch(column_names=column_names, rows=tuple(rows))

    def exists(self, ref: ResolvedStorageRef) -> bool:
        """Return True if the file for ``ref`` exists."""
        return ref.exists()

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        """Remove the file for ``ref`` if present."""
        ref.unlink()
