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
    ResolvedStorageRef,
    TabularBatch,
)

from .tabular_file_utils import (
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

    def storage_object_name(self, logical_name: str) -> str:
        """Return filename including ``.csv`` suffix."""
        return f"{safe_filename_stem(logical_name)}.csv"

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
        if self.csv_engine == "pyarrow":
            pa = try_import_pyarrow()
            import pyarrow.csv as pacsv

            cols = {c: [row[c] for row in data.rows] for c in data.column_names}
            table = pa.Table.from_pydict(cols)
            with ref.open_binary("wb") as out:
                pacsv.write_csv(table, out)
            return
        if self.csv_engine != "stdlib":
            raise ValidationError(
                "csv_engine must be 'stdlib' or 'pyarrow', "
                f"got {self.csv_engine!r}"
            )
        with ref.open_text("w", encoding=self.encoding, newline="") as fh:
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
        if not ref.exists():
            raise FileNotFoundError(ref.uri)
        if self.csv_engine == "pyarrow":
            try_import_pyarrow()
            import pyarrow.csv as pacsv

            with ref.open_binary("rb") as inp:
                table = pacsv.read_csv(inp)
            column_names = tuple(str(c) for c in table.column_names)
            rows_py = table.to_pylist()
            normalized = [
                {c: normalize_arrow_scalar(r.get(c)) for c in column_names}
                for r in rows_py
            ]
            return TabularBatch(
                column_names=column_names, rows=tuple(normalized)
            )
        with ref.open_text("r", encoding=self.encoding, newline="") as fh:
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
