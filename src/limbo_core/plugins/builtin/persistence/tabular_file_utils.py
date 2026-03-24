"""Shared helpers for tabular file persistence."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from limbo_core.adapters.connections.errors import MissingPackageError
from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import CellValue, TabularBatch


def safe_filename_stem(name: str) -> str:
    """Return a single path component safe for use as a file basename.

    Returns:
        A sanitized filename stem derived from ``name``.

    Raises:
        ValidationError: If the name is empty or unusable as a filename stem.
    """
    stem = Path(name).name
    if not stem or stem in {".", ".."}:
        raise ValidationError(f"Invalid artifact name {name!r}")
    return stem.replace("\x00", "")


def ensure_parent_dir(path: Path) -> None:
    """Create parent directories for a file path if missing."""
    path.parent.mkdir(parents=True, exist_ok=True)


def try_import_pyarrow() -> Any:
    """Import the pyarrow module.

    Returns:
        The imported ``pyarrow`` module.

    Raises:
        MissingPackageError: If pyarrow is not installed.
    """
    try:
        import pyarrow as pa
    except ImportError as err:
        raise MissingPackageError("pyarrow") from err
    return pa


def normalize_arrow_scalar(value: Any) -> CellValue:
    """Normalize a PyArrow / Python scalar to CellValue (CSV/Parquet read).

    Returns:
        A value suitable for ``TabularBatch`` cells.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (str, int, float, date, datetime)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def cell_to_json_value(value: CellValue) -> Any:
    """Convert a cell to a JSON-serializable value (tagged for dates).

    Returns:
        A JSON-encodable object representing ``value``.
    """
    if isinstance(value, datetime):
        return {"__type__": "datetime", "v": value.isoformat()}
    if isinstance(value, date):
        return {"__type__": "date", "v": value.isoformat()}
    return value


def cell_from_json_value(value: Any) -> CellValue:
    """Restore a cell value from JSON-loaded data.

    Returns:
        The decoded cell value.

    Raises:
        ValidationError: If the payload shape or type is unsupported.
    """
    if isinstance(value, dict) and "__type__" in value:
        t = value.get("__type__")
        raw = value.get("v")
        if t == "datetime" and isinstance(raw, str):
            return datetime.fromisoformat(raw)
        if t == "date" and isinstance(raw, str):
            return date.fromisoformat(raw)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (str, int, float)):
        return value
    msg = f"Unsupported JSON cell value: {type(value).__name__}"
    raise ValidationError(msg)


def tabular_batch_to_json_document(batch: TabularBatch) -> dict[str, Any]:
    """Serialize batch to a JSON-friendly dict.

    Returns:
        A mapping with ``column_names`` and ``rows`` suitable for JSON.
    """
    return {
        "column_names": list(batch.column_names),
        "rows": [
            {k: cell_to_json_value(row[k]) for k in batch.column_names}
            for row in batch.rows
        ],
    }


def tabular_batch_from_json_document(doc: Any) -> TabularBatch:
    """Deserialize batch from a JSON document dict.

    Returns:
        The reconstructed ``TabularBatch``.

    Raises:
        ValidationError: If ``doc`` is not a valid batch document.
    """
    if not isinstance(doc, dict):
        raise ValidationError("Tabular JSON root must be an object")
    cols = doc.get("column_names")
    rows_raw = doc.get("rows")
    if not isinstance(cols, list) or not all(isinstance(c, str) for c in cols):
        raise ValidationError("column_names must be a list of strings")
    if not isinstance(rows_raw, list):
        raise ValidationError("rows must be a list")
    column_names = tuple(cols)
    rows: list[dict[str, CellValue]] = []
    for i, item in enumerate(rows_raw):
        if not isinstance(item, dict):
            raise ValidationError(f"rows[{i}] must be an object")
        rows.append({k: cell_from_json_value(item[k]) for k in column_names})
    return TabularBatch(column_names=column_names, rows=tuple(rows))


def dump_json_bytes(doc: dict[str, Any]) -> bytes:
    """Serialize document to UTF-8 bytes (orjson if available).

    Returns:
        UTF-8 encoded JSON bytes.
    """
    try:
        import orjson

        return orjson.dumps(doc)
    except ImportError:
        return json.dumps(doc, separators=(",", ":")).encode("utf-8")


def load_json_document_from_bytes(raw: bytes) -> dict[str, Any]:
    """Parse a JSON object from UTF-8 bytes (orjson if available).

    Returns:
        The parsed root object as a dict.

    Raises:
        ValidationError: If the root JSON value is not an object.
    """
    try:
        import orjson

        result = orjson.loads(raw)
    except ImportError:
        result = json.loads(raw.decode("utf-8"))
    if not isinstance(result, dict):
        raise ValidationError("JSON root must be an object")
    return result
