"""Tests for built-in tabular file PersistenceWriteBackend implementations."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import TabularBatch
from limbo_core.plugins.builtin.persistence import (
    CsvFilePersistenceWriteBackend,
    JsonFilePersistenceWriteBackend,
    JsonlFilePersistenceWriteBackend,
    ParquetFilePersistenceWriteBackend,
)


def _sample_batch() -> TabularBatch:
    return TabularBatch(
        column_names=("id", "flag", "note", "d", "ts"),
        rows=(
            {
                "id": 1,
                "flag": True,
                "note": None,
                "d": date(2024, 1, 2),
                "ts": datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            },
            {
                "id": 2,
                "flag": False,
                "note": "a'b",
                "d": date(2025, 6, 7),
                "ts": datetime(2025, 6, 7, 8, 9, 10, tzinfo=UTC),
            },
        ),
    )


def _empty_row_batch() -> TabularBatch:
    return TabularBatch(column_names=("id",), rows=())


@pytest.mark.parametrize(
    "backend_cls",
    [
        JsonFilePersistenceWriteBackend,
        JsonlFilePersistenceWriteBackend,
        ParquetFilePersistenceWriteBackend,
    ],
)
def test_round_trip_save_load(tmp_path: Path, backend_cls: type) -> None:
    """save then load returns an equivalent batch (lossless formats)."""
    d = tmp_path / "out"
    d.mkdir()
    backend = backend_cls(directory=d)
    batch = _sample_batch()
    backend.save("users", batch)
    assert backend.exists("users")
    loaded = backend.load("users")
    assert loaded.column_names == batch.column_names
    assert loaded.rows == batch.rows
    backend.cleanup("users")
    assert not backend.exists("users")


def test_csv_stdlib_round_trip_coerces_scalars(tmp_path: Path) -> None:
    """CSV load yields strings; bool/date/datetime written as text."""
    d = tmp_path / "csvout"
    d.mkdir()
    backend = CsvFilePersistenceWriteBackend(directory=d)
    batch = _sample_batch()
    backend.save("users", batch)
    loaded = backend.load("users")
    assert loaded.column_names == batch.column_names
    assert len(loaded.rows) == 2
    assert loaded.rows[0]["id"] == "1"
    assert loaded.rows[0]["flag"] == "true"
    assert loaded.rows[0]["note"] is None
    assert loaded.rows[0]["d"] == "2024-01-02"
    assert loaded.rows[0]["ts"].startswith("2024-01-02T03:04:05")


@pytest.mark.parametrize(
    "backend_cls",
    [
        JsonFilePersistenceWriteBackend,
        JsonlFilePersistenceWriteBackend,
        ParquetFilePersistenceWriteBackend,
    ],
)
def test_empty_rows_round_trip(tmp_path: Path, backend_cls: type) -> None:
    """Zero-row batches round-trip for JSON, JSONL, Parquet."""
    d = tmp_path / "empty"
    d.mkdir()
    backend = backend_cls(directory=d)
    batch = _empty_row_batch()
    backend.save("t", batch)
    loaded = backend.load("t")
    assert loaded == batch


def test_csv_empty_rows_round_trip(tmp_path: Path) -> None:
    """CSV with header only round-trips to zero rows."""
    d = tmp_path / "cempty"
    d.mkdir()
    backend = CsvFilePersistenceWriteBackend(directory=d)
    batch = _empty_row_batch()
    backend.save("t", batch)
    loaded = backend.load("t")
    assert loaded.column_names == batch.column_names
    assert loaded.rows == ()


@pytest.mark.parametrize(
    "backend_cls",
    [
        CsvFilePersistenceWriteBackend,
        JsonFilePersistenceWriteBackend,
        JsonlFilePersistenceWriteBackend,
        ParquetFilePersistenceWriteBackend,
    ],
)
def test_load_missing_raises(tmp_path: Path, backend_cls: type) -> None:
    backend = backend_cls(directory=tmp_path)
    with pytest.raises(FileNotFoundError):
        backend.load("nope")


@pytest.mark.parametrize(
    "backend_cls",
    [
        CsvFilePersistenceWriteBackend,
        JsonFilePersistenceWriteBackend,
        JsonlFilePersistenceWriteBackend,
        ParquetFilePersistenceWriteBackend,
    ],
)
def test_invalid_name_raises(backend_cls: type, tmp_path: Path) -> None:
    d = tmp_path / "inv"
    d.mkdir()
    backend = backend_cls(directory=d)
    with pytest.raises(ValidationError):
        backend.save("", _sample_batch())


def test_csv_pyarrow_round_trip(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    d = tmp_path / "pcsv"
    d.mkdir()
    backend = CsvFilePersistenceWriteBackend(directory=d, csv_engine="pyarrow")
    batch = _sample_batch()
    backend.save("u", batch)
    loaded = backend.load("u")
    assert loaded.column_names == batch.column_names
    assert len(loaded.rows) == len(batch.rows)


def test_builtin_registers_tabular_write_backends() -> None:
    """Builtin plugin registers csv, json, jsonl, parquet write types."""
    from limbo_core.adapters.connections import ConnectionRegistry
    from limbo_core.adapters.generators import GeneratorRegistry
    from limbo_core.adapters.persistence import (
        PersistenceReadRegistry,
        PersistenceWriteRegistry,
    )
    from limbo_core.adapters.value_reader import ValueReaderRegistry
    from limbo_core.plugins import PluginManager

    manager = PluginManager(
        connection_registry=ConnectionRegistry(),
        value_reader_registry=ValueReaderRegistry(),
        path_backend_registry=PersistenceReadRegistry(),
        persistence_write_registry=PersistenceWriteRegistry(),
        generator_registry=GeneratorRegistry(),
    )
    manager.load_plugins()
    types = manager._persistence_write_registry.get_types()
    for key in ("csv", "json", "jsonl", "parquet"):
        assert key in types
