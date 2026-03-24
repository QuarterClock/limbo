"""Tests for built-in tabular file DataPersistenceBackend implementations."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import pytest

from limbo_core.adapters.persistence import PathResolverRegistry
from limbo_core.domain.entities.resources.path_spec import PathSpec
from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import ResolvedStorageRef, TabularBatch
from limbo_core.plugins.builtin.persistence import (
    CsvFileDataPersistenceBackend,
    FilesystemPathResolver,
    JsonFileDataPersistenceBackend,
    JsonlFileDataPersistenceBackend,
    ParquetFileDataPersistenceBackend,
)
from limbo_core.plugins.plugin_manager import PluginManager

if TYPE_CHECKING:
    from pathlib import Path

    from limbo_core.application.interfaces.persistence import (
        DataPersistenceBackend,
    )


def _path_registry() -> PathResolverRegistry:
    reg = PathResolverRegistry()
    reg.register("file", FilesystemPathResolver)
    return reg


def _tabular_ref(
    backend: DataPersistenceBackend, logical_name: str
) -> ResolvedStorageRef:
    pr = _path_registry()
    artifact = backend.storage_object_name(logical_name)
    spec = PathSpec(backend="file", location=artifact, base=None)
    return pr.resolve_spec(spec, base=backend.directory, allow_missing=True)


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
        JsonFileDataPersistenceBackend,
        JsonlFileDataPersistenceBackend,
        ParquetFileDataPersistenceBackend,
    ],
)
def test_round_trip_save_load(tmp_path: Path, backend_cls: type) -> None:
    """save then load returns an equivalent batch (lossless formats)."""
    if backend_cls is ParquetFileDataPersistenceBackend:
        pytest.importorskip("pyarrow")
    d = tmp_path / "out"
    d.mkdir()
    backend = backend_cls(directory=d)
    batch = _sample_batch()
    ref = _tabular_ref(backend, "users")
    backend.save(ref, batch)
    assert backend.exists(ref)
    loaded = backend.load(ref)
    assert loaded.column_names == batch.column_names
    assert loaded.rows == batch.rows
    backend.cleanup(ref)
    assert not backend.exists(_tabular_ref(backend, "users"))


def test_csv_stdlib_round_trip_coerces_scalars(tmp_path: Path) -> None:
    """CSV load yields strings; bool/date/datetime written as text."""
    d = tmp_path / "csvout"
    d.mkdir()
    backend = CsvFileDataPersistenceBackend(directory=d)
    batch = _sample_batch()
    ref = _tabular_ref(backend, "users")
    backend.save(ref, batch)
    loaded = backend.load(ref)
    assert loaded.column_names == batch.column_names
    assert len(loaded.rows) == 2
    assert loaded.rows[0]["id"] == "1"
    assert loaded.rows[0]["flag"] == "true"
    assert loaded.rows[0]["note"] is None
    assert loaded.rows[0]["d"] == "2024-01-02"
    ts_cell = loaded.rows[0]["ts"]
    assert isinstance(ts_cell, str)
    assert ts_cell.startswith("2024-01-02T03:04:05")


@pytest.mark.parametrize(
    "backend_cls",
    [
        JsonFileDataPersistenceBackend,
        JsonlFileDataPersistenceBackend,
        ParquetFileDataPersistenceBackend,
    ],
)
def test_empty_rows_round_trip(tmp_path: Path, backend_cls: type) -> None:
    """Zero-row batches round-trip for JSON, JSONL, Parquet."""
    if backend_cls is ParquetFileDataPersistenceBackend:
        pytest.importorskip("pyarrow")
    d = tmp_path / "empty"
    d.mkdir()
    backend = backend_cls(directory=d)
    batch = _empty_row_batch()
    ref = _tabular_ref(backend, "t")
    backend.save(ref, batch)
    loaded = backend.load(ref)
    assert loaded == batch


def test_csv_empty_rows_round_trip(tmp_path: Path) -> None:
    """CSV with header only round-trips to zero rows."""
    d = tmp_path / "cempty"
    d.mkdir()
    backend = CsvFileDataPersistenceBackend(directory=d)
    batch = _empty_row_batch()
    ref = _tabular_ref(backend, "t")
    backend.save(ref, batch)
    loaded = backend.load(ref)
    assert loaded.column_names == batch.column_names
    assert loaded.rows == ()


@pytest.mark.parametrize(
    "backend_cls",
    [
        CsvFileDataPersistenceBackend,
        JsonFileDataPersistenceBackend,
        JsonlFileDataPersistenceBackend,
        ParquetFileDataPersistenceBackend,
    ],
)
def test_load_missing_raises(tmp_path: Path, backend_cls: type) -> None:
    if backend_cls is ParquetFileDataPersistenceBackend:
        pytest.importorskip("pyarrow")
    backend = backend_cls(directory=tmp_path)
    ref = _tabular_ref(backend, "nope")
    with pytest.raises(FileNotFoundError):
        backend.load(ref)


@pytest.mark.parametrize(
    "backend_cls",
    [
        CsvFileDataPersistenceBackend,
        JsonFileDataPersistenceBackend,
        JsonlFileDataPersistenceBackend,
        ParquetFileDataPersistenceBackend,
    ],
)
def test_invalid_name_raises(backend_cls: type, tmp_path: Path) -> None:
    if backend_cls is ParquetFileDataPersistenceBackend:
        pytest.importorskip("pyarrow")
    d = tmp_path / "inv"
    d.mkdir()
    backend = backend_cls(directory=d)
    with pytest.raises(ValidationError):
        backend.storage_object_name("")


def test_csv_pyarrow_round_trip(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    d = tmp_path / "pcsv"
    d.mkdir()
    backend = CsvFileDataPersistenceBackend(directory=d, csv_engine="pyarrow")
    batch = _sample_batch()
    ref = _tabular_ref(backend, "u")
    backend.save(ref, batch)
    loaded = backend.load(ref)
    assert loaded.column_names == batch.column_names
    assert len(loaded.rows) == len(batch.rows)


def test_csv_invalid_engine_raises(tmp_path: Path) -> None:
    d = tmp_path / "badeng"
    d.mkdir()
    backend = CsvFileDataPersistenceBackend(
        directory=d, csv_engine="not-a-mode"
    )
    ref = _tabular_ref(backend, "x")
    with pytest.raises(ValidationError, match="csv_engine"):
        backend.save(ref, _sample_batch())


def test_csv_stdlib_load_without_header_raises(tmp_path: Path) -> None:
    d = tmp_path / "nohdr"
    d.mkdir()
    p = d / "t.csv"
    p.write_bytes(b"")
    backend = CsvFileDataPersistenceBackend(directory=d)
    ref = _tabular_ref(backend, "t")
    with pytest.raises(ValidationError, match="no header"):
        backend.load(ref)


def test_csv_exists_and_cleanup(tmp_path: Path) -> None:
    d = tmp_path / "ex"
    d.mkdir()
    backend = CsvFileDataPersistenceBackend(directory=d)
    ref = _tabular_ref(backend, "f")
    assert not backend.exists(ref)
    backend.save(ref, _empty_row_batch())
    assert backend.exists(ref)
    backend.cleanup(ref)
    assert not backend.exists(ref)


def test_jsonl_load_whitespace_only_raises(tmp_path: Path) -> None:
    d = tmp_path / "jwl"
    d.mkdir()
    p = d / "e.jsonl"
    p.write_bytes(b"\n  \n\t\n")
    backend = JsonlFileDataPersistenceBackend(directory=d)
    ref = _tabular_ref(backend, "e")
    with pytest.raises(ValidationError, match="empty"):
        backend.load(ref)


def test_jsonl_row_key_mismatch_raises(tmp_path: Path) -> None:
    d = tmp_path / "jkm"
    d.mkdir()
    p = d / "m.jsonl"
    p.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
    backend = JsonlFileDataPersistenceBackend(directory=d)
    ref = _tabular_ref(backend, "m")
    with pytest.raises(ValidationError, match="row 1"):
        backend.load(ref)


def test_builtin_registers_tabular_write_backends() -> None:
    """Builtin plugin registers csv, json, jsonl, parquet data backends."""
    from limbo_core.adapters.connections import ConnectionRegistry
    from limbo_core.adapters.generators import GeneratorRegistry
    from limbo_core.adapters.persistence import (
        DataPersistenceRegistry,
        PathResolverRegistry,
    )
    from limbo_core.adapters.value_reader import ValueReaderRegistry

    path_reg = PathResolverRegistry()
    manager = PluginManager(
        connection_registry=ConnectionRegistry(),
        value_reader_registry=ValueReaderRegistry(),
        path_resolver_registry=path_reg,
        data_persistence_registry=DataPersistenceRegistry(
            path_resolver_registry=path_reg
        ),
        generator_registry=GeneratorRegistry(),
    )
    manager.load_plugins()
    types = manager._data_persistence_registry.get_types()
    for key in ("csv", "json", "jsonl", "parquet"):
        assert key in types
