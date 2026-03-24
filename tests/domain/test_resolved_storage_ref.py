"""Tests for ResolvedStorageRef / LocalFilesystemStorageRef."""

from __future__ import annotations

from pathlib import Path

import pytest

from limbo_core.domain.value_objects import LocalFilesystemStorageRef


def _ref(
    *,
    backend: str = "file",
    uri: str = "file:///tmp/x",
    local_path: Path | None = None,
    metadata: dict[str, object] | None = None,
) -> LocalFilesystemStorageRef:
    return LocalFilesystemStorageRef(
        backend=backend,
        uri=uri,
        local_path=local_path or Path("/tmp/x"),
        metadata=metadata,
    )


def test_hash_raises_type_error() -> None:
    """Refs are explicitly unhashable."""
    ref = _ref()
    with pytest.raises(TypeError, match="unhashable"):
        hash(ref)


def test_eq_same_values() -> None:
    p = Path("/a/b")
    a = _ref(backend="f", uri="u", local_path=p, metadata={"k": 1})
    b = _ref(backend="f", uri="u", local_path=p, metadata={"k": 1})
    assert a == b


def test_eq_different_metadata() -> None:
    p = Path("/a/b")
    a = _ref(local_path=p, metadata={"x": 1})
    b = _ref(local_path=p, metadata={"x": 2})
    assert a != b


def test_eq_not_implemented_for_other_types() -> None:
    ref = _ref()
    assert ref.__eq__("not-a-ref") is NotImplemented


def test_uri_and_metadata_properties() -> None:
    ref = _ref(
        backend="s3",
        uri="s3://b/k",
        local_path=Path("/x"),
        metadata={"region": "eu"},
    )
    assert ref.backend == "s3"
    assert ref.uri == "s3://b/k"
    assert ref.metadata == {"region": "eu"}


def test_read_write_bytes_round_trip(tmp_path: Path) -> None:
    p = tmp_path / "sub" / "f.bin"
    ref = _ref(local_path=p, uri=str(p))
    ref.write_bytes(b"hello")
    assert ref.read_bytes() == b"hello"
    assert ref.exists()


def test_open_text_write_read(tmp_path: Path) -> None:
    p = tmp_path / "t.txt"
    ref = _ref(local_path=p, uri=str(p))
    with ref.open_text("w", encoding="utf-8", newline="") as fh:
        fh.write("line1\n")
    with ref.open_text("r", encoding="utf-8", newline="") as fh:
        assert fh.read() == "line1\n"


def test_open_binary_write_read_append(tmp_path: Path) -> None:
    p = tmp_path / "b.bin"
    ref = _ref(local_path=p, uri=str(p))
    with ref.open_binary("wb") as out:
        out.write(b"ab")
    with ref.open_binary("rb") as inp:
        assert inp.read() == b"ab"
    with ref.open_binary("ab") as out:
        out.write(b"c")
    with ref.open_binary("rb") as inp:
        assert inp.read() == b"abc"
