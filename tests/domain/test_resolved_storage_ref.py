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
