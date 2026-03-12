"""Adapter tests for environment interpolation and seed paths."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from limbo_core.adapters.filesystem import PathBackendRegistry
from limbo_core.adapters.filesystem.errors import UnknownPathBackendError
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.adapters.value_reader.errors import (
    LookupValueNotFoundError,
    UnknownValueReaderError,
)
from limbo_core.application.interfaces import PathBackend, ValueReaderBackend
from limbo_core.application.parsers.common import InvalidPathSpecError
from limbo_core.context import ParsingContext
from limbo_core.domain.entities import (
    LookupValue,
    PathBackendSpec,
    PathSpec,
    ResolvedResource,
    ValueReaderBackendSpec,
)
from limbo_core.plugins.builtin.path_backends import FilesystemPathBackend
from limbo_core.plugins.builtin.value_readers import OsEnvReader

if TYPE_CHECKING:
    from pathlib import Path


class _StaticReader(ValueReaderBackend):
    def get(self, key: str, default: str | None = None) -> str | None:
        return f"value:{key}"


class _SpyBackend(PathBackend):
    last_path_spec: PathSpec | None = None
    last_paths: dict[str, object] | None = None

    def resolve(
        self, path_spec: PathSpec, *, paths: dict[str, object]
    ) -> ResolvedResource:
        type(self).last_path_spec = path_spec
        type(self).last_paths = paths
        return ResolvedResource(backend="spy", uri="spy://resource")


# ---------------------------------------------------------------------------
# Value reader registry
# ---------------------------------------------------------------------------


class TestValueReaderRegistryResolve:
    """Tests for ValueReaderRegistry.resolve happy-path behaviour."""

    @pytest.fixture
    def registry(self) -> ValueReaderRegistry:
        """Registry pre-loaded with the OS env reader."""
        reg = ValueReaderRegistry()
        reg.register("env", OsEnvReader)
        return reg

    def test_reads_single_variable(
        self, registry: ValueReaderRegistry, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Resolver reads one key from registered env reader."""
        monkeypatch.setenv("TEST_HOST", "db.example.com")
        assert (
            registry.resolve(LookupValue(reader="env", key="TEST_HOST"))
            == "db.example.com"
        )

    def test_uses_default_for_missing_key(
        self, registry: ValueReaderRegistry, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing keys with defaults resolve to fallback values."""
        monkeypatch.delenv("MISSING_VAR", raising=False)
        assert (
            registry.resolve(
                LookupValue(reader="env", key="MISSING_VAR", default="fallback")
            )
            == "fallback"
        )

    def test_missing_without_default_raises(
        self, registry: ValueReaderRegistry, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing keys without defaults raise explicit lookup errors."""
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(LookupValueNotFoundError):
            registry.resolve(LookupValue(reader="env", key="MISSING_VAR"))

    def test_registers_and_uses_custom_reader(self) -> None:
        """Resolver can register and use readers after initialization."""
        registry = ValueReaderRegistry()
        registry.register("static", _StaticReader)
        assert (
            registry.resolve(LookupValue(reader="static", key="MY_KEY"))
            == "value:MY_KEY"
        )

    def test_uses_named_reader_binding(self) -> None:
        """Resolver can resolve lookups through configured reader aliases."""
        registry = ValueReaderRegistry()
        registry.register("static", _StaticReader)
        registry.configure(
            ValueReaderBackendSpec(name="named_static", type="static")
        )
        assert (
            registry.resolve(LookupValue(reader="named_static", key="MY_KEY"))
            == "value:MY_KEY"
        )

    def test_resolve_falls_back_to_type_key_if_no_instance(self) -> None:
        """Resolve uses the type registry when no named instance exists."""
        registry = ValueReaderRegistry()
        registry.register("static", _StaticReader)
        # No configure() — only a type is registered, no named instance.
        result = registry.resolve(LookupValue(reader="static", key="FALL"))
        assert result == "value:FALL"

    def test_create_many_from_specs(self) -> None:
        """create_many instantiates readers from spec list."""
        registry = ValueReaderRegistry()
        registry.register("static", _StaticReader)
        specs = [
            ValueReaderBackendSpec(name="a", type="static"),
            ValueReaderBackendSpec(name="b", type="static"),
        ]
        readers = registry.create_many(specs)
        assert len(readers) == 2
        assert all(isinstance(r, _StaticReader) for r in readers)


class TestValueReaderRegistryErrors:
    """Tests for ValueReaderRegistry error paths."""

    def test_unknown_reader_raises(self) -> None:
        """Resolver raises when lookup reader is not configured."""
        registry = ValueReaderRegistry()
        registry.register("env", OsEnvReader)
        with pytest.raises(
            UnknownValueReaderError, match="Unknown value reader"
        ):
            registry.resolve(LookupValue(reader="missing", key="DB_HOST"))


# ---------------------------------------------------------------------------
# Path backend registry
# ---------------------------------------------------------------------------


class TestPathBackendRegistryResolve:
    """Tests for PathBackendRegistry.resolve happy-path behaviour."""

    @pytest.fixture
    def file_registry(self) -> PathBackendRegistry:
        """Registry pre-loaded with the filesystem backend."""
        reg = PathBackendRegistry()
        reg.register("file", FilesystemPathBackend)
        return reg

    def test_resolves_structured_local_path(
        self, file_registry: PathBackendRegistry, tmp_path: Path
    ) -> None:
        """Registry resolves local specs relative to configured root alias."""
        (tmp_path / "data.csv").write_text("id\n1\n")
        context = ParsingContext(paths={"this": tmp_path})
        spec = {
            "path_from": {
                "backend": "file",
                "base": "this",
                "location": "data.csv",
            }
        }
        resolved = file_registry.resolve(spec, paths=context.paths)
        assert resolved.backend == "file"
        assert resolved.local_path == tmp_path / "data.csv"

    def test_resolves_existing_relative_path(
        self,
        file_registry: PathBackendRegistry,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Registry validates existence for plain local-path shorthand."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "seed.csv").write_text("id\n1\n")
        resolved = file_registry.resolve("seed.csv", paths={})
        assert resolved.local_path is not None
        assert resolved.local_path.name == "seed.csv"

    def test_dispatches_custom_backend(self) -> None:
        """Path registry dispatches structured backend specs correctly."""
        registry = PathBackendRegistry()
        registry.register("s3", _SpyBackend)
        resolved = registry.resolve(
            {"path_from": {"backend": "s3", "location": "s3://bucket/key.csv"}},
            paths={"scope": "any"},
        )
        assert resolved.backend == "spy"
        assert _SpyBackend.last_path_spec == PathSpec(
            backend="s3", location="s3://bucket/key.csv"
        )
        assert _SpyBackend.last_paths == {"scope": "any"}

    def test_uses_named_backend_binding(self) -> None:
        """Path registry dispatches through configured backend aliases."""
        registry = PathBackendRegistry()
        registry.register("s3", _SpyBackend)
        registry.configure(PathBackendSpec(name="archive", type="s3"))
        resolved = registry.resolve(
            {
                "path_from": {
                    "backend": "archive",
                    "location": "s3://bucket/key.csv",
                }
            },
            paths={"scope": "any"},
        )
        assert resolved.backend == "spy"
        assert _SpyBackend.last_path_spec == PathSpec(
            backend="archive", location="s3://bucket/key.csv"
        )

    def test_resolve_falls_back_to_type_key_if_no_instance(self) -> None:
        """Resolve uses type registry when no named instance exists."""
        registry = PathBackendRegistry()
        registry.register("s3", _SpyBackend)
        # No configure() — only a type is registered, no named instance.
        resolved = registry.resolve(
            {
                "path_from": {
                    "backend": "s3",
                    "location": "s3://bucket/fallback.csv",
                }
            },
            paths={},
        )
        assert resolved.backend == "spy"
        assert _SpyBackend.last_path_spec == PathSpec(
            backend="s3", location="s3://bucket/fallback.csv"
        )

    def test_create_many_from_specs(self) -> None:
        """create_many instantiates backends from a list of PathBackendSpec."""
        registry = PathBackendRegistry()
        registry.register("s3", _SpyBackend)
        specs = [
            PathBackendSpec(name="a", type="s3"),
            PathBackendSpec(name="b", type="s3"),
        ]
        backends = registry.create_many(specs)
        assert len(backends) == 2
        assert all(isinstance(b, _SpyBackend) for b in backends)

    def test_configure_from_spec(self) -> None:
        """configure stores a named backend instance from a PathBackendSpec."""
        registry = PathBackendRegistry()
        registry.register("s3", _SpyBackend)
        registry.configure(PathBackendSpec(name="archive", type="s3"))
        instances = registry.get_instances()
        assert "archive" in instances
        assert isinstance(instances["archive"], _SpyBackend)


class TestPathBackendRegistryErrors:
    """Tests for PathBackendRegistry error paths."""

    @pytest.fixture
    def file_registry(self) -> PathBackendRegistry:
        """Registry pre-loaded with the filesystem backend."""
        reg = PathBackendRegistry()
        reg.register("file", FilesystemPathBackend)
        return reg

    def test_invalid_input_type_raises(
        self, file_registry: PathBackendRegistry
    ) -> None:
        """Registry raises explicit domain errors for malformed inputs."""
        with pytest.raises(InvalidPathSpecError):
            file_registry.resolve(123, paths={})  # type: ignore[arg-type]

    def test_absolute_path_passes_through(
        self, file_registry: PathBackendRegistry, tmp_path: Path
    ) -> None:
        """Registry resolves absolute paths embedded in structured specs."""
        spec = {
            "path_from": {
                "backend": "file",
                "location": str(tmp_path / "abs.csv"),
            }
        }
        resolved = file_registry.resolve(spec, paths={})
        assert resolved.local_path == tmp_path / "abs.csv"

    def test_unknown_backend_raises(self) -> None:
        """Path registry rejects specs for unregistered backends."""
        registry = PathBackendRegistry()
        with pytest.raises(UnknownPathBackendError, match="s3"):
            registry.resolve(
                {
                    "path_from": {
                        "backend": "s3",
                        "location": "s3://bucket/key.csv",
                    }
                },
                paths={},
            )


# ---------------------------------------------------------------------------
# Filesystem path backend
# ---------------------------------------------------------------------------


class TestFilesystemPathBackend:
    """Tests for the built-in FilesystemPathBackend."""

    @pytest.fixture
    def backend(self) -> FilesystemPathBackend:
        """Fresh filesystem backend instance."""
        return FilesystemPathBackend()

    def test_accepts_generic_path_spec(
        self, backend: FilesystemPathBackend
    ) -> None:
        """Filesystem backend operates on generic path specs."""
        with pytest.raises(FileNotFoundError, match="does not exist"):
            backend.resolve(
                PathSpec(backend="s3", location="missing.csv"), paths={}
            )

    def test_missing_relative_path_raises(
        self,
        backend: FilesystemPathBackend,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Filesystem backend raises when relative target does not exist."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError, match="does not exist"):
            backend.resolve(
                PathSpec(backend="file", location="missing.csv"), paths={}
            )

    def test_resolves_local_root_without_path(
        self, backend: FilesystemPathBackend, tmp_path: Path
    ) -> None:
        """Local path specs can resolve root aliases with empty path."""
        resolved = backend.resolve(
            PathSpec(backend="file", location="", base="this"),
            paths={"this": tmp_path},
        )
        assert resolved.backend == "file"
        assert resolved.local_path == tmp_path

    def test_resolves_nested_alias_path(
        self, backend: FilesystemPathBackend, tmp_path: Path
    ) -> None:
        """Filesystem backend supports dotted alias lookup paths."""

        class _AliasRoot:
            def __init__(self, parent: Path) -> None:
                self.parent = parent

        (tmp_path / "data.csv").write_text("id\n1\n")
        resolved = backend.resolve(
            PathSpec(backend="file", location="data.csv", base="this.parent"),
            paths={"this": _AliasRoot(parent=tmp_path)},
        )
        assert resolved.local_path == tmp_path / "data.csv"
