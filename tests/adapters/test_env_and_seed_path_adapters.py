"""Adapter tests for environment interpolation and seed paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from limbo_core.adapters.persistence import PathResolverRegistry
from limbo_core.adapters.persistence.errors import UnknownPathBackendError
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.adapters.value_reader.errors import (
    LookupValueNotFoundError,
    UnknownValueReaderError,
)
from limbo_core.application.context import ResolutionContext
from limbo_core.application.interfaces import (
    PathResolverBackend,
    ValueReaderBackend,
)
from limbo_core.application.parsers.common import InvalidPathSpecError
from limbo_core.domain.entities import (
    LookupValue,
    PathBackendSpec,
    PathSpec,
    ValueReaderBackendSpec,
)
from limbo_core.domain.value_objects import LocalFilesystemStorageRef
from limbo_core.plugins.builtin.persistence import FilesystemPathResolver
from limbo_core.plugins.builtin.value_readers import OsEnvReader


class _StaticReader(ValueReaderBackend):
    def get(self, key: str, default: str | None = None) -> str | None:
        return f"value:{key}"


class _SpyBackend(PathResolverBackend):
    last_path_spec: PathSpec | None = None
    last_base: object | None = None

    def resolve(
        self,
        path_spec: PathSpec,
        *,
        base: object | None = None,
        allow_missing: bool = False,
    ) -> LocalFilesystemStorageRef:
        type(self).last_path_spec = path_spec
        type(self).last_base = base
        return LocalFilesystemStorageRef(
            backend="spy",
            uri="spy://resource",
            local_path=Path("/__limbo_spy__/resource"),
        )


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
    """Tests for PathResolverRegistry.resolve happy-path behaviour."""

    @pytest.fixture
    def file_registry(self) -> PathResolverRegistry:
        """Registry pre-loaded with the filesystem backend."""
        reg = PathResolverRegistry()
        reg.register("file", FilesystemPathResolver)
        return reg

    def test_resolves_structured_local_path(
        self, file_registry: PathResolverRegistry, tmp_path: Path
    ) -> None:
        """Registry resolves local specs relative to configured root alias."""
        (tmp_path / "data.csv").write_text("id\n1\n")
        ctx = ResolutionContext(source_dir=tmp_path)
        spec = {
            "path_from": {
                "backend": "file",
                "base": "this",
                "location": "data.csv",
            }
        }
        resolved = file_registry.resolve(spec, context=ctx)
        assert resolved.backend == "file"
        assert resolved.as_local_path() == tmp_path / "data.csv"

    def test_resolves_existing_relative_path(
        self,
        file_registry: PathResolverRegistry,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Registry validates existence for plain local-path shorthand."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "seed.csv").write_text("id\n1\n")
        resolved = file_registry.resolve("seed.csv")
        assert resolved.as_local_path().name == "seed.csv"

    def test_dispatches_custom_backend(self) -> None:
        """Path registry dispatches structured backend specs correctly."""
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        resolved = registry.resolve({
            "path_from": {"backend": "s3", "location": "s3://bucket/key.csv"}
        })
        assert resolved.backend == "spy"
        assert _SpyBackend.last_path_spec == PathSpec(
            backend="s3", location="s3://bucket/key.csv"
        )
        assert _SpyBackend.last_base is None

    def test_uses_named_backend_binding(self) -> None:
        """Path registry dispatches through configured backend aliases."""
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        registry.configure(PathBackendSpec(name="archive", type="s3"))
        resolved = registry.resolve({
            "path_from": {
                "backend": "archive",
                "location": "s3://bucket/key.csv",
            }
        })
        assert resolved.backend == "spy"
        assert _SpyBackend.last_path_spec == PathSpec(
            backend="archive", location="s3://bucket/key.csv"
        )

    def test_resolve_falls_back_to_type_key_if_no_instance(self) -> None:
        """Resolve uses type registry when no named instance exists."""
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        resolved = registry.resolve({
            "path_from": {
                "backend": "s3",
                "location": "s3://bucket/fallback.csv",
            }
        })
        assert resolved.backend == "spy"
        assert _SpyBackend.last_path_spec == PathSpec(
            backend="s3", location="s3://bucket/fallback.csv"
        )

    def test_resolves_base_alias_before_dispatch(self) -> None:
        """Registry resolves the base alias and passes it to the backend."""
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        ctx = ResolutionContext(extra_aliases={"bucket": "my-project-bucket"})
        registry.resolve(
            {
                "path_from": {
                    "backend": "s3",
                    "base": "bucket",
                    "location": "data/key.csv",
                }
            },
            context=ctx,
        )
        assert _SpyBackend.last_base == "my-project-bucket"

    def test_resolves_dotted_base_alias(self, tmp_path: Path) -> None:
        """Registry traverses dotted aliases before dispatch."""

        class _AliasRoot:
            def __init__(self, parent: Path) -> None:
                self.parent = parent

        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        ctx = ResolutionContext(
            extra_aliases={"root": _AliasRoot(parent=tmp_path)}
        )
        registry.resolve(
            {
                "path_from": {
                    "backend": "s3",
                    "base": "root.parent",
                    "location": "key.csv",
                }
            },
            context=ctx,
        )
        assert _SpyBackend.last_base == tmp_path

    def test_create_many_from_specs(self) -> None:
        """create_many instantiates backends from a list of PathBackendSpec."""
        registry = PathResolverRegistry()
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
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        registry.configure(PathBackendSpec(name="archive", type="s3"))
        instances = registry.get_instances()
        assert "archive" in instances
        assert isinstance(instances["archive"], _SpyBackend)

    def test_resolve_spec_resolves_base_from_context_when_spec_has_base(
        self, file_registry: PathResolverRegistry, tmp_path: Path
    ) -> None:
        """``path_spec.base`` comes from context, not the ``base`` parameter."""
        ctx = ResolutionContext(source_dir=tmp_path)
        spec = PathSpec(backend="file", location="nested/out.csv", base="this")
        ref = file_registry.resolve_spec(
            spec,
            base=tmp_path / "should_not_be_used",
            context=ctx,
            allow_missing=True,
        )
        assert ref.as_local_path() == tmp_path / "nested" / "out.csv"

    def test_resolve_spec_reuses_configured_backend_instance(
        self, tmp_path: Path
    ) -> None:
        """Uses an existing named backend instance instead of ``create``."""
        registry = PathResolverRegistry()
        registry.register("file", FilesystemPathResolver)
        registry.configure(PathBackendSpec(name="file", type="file"))
        (tmp_path / "seed.csv").write_text("id\n1\n")
        spec = PathSpec(backend="file", location="seed.csv", base=None)
        ref = registry.resolve_spec(spec, base=tmp_path, allow_missing=False)
        assert ref.as_local_path() == tmp_path / "seed.csv"


class TestPathBackendRegistryErrors:
    """Tests for PathBackendRegistry error paths."""

    @pytest.fixture
    def file_registry(self) -> PathResolverRegistry:
        """Registry pre-loaded with the filesystem backend."""
        reg = PathResolverRegistry()
        reg.register("file", FilesystemPathResolver)
        return reg

    def test_invalid_input_type_raises(
        self, file_registry: PathResolverRegistry
    ) -> None:
        """Registry raises explicit domain errors for malformed inputs."""
        with pytest.raises(InvalidPathSpecError):
            file_registry.resolve(123)

    def test_absolute_path_passes_through(
        self, file_registry: PathResolverRegistry, tmp_path: Path
    ) -> None:
        """Registry resolves absolute paths embedded in structured specs."""
        spec = {
            "path_from": {
                "backend": "file",
                "location": str(tmp_path / "abs.csv"),
            }
        }
        resolved = file_registry.resolve(spec)
        assert resolved.as_local_path() == tmp_path / "abs.csv"

    def test_unknown_backend_raises(self) -> None:
        """Path registry rejects specs for unregistered backends."""
        registry = PathResolverRegistry()
        with pytest.raises(UnknownPathBackendError, match="s3"):
            registry.resolve({
                "path_from": {
                    "backend": "s3",
                    "location": "s3://bucket/key.csv",
                }
            })

    def test_empty_base_alias_raises(self) -> None:
        """Empty base alias is rejected with a specific error."""
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        ctx = ResolutionContext(extra_aliases={})

        with pytest.raises(
            InvalidPathSpecError, match=r"`path_from\.base` cannot be empty"
        ):
            registry.resolve(
                {
                    "path_from": {
                        "backend": "s3",
                        "base": "",
                        "location": "key.csv",
                    }
                },
                context=ctx,
            )

    def test_unknown_base_alias_raises(self) -> None:
        """Unknown base alias key is rejected explicitly."""
        registry = PathResolverRegistry()
        registry.register("s3", _SpyBackend)
        ctx = ResolutionContext(extra_aliases={})

        with pytest.raises(
            InvalidPathSpecError,
            match=r"`path_from\.base` unknown key: missing",
        ):
            registry.resolve(
                {
                    "path_from": {
                        "backend": "s3",
                        "base": "missing",
                        "location": "key.csv",
                    }
                },
                context=ctx,
            )


# ---------------------------------------------------------------------------
# Filesystem path backend
# ---------------------------------------------------------------------------


class TestFilesystemPathBackend:
    """Tests for the built-in FilesystemPathResolver."""

    @pytest.fixture
    def backend(self) -> FilesystemPathResolver:
        """Fresh filesystem backend instance."""
        return FilesystemPathResolver()

    def test_accepts_generic_path_spec(
        self, backend: FilesystemPathResolver
    ) -> None:
        """Filesystem backend operates on generic path specs."""
        with pytest.raises(FileNotFoundError, match="does not exist"):
            backend.resolve(PathSpec(backend="s3", location="missing.csv"))

    def test_missing_relative_path_raises(
        self,
        backend: FilesystemPathResolver,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Filesystem backend raises when relative target does not exist."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError, match="does not exist"):
            backend.resolve(PathSpec(backend="file", location="missing.csv"))

    def test_resolves_with_base_path(
        self, backend: FilesystemPathResolver, tmp_path: Path
    ) -> None:
        """Backend resolves relative paths against a pre-resolved base."""
        resolved = backend.resolve(
            PathSpec(backend="file", location=""), base=tmp_path
        )
        assert resolved.backend == "file"
        assert resolved.as_local_path() == tmp_path

    def test_resolves_relative_against_base(
        self, backend: FilesystemPathResolver, tmp_path: Path
    ) -> None:
        """Backend joins location with pre-resolved base path."""
        (tmp_path / "data.csv").write_text("id\n1\n")
        resolved = backend.resolve(
            PathSpec(backend="file", location="data.csv"), base=tmp_path
        )
        assert resolved.as_local_path() == tmp_path / "data.csv"
