"""Tests for SeedFile and PathFactory validation."""

from pathlib import Path

import pytest

from limbo_core.context import Context
from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.seeds.file import SeedFile
from limbo_core.yaml_schema.seeds.path_factory import PathFactory


class TestSeedFile:
    """Test cases for SeedFile model validation."""

    def test_resolves_prefixed_path(self, tmp_project_dir: Path) -> None:
        """Verify SeedFile resolves ${path:this} prefixed paths correctly."""
        context = Context(generators={}, paths={"this": tmp_project_dir})

        seed_file = SeedFile.model_validate(
            {"path": "${path:this}/seeds/sex.csv"}, context=context
        )
        assert seed_file.path == tmp_project_dir / "seeds" / "sex.csv"

    def test_invalid_without_context_raises(self) -> None:
        """Verify SeedFile validation fails without context."""
        with pytest.raises(ContextMissingError):
            SeedFile.model_validate({"path": "${path:this}/seeds/sex.csv"})

    def test_relative_plain_path_must_exist(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify plain relative paths are resolved from current directory."""
        context = Context(generators={}, paths={})
        monkeypatch.chdir(tmp_path)

        rel = Path("file.csv")
        rel.write_text("a,b\n")

        seed_file = SeedFile.model_validate({"path": str(rel)}, context=context)
        assert seed_file.path.name == "file.csv"


class TestPathFactory:
    """Test cases for PathFactory path resolution."""

    @pytest.fixture
    def factory(self) -> PathFactory:
        """Create a PathFactory with empty context."""
        return PathFactory(Context(generators={}, paths={}))

    @pytest.fixture
    def factory_with_paths(self, tmp_project_dir: Path) -> PathFactory:
        """Create a PathFactory with path context."""
        return PathFactory(
            Context(generators={}, paths={"this": tmp_project_dir})
        )

    def test_non_string_raises(self, factory: PathFactory) -> None:
        """Verify non-string input raises ValueError."""
        with pytest.raises(ValueError, match="Raw YAML value is not a string"):
            factory.from_raw(123)  # type: ignore[arg-type]

    def test_absolute_path_raises(
        self, factory: PathFactory, tmp_path: Path
    ) -> None:
        """Verify absolute paths are rejected."""
        abs_file = tmp_path / "abs.csv"
        abs_file.write_text("id\n1\n")

        with pytest.raises(ValueError, match="Path is absolute"):
            factory.from_raw(str(abs_file))

    def test_nonexistent_relative_path_raises(
        self,
        factory: PathFactory,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify nonexistent relative paths raise FileNotFoundError."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(FileNotFoundError):
            factory.from_raw("missing.csv")

    def test_multiple_prefixes_raises(
        self, factory_with_paths: PathFactory
    ) -> None:
        """Verify multiple path prefixes raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            factory_with_paths.from_raw("${path:this}/${path:this}")

    def test_unsupported_prefix_raises(
        self, factory_with_paths: PathFactory
    ) -> None:
        """Verify unsupported prefixes raise ValueError."""
        with pytest.raises(ValueError, match="Prefix ref is not supported"):
            factory_with_paths.from_raw("${ref:users.id}")

    def test_base_only_prefix_returns_base(
        self, factory_with_paths: PathFactory, tmp_project_dir: Path
    ) -> None:
        """Verify ${path:this} without suffix returns base path."""
        result = factory_with_paths.from_raw("${path:this}")
        assert result == tmp_project_dir

    def test_nested_attribute_resolution(
        self, factory_with_paths: PathFactory, tmp_project_dir: Path
    ) -> None:
        """Verify nested path attributes are resolved correctly."""
        result = factory_with_paths.from_raw(
            "${path:this.parent}/seeds/sex.csv"
        )
        assert result == tmp_project_dir.parent / "seeds" / "sex.csv"
