from pathlib import Path

import pytest

from limbo_core.context import Context
from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.seeds.file import SeedFile
from limbo_core.yaml_schema.seeds.path_factory import PathFactory


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    # Create a fake project with a nested file
    project_root = tmp_path / "proj"
    seeds_dir = project_root / "seeds"
    seeds_dir.mkdir(parents=True)
    # create a file to point to
    (seeds_dir / "sex.csv").write_text("id,sex\n1,M\n2,F\n")
    return project_root


def test_seed_file_resolves_prefixed_path(tmp_project: Path) -> None:
    # Context exposes the project root path via `this`convention
    context = Context(generators={}, paths={"this": tmp_project})

    sf = SeedFile.model_validate(
        {"path": "${path:this}/seeds/sex.csv"}, context=context
    )
    assert sf.path == tmp_project / "seeds" / "sex.csv"


def test_seed_file_invalid_without_context_raises() -> None:
    with pytest.raises(ContextMissingError):
        SeedFile.model_validate({"path": "${path:this}/seeds/sex.csv"})


def test_seed_file_relative_plain_path_must_exist(
    tmp_path: Path, monkeypatch
) -> None:
    context = Context(generators={}, paths={})
    # chdir into tmp_path so relative path exists
    monkeypatch.chdir(tmp_path)
    rel = Path("file.csv")
    rel.write_text("a,b\n")
    sf = SeedFile.model_validate({"path": str(rel)}, context=context)
    assert sf.path.name == "file.csv"


def test_path_factory_non_string_raises() -> None:
    factory = PathFactory(Context(generators={}, paths={}))
    with pytest.raises(ValueError, match="Raw YAML value is not a string"):
        factory.from_raw(123)  # type: ignore[arg-type]


def test_path_factory_absolute_path_raises(tmp_path: Path) -> None:
    abs_file = tmp_path / "abs.csv"
    abs_file.write_text("id\n1\n")
    factory = PathFactory(Context(generators={}, paths={}))
    with pytest.raises(ValueError, match="Path is absolute"):
        factory.from_raw(str(abs_file))


def test_path_factory_nonexistent_relative_path_raises(
    tmp_path: Path, monkeypatch
) -> None:
    factory = PathFactory(Context(generators={}, paths={}))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        factory.from_raw("missing.csv")


def test_path_factory_multiple_prefixes_raises(tmp_project: Path) -> None:
    factory = PathFactory(Context(generators={}, paths={"this": tmp_project}))
    with pytest.raises(NotImplementedError):
        factory.from_raw("${path:this}/${path:this}")


def test_path_factory_unsupported_prefix_raises(tmp_project: Path) -> None:
    factory = PathFactory(Context(generators={}, paths={"this": tmp_project}))
    with pytest.raises(ValueError, match="Prefix ref is not supported"):
        factory.from_raw("${ref:users.id}")


def test_path_factory_base_only_prefix_returns_base(tmp_project: Path) -> None:
    factory = PathFactory(Context(generators={}, paths={"this": tmp_project}))
    result = factory.from_raw("${path:this}")
    assert result == tmp_project


def test_path_factory_nested_attribute_resolution(tmp_project: Path) -> None:
    # Using Path attribute 'parent' to resolve nested content
    factory = PathFactory(Context(generators={}, paths={"this": tmp_project}))
    result = factory.from_raw("${path:this.parent}/seeds/sex.csv")
    assert result == tmp_project.parent / "seeds" / "sex.csv"
