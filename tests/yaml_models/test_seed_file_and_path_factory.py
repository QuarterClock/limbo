from pathlib import Path

import pytest

from limbo_core.context import Context
from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.seeds.file import SeedFile


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
