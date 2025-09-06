from pathlib import Path


def _as_module(fixture_path: Path) -> str:
    return (
        str(fixture_path)
        .replace("/", ".")
        .replace("\\", ".")
        .replace(".py", "")
    )


pytest_plugins = [
    _as_module(fixture) for fixture in Path("tests/fixtures").glob("[!_]*.py")
]
