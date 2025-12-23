"""Pytest configuration and shared fixtures registration."""

from pathlib import Path


def _as_module(fixture_path: Path) -> str:
    """Convert a fixture path to a Python module path."""
    return (
        str(fixture_path)
        .replace("/", ".")
        .replace("\\", ".")
        .replace(".py", "")
    )


pytest_plugins = [
    _as_module(fixture) for fixture in Path("tests/fixtures").glob("[!_]*.py")
]
