"""Fixtures for test project data files."""

from pathlib import Path

import pytest


@pytest.fixture
def test_project_path() -> Path:
    """Return path to the test project directory."""
    return Path(__file__).parent / "test_project"


@pytest.fixture
def test_project_yaml(test_project_path: Path) -> Path:
    """Return path to the main project.yaml file."""
    return test_project_path / "project.yaml"


@pytest.fixture
def test_project_users_yaml(test_project_path: Path) -> Path:
    """Return path to the users table YAML file."""
    return test_project_path / "tables" / "users.yaml"


@pytest.fixture
def test_project_posts_yaml(test_project_path: Path) -> Path:
    """Return path to the posts table YAML file."""
    return test_project_path / "tables" / "posts.yaml"


@pytest.fixture
def test_project_comments_yaml(test_project_path: Path) -> Path:
    """Return path to the comments table YAML file."""
    return test_project_path / "tables" / "comments.yaml"


@pytest.fixture
def test_project_seeds_dir(test_project_path: Path) -> Path:
    """Return path to the seeds directory."""
    return test_project_path / "seeds"


@pytest.fixture
def test_project_sources_dir(test_project_path: Path) -> Path:
    """Return path to the sources directory."""
    return test_project_path / "sources"
