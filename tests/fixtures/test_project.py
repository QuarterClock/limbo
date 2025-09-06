from pathlib import Path

import pytest


@pytest.fixture
def test_project() -> Path:
    return Path(__file__).parent / "test_project"


@pytest.fixture
def test_project_users_yaml(test_project) -> Path:
    return test_project / "tables" / "users.yaml"


@pytest.fixture
def test_project_posts_yaml(test_project) -> Path:
    return test_project / "tables" / "posts.yaml"


@pytest.fixture
def test_project_comments_yaml(test_project) -> Path:
    return test_project / "tables" / "comments.yaml"


@pytest.fixture
def test_project_project_yaml(test_project) -> Path:
    return test_project / "project.yaml"
