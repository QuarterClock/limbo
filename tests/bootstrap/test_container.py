"""Tests for bootstrap container wiring."""

from __future__ import annotations

from limbo_core.bootstrap import Container, get_container


class TestContainer:
    """Tests for the bootstrap dependency container."""

    def test_get_container_returns_singleton_instance(self) -> None:
        """Ensure bootstrap returns the same default container."""
        first = get_container()
        second = get_container()
        assert first is second

    def test_load_project_delegates_to_loader_service(self) -> None:
        """Container.load_project returns parsed project from wired service."""
        payload = {
            "connections": [],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "integer",
                            "generator": "primary_key.incrementing_id",
                        }
                    ],
                    "config": {},
                }
            ],
            "seeds": [
                {
                    "name": "seed_users",
                    "columns": [{"name": "id", "data_type": "integer"}],
                    "seed_file": {
                        "path": "tests/fixtures/test_project/seeds/sex.csv"
                    },
                    "config": {},
                }
            ],
            "sources": [
                {
                    "name": "source_users",
                    "columns": [{"name": "id", "data_type": "integer"}],
                    "config": {"connection": "main"},
                }
            ],
        }
        project = Container().load_project(payload)
        assert project.tables[0].name == "users"
