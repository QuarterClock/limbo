from pathlib import Path

import yaml

from limbo_core.yaml_schema import (
    Context,
    DataGenerationSchema,
    PrimitiveValue,
    ReferenceValue,
)


def _mock_context() -> Context:
    return Context(
        generators={
            # common
            "primary_key.incrementing_id": object(),
            "datetime.random": object(),
            # users
            "pii.full_name": object(),
            "pii.email": object(),
            "pii.password": object(),
            # posts/comments
            "foreign_key.key": object(),
            "foreign_key.value": object(),
            "llm.generate": object(),
            "text.text": object(),
        }
    )


def _load_schema(yaml_file: Path) -> DataGenerationSchema:
    data = yaml.safe_load(yaml_file.read_text())
    return DataGenerationSchema.model_validate(data, context=_mock_context())


def _get_column(table, name: str):
    return next(col for col in table.columns if col.name == name)


def test_users_yaml_options(tmp_path: Path):
    yaml_path = Path(__file__).parent / "test_project" / "models" / "users.yaml"
    schema = _load_schema(yaml_path)
    table = schema.tables[0]

    created_at = _get_column(table, "created_at")
    assert isinstance(created_at.options["start_date"], PrimitiveValue)
    assert created_at.options["start_date"].value == "2020-01-01"
    assert isinstance(created_at.options["end_date"], PrimitiveValue)
    assert created_at.options["end_date"].value == "2025-01-01"

    updated_at = _get_column(table, "updated_at")
    assert isinstance(updated_at.options["start_date"], PrimitiveValue)
    assert updated_at.options["start_date"].value == "2020-01-01"
    assert isinstance(updated_at.options["end_date"], PrimitiveValue)
    assert updated_at.options["end_date"].value == "2025-01-01"


def test_posts_yaml_options(tmp_path: Path):
    yaml_path = Path(__file__).parent / "test_project" / "models" / "posts.yaml"
    schema = _load_schema(yaml_path)
    table = schema.tables[0]

    user_id = _get_column(table, "user_id")
    assert isinstance(user_id.options["reference"], ReferenceValue)
    assert user_id.options["reference"].ref == "users_data.id"

    title = _get_column(table, "title")
    assert isinstance(title.options["prompt"], PrimitiveValue)
    assert "Generate a title" in title.options["prompt"].value

    created_at = _get_column(table, "created_at")
    assert isinstance(created_at.options["start_date"], ReferenceValue)
    assert created_at.options["start_date"].ref == "users_data.created_at"
    assert isinstance(created_at.options["end_date"], PrimitiveValue)
    assert created_at.options["end_date"].value == "2025-01-01"

    updated_at = _get_column(table, "updated_at")
    assert isinstance(updated_at.options["start_date"], ReferenceValue)
    assert updated_at.options["start_date"].ref == "this.created_at"
    assert isinstance(updated_at.options["end_date"], PrimitiveValue)
    assert updated_at.options["end_date"].value == "2025-01-01"


def test_comments_yaml_options(tmp_path: Path):
    yaml_path = (
        Path(__file__).parent / "test_project" / "models" / "comments.yaml"
    )
    schema = _load_schema(yaml_path)
    table = schema.tables[0]

    post_id = _get_column(table, "post_id")
    assert isinstance(post_id.options["reference"], ReferenceValue)
    assert post_id.options["reference"].ref == "posts_data.id"

    user_id = _get_column(table, "user_id")
    assert isinstance(user_id.options["reference"], ReferenceValue)
    assert user_id.options["reference"].ref == "users_data.id"

    user_full_name = _get_column(table, "user_full_name")
    assert isinstance(user_full_name.options["reference"], ReferenceValue)
    assert user_full_name.options["reference"].ref == "users_data.full_name"

    created_at = _get_column(table, "created_at")
    assert isinstance(created_at.options["start_date"], ReferenceValue)
    assert created_at.options["start_date"].ref == "posts_data.created_at"
    assert isinstance(created_at.options["end_date"], PrimitiveValue)
    assert created_at.options["end_date"].value == "2025-01-01"
