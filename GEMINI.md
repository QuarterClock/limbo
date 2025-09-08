# Project: limbo-core

## Project Overview

`limbo-core` is a Python library for data generation. It appears to be in the
early stages of development. The core concept revolves around a `Context` that
manages data generators and paths. The library uses `pydantic` for data
modeling and validation, which suggests a focus on structured data. The project
is set up with a strong emphasis on code quality, utilizing tools like `ruff`
for linting, `mypy` for type checking, and `pytest` for testing.

## Building and Running

### Dependencies

The project uses `uv` for dependency management. To install dependencies, you
would typically run:

```bash
uv sync --dev
```

However, since this project uses `pyproject.toml`, you can install the
dependencies (including dev dependencies) with:

```bash
uv pip install -e .[dev]
```

### Running Tests

The project uses `pytest` for testing. The configuration is in `pyproject.toml`.
To run the tests, execute:

```bash
uv run pytest
```

To run tests with coverage, you can use:

```bash
uv run pytest --cov --cov-report=term-missing
```

### Linting and Type Checking

The project uses `ruff` for linting and `mypy` for type checking.

To run the linter:

```bash
uv run ruff check
```

To run the type checker:

```bash
uv run mypy
```

## Development Conventions

- **Code Style:** The project uses `ruff` to enforce a consistent code style.
  The configuration in `pyproject.toml` specifies a line length of 80
  characters and a Google-style for docstrings.
- **Testing:** The project uses `pytest`. Tests are located in the `tests`
  directory.
- **Type Hinting:** The project uses `mypy` and type hints are expected for all
  code.
- **Pre-commit Hooks:** The project uses `pre-commit` to run checks before
  committing code. The configuration is in `.pre-commit-config.yaml`.
