#!/usr/bin/env bash

source "$HOME/.local/bin.env"

uv sync --locked --group typing --all-extras
uv run mypy
