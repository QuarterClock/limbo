#!/usr/bin/env bash

source "$HOME/.local/bin.env"

uv sync --locked --group typing
uv run mypy
