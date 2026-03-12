"""Project loading orchestration service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from limbo_core.validation import require_mapping

if TYPE_CHECKING:
    from limbo_core.application.context import ResolutionContext, RuntimeContext
    from limbo_core.application.interfaces import PluginLoader
    from limbo_core.application.parsers import ProjectParser
    from limbo_core.application.services.project_validator import (
        ProjectValidatorService,
    )
    from limbo_core.domain.entities import Project


@dataclass(slots=True)
class ProjectLoaderService:
    """Orchestrate plugin loading and project validation."""

    plugin_loader: PluginLoader
    parser: ProjectParser
    validator: ProjectValidatorService

    def load(
        self,
        raw_project: dict[str, Any],
        *,
        context: RuntimeContext | None = None,
        resolution_context: ResolutionContext | None = None,
    ) -> Project:
        """Load plugins and validate project payload.

        Returns:
            Parsed project object validated against runtime context.
        """
        payload = require_mapping(raw_project, model_name="Project")
        self.plugin_loader.load_plugins()
        project = self.parser.parse(payload)
        if context is None:
            return project
        return self.validator.validate(
            project, context=context, resolution_context=resolution_context
        )
