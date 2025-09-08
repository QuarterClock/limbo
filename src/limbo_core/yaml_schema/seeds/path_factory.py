import re
from pathlib import Path
from typing import Any, ClassVar

from limbo_core.context import Context


class PathFactory:
    """Factory for creating Path instances from raw YAML values."""

    _PREFIX_RE: ClassVar[re.Pattern] = re.compile(r"\$\{([^:}]+):([^}]+)\}")

    def __init__(self, context: Context):
        """Initialize the Path factory.

        Args:
            context: The context to use for the Path factory.
        """
        self._context = context

    def from_raw(self, raw: Any) -> Path:
        """Create a Path instance from a raw YAML value.

        Args:
            raw: The raw YAML value to create a Path instance from.

        Returns:
            The created Path instance.

        Raises:
            ValueError: If the raw YAML value is not a valid path.
            NotImplementedError: If the raw YAML value is not supported.
            FileNotFoundError: If the path does not exist.
        """
        if not isinstance(raw, str):
            raise ValueError("Raw YAML value is not a string")

        matches = list(self._PREFIX_RE.finditer(raw))
        if not matches:
            path = Path(raw)
            if path.is_absolute():
                raise ValueError(
                    "Path is absolute.Must be relative to the project root."
                )
            if not path.exists():
                raise FileNotFoundError(f"Path {path} does not exist")
            return path
        if len(matches) > 1:
            raise NotImplementedError("Multiple path prefixes found")

        prefix, content = (
            matches[0].group(1).strip().lower(),
            matches[0].group(2),
        )
        if not prefix == "path":
            raise ValueError(f"Prefix {prefix} is not supported")

        path = self._resolve_path(content)
        derivative = self._PREFIX_RE.sub("", raw).strip("/ ")
        if not derivative:
            return path
        return Path(path / derivative)

    def _resolve_path(self, content: str) -> Path:
        """Resolve a path from a content string.

        Args:
            content: The content string to resolve a path from.

        Returns:
            The resolved path.
        """
        parts = content.split(".")
        attr = self._context.paths[parts[0]]
        for part in parts[1:]:
            attr = getattr(attr, part)
        return Path(attr)
