"""String interpolation utilities for YAML values."""

import os
import re
from typing import ClassVar


class EnvInterpolator:
    """Interpolates environment variables in strings.

    Supports the ${env:VAR_NAME} syntax for environment variable interpolation.
    Also supports ${env:VAR_NAME:-default} for default values.
    """

    _ENV_RE: ClassVar[re.Pattern[str]] = re.compile(
        r"\$\{env:([^}:]+)(?::-([^}]*))?\}"
    )

    @classmethod
    def interpolate(cls, value: str) -> str:
        """Interpolate environment variables in a string.

        Args:
            value: The string to interpolate.

        Returns:
            The interpolated string.
        """

        def replace_match(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default = match.group(2)
            env_value = os.environ.get(var_name)

            if env_value is not None:
                return env_value
            if default is not None:
                return default
            raise ValueError(
                f"Environment variable '{var_name}' is not set "
                f"and no default provided"
            )

        return cls._ENV_RE.sub(replace_match, value)

    @classmethod
    def has_env_vars(cls, value: str) -> bool:
        """Check if a string contains environment variable references.

        Args:
            value: The string to check.

        Returns:
            True if the string contains environment variable references.
        """
        return bool(cls._ENV_RE.search(value))
