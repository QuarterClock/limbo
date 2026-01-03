"""Connection registry for storing and validating connection types."""

from typing import Annotated, Any, ClassVar, Union

from pydantic import Discriminator, TypeAdapter

from .base import Connection


class ConnectionRegistry:
    """Simple registry for connection types.

    Stores connection types registered by plugins and provides
    a TypeAdapter for Pydantic validation with discriminated unions.

    This class is primarily used internally by the plugin system.
    Connection types are registered via plugins implementing the
    `limbo_register_connections` hook.
    """

    _types: ClassVar[dict[str, type[Connection]]] = {}
    _adapter: ClassVar[TypeAdapter[Connection] | None] = None

    @classmethod
    def add(cls, connection_class: type[Connection]) -> None:
        """Add a connection type to the registry.

        Called by PluginManager when loading plugins.

        Args:
            connection_class: The connection class to add.

        Raises:
            ValueError: If the class is missing a 'type' field with a default.
        """
        from pydantic_core import PydanticUndefined

        type_field = connection_class.model_fields.get("type")
        if type_field is None:
            msg = f"{connection_class.__name__}: missing 'type' field"
            raise ValueError(msg)

        type_value = type_field.default
        if type_value is None or type_value is PydanticUndefined:
            msg = f"{connection_class.__name__}: missing 'type' field default"
            raise ValueError(msg)

        cls._types[type_value] = connection_class
        cls._adapter = None  # Invalidate cached adapter

    @classmethod
    def get_types(cls) -> dict[str, type[Connection]]:
        """Get all registered connection types.

        Returns:
            Dictionary mapping type names to connection classes.
        """
        return cls._types.copy()

    @classmethod
    def get_adapter(cls) -> TypeAdapter[Connection]:
        """Get a TypeAdapter for all registered connection types.

        Returns:
            A Pydantic TypeAdapter that can validate any registered connection.
        """
        if cls._adapter is None:
            types = tuple(cls._types.values())
            if not types:
                cls._adapter = TypeAdapter(Connection)
            elif len(types) == 1:
                cls._adapter = TypeAdapter(types[0])
            else:
                union_type = Union[types]  # type: ignore[valid-type]  # noqa: UP007
                annotated_type = Annotated[union_type, Discriminator("type")]
                cls._adapter = TypeAdapter(annotated_type)
        return cls._adapter

    @classmethod
    def validate(cls, data: Any) -> Connection:
        """Validate and create a connection instance from data.

        Args:
            data: Raw data (dict) to validate.

        Returns:
            A validated connection instance.
        """
        return cls.get_adapter().validate_python(data)

    @classmethod
    def validate_list(cls, data: list[Any]) -> list[Connection]:
        """Validate a list of connection data.

        Args:
            data: List of raw data (dicts) to validate.

        Returns:
            List of validated connection instances.
        """
        return [cls.validate(item) for item in data]

    @classmethod
    def clear(cls) -> None:
        """Clear all registered types.

        Primarily useful for testing.
        """
        cls._types.clear()
        cls._adapter = None
