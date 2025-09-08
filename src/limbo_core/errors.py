class ContextMissingError(Exception):
    """Exception raised when a context is missing."""

    def __init__(self) -> None:
        """Initialize the ContextMissingError."""
        super().__init__("Context is missing")
