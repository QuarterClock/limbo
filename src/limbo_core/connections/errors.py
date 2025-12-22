class MissingPackageError(Exception):
    """Exception raised when a package is missing."""

    def __init__(self, package_name: str) -> None:
        """Initialize the MissingPackageError."""
        super().__init__(
            f"Package {package_name} is required for this connection type."
        )
