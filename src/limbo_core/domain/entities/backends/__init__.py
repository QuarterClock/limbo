"""Project-declared backend specification entities."""

from .backend_spec import BackendSpec
from .connection_spec import ConnectionBackendSpec
from .path_backend_spec import PathBackendSpec
from .value_reader_backend_spec import ValueReaderBackendSpec

__all__ = [
    "BackendSpec",
    "ConnectionBackendSpec",
    "PathBackendSpec",
    "ValueReaderBackendSpec",
]
