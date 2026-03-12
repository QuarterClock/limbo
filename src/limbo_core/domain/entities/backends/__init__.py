"""Project-declared backend specification entities."""

from .backend_spec import BackendSpec
from .connection_spec import ConnectionBackendSpec
from .destination_backend_spec import DestinationBackendSpec
from .path_backend_spec import PathBackendSpec
from .value_reader_backend_spec import ValueReaderBackendSpec

__all__ = [
    "BackendSpec",
    "ConnectionBackendSpec",
    "DestinationBackendSpec",
    "PathBackendSpec",
    "ValueReaderBackendSpec",
]
