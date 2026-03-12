"""Table domain entities."""

from .column import TableColumn
from .config import TableConfig
from .reference import TableReference, TableRelationship
from .table import Table

__all__ = [
    "Table",
    "TableColumn",
    "TableConfig",
    "TableReference",
    "TableRelationship",
]
