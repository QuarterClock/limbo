"""Table reference and relationship entities."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class TableRelationship(StrEnum):
    """Table relationship."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


@dataclass(slots=True)
class TableReference:
    """Reference to another table."""

    type: Literal["table", "seed", "source"]
    name: str
    relationship: TableRelationship
