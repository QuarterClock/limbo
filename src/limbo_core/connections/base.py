from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class Connection(BaseModel, ABC):
    """Abstract base class for all database connections."""

    type: str
    name: str

    @abstractmethod
    def connect(self) -> Any:
        """Establishes the connection."""
