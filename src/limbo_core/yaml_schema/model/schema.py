from typing import Annotated

from pydantic import BaseModel, Field

from .table import Table


class DataGenerationSchema(BaseModel):
    """Root schema for data generation configuration."""

    version: str
    tables: Annotated[list[Table], Field(min_length=1)]
