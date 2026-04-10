from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DatasetInfo(BaseModel):
    """A dataset available for querying."""

    name: str
    source: str
    description: str = ""


class FieldInfo(BaseModel):
    """A single field in a dataset schema."""

    name: str
    type: str


class DatasetSchema(BaseModel):
    """Schema for a dataset: field names and types."""

    dataset: str
    fields: list[FieldInfo]


class QueryInput(BaseModel):
    """Validated input for an APL query."""

    dataset: str = Field(min_length=1)
    apl: str = Field(min_length=1)
    cursor: str | None = None


class QueryResult(BaseModel):
    """Result of an APL query."""

    columns: list[str]
    rows: list[list[Any]]
    cursor: str | None = None
    has_more: bool = False
