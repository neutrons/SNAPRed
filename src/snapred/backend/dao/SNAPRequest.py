from typing import Any, Callable, Optional

from pydantic import BaseModel, Field


class SNAPRequest(BaseModel, arbitrary_types_allowed=True):
    """

    `SNAPRequest` represents the structure of a request within the SNAPRed,
    containing a path indicating the target of the request and an optional
    payload that carries the request's data.

    """

    path: str
    payload: Optional[Any] = None
    hooks: Optional[dict[str, Callable]] = Field(exclude=True)
