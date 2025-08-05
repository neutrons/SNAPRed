from typing import Any, List, Optional

from pydantic import BaseModel, Field

from snapred.backend.dao.Hook import Hook


class SNAPRequest(BaseModel, arbitrary_types_allowed=True):
    """

    `SNAPRequest` represents the structure of a request within the SNAPRed,
    containing a path indicating the target of the request and an optional
    payload that carries the request's data.

    """

    path: str
    payload: Optional[Any] = None
    hooks: Optional[dict[str, List[Hook]]] = Field(default=None)
