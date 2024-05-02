from typing import Optional

from pydantic import BaseModel


class SNAPRequest(BaseModel):
    """

    `SNAPRequest` represents the structure of a request within the SNAPRed,
    containing a path indicating the target of the request and an optional
    payload that carries the request's data.

    """

    path: str
    payload: Optional[str] = None
