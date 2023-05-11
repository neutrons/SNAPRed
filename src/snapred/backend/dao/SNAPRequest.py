from typing import Optional

from pydantic import BaseModel


class SNAPRequest(BaseModel):
    """"""

    path: str
    payload: Optional[str]
