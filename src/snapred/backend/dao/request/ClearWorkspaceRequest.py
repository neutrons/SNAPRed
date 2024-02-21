from typing import List

from pydantic import BaseModel


class ClearWorkspaceRequest(BaseModel):
    exclude: List[str] = []
    cache: bool = False
