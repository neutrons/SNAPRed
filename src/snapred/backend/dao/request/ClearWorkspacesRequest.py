from typing import List

from pydantic import BaseModel


class ClearWorkspacesRequest(BaseModel):
    # List of workspaces to retain
    exclude: List[str]

    # True => also clear cached workspaces
    clearCache: bool
