from pydantic import BaseModel


class ListWorkspacesRequest(BaseModel):
    # True => exclude the cached workspaces from the list
    excludeCache: bool
