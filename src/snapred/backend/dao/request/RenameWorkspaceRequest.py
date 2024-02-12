from pydantic import BaseModel


class RenameWorkspaceRequest(BaseModel):
    oldName: str
    newName: str
