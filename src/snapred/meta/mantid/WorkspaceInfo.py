from pydantic import BaseModel
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class WorkspaceInfo(BaseModel):
    """Class to hold information about a mantid workspace."""

    name: WorkspaceName
    type: str
