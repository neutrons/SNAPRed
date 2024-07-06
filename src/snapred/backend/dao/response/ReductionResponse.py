from typing import List

from pydantic import BaseModel, ConfigDict

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ReductionResponse(BaseModel):
    workspaces: List[WorkspaceName]

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
