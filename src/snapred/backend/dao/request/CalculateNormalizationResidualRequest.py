from pydantic import BaseModel, ConfigDict

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CalculateNormalizationResidualRequest(BaseModel):
    runNumber: int
    dataWorkspace: WorkspaceName
    calculationWorkspace: WorkspaceName

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
