from pydantic import BaseModel, ConfigDict

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CalculateResidualRequest(BaseModel):
    inputWorkspace: WorkspaceName
    outputWorkspace: WorkspaceName
    fitPeaksDiagnosticWorkspace: WorkspaceName

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
