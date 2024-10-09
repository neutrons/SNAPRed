from pydantic import BaseModel

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CreateArtificialNormalizationRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    peakWindowClippingSize: int
    smoothingParameter: float
    decreaseParameter: bool = True
    lss: bool = True
    diffractionWorkspace: WorkspaceName
