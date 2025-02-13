from pydantic import BaseModel, ConfigDict

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CreateArtificialNormalizationRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    peakWindowClippingSize: int
    smoothingParameter: float
    decreaseParameter: bool = True
    lss: bool = True
    diffractionWorkspace: WorkspaceName
    outputWorkspace: WorkspaceName

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Allow arbitrary types like WorkspaceName
        extra="forbid",  # Forbid extra fields
        validate_assignment=True,  # Enable dynamic validation
    )
