from pydantic import BaseModel, root_validator

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CreateArtificialNormalizationRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    peakWindowClippingSize: int
    smoothingParameter: float
    decreaseParameter: bool = True
    lss: bool = True
    diffractionWorkspace: WorkspaceName
    outputWorkspace: WorkspaceName = None

    @root_validator(pre=True)
    def set_output_workspace(cls, values):
        if values.get("diffractionWorkspace") and not values.get("outputWorkspace"):
            values["outputWorkspace"] = WorkspaceName(f"{values['diffractionWorkspace']}_artificialNorm")
        return values

    class Config:
        arbitrary_types_allowed = True  # Allow arbitrary types like WorkspaceName
        extra = "forbid"  # Forbid extra fields
        validate_assignment = True  # Enable dynamic validation
