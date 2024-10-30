from typing import List, NamedTuple, Optional, Tuple

from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

Versions = NamedTuple("Versions", [("calibration", Optional[int]), ("normalization", Optional[int])])


class ReductionRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    timestamp: Optional[float] = None
    focusGroups: List[FocusGroup] = []

    keepUnfocused: bool = False
    convertUnitsTo: str = None

    # Calibration and normalization versions:
    #   `None` => <use latest version>
    versions: Versions = Versions(None, None)

    pixelMasks: List[WorkspaceName] = []
    artificialNormalization: Optional[str] = None

    # TODO: Move to SNAPRequest
    continueFlags: Optional[ContinueWarning.Type] = ContinueWarning.Type.UNSET

    @field_validator("versions")
    @classmethod
    def validate_versions(cls, v) -> Versions:
        if not isinstance(v, Versions):
            if not isinstance(v, Tuple):
                raise ValueError("'versions' must be a tuple: '(<calibration version>, <normalization version>)'")
            v = Versions(v)
        return v

    model_config = ConfigDict(
        extra="forbid",
        # Required in order to use `WorkspaceName`:
        arbitrary_types_allowed=True,
    )
