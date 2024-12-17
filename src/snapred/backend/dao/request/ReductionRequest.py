from typing import List, NamedTuple, Optional, Tuple

from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

Versions = NamedTuple("Versions", [("calibration", Version), ("normalization", Version)])


class ReductionRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    timestamp: Optional[float] = None
    focusGroups: List[FocusGroup] = []

    keepUnfocused: bool = False
    convertUnitsTo: str = wng.Units.DSP

    # Calibration and normalization versions:
    #   `None` => <use latest version>
    versions: Versions = Versions(VersionState.LATEST, VersionState.LATEST)

    pixelMasks: List[WorkspaceName] = []
    artificialNormalizationIngredients: Optional[ArtificialNormalizationIngredients] = None

    # TODO: Move to SNAPRequest
    continueFlags: Optional[ContinueWarning.Type] = ContinueWarning.Type.UNSET

    @field_validator("versions")
    @classmethod
    def validate_versions(cls, v) -> Versions:
        if not isinstance(v, Versions):
            if not isinstance(v, Tuple):
                raise ValueError("'versions' must be a tuple: '(<calibration version>, <normalization version>)'")
            v = Versions(v)
            if v.calibration is None:
                raise ValueError("Calibration version must be specified")
            if v.normalization is None:
                raise ValueError("Normalization version must be specified")
        return v

    model_config = ConfigDict(
        extra="forbid",
        # Required in order to use `WorkspaceName`:
        arbitrary_types_allowed=True,
    )
