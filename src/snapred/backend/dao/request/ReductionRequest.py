import datetime
from typing import List, NamedTuple, Optional, Tuple
from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

Versions = NamedTuple("Versions", [("calibration", Optional[int]), ("normalization", Optional[int])])


class ReductionRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    liveDataMode: Optional[bool] = False
    liveDataDuration: Optional[datetime.timedelta] = None
    timestamp: Optional[float] = None
    focusGroups: List[FocusGroup] = []

    keepUnfocused: bool = False
    convertUnitsTo: str = wng.Units.DSP

    # Calibration and normalization versions:
    #   `None` => <use latest version>
    versions: Versions = Versions(None, None)

    pixelMasks: List[WorkspaceName] = []
    artificialNormalizationIngredients: Optional[ArtificialNormalizationIngredients] = None

    # Live-data info (these should only be the corresponding live-data values from the 'application.yml'):
    #   'facility' and 'instrument' have a special meaning when the live-data listener
    #   is mocked out for testing.
    facility: str = Config["liveData.facility.name"] 
    instrument: str = Config["liveData.instrument.name"]
    duration: int = 1 # minimum duration is 1 second
    
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
