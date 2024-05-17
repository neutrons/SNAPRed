from typing import List, Literal, Optional, Union

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config

class ReductionRequest(BaseModel):
    runNumber: str
    useLitemode: bool
    focusGroups: Union[Optional[FocusGroup], List[FocusGroup]]
    calibrantSamplePath: str

    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    peakIntensityThrehold: float = Config["constants.PeakIntensityFractionThreshold"]
    version: Union[int, Literal["*"]] = "*"

