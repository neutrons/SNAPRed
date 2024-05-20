from typing import List, Literal, Optional, Union

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class ReductionRequest(BaseModel):
    runNumber: Union[str, List[str]]
    useLiteMode: bool
    focusGroup: Union[Optional[FocusGroup], List[FocusGroup]]
    calibrantSamplePath: str
    userSelectedMaskPath: Optional[str]
    peakIntensityThreshold: float = Config["constants.PeakIntensityFractionThreshold"]
    version: Union[int, Literal["*"]] = "*"
