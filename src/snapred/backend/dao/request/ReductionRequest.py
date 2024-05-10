from typing import List, Literal, Union

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup

class ReductionRequest(BaseModel):
    runNumber: str
    useLitemode: bool
    focusGroup: List[FocusGroup]
    calibrantSamplePath: str

    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    peakIntensityThrehold: float = Config["constaants.PeakIntensityFractionThreshold"]
    version: Union[int, Literal["*"]]

