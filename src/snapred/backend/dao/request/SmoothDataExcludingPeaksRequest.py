from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class SmoothDataExcludingPeaksRequest(BaseModel):
    runNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    focusGroup: FocusGroup

    calibrantSamplePath: str

    inputWorkspace: str
    outputWorkspace: str

    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    crystalDMin: float = Config["constants.CrystallographicInfo.dMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.dMax"]
    peakIntensityThreshold: float = Config["constants.PeakIntensityFractionThreshold"]
