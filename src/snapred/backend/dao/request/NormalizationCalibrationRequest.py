from pydantic import BaseModel

from snapred.backend.dao import Limit
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class NormalizationCalibrationRequest(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    focusGroup: FocusGroup
    calibrantSamplePath: str
    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    crystalDMin: float = Config["constants.CrystallographicInfo.dMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.dMax"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
