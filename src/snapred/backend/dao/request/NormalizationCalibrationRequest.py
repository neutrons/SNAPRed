from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.RunConfig import RunConfig


class NormalizationCalibrationRequest(BaseModel):
    runNumber: RunConfig
    backgroundRunNumber: RunConfig
    samplePath: str
    groupingFiles: list[str]
    calibrantPath: str
    smoothingParameter: Optional[float]
