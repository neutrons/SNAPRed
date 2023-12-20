from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.RunConfig import RunConfig


class NormalizationCalibrationRequest(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    samplePath: str
    groupingPath: str
    smoothingParameter: float
