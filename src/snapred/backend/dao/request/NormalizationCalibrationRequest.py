from pydantic import BaseModel

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.RunConfig import RunConfig


class NormalizationCalibrationRequest(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    samplePath: str
    groupingPath: str
    smoothingParameter: float
    dMin: float
