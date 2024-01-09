from pydantic import BaseModel

from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients


class SmoothDataExcludingPeaksRequest(BaseModel):
    inputWorkspace: str
    outputWorkspace: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    groupingPath: str
    samplePath: str
    runNumber: str
    smoothingParameter: float
    dMin: float
