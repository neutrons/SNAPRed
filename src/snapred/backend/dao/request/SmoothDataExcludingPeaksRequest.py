from pydantic import BaseModel

from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.state.FocusGroup import FocusGroup


class SmoothDataExcludingPeaksRequest(BaseModel):
    runNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    focusGroup: FocusGroup

    calibrantSamplePath: str

    inputWorkspace: str
    outputWorkspace: str

    smoothingParameter: float
    dMin: float
