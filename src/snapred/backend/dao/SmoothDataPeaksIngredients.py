from pydantic import BaseModel

from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo

class SmoothDataPeaksIngredients(BaseModel):
    # Class to hold the necessary ingrediants for smoothing data peaks calculation

    crystalInfo: CrystallographicInfo
    instrumentState: InstrumentState
    peaksWorkspace = str
    weightsWorkspace = str