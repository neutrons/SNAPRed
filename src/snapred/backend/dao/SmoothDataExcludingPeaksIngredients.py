from pydantic import BaseModel

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState
class SmoothDataExcludingPeaksIngredients(BaseModel):

    InstrumentState: InstrumentState
    CrystalInfo: CrystallographicInfo
