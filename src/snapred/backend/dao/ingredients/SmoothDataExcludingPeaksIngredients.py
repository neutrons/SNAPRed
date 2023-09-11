from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState


class SmoothDataExcludingPeaksIngredients(BaseModel):
    smoothingParameter: Optional[float]
    instrumentState: InstrumentState
    crystalInfo: CrystallographicInfo
