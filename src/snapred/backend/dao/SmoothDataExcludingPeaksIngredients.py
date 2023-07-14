from pydantic import BaseModel

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.calibration.Calibration import Calibration

class SmoothDataExcludingPeaksIngredients(BaseModel):

    InputWorkspace: str
    InstrumentState: Calibration
    CrystalInfo: CrystallographicInfo