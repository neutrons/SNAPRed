from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo


class SmoothDataExcludingPeaksIngredients(BaseModel):
    InputWorkspace: str
    InstrumentState: Calibration
    CrystalInfo: CrystallographicInfo
