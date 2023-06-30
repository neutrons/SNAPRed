from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo

class SmoothDataPeaksIngredients(BaseModel):
    # Class to hold the necessary ingrediants for smoothing data peaks calculation

    crystalInfo: CrystallographicInfo
    calibrationState: Calibration
    instrumentDefinitonFile: str
    inputWorkspace = str