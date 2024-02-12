from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration


class NormalizationRecord(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    calibration: Calibration
    workspaceNames: List[str] = []
    version: Optional[int]
    dMin: float
