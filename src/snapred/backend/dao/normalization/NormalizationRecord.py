from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.meta.mantid.WorkspaceInfo import WorkspaceInfo


class NormalizationRecord(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    calibration: Calibration
    workspaceList: List[WorkspaceInfo] = []
    version: Optional[int]
    dMin: float
