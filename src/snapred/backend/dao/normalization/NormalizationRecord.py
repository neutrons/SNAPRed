from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao import CrystallographicInfo
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.state import FocusGroupParameters


class NormalizationRecord(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    normalization: Normalization
    workspaceNames: List[str]
    version: Optional[int]
