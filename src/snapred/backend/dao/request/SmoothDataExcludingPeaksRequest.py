from typing import List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.meta.Config import Config


class SmoothDataExcludingPeaksRequest(BaseModel):
    inputWorkspace: str
    outputWorkspace: str
    detectorPeaks: List[GroupPeakList]
    smoothingParameter: float
