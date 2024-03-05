from typing import List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList


class NormalizationResponse(BaseModel):
    correctedVanadium: str
    focusedVanadium: str
    smoothedVanadium: str
    detectorPeaks: List[GroupPeakList]
