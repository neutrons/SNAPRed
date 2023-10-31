from typing import List, Optional

from pydantic import BaseModel
from snapred.backend.dao import CrystallographicInfo
from snapred.backend.dao.normalization.Normalization import Normalization


class NormalizationRecord(BaseModel):
    runNumber: str
    crystalInfo: CrystallographicInfo
    normalization: Normalization
    workspaceNames: List[str]
    version: Optional[int]
