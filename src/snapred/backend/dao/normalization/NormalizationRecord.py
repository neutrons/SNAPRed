from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.normalization.Normalization import Normalization


class NormalizationRecord(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    normalization: Normalization
    workspaceNames: Optional[List[str]]
    version: Optional[int]
