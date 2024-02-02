from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.meta.mantid.WorkspaceInfo import WorkspaceInfo


class NormalizationRecord(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    normalization: Normalization
    workspaceList: Optional[List[WorkspaceInfo]]
    version: Optional[int]
