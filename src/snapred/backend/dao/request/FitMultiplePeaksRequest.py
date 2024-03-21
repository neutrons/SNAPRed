from typing import List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class FitMultiplePeaksRequest(BaseModel):
    inputWorkspace: WorkspaceName
    outputWorkspaceGroup: WorkspaceName
    detectorPeaks: List[GroupPeakList]
