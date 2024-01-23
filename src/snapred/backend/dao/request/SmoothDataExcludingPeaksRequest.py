from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.state.FocusGroup import FocusGroup


class SmoothDataExcludingPeaksRequest(BaseModel):
    runNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    focusGroup: FocusGroup

    calibrantSamplePath: str

    inputWorkspace: str
    outputWorkspace: str
    detectorPeaks: Optional[List[GroupPeakList]]
    smoothingParameter: float
