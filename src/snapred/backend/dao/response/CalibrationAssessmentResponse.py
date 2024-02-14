from typing import List

from pydantic import BaseModel
from snapred.backend.dao.calibration import CalibrationRecord
from snapred.meta.mantid.WorkspaceInfo import WorkspaceInfo


class CalibrationAssessmentResponse(BaseModel):
    """Response model for the calibration assessment"""

    record: CalibrationRecord
    metricWorkspaces: List[WorkspaceInfo]
