from typing import List

from pydantic import BaseModel
from snapred.backend.dao.calibration import CalibrationRecord


class CalibrationAssessmentResponse(BaseModel):
    """Response model for the calibration assessment"""

    record: CalibrationRecord
    metricWorkspaces: List[str]
