from typing import List

from pydantic import BaseModel

from snapred.backend.dao.calibration import CalibrationRecord


class CalibrationAssessmentResponse(BaseModel):
    """

    The CalibrationAssessmentResponse class serves as a response model specifically designed
    for summarizing the outcomes of calibration assessments. It incorporates a CalibrationRecord
    to detail the calibration performed and includes a list of metricWorkspaces, which are strings
    identifying the workspaces where the calibration metrics are stored.

    """

    record: CalibrationRecord
    metricWorkspaces: List[str]
