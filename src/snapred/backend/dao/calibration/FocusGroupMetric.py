from typing import List

from pydantic import BaseModel
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric


class FocusGroupMetric(BaseModel):
    """
    Join object that maps a list of Calibration Metrics to a focus group.
    """

    focusGroupName: str
    calibrationMetric: List[CalibrationMetric]
