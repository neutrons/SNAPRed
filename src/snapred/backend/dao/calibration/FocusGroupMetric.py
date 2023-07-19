from typing import List

from pydantic import BaseModel
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric


class FocusGroupMetric(BaseModel):
    focusGroupName: str
    calibrationMetric: List[CalibrationMetric]
