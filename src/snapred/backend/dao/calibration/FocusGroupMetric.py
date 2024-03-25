from typing import List

from pydantic import BaseModel

from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state.FocusGroup import FocusGroup


class FocusGroupMetric(BaseModel):
    """

    The FocusGroupMetric class, built with Pydantic, links a specific FocusGroup with a
    collection of CalibrationMetric instances, facilitating a structured association
    between focus groups and their corresponding calibration metrics. It features a
    focusGroupName to identify the focus group, along with calibrationMetric, a list
    that encapsulates multiple CalibrationMetric objects. This design efficiently organizes
    calibration metrics by focus group, enhancing the analysis and comparison of calibration
    quality within specific groupings or contexts in scientific data processing.

    """

    focusGroupName: str
    calibrationMetric: List[CalibrationMetric]
