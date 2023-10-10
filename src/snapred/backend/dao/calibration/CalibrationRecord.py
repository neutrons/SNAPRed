from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao import CrystallographicInfo
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.state import FocusGroupParameters


class CalibrationRecord(BaseModel):
    """This records the inputs used to generate a given calibration version"""

    runNumber: str
    crystalInfo: CrystallographicInfo
    calibrationFittingIngredients: Calibration
    focusGroupParameters: List[FocusGroupParameters]
    focusGroupCalibrationMetrics: FocusGroupMetric
    workspaceNames: List[str]
    version: Optional[int]
