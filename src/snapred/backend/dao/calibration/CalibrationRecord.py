from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao import ReductionIngredients
from snapred.backend.dao.calibration import Calibration, FocusGroupMetric
from snapred.backend.dao.state import FocusGroupParameters


class CalibrationRecord(BaseModel):
    """This records the inputs used to generate a given calibration version"""

    reductionIngredients: ReductionIngredients
    calibrationFittingIngredients: Calibration
    focusGroupParameters: List[FocusGroupParameters]
    focusGroupCalibrationMetrics: List[FocusGroupMetric]
    workspaceNames: List[str]
    version: Optional[int]
