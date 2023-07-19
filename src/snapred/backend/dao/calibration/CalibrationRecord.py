from typing import List, Optional

from pydantic import BaseModel
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.state.FocusGroupParameters import FocusGroupParameters


class CalibrationRecord(BaseModel):
    """This records the inputs used to generate a given calibration version"""

    reductionIngredients: ReductionIngredients
    calibrationFittingIngredients: Calibration
    focusGroupParameters: List[FocusGroupParameters]
    focusGroupCalibrationMetrics: List[FocusGroupMetric]
    workspaceNames: List[str]
    version: Optional[int]
