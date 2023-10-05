from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.ingredients import FitMultiplePeaksIngredients, ReductionIngredients
from snapred.backend.dao.state import FocusGroupParameters


class CalibrationRecord(BaseModel):
    """This records the inputs used to generate a given calibration version"""

    reductionIngredients: ReductionIngredients
    calibrationFittingIngredients: FitMultiplePeaksIngredients
    focusGroupParameters: List[FocusGroupParameters]
    focusGroupCalibrationMetrics: FocusGroupMetric
    workspaceNames: List[str]
    version: Optional[int]
