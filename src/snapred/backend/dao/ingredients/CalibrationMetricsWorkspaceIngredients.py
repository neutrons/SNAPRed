from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration import CalibrationRecord


class CalibrationMetricsWorkspaceIngredients(BaseModel):
    """

    The CalibrationMetricsWorkspaceIngredients class is designed to encapsulate the essential components
    required for generating workspaces dedicated to calibration metrics. It includes a calibrationRecord
    to reference the specific calibration data being analyzed and an optional timestamp to mark the time
    of workspace generation.

    """

    calibrationRecord: CalibrationRecord
    timestamp: Optional[int]
