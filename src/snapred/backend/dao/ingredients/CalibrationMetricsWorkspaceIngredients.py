from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration import CalibrationRecord


class CalibrationMetricsWorkspaceIngredients(BaseModel):
    """Class to hold the ingredients necessary to generate calibration metrics workspaces."""

    calibrationRecord: CalibrationRecord
    timestamp: Optional[int]
