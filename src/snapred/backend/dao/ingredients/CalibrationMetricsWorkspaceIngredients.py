from typing import Any, Optional

from pydantic import BaseModel, model_validator

from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.indexing.Versioning import Version


class CalibrationMetricsWorkspaceIngredients(BaseModel):
    """

    The CalibrationMetricsWorkspaceIngredients class is designed to encapsulate the essential components
    required for generating workspaces dedicated to calibration metrics. It includes a calibrationRecord
    to reference the specific calibration data being analyzed and an optional timestamp to mark the time
    of workspace generation.

    """

    runNumber: int
    version: Version
    focusGroupCalibrationMetrics: FocusGroupMetric
    timestamp: Optional[float] = None

    @model_validator(mode="before")
    @classmethod
    def validate_timestamp(cls, v: Any):
        if isinstance(v, dict):
            if "timestamp" in v:
                timestamp = v["timestamp"]
                # support reading the _legacy_ timestamp integer encoding
                if isinstance(timestamp, int):
                    v["timestamp"] = float(timestamp) / 1000.0
        return v
