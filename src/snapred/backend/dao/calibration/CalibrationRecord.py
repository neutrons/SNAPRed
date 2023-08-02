from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao import ReductionIngredients


class CalibrationRecord(BaseModel):
    """Class to hold Calibration Record data."""

    parameters: ReductionIngredients
    peakFittingFilepath: Optional[str]
    version: Optional[int]
