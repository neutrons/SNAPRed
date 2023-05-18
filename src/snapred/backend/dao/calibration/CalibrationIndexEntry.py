from typing import Optional

from pydantic import BaseModel


class CalibrationIndexEntry(BaseModel):
    """Class to hold Calibration Index Entry data."""

    runNumber: str
    version: Optional[str]
    appliesTo: Optional[str]
    comments: str
    author: str
    timestamp: Optional[int]
