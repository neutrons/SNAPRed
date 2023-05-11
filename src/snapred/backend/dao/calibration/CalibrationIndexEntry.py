from pydantic import BaseModel


class CalibrationIndexEntry(BaseModel):
    """Class to hold Calibration Index Entry data."""

    runNumber: str
    appliesTo: str
    comments: str
    author: str
    timestamp: int
