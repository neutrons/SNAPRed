from pydantic import BaseModel

from snapred.backend.dao.Limit import LimitedValue


class DetectorPeak(BaseModel):
    """Class to hold diffraction peak position and limits in d-spacing"""

    position: LimitedValue[float]
