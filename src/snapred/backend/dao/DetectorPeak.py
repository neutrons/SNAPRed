from pydantic import BaseModel


class DetectorPeak(BaseModel):
    """Class to hold diffraction peak position and limits in d-spacing"""

    position: float
    limLeft: float
    limRight: float
