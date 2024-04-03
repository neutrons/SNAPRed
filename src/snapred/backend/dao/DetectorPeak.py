from pydantic import BaseModel

from snapred.backend.dao.CrystallographicPeak import CrystallographicPeak
from snapred.backend.dao.Limit import LimitedValue


class DetectorPeak(BaseModel):
    """Class to hold diffraction peak position and limits in d-spacing"""

    position: LimitedValue[float]
    peak: CrystallographicPeak

    @property
    def value(self) -> float:
        return self.position.value

    @property
    def minimum(self) -> float:
        return self.position.minimum

    @property
    def maximum(self) -> float:
        return self.position.maximum
