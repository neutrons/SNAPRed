from typing import Any

from pydantic import BaseModel, field_validator

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

    @field_validator("position", mode="before")
    @classmethod
    def validate_position(cls, v: Any) -> LimitedValue[float]:
        if isinstance(v, dict):
            v = LimitedValue[float](**v)
        if not isinstance(v, LimitedValue[float]):
            # Coerce Generic[T]-derived type
            v = LimitedValue[float](**v.dict())
        return v
