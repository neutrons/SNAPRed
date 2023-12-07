from enum import IntEnum
from typing import Literal, Tuple

from pydantic import BaseModel

# class syntax


class GuideState(IntEnum):
    IN = 1
    OUT = 2


class DetectorState(BaseModel):
    arc: Tuple[float, float]
    wav: float
    freq: float
    # Expected to only be 0 or 1, potentially should be a bool
    guideStat: Literal[GuideState.IN, GuideState.OUT]
    # two additional values that don't define state, but are useful
    lin: Tuple[float, float]
