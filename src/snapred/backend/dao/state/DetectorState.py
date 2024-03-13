from enum import IntEnum
from typing import Literal, Tuple

from pydantic import BaseModel


class GuideState(IntEnum):
    IN = 1
    OUT = 2


class DetectorState(BaseModel):
    arc: Tuple[float, float]
    wav: float
    freq: float
    guideStat: Literal[1, 2]
    # two additional values that don't define state, but are useful
    lin: Tuple[float, float]
