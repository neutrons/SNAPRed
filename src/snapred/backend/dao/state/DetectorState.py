from typing import Tuple

from pydantic import BaseModel


class DetectorState(BaseModel):
    arc: Tuple[float, float]
    wav: float
    freq: float
    guideStat: int
    # two additional values that don't define state, but are useful
    lin: Tuple[float, float]
