from typing import Tuple

from pydantic import BaseModel


class DetectorState(BaseModel):
    arc: Tuple[float, float]
    wav: float
    freq: float
    # Expected to only be 0 or 1, potentially should be a bool
    guideStat: int
    # two additional values that don't define state, but are useful
    lin: Tuple[float, float]
