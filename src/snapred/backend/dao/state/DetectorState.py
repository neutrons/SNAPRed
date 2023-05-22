from typing import List

from pydantic import BaseModel, validator


class DetectorState(BaseModel):
    arc: List[float]
    wav: float
    freq: float
    guideStat: int
    # two additional values that don't define state, but are useful
    lin: List[float]

    @validator("arc", "lin", allow_reuse=True)
    def validate_arc(cls, v):
        if len(v) != 2:
            raise ValueError("arc and lin require 2 values each")
