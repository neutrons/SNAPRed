from collections.abc import Mapping
from enum import IntEnum
import h5py
from numbers import Number
from typing import Literal, Tuple
from pydantic import BaseModel, field_validator


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
            
    @field_validator("guideStat", mode="before")
    @classmethod
    def validate_int(cls, v):
        if isinstance(v, Number) and not isinstance(v, int):
            # accept any Number type: convert to `int`:
            #   e.g. hdf5 returns `int64`
            v = int(v)
        return v
