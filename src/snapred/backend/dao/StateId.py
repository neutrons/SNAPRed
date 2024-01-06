from typing import ClassVar
from dataclasses import dataclass
import hashlib
import json
from pydantic import Field

# https://docs.python.org/3/library/dataclasses.html
@dataclass
class StateId:
    vdet_arc1: float
    vdet_arc2: float
    WavelengthUserReq: float
    Frequency: int
    Pos: int

    # Round inputs to reduce number of possible states
    def __init__(self, vdet_arc1: float, vdet_arc2: float, WavelengthUserReq: float, Frequency: float, Pos: int):
        self.vdet_arc1 = float(round(vdet_arc1 * 2) / 2)
        self.vdet_arc2 = float(round(vdet_arc2 * 2) / 2)
        self.WavelengthUserReq = float(round(WavelengthUserReq, 1))
        self.Frequency = int(round(Frequency))
        self.Pos = int(Pos)
