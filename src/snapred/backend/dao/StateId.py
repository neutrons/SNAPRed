from dataclasses import dataclass

from pydantic import Field


# https://docs.python.org/3/library/dataclasses.html
@dataclass
class StateId:
    vdet_arc1: float
    vdet_arc2: float
    WavelengthReq: float = Field(..., serialization_alias="WavelengthUserReq")
    Frequency: int
    Pos: int

    # Round inputs to reduce number of possible states
    def __init__(self, vdet_arc1: float, vdet_arc2: float, WavelengthReq: float, Frequency: float, Pos: int):
        self.vdet_arc1 = float(round(vdet_arc1 * 2) / 2)
        self.vdet_arc2 = float(round(vdet_arc2 * 2) / 2)
        self.WavelengthReq = float(round(WavelengthReq, 1))
        self.Frequency = int(round(Frequency))
        self.Pos = int(Pos)
