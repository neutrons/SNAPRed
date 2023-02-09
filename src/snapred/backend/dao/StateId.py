from dataclasses import dataclass

# https://docs.python.org/3/library/dataclasses.html
@dataclass
class StateId:
    vdet_arc1: float
    vdet_arc2: float
    WavelengthUserReq: float
    Frequency: int
    Pos: int

    # Round inputs to reduced number of possible states
    def __init__(self, vdet_arc1: float, vdet_arc2: float, WavelengthUserReq: float, Frequency: float, Pos: int):
        self.vdet_arc1 = round(vdet_arc1 * 2) / 2
        self.vdet_arc2 = round(vdet_arc2 * 2) / 2
        self.WavelengthUserReq = round(WavelengthUserReq, 1)
        self.Frequency = round(Frequency)
        self.Pos = Pos
