from enum import IntEnum
from numbers import Number
from typing import Dict, Literal, Tuple

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

    @classmethod
    def constructFromLogValues(cls, logValues):
        return DetectorState(
            arc=(float(logValues["det_arc1"]), float(logValues["det_arc2"])),
            lin=(float(logValues["det_lin1"]), float(logValues["det_lin2"])),
            wav=float(logValues["BL3:Chop:Skf1:WavelengthUserReq"]),
            freq=float(logValues["BL3:Det:TH:BL:Frequency"]),
            guideStat=int(logValues["BL3:Mot:OpticsPos:Pos"]),
        )

    def getLogValues(self) -> Dict[str, str]:
        return {
            "det_lin1": str(self.lin[0]),
            "det_lin2": str(self.lin[1]),
            "det_arc1": str(self.arc[0]),
            "det_arc2": str(self.arc[1]),
            "BL3:Chop:Skf1:WavelengthUserReq": str(self.wav),
            "BL3:Det:TH:BL:Frequency": str(self.freq),
            "BL3:Mot:OpticsPos:Pos": str(self.guideStat),
        }
