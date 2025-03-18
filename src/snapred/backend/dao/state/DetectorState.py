from enum import IntEnum
from numbers import Number
from typing import Dict, Literal, Optional, Tuple

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
    title: Optional[str] = None

    @field_validator("guideStat", mode="before")
    @classmethod
    def validate_int(cls, v):
        if isinstance(v, Number) and not isinstance(v, int):
            # accept any Number type: convert to `int`:
            #   e.g. hdf5 returns `int64`
            v = int(v)
        return v

    @classmethod
    def fromLogs(cls, logs: Dict[str, Number]):
        # NeXus/HDF5 and `mantid.api.Run` logs are time-series =>
        #   here we take only the first entry in each series.
        ds = DetectorState(
            arc=(logs["det_arc1"][0], logs["det_arc2"][0]),
            lin=(logs["det_lin1"][0], logs["det_lin2"][0]),
            wav=logs.get("BL3:Chop:Skf1:WavelengthUserReq", logs.get("BL3:Chop:Gbl:WavelengthReq"))[0],
            freq=logs["BL3:Det:TH:BL:Frequency"][0],
            guideStat=int(logs["BL3:Mot:OpticsPos:Pos"][0]),
        )
        if "title" in logs:
            raw = logs["title"]
            val = raw[0] if hasattr(raw, "__getitem__") else raw
            if hasattr(val, "decode"):
                val = val.decode("utf-8")
            ds.title = str(val)

        return ds

    def toLogs(self) -> Dict[str, Number]:
        # NeXus/HDF5 and `mantid.api.Run` logs are time-series =>
        #   here we return each entry as a tuple.
        return {
            "det_lin1": (self.lin[0],),
            "det_lin2": (self.lin[1],),
            "det_arc1": (self.arc[0],),
            "det_arc2": (self.arc[1],),
            "BL3:Chop:Skf1:WavelengthUserReq": (self.wav,),
            "BL3:Det:TH:BL:Frequency": (self.freq,),
            "BL3:Mot:OpticsPos:Pos": (self.guideStat,),
        }
