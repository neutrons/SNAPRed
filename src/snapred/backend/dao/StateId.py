from dataclasses import dataclass

from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.DetectorState import DetectorState


# https://docs.python.org/3/library/dataclasses.html
@dataclass
class StateId:
    vdet_arc1: float
    vdet_arc2: float
    Frequency: int
    Pos: int
    WavelengthUserReq: float

    # Round inputs to reduce number of possible states
    def __init__(self, vdet_arc1: float, vdet_arc2: float, WavelengthUserReq: float, Frequency: float, Pos: int):
        self.vdet_arc1 = float(round(vdet_arc1 * 2) / 2)
        self.vdet_arc2 = float(round(vdet_arc2 * 2) / 2)
        self.WavelengthUserReq = float(round(WavelengthUserReq, 1))
        self.Frequency = int(round(Frequency))
        self.Pos = int(Pos)

    def SHA(self) -> ObjectSHA:
        return ObjectSHA.fromObject(self)

    @classmethod
    def fromDetectorState(cls, detectorState: DetectorState) -> "StateId":
        return StateId(
            vdet_arc1=detectorState.arc[0],
            vdet_arc2=detectorState.arc[1],
            WavelengthUserReq=detectorState.wav,
            Frequency=detectorState.freq,
            Pos=detectorState.guideStat,
            # TODO: these should probably be added:
            #   if they change with the runId, there will be a potential hash collision.
            # det_lin1=detectorState.lin[0],
            # det_lin2=detectorState.lin[1],
        )
