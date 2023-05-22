from pydantic import BaseModel

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState


class InstrumentState(BaseModel):
    bandwidth: float
    maxBandWidth: float
    L1: float
    L2: float
    delTOverT: float
    delLOverL: float
    delTh: float
    defaultGroupingSliceValue: float
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds
