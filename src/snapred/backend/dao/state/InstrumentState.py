from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState, GuideState


class InstrumentState(BaseModel):
    instrumentConfig: InstrumentConfig
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds
    defaultGroupingSliceValue: float
    fwhmMultiplierLimit: Limit[float]
    peakTailCoefficient: float

    @property
    def delTh(self) -> float:
        return (
            self.instrumentConfig.delThWithGuide
            if self.detectorState.guideStat == GuideState.IN
            else self.instrumentConfig.delThNoGuide
        )
