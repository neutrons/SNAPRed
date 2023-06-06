from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.PixelGroupingInstrumentState import PixelGroupingInstrumentState


class InstrumentState(BaseModel):
    instrumentConfig: InstrumentConfig
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds
    pixelGroupingInstrumentState: Optional[List[PixelGroupingInstrumentState]]
    defaultGroupingSliceValue: float
    fwhmMultiplierLimit: Limit[float]
