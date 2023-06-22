from typing import List, Optional

from pydantic import BaseModel
from typing import List

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters


class InstrumentState(BaseModel):
    instrumentConfig: InstrumentConfig
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds
    pixelGroupingInstrumentParameters: Optional[List[PixelGroupingParameters]]
    defaultGroupingSliceValue: float
    fwhmMultiplierLimit: Limit[float]
