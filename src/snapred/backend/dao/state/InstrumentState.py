from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState, GuideState
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup


class InstrumentState(BaseModel):
    instrumentConfig: InstrumentConfig
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds
    defaultGroupingSliceValue: float
    fwhmMultiplierLimit: Limit[float]
    peakTailCoefficient: float
    # TODO this will be completely removed in an upcoming PR.
    # For the moment it is required by DetectorPeakPredictor.
    # Future PR will introduce one set of PeakIngredients for all
    # of the various peaks-related algorithms, which will make
    # this pixelGroup unneeded.
    pixelGroup: Optional[PixelGroup]

    @property
    def delTh(self) -> float:
        return (
            self.instrumentConfig.delThWithGuide
            if self.detectorState.guideStat == GuideState.IN
            else self.instrumentConfig.delThNoGuide
        )
