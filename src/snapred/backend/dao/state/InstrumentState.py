from typing import List, Optional

from pydantic import BaseModel, root_validator

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState, GuideState
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config, Resource

logger = snapredLogger.getLogger(__name__)


class InstrumentState(BaseModel):
    instrumentConfig: InstrumentConfig
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds
    pixelGroup: Optional[PixelGroup]
    defaultGroupingSliceValue: float
    fwhmMultiplierLimit: Limit[float]
    peakTailCoefficient: float
    stateId: str
    groupingMap: GroupingMap

    @property
    def delTh(self) -> float:
        return (
            self.instrumentConfig.delThWithGuide
            if self.detectorState.guideStat == GuideState.IN
            else self.instrumentConfig.delThNoGuide
        )

    @root_validator(pre=False, allow_reuse=True)
    def validate_stateId(cls, v):
        stateId = v.get("stateId")
        groupingMap = v.get("groupinMap")
        if stateId != groupingMap.stateID:
            groupingMap = GroupingMap.parse_raw(
                Resource.read(Config["calibration.grouping.home"] + "/GroupingMap.json")
            )
            logger.warning("InstrumentSate stateId and GroupingMap stateId do not match, default GroupingMap loaded")
            return v
