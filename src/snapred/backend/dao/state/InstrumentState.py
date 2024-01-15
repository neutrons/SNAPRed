from typing import Any, List, Optional

from pydantic import BaseModel, validator

from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState, GuideState
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.StateId import StateId
from snapred.backend.error.StateValidationException import StateValidationException


class InstrumentState(BaseModel):
    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this InstrumentState's file is at its expected location (e.g. it hasn't been moved);
    # * verify that nested JSON components that are in separate files are at their expected locations.
    id: ObjectSHA

    instrumentConfig: InstrumentConfig
    detectorState: DetectorState
    gsasParameters: GSASParameters
    particleBounds: ParticleBounds

    # 'groupingMap' (not really Optional), has its own separate JSON file
    groupingMap: Optional[GroupingMap] = Field(default=None, exclude=True)

    defaultGroupingSliceValue: float
    fwhmMultiplierLimit: Limit[float]
    peakTailCoefficient: float
    # TODO this will be completely removed in an upcoming PR.
    # For the moment it is required by DetectorPeakPredictor.
    # Future PR will introduce one set of PeakIngredients for all
    # of the various peaks-related algorithms, which will make
    # this pixelGroup unneeded.
    pixelGroup: Optional[PixelGroup]

    def _validate(self: BaseModel):
        # Manually trigger _root_validator_.
        *_, validation_error = validate_model(self.__class__, self.__dict__)
        if validation_error:
            raise validation_error

    def attachGroupingMap(self, groupingMap: GroupingMap, coerceStateId=False):
        # Attach a grouping map to the InstrumentState:
        #   * optionally, adjust the GroupingMap.stateId to match that of the state.
        if coerceStateId:
            groupingMap.coerceStateId(self.id)
        self.groupingMap = groupingMap
        self._validate()

    @property
    def delTh(self) -> float:
        return (
            self.instrumentConfig.delThWithGuide
            if self.detectorState.guideStat == GuideState.IN
            else self.instrumentConfig.delThNoGuide
        )

    @root_validator(allow_reuse=True)
    def enforceStateId(cls, v):
        # Enforce that subcomponent stateIds match _this) state's id.
        thisStateId = v.get("id")
        groupingMap = v.get("groupingMap")
        if groupingMap:
            if not groupingMap.stateId == thisStateId:
                raise RuntimeError(
                    f'a state\'s grouping map must have the same \'stateId\' as the state: "{thisStateId}", not "{groupingMap.stateId}".'
                )
        return v

    @validator("id", pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v


