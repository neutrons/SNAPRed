# For the purposes of the state-id transition: bypass normal SNAPRed logging.
import logging

from typing import Any, List, Optional, ClassVar

from pydantic import BaseModel, Field, validator, root_validator

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class InstrumentState(BaseModel):
    # State id associated with the state-id transition period:
    transitionStateId: ClassVar[ObjectSHA] = ObjectSHA(hex="0101010101010101", decodedKey=None)
    
    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this InstrumentState's file is at its expected location (e.g. it hasn't been moved);
    # * verify that any nested JSON components that are in separate files are at their expected locations.
    id: Optional[ObjectSHA] = Field(default=ObjectSHA(hex="0101010101010101", decodedKey=None))

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

    @root_validator(allow_reuse=True)
    def stateIdTransition(cls, v):
        # Add an 'id' field, if necessary, to trigger
        #   "fixup" logging messages for existing data files without state id.
        thisStateId = v.get("id")
        if thisStateId == cls.transitionStateId.hex:
            # a valid 16-character hex digest,
            #   gauranteed to otherwise _fail_:
            logger.warning(\
f"""
The instrumentStates\'s \'id\' field is not present in the JSON file.
    This field is now _mandatory_.
A temporary id has been inserted: \'{cls.transitionStateId.hex}\'.
This is likely to _fail_ later in the validation chain.
"""
            )
 
        return v

    @validator("id", pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v
