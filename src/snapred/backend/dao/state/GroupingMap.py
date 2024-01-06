from typing import List, Optional, Any, ClassVar

from pydantic import BaseModel, Field, validator, root_validator
from pydantic.error_wrappers import ValidationError

from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.StateId import StateId
from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.DetectorState import DetectorState, GuideState
from snapred.backend.dao.state.PixelGroup import PixelGroup


class GroupingMap(BaseModel):
    # This class is a "placeholder" for the moment...
    
    # State id associated with the _default_ grouping map, at 'calibration.grouping.home':
    defaultStateId: ClassVar[ObjectSHA] = ObjectSHA(hex='aabbbcccdddeeeff', decodedKey=None)
    
    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this GroupingMap's file is at its expected location (e.g. it hasn't been moved or copied);
    stateId: ObjectSHA

    _isDirty: bool = Field(default=False, kw_only=True)
    
    def isDefault(self):
        return self.id == GroupingMap.defaultId

    def isDirty(self) -> bool:
        return self._isDirty
    
    def setDirty(self, flag: bool):
        self._isDirty = flag

    def cloneWithNewStateId(self, stateId: ObjectSHA) -> 'GroupingMap':
        return type(self)(\
            stateId = stateId,
            _isDirty = True,\
            # other fields go here...
        )
            
    @validator('stateId', pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v
