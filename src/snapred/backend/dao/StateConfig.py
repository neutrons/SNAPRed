from typing import Any, Optional

from pydantic import BaseModel, Field, root_validator, validate_model, validator

from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.GroupingMap import GroupingMap


class StateConfig(BaseModel):
    calibration: Calibration
    vanadiumFilePath: str = ""  # Needs to be removed when Normalization changes go in

    # 'groupingMap' (not really Optional), has its own separate JSON file:
    #   * Optional: allows it to temporarily be `None` after `__init__` but prior to `attachGroupingMap`.
    groupingMap: Optional[GroupingMap] = Field(default=None, exclude=True)

    stateId: ObjectSHA  # generated.

    def _validate(self: BaseModel):
        # Manually trigger _root_validator_.
        *_, validation_error = validate_model(self.__class__, self.__dict__)
        if validation_error:
            raise validation_error

    def attachGroupingMap(self, groupingMap: GroupingMap, coerceStateId=False):
        # Attach a grouping map to the StateConfig:
        #   * optionally, adjust the GroupingMap.stateId to match that of the state.
        #
        # At present: `StateConfig` is never read or written:
        #   * this method would normally be used inside of the read method.
        if coerceStateId:
            groupingMap.coerceStateId(self.id)
        self.groupingMap = groupingMap
        self._validate()

    @root_validator(allow_reuse=True)
    def enforceStateId(cls, v):
        # Enforce that subcomponent stateIds match _this_ object's stateId.
        thisStateId = v.get("stateId")
        calibration = v.get("calibration")
        if not calibration.instrumentState.id == thisStateId:
            raise RuntimeError(
                "the state configuration's calibration must have the same 'stateId' as the configuration: "
                + f'"{thisStateId}", not "{calibration.instrumentState.id}"'
            )
        groupingMap = v.get("groupingMap")
        if groupingMap:
            if not groupingMap.stateId == thisStateId:
                raise RuntimeError(
                    "the state configuration's grouping map must have the same 'stateId' as the configuration: "
                    + f'"{thisStateId}", not "{groupingMap.stateId}"'
                )
        return v

    @validator("stateId", pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v)
        return v
