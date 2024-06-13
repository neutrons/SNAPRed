from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

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
        self.model_validate(self.__dict__)

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

    @model_validator(mode="after")
    def enforceStateId(self):
        # Enforce that subcomponent stateIds match _this_ object's stateId.
        if not self.calibration.instrumentState.id == self.stateId:
            raise RuntimeError(
                "the state configuration's calibration must have the same 'stateId' as the configuration: "
                + f'"{self.stateId}", not "{self.calibration.instrumentState.id}"'
            )
        if self.groupingMap:
            if not self.groupingMap.stateId == self.stateId:
                raise RuntimeError(
                    "the state configuration's grouping map must have the same 'stateId' as the configuration: "
                    + f'"{self.stateId}", not "{self.groupingMap.stateId}"'
                )
        return self

    @field_validator("stateId", mode="before")
    @classmethod
    def str_to_ObjectSHA(cls, v: str) -> str:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            v = ObjectSHA(hex=v, decodedKey=None)
        return v
