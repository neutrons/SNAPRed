import logging
from pathlib import Path
from typing import Any, ClassVar, Dict, List

from pydantic import BaseModel, Field, root_validator, validator

from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

# logger = snapredLogger.getLogger(__name__)


class GroupingMap(BaseModel):
    # State id associated with the _default_ grouping map, at 'instrument.calibration.powder.grouping.home':
    defaultStateId: ClassVar[ObjectSHA] = ObjectSHA(hex="aabbbcccdddeeeff", decodedKey=None)

    @classmethod
    def calibrationGroupingHome(cls) -> Path:
        return Path(Config["instrument.calibration.powder.grouping.home"])

    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this GroupingMap's file is at its expected location (e.g. it hasn't been moved or copied);
    stateId: ObjectSHA

    nativeFocusGroups: List[FocusGroup] = Field(default=None)
    liteFocusGroups: List[FocusGroup] = Field(default=None)

    _isDirty: bool = False
    _nativeMap: Dict[str, FocusGroup] = None
    _liteMap: Dict[str, FocusGroup] = None

    @property
    def lite(self) -> Dict[str, FocusGroup]:
        return self._liteMap

    @property
    def native(self) -> Dict[str, FocusGroup]:
        return self._nativeMap

    @property
    def isDefault(self):
        return self.stateId == GroupingMap.defaultStateId

    @property
    def isDirty(self) -> bool:
        return self._isDirty

    def setDirty(self, flag: bool):
        object.__setattr__(self, "_isDirty", flag)

    def coerceStateId(self, stateId: ObjectSHA):
        self.stateId = stateId
        self.setDirty(True)

    @validator("stateId", pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v

    @root_validator(pre=False, allow_reuse=True)
    def validate_GroupingMapFile(cls, v):
        _native = {}
        _lite = {}
        if v.get("nativeFocusGroups"):
            _native = {nfg.name: nfg for nfg in v["nativeFocusGroups"]}
        if v.get("liteFocusGroups"):
            _lite = {lfg.name: lfg for lfg in v["liteFocusGroups"]}
        groups = {"native": _native, "lite": _lite}
        supportedExtensions = tuple(Config["instrument.calibration.powder.grouping.extensions"])
        for mode in groups.copy():
            for group in groups[mode].copy():
                fp = Path(groups[mode][group].definition)
                # Check if path is relative
                if str(fp) == fp.name:
                    fp = Path.joinpath(cls.calibrationGroupingHome(), fp)
                if not fp.exists():
                    # logger.warning("File:" + str(fp) + " not found")
                    del groups[mode][group]
                    continue
                if not fp.is_file():
                    # logger.warning("File:" + str(fp) + " is not valid")
                    del groups[mode][group]
                    continue
                if not str(fp).endswith(supportedExtensions):
                    # logger.warning('File format for: "' + str(fp) + '" is not a valid grouping-schema map format')
                    del groups[mode][group]
                    continue
            # if groups[mode] == {}:
            # logger.warning("No valid FocusGroups were specified for mode: '" + mode + "'")

        v["_nativeMap"] = groups["native"]
        v["_liteMap"] = groups["lite"]
        v["_isDirty"] = False
        return v
