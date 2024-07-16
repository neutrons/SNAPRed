from pathlib import Path
from typing import Any, ClassVar, Dict, List, Union

from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator

from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class GroupingMap(BaseModel):
    # State id associated with the _default_ grouping map, at 'instrument.calibration.powder.grouping.home':
    defaultStateId: ClassVar[ObjectSHA] = ObjectSHA(hex="aabbbcccdddeeeff", decodedKey=None)

    @classmethod
    def calibrationGroupingHome(cls) -> Path:
        return Path(Config["instrument.calibration.powder.grouping.home"])

    @classmethod
    def _asAbsolutePath(cls, filePath: Path) -> Path:
        if filePath.is_absolute():
            return filePath
        return cls.calibrationGroupingHome().joinpath(filePath)

    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this GroupingMap's file is at its expected location (e.g. it hasn't been moved or copied);
    stateId: Union[ObjectSHA, str]  # Coerced to `ObjectSHA` at input =>
    #   using 'Union' here suppresses pydantic serialization warning

    # Although the public interface to `GroupingMap` is a mapping, for ease of editing:
    # *  the JSON representation is written using a list format:
    # *  relative vs. absolute path information is retained in the representation.
    nativeFocusGroups: List[FocusGroup] = Field(default=None)
    liteFocusGroups: List[FocusGroup] = Field(default=None)

    _isDirty: bool = PrivateAttr(default=False)
    _nativeMap: Dict[str, FocusGroup] = PrivateAttr(default=None)
    _liteMap: Dict[str, FocusGroup] = PrivateAttr(default=None)

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

    def getMap(self, useLiteMode: bool) -> Dict[str, FocusGroup]:
        if useLiteMode:
            return self._liteMap
        else:
            return self._nativeMap

    def setDirty(self, flag: bool):
        object.__setattr__(self, "_isDirty", flag)

    def coerceStateId(self, stateId: ObjectSHA):
        self.stateId = stateId
        self.setDirty(True)

    @field_validator("stateId", mode="before")
    @classmethod
    def str_to_ObjectSHA(cls, v: Any) -> ObjectSHA:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            v = ObjectSHA(hex=v, decodedKey=None)
        return v

    @model_validator(mode="after")
    def validate_GroupingMap(self):
        # Build `GroupingMap` from input lists:
        _native = {nfg.name: nfg for nfg in self.nativeFocusGroups}
        _lite = {lfg.name: lfg for lfg in self.liteFocusGroups}

        groups = {"native": _native, "lite": _lite}
        supportedExtensions = tuple(Config["instrument.calibration.powder.grouping.extensions"])
        for mode in groups.copy():
            for group in groups[mode].copy():
                fp = Path(groups[mode][group].definition)
                # Check if path is relative
                if not fp.is_absolute():
                    # Do _not_ change the path in the original `FocusGroup`,
                    #   otherwise the path will also change in the on-disk version.
                    fp = self._asAbsolutePath(fp)
                    groups[mode][group] = FocusGroup(name=group, definition=str(fp))
                if not fp.exists():
                    logger.warning("File: " + str(fp) + " not found")
                    del groups[mode][group]
                    continue
                if not fp.is_file():
                    logger.warning("File: " + str(fp) + " is not valid")
                    del groups[mode][group]
                    continue
                if not str(fp).endswith(supportedExtensions):
                    logger.warning('File format for: "' + str(fp) + '" is not a valid grouping-schema map format')
                    del groups[mode][group]
                    continue
            if groups[mode] == {}:
                logger.warning("No valid FocusGroups were specified for mode: '" + mode + "'")
        self._nativeMap = groups["native"]
        self._liteMap = groups["lite"]
        self._isDirty = False
        return self
