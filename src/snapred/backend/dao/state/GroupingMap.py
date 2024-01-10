from typing import Any, ClassVar, Dict
from pathlib import Path

from pydantic import BaseModel, Field, validator, root_validator

from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class GroupingMap(BaseModel):
    # This class is a "placeholder" for the moment...
    
    # State id associated with the _default_ grouping map, at 'calibration.grouping.home':
    defaultStateId: ClassVar[ObjectSHA] = ObjectSHA(hex='aabbbcccdddeeeff', decodedKey=None)
    
    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this GroupingMap's file is at its expected location (e.g. it hasn't been moved or copied);
    stateId: ObjectSHA

    _isDirty: bool = Field(default=False, kw_only=True)
    # availableGroups: Dict[str, Dict[str, Path]] = {}
    nativeGroups: Dict[str, Path] = Field(default=None, exclude=True)
    liteGroups: Dict[str, Path] = Field(default=None, exclude=True)

    @property
    def lite(self) -> Dict[str, Path]:
        return self.liteGroups

    @property
    def native(self) -> Dict[str, Path]:
        return self.nativeGroups
    
    @property
    def isDefault(self):
        return self.id == GroupingMap.defaultId

    @property
    def isDirty(self) -> bool:
        return self._isDirty
    
    def setDirty(self, flag: bool):
        self._isDirty = flag

    def cloneWithNewStateId(self, stateId: ObjectSHA) -> 'GroupingMap':
        return type(self)(
            stateId = stateId,
            _isDirty = True,
            availableGroups = self.availableGroups
        )
            
    @validator('stateId', pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v
    
    @root_validator(pre=False, allow_reuse=True)
    def validate_GroupingMapFile(cls, v):
        nativeMap = {}
        liteMap = {}
        if v.get("nativeFocusGroups"):
            nativeMap = {nfg["name"]: nfg["definition"] for nfg in v["nativeFocusGroups"]}
        if v.get("liteFocusGroups"):
            liteMap = {lfg["name"]: lfg["definition"] for lfg in v["liteFocusGroups"]}
        groups = {"native": nativeMap, "lite": liteMap}
        supportedExtensions = ("H5", "HD5", "HDF", "NXS", "NXS5", "XML")
        for mode in groups.copy():
            for group in groups[mode].copy():
                fp = groups[mode][group]
                if not fp.exists():
                    logger.warning("File:" + str(fp) + " not found")
                    del groups[mode][group]
                    continue
                if not fp.is_file():
                    logger.warning("File supplied:" + str(fp) + " is not valid")
                    del groups[mode][group]
                    continue
                if not str(fp).endswith(supportedExtensions):
                    logger.warning("File format for:" + str(fp) + " is wrong")
                    del groups[mode][group]
                    continue
            if mode == {}:
                logger.warning("No FocusGroups for " + mode + " given")
        return v
