from typing import Any, ClassVar, Dict, List
from pathlib import Path

from pydantic import BaseModel, Field, validator, root_validator

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class GroupingMap(BaseModel):
    # This class is a "placeholder" for the moment...
    
    # State id associated with the _default_ grouping map, at 'calibration.grouping.home':
    defaultStateId: ClassVar[ObjectSHA] = ObjectSHA(hex='aabbbcccdddeeeff', decodedKey=None)
    
    # Use the StateId hash to enforce filesystem-as-database integrity requirements:
    # * verify that this GroupingMap's file is at its expected location (e.g. it hasn't been moved or copied);
    stateId: ObjectSHA

    _isDirty: bool = Field(default=False, kw_only=True)

    nativeFocusGroups: List[FocusGroup] = Field(default=None, exclude=False)
    liteFocusGroups: List[FocusGroup] = Field(default=None, exclude=False)

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
    
    @classmethod
    def getPath(cls) -> Path:
        return Path(Config["instrument.calibration.powder.grouping.home"])
            
    @validator('stateId', pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v
    
    @root_validator(pre=False, allow_reuse=True)
    def validate_GroupingMapFile(cls, v):
        print(type(v))
        _native = {}
        _lite = {}
        if v.get("nativeFocusGroups"):
            _native = {nfg.name: nfg for nfg in v["nativeFocusGroups"]}
        if v.get("liteFocusGroups"):
            _lite = {lfg.name: lfg for lfg in v["liteFocusGroups"]}   
        groups = {'native': _native, 'lite': _lite}
        supportedExtensions = ("H5", "HD5", "HDF", "NXS", "NXS5", "xml")
        for mode in groups.copy():
            for group in groups[mode].copy():
                fp = Path(groups[mode][group].definition)
                #Check if path is relative
                if str(fp) == fp.name:
                    fp = Path.joinpath(cls.getPath(), fp)
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
            if groups[mode] == {}:
                logger.warning("No valid FocusGroups given for mode: " + mode)
        
        v['_nativeMap'] = groups['native']
        v['_liteMap'] = groups['lite']
        return v