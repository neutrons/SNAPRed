import os
from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel

# import json
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config, Resource

logger = snapredLogger.getLogger(__name__)


class GroupingMap(BaseModel):
    availableGroups: Dict[str, Dict[str, Path]] = {}
    stateID: str = None

    @property
    def lite(self) -> Dict[str, Path]:
        return self.availableGroups["lite"]

    @property
    def native(self) -> Dict[str, Path]:
        return self.availableGroups["native"]

    def __init__(
        self,
        liteFocusGroups: List[FocusGroup] = None,
        nativeFocusGroups: List[FocusGroup] = None,
        availableGroups={},
        stateID: str = None,
    ):
        nativeMap = {}
        liteMap = {}
        if not nativeFocusGroups:
            print("empty")
            logger.warning("No FocusGroups for native mode given")
        else:
            for nfg in nativeFocusGroups:
                if not self.validateGroupingFile(nfg["definition"]):
                    continue
                else:
                    nativeMap[nfg["name"]] = nfg["definition"]
        if not liteFocusGroups:
            logger.warning("No FocusGroups for lite mode given")
        else:
            for lfg in liteFocusGroups:
                if not self.validateGroupingFile(lfg["definition"]):
                    continue
                else:
                    liteMap[lfg["name"]] = lfg["definition"]
        availableGroups = {
            "native": nativeMap,
            "lite": liteMap,
        }
        return super().__init__(
            availableGroups=availableGroups,
            stateID=stateID,
        )

    def validateGroupingFile(self, fp: str = None):
        supportedExtensions = ["H5", "HD5", "HDF", "NXS", "NXS5", "XML"]
        if not Path(fp).exists():
            logger.warning("File not found")
            return False
        if not Path().is_file():
            logger.warning("File supplied is not valid")
            return False
        if not fp.endswith(supportedExtensions):
            logger.warning("File format is wrong")
            return False
        return True

    @classmethod
    def load(cls, stateID: str):
        if stateID == "default":
            return GroupingMap.parse_raw(Resource.read(Config["calibration.grouping.home"] + "/GroupingMap.json"))
        else:
            return GroupingMap.parse_raw(
                Resource.read(Config["instrument.state.home"] + "/" + stateID + "/GroupingMap.json")
            )

    def save(self, stateID: str):
        if stateID != "default":
            path = Config["instrument.state.home"] + "/" + stateID + "/GroupingMap.json"
            with Resource.open(path, "w") as f:
                f.write(self.json())
        else:
            logger.warning("Cannot overwrite default mapping, change stateID")
