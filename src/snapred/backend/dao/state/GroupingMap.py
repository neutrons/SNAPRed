from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel

# import json
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config, Resource
from snapred.meta.redantic import write_model

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
        if nativeFocusGroups is None:
            logger.warning("No FocusGroups for native mode given")
        else:
            for nfg in nativeFocusGroups:
                if not self.validateGroupingFile(nfg["definition"]):
                    continue
                else:
                    nativeMap[nfg["name"]] = nfg["definition"]
        if liteFocusGroups is None:
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
            logger.warning("File NOT FOUND")
            return False
        if not Path().is_file():
            logger.warning("not valid file")
            return False
        if not fp.endswith(supportedExtensions):
            logger.warning("file format is wrong")
            return False
        return True

    @classmethod
    def load(cls, stateID: str):
        if stateID == "default":
            # DefaultMap location and overwrite calibration.grouping.home
            # write to file here
            return GroupingMap.parse_raw(Resource.read(Config["calibration.grouping.home"]))
        else:
            # load map from location
            return GroupingMap.parse_raw(Resource.read(stateID + "/inputs/SampleGroupingFile.json"))

    @classmethod
    def save(cls, stateID: str):
        if stateID != "default":
            print("about to write file")
            write_model(cls, stateID + Config["instrument.state.home"])
        else:
            logger.warning("cannot overwrite default mapping, change stateID")
