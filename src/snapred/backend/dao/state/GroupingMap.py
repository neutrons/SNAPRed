import os
import string
from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel, root_validator

from snapred.backend.log.logger import snapredLogger

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

    def __init__(self, *args, **kwargs):
        nativeMap = {}
        liteMap = {}
        if kwargs.get("nativeFocusGroup"):
            nativeMap = {nfg["name"]: nfg["definition"] for nfg in kwargs["nativeFocusGroups"]}
        if kwargs.get("liteFocusGroups"):
            liteMap = {lfg["name"]: lfg["definition"] for lfg in kwargs["liteFocusGroups"]}
        kwargs["availableGroups"] = {"native": nativeMap, "lite": liteMap}
        return super().__init__(*args, **kwargs)

    @root_validator(pre=False, allow_reuse=True)
    def validate_GroupingMapFile(cls, v):
        # Verify stateId is in the correct format
        stateId = v.get("stateID")
        print("stateid")
        print(stateId)
        if not all(c in string.hexdigits for c in stateId):
            logger.warning("StateId: " + stateId + " is in wrong format")
        if len(stateId) != 16:
            logger.warning("StateId: " + stateId + " length must be 16")

        groups = v.get("availableGroups")
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
