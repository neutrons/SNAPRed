from enum import IntEnum
from typing import Dict, List
from pathlib import Path

from pydantic import BaseModel, parse_obj_as

from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.GroupingFileIndex import GroupingFileIndex


class GroupingMap(BaseModel):
    # allow initializtion from either dictionary or list
    groupingFileIndices: Dict[int, GroupingFileIndex] = {}

    @property
    def groupingName(self) -> List[str]:
        return sorted([p.groupingName for p in self.pixelGroupingParameters.values()])

    @property
    def filePath(self) -> List[Path]:
        return [self.pixelGroupingParameters[gid].twoTheta for gid in self.groupID]

    def __getitem__(self, key):
        return self.groupingFileIndices[key]

    def __init__(
        self,
        groupingName: List[str] = None,
        filePath: List[Path] = None,
        groupingFileIndices={},
    ):
        if groupingFileIndices != {}:
            #need to figure out how to key the dict, maybe keyed on groupingName?
            groupingFileIndices = {1 ;groupingName=groupingName[i],filePath=filePath[i],}
        return super().__init__(
            groupingFileIndices=groupingFileIndices,
        )
