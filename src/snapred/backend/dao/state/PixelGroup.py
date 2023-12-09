from enum import IntEnum
from typing import Dict, List, Union

from pydantic import BaseModel, parse_obj_as

from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters


class PixelGroup(BaseModel):
    # allow initializtion from either dictionary or list
    pixelGroupingParameters: Union[List[PixelGroupingParameters], Dict[int, PixelGroupingParameters]] = {}
    numberBinsAcrossPeakWidth: int = 10

    @property
    def groupID(self) -> List[int]:
        return sorted([p.groupID for p in self.pixelGroupingParameters.values()])

    @property
    def twoTheta(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].twoTheta for gid in self.groupID]

    @property
    def dResolution(self) -> List[Limit[float]]:
        return [self.pixelGroupingParameters[gid].dResolution for gid in self.groupID]

    @property
    def dRelativeResolution(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].dRelativeResolution for gid in self.groupID]

    def __getitem__(self, key):
        return self.pixelGroupingParameters[key]

    def __init__(
        self,
        groupID: List[int] = None,
        twoTheta: List[float] = None,
        dResolution: List[Limit[float]] = None,
        dRelativeResolution: List[float] = None,
        pixelGroupingParameters={},
        numberBinsAcrossPeakWidth=10,
    ):
        if pixelGroupingParameters != {}:
            if isinstance(pixelGroupingParameters, list):
                pixelGroupingParameters = {
                    PixelGroupingParameters.parse_obj(p).groupID: p for p in pixelGroupingParameters
                }
        else:
            pixelGroupingParameters = {
                groupID[i]: PixelGroupingParameters(
                    groupID=groupID[i],
                    twoTheta=twoTheta[i],
                    dResolution=dResolution[i],
                    dRelativeResolution=dRelativeResolution[i],
                )
                for i in range(len(groupID))
            }
        return super().__init__(
            pixelGroupingParameters=pixelGroupingParameters,
            numberBinsAcrossPeakWidth=numberBinsAcrossPeakWidth,
        )

    # these are not properties, but they reflect the actual data consumption

    class BinningMode(IntEnum):
        LOG = -1
        LINEAR = 1

    def dMax(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].dResolution.maximum for gid in self.groupID]

    def dMin(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].dResolution.minimum for gid in self.groupID]

    def dBin(self, binningMode: BinningMode):
        sign = -1 if binningMode == self.BinningMode.LOG else 1
        Nbin = self.numberBinsAcrossPeakWidth
        return [sign * abs(self.pixelGroupingParameters[gid].dRelativeResolution) / Nbin for gid in self.groupID]
