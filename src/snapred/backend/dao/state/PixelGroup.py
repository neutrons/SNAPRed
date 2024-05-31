from enum import IntEnum
from typing import Dict, List, Union

from pydantic import BaseModel

from snapred.backend.dao.Limit import BinnedValue, Limit
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.Config import Config


class PixelGroup(BaseModel):
    # allow initializtion from either dictionary or list
    pixelGroupingParameters: Union[List[PixelGroupingParameters], Dict[int, PixelGroupingParameters]] = {}
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    focusGroup: FocusGroup
    timeOfFlight: BinnedValue[float]

    class BinningMode(IntEnum):
        LOG = -1
        LINEAR = 1

    binningMode: BinningMode = BinningMode.LOG

    @property
    def groupIDs(self) -> List[int]:
        return sorted([p for p in self.pixelGroupingParameters.keys()])

    @property
    def isMasked(self) -> List[bool]:
        return [self.pixelGroupingParameters[gid].isMasked for gid in self.groupIDs]

    @property
    def L2(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].L2 for gid in self.groupIDs]

    @property
    def twoTheta(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].twoTheta for gid in self.groupIDs]

    @property
    def azimuth(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].azimuth for gid in self.groupIDs]

    @property
    def dResolution(self) -> List[Limit[float]]:
        return [self.pixelGroupingParameters[gid].dResolution for gid in self.groupIDs]

    @property
    def dRelativeResolution(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].dRelativeResolution for gid in self.groupIDs]

    def __getitem__(self, key):
        return self.pixelGroupingParameters[key]

    def __init__(self, **kwargs):
        if kwargs.get("pixelGroupingParameters") is None:
            groupIDs = kwargs["groupIDs"]
            kwargs["pixelGroupingParameters"] = {
                groupIDs[i]: PixelGroupingParameters(
                    groupID=groupIDs[i],
                    isMasked=kwargs["isMasked"][i],
                    L2=kwargs["L2"][i],
                    twoTheta=kwargs["twoTheta"][i],
                    azimuth=kwargs["azimuth"][i],
                    dResolution=kwargs["dResolution"][i],
                    dRelativeResolution=kwargs["dRelativeResolution"][i],
                )
                for i in range(len(groupIDs))
            }
        elif isinstance(kwargs["pixelGroupingParameters"], list):
            pixelGroupingParametersList = kwargs["pixelGroupingParameters"]
            kwargs["pixelGroupingParameters"] = {
                PixelGroupingParameters.parse_obj(p).groupID: p for p in pixelGroupingParametersList
            }
        return super().__init__(**kwargs)

    # these are not properties, but they reflect the actual data consumption

    def dMax(self, default=0.0) -> List[float]:
        # Warning: if the pixel-group is fully masked, it requires special treatment:
        #   different Mantid algorithms use either "0.0" or "NaN" to indicate an unspecified value,
        #   so these values may need to be filtered.
        return [
            self.pixelGroupingParameters[gid].dResolution.maximum
            if not self.pixelGroupingParameters[gid].isMasked
            else default
            for gid in self.groupIDs
        ]

    def dMin(self, default=0.0) -> List[float]:
        # Warning: if the pixel-group is fully masked, it requires special treatment:
        #   different Mantid algorithms use either "0.0" or "NaN" to indicate an unspecified value,
        #   so these values may need to be filtered.
        return [
            self.pixelGroupingParameters[gid].dResolution.minimum
            if not self.pixelGroupingParameters[gid].isMasked
            else default
            for gid in self.groupIDs
        ]

    def dBin(self) -> List[float]:
        Nbin = self.nBinsAcrossPeakWidth
        return [
            self.binningMode * abs(self.pixelGroupingParameters[gid].dRelativeResolution) / Nbin
            for gid in self.groupIDs
        ]
