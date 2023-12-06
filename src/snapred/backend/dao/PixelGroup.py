from typing import List

from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters


class PixelGroup(BaseModel):
    pixelGroupingParameters: List[PixelGroupingParameters] = []
    numberBinsAcrossSpectrum = 10

    @property
    def groupID(self) -> List[int]:
        return [p.groupID for p in self.pixelGroupingParameters]

    @property
    def twoTheta(self) -> List[float]:
        return [p.twoTheta for p in self.pixelGroupingParameters]

    @property
    def dResolution(self) -> List[Limit[float]]:
        return [p.dResolution for p in self.pixelGroupingParameters]

    @property
    def dRelativeResolution(self) -> List[float]:
        return [p.dRelativeResolution for p in self.pixelGroupingParameters]

    def __init__(
        self,
        groupID: List[int] = None,
        twoTheta: List[float] = None,
        dResolution: List[Limit[float]] = None,
        dRelativeResolution: List[float] = None,
        pixelGroupingParameters=[],
        numberBinsAcrossSpectrum=10,
    ):
        if pixelGroupingParameters != []:
            super().__init__(
                pixelGroupingParameters=pixelGroupingParameters, numberBinsAcrossSpectrum=numberBinsAcrossSpectrum
            )
            return
        pixelGroupingParameters = [
            PixelGroupingParameters(groupID=gg, twoTheta=tt, dResolution=dd, dRelativeResolution=rr)
            for gg, tt, dd, rr in zip(groupID, twoTheta, dResolution, dRelativeResolution)
        ]
        super().__init__(
            pixelGroupingParameters=pixelGroupingParameters, numberBinsAcrossSpectrum=numberBinsAcrossSpectrum
        )
