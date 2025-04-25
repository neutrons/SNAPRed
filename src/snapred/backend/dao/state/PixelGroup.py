from enum import IntEnum
from typing import Dict, List

from pydantic import BaseModel, field_validator, Field

from snapred.backend.dao.Limit import BinnedValue, Limit
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.Config import Config


class PixelGroup(BaseModel):
    # allow initialization from either a dictionary or list
    pixelGroupingParameters: List[PixelGroupingParameters] | Dict[int, PixelGroupingParameters] = {}

    nBinsAcrossPeakWidth: int = Field(default_factory = lambda: Config["calibration.diffraction.nBinsAcrossPeakWidth"])
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

    # these are not properties, but they reflect the actual data consumption

    def dMax(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].dResolution.maximum for gid in self.groupIDs]

    def dMin(self) -> List[float]:
        return [self.pixelGroupingParameters[gid].dResolution.minimum for gid in self.groupIDs]

    def dBin(self) -> List[float]:
        Nbin = self.nBinsAcrossPeakWidth
        return [
            self.binningMode * abs(self.pixelGroupingParameters[gid].dRelativeResolution) / Nbin
            for gid in self.groupIDs
        ]

    @field_validator("timeOfFlight", mode="before")
    @classmethod
    def validate_TOF(cls, v):
        if isinstance(v, dict):
            v = BinnedValue[float](**v)
        if not isinstance(v, BinnedValue[IntEnum]):
            # Coerce the Generic[T]-derived type
            v = BinnedValue[float](**v.dict())
        return v

    @field_validator("pixelGroupingParameters", mode="after")
    @classmethod
    def validate_pixelGroupingParameters(cls, pgps: List[PixelGroupingParameters] | Dict[int, PixelGroupingParameters]):
        if not isinstance(pgps, list):
            pgps = pgps.values()

        # Reconstruct `pixelGroupingParameters` as a `dict`, but
        #   exclude any PGP from fully-masked subgroups.
        pgps = {p.groupID: p for p in pgps if not p.isMasked}
        return pgps
