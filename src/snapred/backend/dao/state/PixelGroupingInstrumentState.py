from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit


class PixelGroupingInstrumentState(BaseModel):
    twoThetaAverage: float  # the average two theta in radians
    dhkl: Limit[float]  # the minimum and maximum d-spacings in Angstroms
    delta_dhkl_over_dhkl: float  # the diffraction resolution
