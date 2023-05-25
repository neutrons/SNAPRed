from pydantic import BaseModel
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.InstrumentState import InstrumentState


class PixelGroupingInstrumentState(InstrumentState):
    twoThetaAverage: float  # the average two theta in radians
    dhkl: Limit[float]  # the minimum and maximum d-spacings in Angstroms
    delta_dhkl_over_dhkl: float  # the diffraction resolution
