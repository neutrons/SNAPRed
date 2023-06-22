from pydantic import BaseModel
from snapred.backend.dao.Limit import Limit

class PixelGroupingParameters(BaseModel):
    twoTheta: float  # the average two-theta (radians) for pixel grouping
    dResolution: Limit[float]  # the minimum and maximum diffraction-resolvable d-spacing (Angstrom) for pixel grouping
    dRelativeResolution: float  # the relative diffraction resolution for pixel grouping
