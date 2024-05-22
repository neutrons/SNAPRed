import numpy as np
from pydantic import BaseModel, Field, validator

from snapred.backend.dao.Limit import Limit


class PixelGroupingParameters(BaseModel):
    groupID: int

    # True => this group is fully masked
    isMasked: bool

    # the mean sample to pixel distance (metres) for this pixel grouping
    L2: float

    # the mean two-theta (two times the polar angle in radians) for this pixel grouping
    twoTheta: float = Field(ge=0.0, le=2.0 * np.pi)

    # the mean azimuth (radians) for this pixel grouping
    azimuth: float = Field(ge=-np.pi, le=np.pi)

    # the minimum and maximum diffraction-resolvable d-spacing (Angstrom) for pixel grouping
    dResolution: Limit[float]

    # the relative diffraction resolution (Angstrom) for pixel grouping
    dRelativeResolution: float = Field(gt=0.0)

    @validator("dResolution", pre=True, allow_reuse=True)
    def validate_resolution(cls, resolution):
        # minimum <= maximum validated at `Limit` itself.
        minimum = resolution.minimum if isinstance(resolution, Limit) else resolution.get("minimum")
        if minimum <= 0.0:
            raise ValueError("any valid d-spacing must be > 0.0")
        return resolution
